import contextlib
from datetime import datetime, timezone
import io
import json
import subprocess
import unittest
from unittest.mock import patch

import helper
from test_restore import SCRIPT


class RestoreWitnesses(unittest.TestCase):
    def setUp(self):
        self.stamp = "2025-10-04T12:00:00+00:00"
        self.epoch = datetime(2025,10,4,12,tzinfo=timezone.utc).timestamp()
        self.modify = None
        self.latest = 105
        self.unbonding = "604800s"
        self.status_network = {}
        self.row = {"kind":"native", "height":100, "app_hash":f"{1101:064X}",
                    "trust_height":99, "trust_hash":f"{99:064X}", "hours":144}
        for mock in (patch.object(helper,"rpc",side_effect=self.rpc),
                     patch.object(helper.time,"time",return_value=self.epoch),
                     patch.object(helper,"read_url",side_effect=lambda _:json.dumps(
                         {"params":{"unbonding_time":self.unbonding}}).encode())):
            mock.start()
            self.addCleanup(mock.stop)

    def rpc(self, server, method):
        if method == "status":
            return {"node_info":{"network":self.status_network.get(server, helper.CHAIN)},
                    "sync_info":{"latest_block_height":str(self.latest)}}
        height = int(method.split("=")[1])
        header = {"chain_id":helper.CHAIN, "height":str(height), "time":self.stamp,"app_hash":f"{height+1000:064X}"}
        commit = {"height":str(height), "block_id":{"hash":f"{height:064X}"}}
        if self.modify:
            self.modify(server,height,header,commit)
        return {"signed_header":{"header":header,"commit":commit}}

    def test_native_anchor_checks_h_plus_one_app_and_independent_trust_block(self):
        verified = helper.anchor(self.row.copy())
        self.assertEqual(verified["block_hash"], f"{100:064X}")
        for change in ({"app_hash":f"{1100:064X}"}, {"trust_hash":f"{98:064X}"}, {"block_hash":f"{99:064X}"}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                helper.anchor(dict(self.row, **change))
        for divergent in (99,100,101,102):
            def modify(server,height,header,commit):
                if server == helper.RPCS[1] and height == divergent:
                    header["app_hash"] = "AA"*32
            self.modify = modify
            with self.subTest(at=divergent), self.assertRaisesRegex(ValueError, "independent RPCs disagree"):
                helper.anchor(self.row.copy())

    def test_trust_requires_more_than_an_hour_and_strictly_shorter_than_unbonding(self):
        with patch.object(helper.time,"time",return_value=self.epoch+143*3600), self.assertRaisesRegex(ValueError,"expired"):
            helper.anchor(self.row.copy())
        with patch.object(helper.time,"time",return_value=self.epoch+143*3600-1):
            helper.anchor(self.row.copy())
        self.unbonding = "518400s"
        with self.assertRaisesRegex(ValueError,"unbonding"):
            helper.anchor(self.row.copy())

    def test_final_verification_checks_local_header_commit_and_both_network_heads(self):
        with contextlib.redirect_stdout(io.StringIO()):
            helper.verify_head("local", 100)
        for part,key,value in (("header","chain_id","other"), ("header","height","99"), ("commit","height","99"),
                               ("header","app_hash","AA"*32), ("commit","block_id",{"hash":"AB"*32})):
            def modify(server,height,header,commit):
                if server=="local":
                    (header if part=="header" else commit)[key] = value
            self.modify = modify
            with self.subTest(key=key), self.assertRaises(ValueError):
                helper.verify_head("local", 100)
        self.modify = None
        for latest in (94,106):
            self.latest = latest
            with self.assertRaises(SystemExit) as wait:
                helper.verify_head("local",100)
            self.assertEqual(wait.exception.code,75)

    def test_wrong_status_chain_of_either_witness_rejects_matching_blocks(self):
        for server in helper.RPCS:
            self.status_network = {server: "other-chain"}
            with self.subTest(server=server), self.assertRaisesRegex(ValueError, "wrong witness chain"):
                helper.verify_head("local", 100)


class SourceChoice(unittest.TestCase):
    def test_explicit_source_or_smallest_available_and_unavailable_selection(self):
        sample = [{"id":"beehive-full","kind":"full","bytes":60,"height":11},
                  {"id":"missing","error":"offline"}, {"id":"beehive-native","kind":"native","bytes":5,"height":10}]
        command = '''
            source "$1"
            action=plan; source=$2; manifest=; payload=$3; yes=1
            support() { case "$1" in catalog) printf '%s\n' "$payload";; anchor) printf '%s\n' "$2";; *) return 2;; esac; }
            choose
            field id
        '''
        for source, expected in (("auto","beehive-native"),("beehive-full","beehive-full"),("missing",None)):
            with self.subTest(source=source):
                run = subprocess.run(["bash","-c",command,"choose",str(SCRIPT),source,json.dumps(sample)],
                                     text=True, capture_output=True, timeout=10)
                if expected is None:
                    self.assertNotEqual(run.returncode,0)
                    self.assertIn("Requested snapshot is unavailable",run.stderr)
                else:
                    self.assertEqual(run.returncode,0,run.stderr)
                    self.assertEqual(run.stdout.strip(),expected)
