"""Exercise the production witness comparison with independent RPC response fixtures."""
from collections import Counter
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import chain


class Witnesses(unittest.TestCase):
    height = 105
    now = datetime(2025, 5, 15, tzinfo=timezone.utc)

    def setUp(self):
        self.age = timedelta(hours=24)
        self.period = "604800s"
        self.modify = None
        self.calls = []
        self.clock = patch.object(chain, "now", return_value=self.now)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        transport = patch.object(chain, "get", side_effect=self.get)
        transport.start()
        self.addCleanup(transport.stop)

    def get(self, url):
        self.calls.append(url)
        if url == chain.PARAMS:
            return {"params":{"unbonding_time":self.period}}
        server, height = url.split("/commit?height=")
        h = int(height)
        stamp = self.now-self.age+timedelta(seconds=h-(self.height-1))
        header = {"chain_id":chain.CHAIN, "height":height, "app_hash":f"{h+1000:064X}", "time":stamp.isoformat()}
        commit = {"height":height, "block_id":{"hash":f"{h:064X}"}}
        if self.modify:
            self.modify(server, h, header, commit)
        return {"result":{"signed_header":{"header":header, "commit":commit}}}

    def agree(self):
        return chain.agree(self.height, f"{self.height+1001:064X}")

    def test_both_witnesses_at_each_height_and_h_plus_one_application_state(self):
        proof = self.agree()
        self.assertEqual(Counter(self.calls), Counter([chain.PARAMS] +
                         [f"{s}/commit?height={h}" for s in chain.WITNESSES for h in range(104,108)]))
        self.assertEqual(proof["block_hash"], f"{105:064X}")
        self.assertEqual(proof["snapshot_app_hash"], f"{1106:064X}")
        self.assertEqual(proof["fixed_trust_anchor"]["hash"], f"{104:064X}")
        self.assertEqual(proof["fixed_trust_anchor"]["height"], 104)
        self.assertEqual(chain.parse_time(proof["fixed_trust_anchor"]["expires_at_utc"]),
                         self.now+timedelta(hours=120))
        with self.assertRaisesRegex(ValueError,"AppHash"):
            chain.agree(self.height, f"{1105:064X}")

    def test_one_witness_changes_one_field_at_each_requested_height(self):
        for h in range(104, 108):
            for group, key, value in (("header","app_hash","F"*64), ("header","time",self.now.isoformat()),
                                      ("commit","block_id",{"hash":"F"*64})):
                with self.subTest(h=h,key=key):
                    def change(server, at, header, commit):
                        if server == chain.WITNESSES[1] and at == h:
                            (header if group == "header" else commit)[key] = value
                    self.modify = change
                    with self.assertRaisesRegex(ValueError, "disagree"):
                        self.agree()

    def test_agreement_on_invalid_chain_height_or_digest_is_rejected(self):
        for part, key, bad in (("h","chain_id","other"),("h","height","900"),
                              ("c","height","900"),("h","app_hash","AB"),
                              ("c","block_id",{"hash":"Z"*64})):
            with self.subTest(key=key):
                def change(server, h, header, commit):
                    if h == 105:
                        (header if part == "h" else commit)[key] = bad
                self.modify = change
                with self.assertRaises(ValueError):
                    self.agree()

    def test_independent_unbonding_and_anchor_expiry_boundaries(self):
        for duration in ("518400s","518399.9s","n/a"):
            self.period = duration
            with self.subTest(duration=duration), self.assertRaisesRegex(ValueError,"Unbonding"):
                self.agree()
        self.period = "518400.1s"
        self.age = timedelta(hours=96)
        with self.assertRaisesRegex(ValueError, "48 hours"):
            self.agree()
        self.age -= timedelta(seconds=1)
        self.agree()

    def test_missing_witness_never_counts_as_agreement(self):
        def unavailable(server, h, header, commit):
            if server == chain.WITNESSES[1]:
                raise OSError("unavailable")
        self.modify = unavailable
        with self.assertRaises(OSError):
            self.agree()
        self.assertNotIn(chain.PARAMS,self.calls)


class ReplicaChecks(unittest.TestCase):
    def test_only_current_nonsigning_replica_with_expected_id_and_version(self):
        cfg = SimpleNamespace(db="fixture", rpc="http://127.0.0.1:27777", node_id="abcd")
        now = datetime(2025,5,10,tzinfo=timezone.utc)
        for field, value, valid in ((None,None,True), ("network","wrong",False), ("id","other",False),
                                    ("voting_power","1",False), ("catching_up",True,False),
                                    ("age",120,False), ("age",-10,False), ("age",119,True),
                                    ("version","v0",False)):
            with self.subTest(field=field,value=value):
                conf = {"network":chain.CHAIN,"id":cfg.node_id,"voting_power":"0","catching_up":False,
                        "age":0,"version":chain.VERSION}
                if field:
                    conf[field] = value
                response = {"node_info":{"id":conf["id"],"network":conf["network"]},
                            "sync_info":{"catching_up":conf["catching_up"], "latest_block_height":"42",
                                         "latest_block_time":(now-timedelta(seconds=conf["age"])).isoformat()},
                            "validator_info":{"voting_power":conf["voting_power"]}}
                def rpc(endpoint, method):
                    self.assertEqual(endpoint, cfg.rpc)
                    return {"status":response,"abci_info":{"response":{"version":conf["version"]}}}[method]
                with patch.object(chain,"local_pid",return_value=74) as owner, patch.object(chain,"rpc",rpc), \
                     patch.object(chain,"now",return_value=now):
                    if valid:
                        self.assertEqual(chain.replica(cfg),{"pid":74,"height":42,"node_id":"abcd"})
                    else:
                        with self.assertRaises(ValueError):
                            chain.replica(cfg)
                    owner.assert_called_once_with(cfg.db, 27777)
