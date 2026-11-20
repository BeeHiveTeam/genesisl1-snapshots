"""Regression checks for the public, self-contained Bash recovery script."""
import hashlib
import io
import json
import shutil
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch

import helper

SCRIPT = Path(__file__).with_name("beehive-sync.sh").resolve()


def bash(script, *args):
    return subprocess.run(["bash", "-c", 'source "$1"; shift\n' + script, "test", str(SCRIPT), *map(str,args)],
                          text=True,capture_output=True)


def varint(n):
    data = bytearray()
    while n >= 128:
        data.append((n & 127) | 128)
        n >>= 7
    return bytes(data) + bytes([n])


def field(n, value):
    if isinstance(value, int):
        return varint(n << 3) + varint(value)
    return varint(n << 3 | 2) + varint(len(value)) + value


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_banner_and_inspect(self):
        node = self.root/"operator"
        (node/"config").mkdir(parents=True)
        (node/"config/config.toml").write_text('[statesync]\nenable = true\n')
        (node/"config/app.toml").write_text('[memiavl]\nenable = false\n')
        standalone = self.root/"download.sh"
        shutil.copyfile(SCRIPT,standalone)
        p=subprocess.run(["bash",str(standalone),"--check","--inspect-home",str(node)],text=True,capture_output=True,check=True)
        self.assertIn("|____/|_____|",p.stderr)
        self.assertIn("P2P state sync flag: True",p.stdout)
        self.assertFalse((node/"data").exists())

    def test_target_protection(self):
        observed=self.root/"validator"
        (observed/"config").mkdir(parents=True)
        (observed/"config/config.toml").write_text("")
        bad=self.root/"unknown"
        bad.mkdir(); (bad/"existing-data").touch()
        symlink=self.root/"other"; symlink.symlink_to(bad)
        for path in (observed,observed/"child",self.root,bad,symlink):
            with self.subTest(path=path), self.assertRaises(ValueError):
                helper.check_target(path,observed)
        helper.check_target(self.root/"fresh",observed)
        self.assertTrue((bad/"existing-data").exists())

    def test_even_an_empty_inspected_home_is_separate_from_the_target(self):
        observed = self.root / "empty-home"
        observed.mkdir()
        for target in (observed, observed / "child", observed.parent):
            with self.subTest(target=target), self.assertRaisesRegex(ValueError, "separate"):
                helper.check_target(target, observed)

    def test_build_from_sources_creates_client_and_checksum(self):
        for name in ("client.sh.in", "helper.py", "build_client.py"):
            shutil.copyfile(SCRIPT.with_name(name), self.root / name)
        subprocess.run([sys.executable, "-B", "build_client.py"], cwd=self.root, check=True)
        client = self.root / SCRIPT.name
        self.assertEqual(client.read_bytes(), SCRIPT.read_bytes())
        digest = hashlib.sha256(client.read_bytes()).hexdigest()
        self.assertEqual(client.with_suffix(".sh.sha256").read_text(), f"{digest}  {client.name}\n")

    def test_provider_catalog_contract(self):
        meta={"chain_id":"genesis_29-2","file":"example.tar.lz4","height":123,"version":"v1.6.2","bytes":7,
              "sha256":"11"*32,"app_hash":"22"*32,"block_hash":"33"*32,"memiavl_enabled":False,"versiondb_enabled":False}
        with patch.multiple(helper,read_url=lambda *_:json.dumps(meta).encode(),head=lambda _:7):
            row=helper.artifact("firsttensor","https://assets.firsttensor.com/latest.json")
            self.assertEqual((row["height"],row["kind"],row["url"]),(123,"full","https://assets.firsttensor.com/example.tar.lz4"))
            with self.assertRaises(ValueError):
                helper.artifact("firsttensor","https://example.org/latest.json")
            meta["chain_id"]="other-chain"
            with self.assertRaises(ValueError):
                helper.artifact("lcserve","https://lcserve.net/latest.json")

    def test_expired_checkpoint_and_wrong_content_length(self):
        meta=dict(chain_id="genesis_29-2",file="native.tar.gz",version="1.6.2",snapshot_format=3,snapshot_height=501,
                  bytes=10,sha256="11"*32,snapshot_app_hash="22"*32,chunk_count=1,
                  fixed_trust_anchor=dict(trust_period="144h",height=500,hash="33"*32,expires_at_utc="2000-01-01T00:00:00Z"))
        with patch.multiple(helper,read_url=lambda *_:json.dumps(meta).encode(),head=lambda _:11):
            with self.assertRaisesRegex(ValueError,"expired"):
                helper.artifact("beehive-native","https://example.org/latest.json")
            meta["fixed_trust_anchor"]["expires_at_utc"]="2100-01-01T00:00:00Z"
            with self.assertRaisesRegex(ValueError,"size"):
                helper.artifact("beehive-native","https://example.org/latest.json")

    def test_discovery_uses_links_published_by_firsttensor(self):
        page=b'<a href="/snapshots/latest.json">Current snapshot</a>'
        with patch.multiple(helper,read_url=lambda *_:page,artifact=lambda provider,url:(provider,url)):
            self.assertEqual(helper.firsttensor(),("firsttensor","https://firsttensor.com/snapshots/latest.json"))
        with patch.multiple(helper,read_url=lambda *_:b'<a href="/grpc">gRPC</a>'):
            with self.assertRaisesRegex(ValueError,"no downloadable"):
                helper.firsttensor()

    def test_config_edit_preserves_other_settings(self):
        f=self.root/"config.toml"
        f.write_text('mode = "old"\n[statesync]\nenable = true\nval = "keep"\n')
        helper.change_toml(f,{("statesync","enable"):False})
        self.assertIn('val = "keep"',f.read_text())
        with self.assertRaises(ValueError):
            helper.change_toml(f,{("missing","enable"):True})

    def test_native_configuration_keeps_p2p_state_sync_disabled(self):
        config = self.root / "config"
        config.mkdir()
        comet = config / "config.toml"
        comet.write_text('''proxy_app = ""
[rpc]
laddr = ""
pprof_laddr = ""
grpc_laddr = ""
[p2p]
laddr = ""
persistent_peers = ""
max_packet_msg_payload_size = 1024
[statesync]
enable = true
rpc_servers = ""
trust_height = 0
trust_hash = ""
trust_period = ""
[consensus]
timeout_commit = "1s"
[instrumentation]
prometheus = true
''')
        app = config / "app.toml"
        app.write_text('''minimum-gas-prices = ""
iavl-disable-fastnode = false
[api]
enable = true
[grpc]
enable = true
[grpc-web]
enable = true
[json-rpc]
enable = true
[memiavl]
enable = false
snapshot-interval = 1
[versiondb]
enable = true
[state-sync]
snapshot-interval = 1
''')
        plan = {"kind": "native", "trust_height": 99, "trust_hash": "AB" * 32, "hours": 144}
        peer = b"f" * 40 + b"@127.0.0.1:26656"
        with patch.object(helper, "read_url", return_value=peer), patch.object(helper, "rpc", return_value={"peers": []}):
            helper.configure(self.root, plan, 36656)
        conf = tomllib.loads(comet.read_text())
        self.assertEqual(conf["statesync"], {
            "enable": False, "rpc_servers": ",".join(helper.RPCS), "trust_height": 99,
            "trust_hash": "AB" * 32, "trust_period": "144h"})
        self.assertEqual(conf["rpc"]["laddr"], "tcp://127.0.0.1:36657")
        self.assertIs(tomllib.loads(app.read_text())["memiavl"]["enable"], True)

    def write_store_roots(self, home):
        fixture=SCRIPT.with_name("fixtures")/"app-hash.json"
        record=json.loads(fixture.read_text()); height=record["height"]
        raw=field(1,height)
        for name,root in record["store_roots"].items():
            raw+=field(2,field(1,name.encode())+field(2,field(1,height)+field(2,bytes.fromhex(root))))
        mem=home/"data/memiavl.db"; snap=mem/f"snapshot-{height:020}"
        snap.mkdir(parents=True); (mem/"current").symlink_to(snap.name)
        (snap/"__metadata").write_bytes(field(1,raw))
        return record

    def test_app_hash_from_mainnet_store_roots(self):
        record = self.write_store_roots(self.root)
        height = record["height"]
        helper.check_memiavl(self.root,height,record["app_hash"])
        with self.assertRaisesRegex(ValueError,"AppHash"):
            helper.check_memiavl(self.root,height,"00"*32)

    def test_native_app_hash_mismatch_keeps_phase_unrestored(self):
        record = self.write_store_roots(self.root / "node")
        plan = {"chunks": 1, "app_hash": "00" * 32}
        run = bash('job=$1; node="$job/node"; height=$2; plan=$3; phase=loaded; daemon=(true); native_restore',
                   self.root, record["height"], json.dumps(plan))
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("restored AppHash mismatch", run.stderr)
        self.assertEqual((self.root / "phase").read_text(), "restoring\n")

    def archive(self):
        file=self.root/"fixture.tar.lz4"
        buf=io.BytesIO()
        with tarfile.open(fileobj=buf,mode="w") as tar:
            for path in ("data","data/application.db","data/state.db","data/blockstore.db"):
                info=tarfile.TarInfo(path); info.type=tarfile.DIRTYPE; tar.addfile(info)
            for name,data in (("data/application.db/CURRENT",b"test-table"),("data/priv_validator_state.json",b'{"height":"843"}')):
                info=tarfile.TarInfo(name); info.size=len(data)
                tar.addfile(info,io.BytesIO(data))
        file.write_bytes(subprocess.run(["lz4","-zc"],input=buf.getvalue(),capture_output=True,check=True).stdout)
        return file

    def test_full_restore_preserves_receiver_state(self):
        archive=self.archive()
        data=self.root/"node/data"; data.mkdir(parents=True)
        state='{"height":"0","round":0,"step":0}'
        (data/"priv_validator_state.json").write_text(state)
        p=bash('job=$1; node="$job/node"; archive=$2; phase=downloaded; install_full',self.root,archive)
        self.assertEqual(p.returncode,0,p.stderr)
        self.assertEqual((data/"priv_validator_state.json").read_text(),state)
        self.assertEqual((data/"application.db/CURRENT").read_bytes(),b"test-table")
        self.assertEqual((self.root/"phase").read_text(),"restored\n")

    def test_partial_transfer_and_checksum(self):
        fixture=self.root/"payload"; fixture.write_bytes(b"snapshot fixture"*1024)
        archive=self.root/"archive"; part=self.root/"archive.part"
        part.write_bytes(fixture.read_bytes()[:300])
        plan={"bytes":fixture.stat().st_size,"sha256":hashlib.sha256(fixture.read_bytes()).hexdigest(),
              "url":"https://example.org/fixture","file":"fixture"}
        # Simulate the transport only. The public Bash entry's normal length/SHA/rename path runs.
        run=r'''job=$1; archive=$2; plan=$3; fixture=$4
curl() { local out='' resume=0; while (($#)); do case "$1" in --output) out=$2; shift;; --continue-at) [[ $2 == - ]] || return 9; resume=1; shift;; esac; shift; done
    (( resume )) || return 9; local n=0; [[ ! -f $out ]] || n=$(stat -c %s "$out"); tail -c "+$((n+1))" "$fixture" >>"$out"; }
download'''
        p=bash(run,self.root,archive,json.dumps(plan),fixture)
        self.assertEqual(p.returncode,0,p.stderr)
        self.assertEqual(archive.read_bytes(),fixture.read_bytes())
        archive.unlink()
        plan["sha256"]="00"*32
        p=bash(run,self.root,archive,json.dumps(plan),fixture)
        self.assertNotEqual(p.returncode,0)
        self.assertFalse(archive.exists())
        self.assertEqual(len(list(self.root.glob("archive.part.rejected.*"))),1)

    def test_cached_archive_of_expected_size_still_needs_its_digest(self):
        archive = self.root / "archive"
        archive.write_bytes(b"invalid")
        plan = {"bytes": 7, "sha256": hashlib.sha256(b"correct").hexdigest(), "url": "https://example.org/fixture"}
        run = bash('job=$1; archive="$job/archive"; plan=$2; curl() { return 7; }; download',
                   self.root, json.dumps(plan))
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("Cached archive SHA-256 mismatch", run.stderr)
        self.assertFalse((self.root / "phase").exists())

    def test_replay_pre_first_block_only(self):
        node=self.root/"node"
        for name in ("data/state.db","data/blockstore.db","bootstrap-backup/state.db","bootstrap-backup/blockstore.db"):
            (node/name).mkdir(parents=True)
            (node/name/"CURRENT").write_text(name)
        (node/"data/priv_validator_state.json").write_text('{"height":"0"}')
        (self.root/"node.log").write_text("panic: state (501) and store (0) height mismatch\n")
        cmd='job=$1; node="$job/node"; kind=native; height=501; recover_checkpoint'
        p=bash(cmd,self.root)
        self.assertEqual(p.returncode,0,p.stderr)
        self.assertTrue((self.root/"checkpoint-replayed").exists())
        self.assertEqual((node/"data/state.db/CURRENT").read_text(),"bootstrap-backup/state.db")
        self.assertNotEqual(bash(cmd,self.root).returncode,0)

    def test_follow_needs_snapshot_and_independent_head(self):
        node=self.root/"node"; node.mkdir()
        daemon=self.root/"fake-daemon"
        daemon.write_text("#!/bin/bash\nfor i in {1..100}; do [[ -e $1/verified-height ]] && exit 0; /bin/sleep .02; done\nexit 4\n")
        daemon.chmod(0o700)
        cmd=r'''job=$1; node="$job/node"; port=36656; height=100; kind=full
plan='{"app_hash":"ABCD"}'; expected_identity='{"address":"fixture"}'; daemon=("$job/fake-daemon" "$job")
support() { if [[ $1 == head-verify ]]; then [[ $3 == 123 ]] || return 1; fi; return 0; }
curl() { case "${@: -1}" in */status) echo '{"result":{"sync_info":{"latest_block_height":"124","catching_up":false},"node_info":{"network":"genesis_29-2"},"validator_info":{"address":"fixture","voting_power":"0"}}}';;
*) echo '{"result":{"signed_header":{"header":{"app_hash":"ABCD"}}}}';; esac; }
sleep() { /bin/sleep .02; }
follow'''
        p=bash(cmd,self.root)
        self.assertEqual(p.returncode,0,p.stderr)
        self.assertEqual((self.root/"snapshot-proof").read_text(),"ABCD\n")
        self.assertEqual((self.root/"verified-height").read_text(),"124\n")


if __name__=="__main__":
    unittest.main()
