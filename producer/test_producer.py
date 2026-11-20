import base64
from datetime import timedelta
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

import archive
import producer as p
from capture import freeze


class ProducerTest(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        root = self.root = Path(folder.name)
        cfg = {"replica_rpc":"http://127.0.0.1:26657", "replica_node_id":"a"*40,
               "node_home":str(root/"node"), "work_directory":str(root/"work"),
               "output_directory":str(root/"public"), "exporter":str(root/"exporter"),
               "exporter_sha256":"0"*64, "operator":"Example", "public_base_url":"https://example.org/native"}
        (root/"config").write_text(json.dumps(cfg))
        self.c = c = p.Config(root/"config")
        c.public.mkdir()
        with p.locked(c, True):
            c.job.mkdir()
        chunks = self.chunks = [b"example chunk"*1000, b"other content"]
        b64 = lambda d: base64.b64encode(hashlib.sha256(d).digest()).decode()
        snap = self.snap = {"height":100, "format":3, "chunks":2, "hash":b64(b"".join(chunks)),
                            "metadata":{"chunk_hashes":[b64(d) for d in chunks]}}
        dest = self.dest = c.job/"chunks"
        files = self.files = dest/"100"/"3"
        files.mkdir(parents=True)
        for i,d in enumerate(chunks):
            (files/str(i)).write_bytes(d)
        (dest/"snapshot.pb").write_bytes(archive.sdk_metadata(snap)[0])
        self.proof = {"snapshot_app_hash":"A"*64, "snapshot_block_time_utc":p.now().isoformat(),
                      "fixed_trust_anchor":{"height":99, "hash":"B"*64, "trust_period":"144h",
                                            "expires_at_utc":(p.now()+timedelta(hours=143)).isoformat()}}
        p.put(c.ready, {"snapshot":snap, "proof":self.proof})

    def test_publish_and_fail_closed(self):
        c = self.c
        previous = {"chain_id":p.CHAIN,"snapshot_format":3,"snapshot_height":80,"file":p.filename(80),"bytes":3}
        (c.public/p.filename(80)).write_bytes(b"old")
        p.put(c.latest, previous)
        with patch.object(p, "agree", return_value=self.proof), patch.object(c, "space"):
            (self.files/"0").write_bytes(b"corrupted")
            with self.assertRaisesRegex(RuntimeError,"Fresh export"):
                p.publish(c)
            self.assertEqual(p.read(c.rebuild)["height"],100)
            self.assertEqual(p.read(c.latest),previous)
            self.assertFalse((c.public/p.filename(100)).exists())
            (self.files/"0").write_bytes(self.chunks[0])
            p.publish(c)
            manifest = p.read(c.latest)
            f = c.public/manifest["file"]
            self.assertEqual(p.sha(f),manifest["sha256"])
            self.assertTrue((c.public/(manifest["file"]+".sha256")).is_file())
            self.assertFalse(c.rebuild.exists())
            with tarfile.open(f,"r:gz") as tar:
                self.assertEqual(tar.getnames(),["_snapshot","0","1"])
                self.assertEqual(tar.extractfile("_snapshot").read(),(self.dest/"snapshot.pb").read_bytes())
                self.assertEqual(tar.extractfile("0").read(),self.chunks[0])
            p.publish(c)
            self.assertEqual(p.read(c.latest),manifest)
            self.assertFalse(c.due(manifest))
            manifest["fixed_trust_anchor"]["expires_at_utc"] = (p.now()+timedelta(hours=80)).isoformat()
            self.assertTrue(c.due(manifest))
            self.assertTrue((c.public/p.filename(80)).is_file())

    def test_witness_failure_before_pointer(self):
        with patch.object(p, "agree", side_effect=[self.proof, ValueError("witness mismatch")]), patch.object(self.c,"space"):
            with self.assertRaisesRegex(ValueError,"witness mismatch"):
                p.publish(self.c)
            self.assertFalse(self.c.latest.exists())
            self.assertFalse((self.c.public/p.filename(100)).exists())

    def test_real_sdk_import(self):
        binary = os.getenv("GENESISD_TEST")
        if not binary:
            self.skipTest("Set GENESISD_TEST for stock Cosmos SDK compatibility test")
        file = self.root/"fixture.tar.gz"
        archive.package(self.snap, self.dest, file, lambda *args, **kwargs: None)
        home = self.root/"test-home"
        run = lambda args: subprocess.run([binary,*args,"--home",str(home)], check=True,
                                          env=dict(os.environ, HOME=str(self.root)),
                                          capture_output=True, text=True, timeout=30)
        run(["init","fixture","--chain-id",p.CHAIN])
        run(["snapshots","load",str(file)])
        self.assertIn("100",run(["snapshots","list"]).stdout)

    def test_frozen_view(self):
        src, target = self.root/"source", self.root/"pin"/"application.db"
        src.mkdir()
        for name,data in {"CURRENT":b"MANIFEST-1\n","MANIFEST-1":b"manifest","2.log":b"wal","3.ldb":b"table"}.items():
            (src/name).write_bytes(data)
        stats=freeze(src,target)
        self.assertEqual(stats["table_count"],1)
        self.assertEqual((src/"3.ldb").stat().st_ino,(target/"3.ldb").stat().st_ino)
        self.assertNotEqual((src/"2.log").stat().st_ino,(target/"2.log").stat().st_ino)
        (src/"2.log").write_bytes(b"changed")
        (src/"3.ldb").unlink()
        self.assertEqual((target/"2.log").read_bytes(),b"wal")
        self.assertEqual((target/"3.ldb").read_bytes(),b"table")


if __name__ == "__main__":
    unittest.main()
