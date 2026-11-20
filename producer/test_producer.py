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
from types import SimpleNamespace
import unittest
from unittest.mock import call, patch

import archive
import producer as p
from capture import freeze


# Cosmos SDK Snapshot: fields 1/2/3, field 4 (32-byte SHA-256), then
# field 5 (68 bytes containing two field-1 digests). Independent wire fixture.
SDK_FIXTURE = bytes.fromhex(
    "08 64 10 03 18 02 22 20 c3aad96876da265d86a3d0923b7958cefd58692eff127db3dc110c2f2c62ed9c"
    " 2a 44 0a 20 b7d99a3ac66861054bc52ced4851c4a64126afaaf5f0cca84d345ae5aa81966f"
    " 0a 20 923b805711041e23a99f07e146591c500261d1c289f62a9d39f8581ceb8a10ca"
)


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
        (dest/"snapshot.pb").write_bytes(SDK_FIXTURE)
        self.proof = {"snapshot_app_hash":"A"*64, "snapshot_block_time_utc":p.now().isoformat(),
                      "fixed_trust_anchor":{"height":99, "hash":"B"*64, "trust_period":"144h",
                                            "expires_at_utc":(p.now()+timedelta(hours=143)).isoformat()}}
        p.put(c.ready, {"snapshot":snap, "proof":self.proof})

    def test_sdk_wire_format_and_varint_boundaries(self):
        self.assertEqual(archive.sdk_metadata(self.snap)[0], SDK_FIXTURE)
        for number, encoded in [(0, "00"), (127, "7f"), (128, "8001"),
                                (16383, "ff7f"), (16384, "808001")]:
            with self.subTest(number=number):
                self.assertEqual(archive.varint(number), bytes.fromhex(encoded))

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

    def test_build_space_tracks_previous_export_and_table_growth(self):
        c = self.c
        c.db.mkdir(parents=True)
        (c.db / "1.ldb").write_bytes(b"x" * 600)
        (c.db / "2.sst").write_bytes(b"x" * 200)
        (c.db / "3.log").write_bytes(b"ignored")
        for manifest, expected in ((None, 1000), ({"bytes": 240}, 300),
                                   ({"bytes": 220, "compressed_chunk_bytes": 200, "source_table_bytes": 400}, 500),
                                   ({"bytes": 220, "compressed_chunk_bytes": 200, "source_table_bytes": 1600}, 250)):
            with self.subTest(manifest=manifest), patch.object(c, "space") as space:
                p.check_build_space(c, manifest)
                space.assert_called_once_with(c.work, 2 * expected)
        with patch.object(c, "space") as space, patch.object(Path, "stat", autospec=True,
                side_effect=lambda path: SimpleNamespace(st_dev=1 if path == c.work else 2)):
            p.check_build_space(c, {"bytes": 240})
            self.assertEqual(space.call_args_list, [call(c.work, 300), call(c.public, 300)])

    def test_publish_rejects_insufficient_room_before_opening_large_archive(self):
        c = self.c
        previous = {"chain_id": p.CHAIN, "snapshot_format": 3, "snapshot_height": 80,
                    "file": p.filename(80), "bytes": 3}
        p.put(c.latest, previous)
        (c.public / p.filename(80)).write_bytes(b"old")
        original = Path.lstat
        def large_chunk(path):
            info = original(path)
            if path == self.files / "0":
                return SimpleNamespace(st_mode=info.st_mode, st_size=80 * p.GIB)
            return info
        with patch.object(p, "agree", return_value=self.proof), patch.object(Path, "lstat", large_chunk), \
             patch.object(shutil, "disk_usage", return_value=SimpleNamespace(free=c.reserve + 65 * p.GIB)), \
             patch.object(tarfile, "open") as open_archive:
            with self.assertRaisesRegex(ValueError, "Insufficient space"):
                p.publish(c)
            open_archive.assert_not_called()
        self.assertEqual(p.read(c.latest), previous)
        self.assertFalse((c.public / ".beehive-native-stage/archive.partial").exists())
        self.assertFalse(c.rebuild.exists())

    def test_archive_estimate_covers_every_tar_member_and_stale_partial_is_removed(self):
        c = self.c
        stage = c.public / ".beehive-native-stage"
        stage.mkdir()
        partial = stage / "archive.partial"
        partial.write_bytes(b"old attempt")
        requested = []
        def check_space(path, size=0):
            if size:
                self.assertFalse(partial.exists())
                requested.append(size)
        with patch.object(p, "agree", return_value=self.proof), patch.object(c, "space", side_effect=check_space):
            p.publish(c)
        actual = (c.public / p.filename(100)).stat().st_size
        members = [(self.dest / "snapshot.pb").stat().st_size, *(len(d) for d in self.chunks)]
        self.assertEqual(requested, [archive.archive_space(members)])
        self.assertGreater(requested[0], actual)
        self.assertGreater(archive.archive_space([1] * 1000), archive.archive_space([1000]))

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
        self.assertEqual(stats["table_bytes"],len(b"table"))
        self.assertEqual((src/"3.ldb").stat().st_ino,(target/"3.ldb").stat().st_ino)
        self.assertNotEqual((src/"2.log").stat().st_ino,(target/"2.log").stat().st_ino)
        (src/"2.log").write_bytes(b"changed")
        (src/"3.ldb").unlink()
        self.assertEqual((target/"2.log").read_bytes(),b"wal")
        self.assertEqual((target/"3.ldb").read_bytes(),b"table")


if __name__ == "__main__":
    unittest.main()
