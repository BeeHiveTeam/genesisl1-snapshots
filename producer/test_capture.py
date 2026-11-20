from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import capture


class Capture(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.src = self.root/"source"
        self.dst = self.root/"capture"
        self.src.mkdir()
        for name, data in (("CURRENT",b"MANIFEST-3\n"),("MANIFEST-3",b"manifest"),
                           ("1.sst",b"immutable"),("4.log",b"journal")):
            (self.src/name).write_bytes(data)

    def test_invalid_current_and_missing_journal_cannot_create_a_checkpoint(self):
        for value in (b"MANIFEST-3", b"not-a-manifest\n"):
            (self.src/"CURRENT").write_bytes(value)
            with self.assertRaisesRegex(RuntimeError,"CURRENT"):
                capture.inventory(self.src)
        (self.src/"CURRENT").write_bytes(b"MANIFEST-3\n")
        (self.src/"4.log").unlink()
        with self.assertRaisesRegex(RuntimeError,"WAL"):
            capture.inventory(self.src)

    def test_changing_wal_discards_first_observation_and_retries(self):
        actual = capture.inventory
        calls = []
        def changing(source):
            calls.append(source)
            if len(calls) == 2:
                (source/"4.log").write_bytes(b"new generation")
            return actual(source)
        with patch.object(capture,"inventory",changing), patch.object(capture.time,"sleep") as wait:
            record = capture.freeze(self.src,self.dst)
        wait.assert_called_once()
        self.assertEqual(len(calls),4)
        self.assertEqual(record, {"table_count":1,"table_bytes":9,"wal_count":1,"manifest":"MANIFEST-3"})
        self.assertEqual((self.dst/"4.log").read_bytes(), b"new generation")
        self.assertEqual((self.src/"4.log").read_bytes(), b"new generation")

    def test_pin_accepts_existing_hardlink_but_rejects_a_different_inode(self):
        self.dst.mkdir()
        source = self.src / "1.sst"
        target = self.dst / source.name
        target.write_bytes(source.read_bytes())
        pinned = set()
        with self.assertRaisesRegex(RuntimeError, "changed inode"):
            capture.pin(self.src, self.dst, {source.name}, pinned)
        self.assertEqual(pinned, set())
        self.assertFalse(target.samefile(source))

        target.unlink()
        target.hardlink_to(source)
        capture.pin(self.src, self.dst, {source.name}, pinned)
        self.assertEqual(pinned, {source.name})

    def test_existing_destination_and_source_overlap_are_preserved(self):
        self.dst.mkdir()
        kept = self.dst/"existing"
        kept.write_bytes(b"keep")
        for path in (self.src, self.src/"child", self.root, self.dst):
            with self.subTest(path=path), self.assertRaises(ValueError):
                capture.freeze(self.src,path)
        self.assertEqual(kept.read_bytes(),b"keep")
        self.assertEqual((self.src/"1.sst").read_bytes(),b"immutable")
