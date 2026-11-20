"""Retention runs only on a temporary catalog owned by this test."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import tempfile
import unittest
from unittest.mock import patch

import producer as p


class Retention(unittest.TestCase):
    now = datetime(2025, 7, 20, tzinfo=timezone.utc)

    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.public = Path(folder.name)

    def edition(self, height, *, age=10, expired=5, data=None, **changes):
        name = p.filename(height)
        files = (self.public/name, self.public/(name + ".sha256"),
                 self.public/(name.removesuffix(".tar.gz") + ".json"))
        manifest = dict(publisher_schema="beehive-native-v1", chain_id=p.CHAIN, snapshot_format=3,
                        snapshot_height=height, file=name, published_at_utc=(self.now-timedelta(days=age)).isoformat(),
                        fixed_trust_anchor={"expires_at_utc": (self.now-timedelta(days=expired)).isoformat()})
        if data is None:
            data = name.encode()
        manifest.update(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
        files[0].write_bytes(data)
        files[1].write_text(manifest["sha256"]+"  "+name+"\n")
        p.put(files[2], {**manifest, **changes})
        return files

    def retire(self, height=200):
        with patch.object(p, "now", return_value=self.now):
            p.retire(self.public, height)

    def assert_present(self, edition, present=True):
        self.assertEqual([f.exists() for f in edition], [present]*3)

    def test_two_latest_by_numeric_height_and_future_edition_survive(self):
        versions = {h:self.edition(h) for h in (10, 100, 2, 200, 205)}
        pointer = self.public / "latest.json"
        pointer.write_bytes(versions[200][2].read_bytes())
        old_pointer = pointer.read_bytes()
        self.retire()
        self.retire()
        for h, files in versions.items():
            self.assert_present(files, h in (100, 200, 205))
        self.assertEqual(pointer.read_bytes(), old_pointer)

    def test_both_deadlines_include_equality(self):
        self.edition(100)
        self.edition(200)
        just_published = self.edition(10, age=6.999)
        just_expired = self.edition(20, expired=0.999)
        eligible = self.edition(30, age=7, expired=1)
        self.retire()
        self.assert_present(just_published)
        self.assert_present(just_expired)
        self.assert_present(eligible, False)

    def test_only_an_owned_matching_manifest_authorizes_its_own_files(self):
        self.edition(100)
        self.edition(200)
        untouched = [
            self.edition(10, publisher_schema="manual"),
            self.edition(11, chain_id="another-chain"),
            self.edition(12, snapshot_format=2),
            self.edition(13, file=p.filename(100)),
            self.edition(14, snapshot_height=9, file=p.filename(9)),
            self.edition(15, published_at_utc="unknown"),
        ]
        # Keep an unrelated archive with the internally claimed height.
        unrelated = self.public/p.filename(9)
        unrelated.write_bytes(b"kept")
        manual = self.edition(16)
        manual[2].write_text("{}")
        broken = self.edition(17)
        broken[2].write_text("{")
        linked = self.edition(18)
        original = self.public/"manual.json"
        linked[2].rename(original)
        linked[2].symlink_to(original)
        self.retire()
        for files in (*untouched, manual, broken, linked):
            self.assert_present(files)
        self.assertEqual(unrelated.read_bytes(), b"kept")

    def test_foreign_owner_and_replaced_artifact_keep_whole_edition(self):
        self.edition(100)
        self.edition(200)
        for index in (0, 1, 2):
            with self.subTest(index=index):
                old = self.edition(10)
                real = Path.lstat
                def lstat(path):
                    result = real(path)
                    if path == old[index]:
                        from types import SimpleNamespace
                        return SimpleNamespace(st_mode=result.st_mode, st_uid=result.st_uid+1)
                    return result
                with patch.object(Path, "lstat", lstat):
                    self.retire()
                self.assert_present(old)
        old[0].unlink()
        old[0].symlink_to(self.public / p.filename(200))
        self.retire()
        self.assert_present(old)
        self.assertTrue(old[0].is_symlink())

    def test_only_a_valid_positive_byte_count_and_digest_authorize_cleanup(self):
        self.edition(100)
        self.edition(200)
        single_byte = self.edition(2, data=b"x")
        invalid = [
            self.edition(10, bytes=0), self.edition(11, bytes=-1),
            self.edition(12, bytes=True), self.edition(13, bytes="1"),
            self.edition(14, sha256="a"*63), self.edition(15, sha256=None),
        ]
        self.retire()
        self.assert_present(single_byte, False)
        for files in invalid:
            self.assert_present(files)

    def test_foreign_current_manifest_keeps_all_older_editions(self):
        from types import SimpleNamespace
        versions = [self.edition(h) for h in (10, 20, 100, 200)]
        current = versions[-1][2]
        real = Path.lstat

        def other_owner(path):
            info = real(path)
            if path != current:
                return info
            fields = {key: getattr(info, key) for key in dir(info) if key.startswith("st_")}
            return SimpleNamespace(**{**fields, "st_uid": info.st_uid + 1})

        with patch.object(Path, "lstat", other_owner):
            self.retire()
        for files in versions:
            self.assert_present(files)

    def test_incomplete_newest_or_previous_copy_keeps_older_fallback(self):
        fallback = self.edition(10)
        for height in (100, 200):
            for damage in ("missing", "short archive", "wrong checksum"):
                with self.subTest(height=height, damage=damage):
                    editions = {h:self.edition(h) for h in (100, 200)}
                    archive, checksum, _ = editions[height]
                    if damage == "missing":
                        archive.unlink()
                    elif damage == "short archive":
                        archive.write_bytes(b"short")
                    else:
                        checksum.write_text("0"*64+"  "+archive.name+"\n")
                    self.retire()
                    self.assert_present(fallback)
        self.edition(200)
        self.retire()
        self.assert_present(fallback, False)

    def test_interrupted_retirement_can_finish_without_touching_other_editions(self):
        old = self.edition(10)
        keep = self.edition(20)
        current = self.edition(200)
        old[0].unlink()
        self.retire()
        self.assert_present(old, False)
        self.assert_present(keep)
        self.assert_present(current)

    def test_same_size_damage_preserves_the_last_good_edition(self):
        fallback = self.edition(10)
        for damaged in ((100,), (200,), (100, 200)):
            with self.subTest(damaged=damaged):
                editions = {h: self.edition(h) for h in (100, 200)}
                for h in damaged:
                    archive = editions[h][0]
                    original = archive.read_bytes()
                    archive.write_bytes(bytes([original[0] ^ 1]) + original[1:])
                self.retire()
                self.assert_present(fallback)
        for h in (100, 200):
            self.edition(h)
        self.retire()
        self.assert_present(fallback, False)

    def test_hash_reads_wait_until_an_older_edition_is_due(self):
        self.edition(100)
        self.edition(200)
        old = self.edition(10, age=6)
        with patch.object(hashlib, "file_digest", wraps=hashlib.file_digest) as digest:
            self.retire()
            self.assert_present(old)
            digest.assert_not_called()
            self.edition(10, age=7)
            self.retire()
            self.assertEqual(digest.call_count, 2)
            self.assert_present(old, False)

    def test_read_error_keeps_previous_edition_until_both_hashes_can_be_checked(self):
        fallback = self.edition(10)
        self.edition(100)
        self.edition(200)
        with patch.object(hashlib, "file_digest", side_effect=OSError("fixture read error")):
            self.retire()
            self.assert_present(fallback)
        self.retire()
        self.assert_present(fallback, False)

    def test_intact_contents_through_a_symlink_do_not_authorize_removal(self):
        fallback = self.edition(10)
        self.edition(100)
        newest = self.edition(200)
        for path in newest[:2]:
            with self.subTest(path=path.name):
                original = path.with_name("original")
                path.rename(original)
                path.symlink_to(original)
                self.retire()
                self.assert_present(fallback)
                original.replace(path)
        self.retire()
        self.assert_present(fallback, False)

    def test_replacement_while_verifying_the_other_archive_preserves_the_fallback(self):
        fallback = self.edition(10)
        self.edition(100)
        newest = self.edition(200)[0]
        real_digest = hashlib.file_digest
        calls = 0

        def digest(stream, algorithm):
            nonlocal calls
            calls += 1
            if calls == 2:
                replacement = newest.with_suffix(".partial")
                replacement.write_bytes(b"X" * newest.stat().st_size)
                replacement.replace(newest)
            return real_digest(stream, algorithm)

        with patch.object(hashlib, "file_digest", side_effect=digest):
            self.retire()
        self.assertEqual(calls, 2)
        self.assert_present(fallback)
