"""Exercise catalog discovery against a CDN that requires our client identifier."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


SCRIPT = Path(__file__).with_name("beehive-sync.sh").resolve()


class SourceHttpTest(unittest.TestCase):
    def test_manifest_get_and_archive_head_use_the_same_user_agent(self):
        # Intercept urllib inside the Bash client's Python process. No network or node is needed.
        fixture = """
            import io
            import json
            import urllib.request
            def open_fixture(request, timeout):
                base = "https://l1-snapshots.bee-hive.work:8545/"
                if request.get_header("User-agent") != "BeeHive-GenesisL1-Sync/1":
                    raise OSError("CDN rejected User-Agent")
                if request.full_url == base + "latest.json" and request.get_method() == "GET":
                    manifest = {"chain_id": "genesis_29-2", "version": "1.6.2",
                                "file": "fixture.tar.lz4", "memiavl_enabled": False,
                                "versiondb_enabled": False, "height": 100, "bytes": 3,
                                "app_hash": "ab"*32, "block_hash": "cd"*32, "sha256": "ef"*32}
                    response = io.BytesIO(json.dumps(manifest).encode())
                elif request.full_url == base + "fixture.tar.lz4" and request.get_method() == "HEAD":
                    response = io.BytesIO()
                    response.headers = {"Content-Length": "3"}
                else:
                    raise OSError("Fixture offers only the full archive")
                response.url = request.full_url
                return response
            urllib.request.urlopen = open_fixture
            urllib.request.OpenerDirector.open = lambda self, req, timeout: open_fixture(req, timeout)
        """
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "sitecustomize.py").write_text(textwrap.dedent(fixture))
            result = subprocess.run(
                ["bash", "-c", 'source "$1"; support catalog beehive-full ""', "catalog-test", str(SCRIPT)],
                env=dict(os.environ, PYTHONPATH=directory, PYTHONDONTWRITEBYTECODE="1"),
                check=True, capture_output=True, text=True, timeout=10)
        full = next(row for row in json.loads(result.stdout) if row["id"] == "beehive-full")
        self.assertEqual(full.get("bytes"), 3, full)


if __name__ == "__main__":
    unittest.main()
