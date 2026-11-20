import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class ProducerCommands(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = Path(__file__).resolve().parent

    def test_create_native_returns_child_exit_status_and_preserves_arguments(self):
        wrapper = shutil.copy(self.source / "create-native.sh", self.root)
        (self.root / "producer.py").write_text(
            "import json, sys\nprint(json.dumps(sys.argv[1:]))\nraise SystemExit(37)\n"
        )
        args = ["check", "--config", str(self.root / "config with spaces.json")]
        result = subprocess.run(["bash", wrapper, *args], cwd=self.root, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 37, result.stderr)
        self.assertEqual(json.loads(result.stdout.splitlines()[-1]), args)

    def test_build_exporter_does_not_replace_binary_after_failed_go_test(self):
        wrapper = shutil.copy(self.source / "build-exporter.sh", self.root)
        exporter = self.root / "exporter"
        exporter.mkdir()
        for name in ("go.mod", "go.sum", "main.go", "main_test.go"):
            (exporter / name).touch()
        (exporter / "iavl-v1.2.6.patch").write_text("--- /dev/null\n+++ b/fixture\n@@ -0,0 +1 @@\n+fixture\n")
        upstream = self.root / "upstream"
        upstream.mkdir()
        shim = self.root / "go"
        shim.write_text(
            '#!/bin/sh\nprintf "%s\\n" "$1" >> "$GO_CALLS"\n'
            'case "$1" in\n'
            '  mod) printf "%s\\n" "$MODULE" ;;\n'
            '  test) exit 37 ;;\n'
            '  *) exit 99 ;;\n'
            'esac\n'
        )
        shim.chmod(0o755)
        calls = self.root / "go-calls"
        module = {"Sum": "h1:Hs3LndJbkIB+rEvToKJFXZvKo6Vy0Ex1SJ54hhtioIs=", "Dir": str(upstream)}
        env = {**os.environ, "PATH": str(self.root) + os.pathsep + os.defpath, "TMPDIR": str(self.root),
               "GO_CALLS": str(calls), "MODULE": json.dumps(module)}
        target = self.root / "installed"
        target.write_bytes(b"known-good binary")
        result = subprocess.run(["bash", wrapper, target], cwd=self.root, env=env,
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 37, result.stderr)
        self.assertEqual(calls.read_text().splitlines(), ["mod", "test"])
        self.assertEqual(target.read_bytes(), b"known-good binary")
