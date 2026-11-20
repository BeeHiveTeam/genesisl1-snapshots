import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

import producer


class ConfigBounds(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.root = Path(folder.name)
        self.file = self.root/"config.json"
        self.values = json.loads(Path(__file__).with_name("config.example.json").read_text())
        self.values.update(node_home=str(self.root/"node"), work_directory=str(self.root/"work"),
                           output_directory=str(self.root/"public"), exporter=str(self.root/"exporter"),
                           replica_node_id="1"*40, exporter_sha256="a"*64)

    def config(self, **changes):
        self.file.write_text(json.dumps({**self.values, **changes}))
        return producer.Config(self.file)

    def test_invalid_identity_rpc_paths_and_public_url(self):
        (self.root/"alias").symlink_to(self.root/"node")
        for key, values in {
            "replica_rpc":["http://localhost:26657","http://127.0.0.1:0","http://127.0.0.1:65536","https://127.0.0.1:443"],
            "replica_node_id":["1"*39, "x"*40], "exporter_sha256":["a"*63, "g"*64],
            "operator":["", "o"*121], "work_directory":["relative","/",str(self.root/"alias"),
                                                        str(self.root/"node"),str(self.root/"node/sub")],
            "output_directory":[str(self.root), str(self.root/"work/sub")],
            "public_base_url":["http://example.org","https://example.org/?x=1","https://user@example.org","https://example.org/#frag"],
        }.items():
            for value in values:
                with self.subTest(key=key,value=value), self.assertRaises(ValueError):
                    self.config(**{key:value})

    def test_resource_limits_and_disk_reserve_at_exact_boundary(self):
        for key, low, high in (("cache_mib",256,65536), ("export_workers",1,64), ("open_files",128,512000)):
            for value in (low, high):
                self.config(**{key:value})
            for value in (low-1, high+1):
                with self.subTest(key=key,value=value), self.assertRaises(ValueError):
                    self.config(**{key:value})
        with self.assertRaises(ValueError):
            self.config(disk_reserve_gib=19)
        c = self.config(disk_reserve_gib=20, operator="o"*120)
        with patch.object(producer.shutil, "disk_usage", return_value=SimpleNamespace(free=20*producer.GIB+5)):
            c.space(c.work, 5)
            with self.assertRaises(ValueError):
                c.space(c.work, 6)
