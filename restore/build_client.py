"""Assemble the single-file Bash download from its shell and Python sources."""
import argparse
import hashlib
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check the bundle and checksum without writing")
    args = parser.parse_args()
    shell = (HERE / "client.sh.in").read_bytes()
    helper = (HERE / "helper.py").read_bytes()
    marker = b"@BEEHIVE_RESTORE_HELPER@\n"
    if shell.count(marker) != 1 or b"PYTHON" in helper.splitlines() or not helper.endswith(b"\n"):
        parser.error("Invalid source template or heredoc delimiter")
    bundle = shell.replace(marker, helper).replace(
        b"#!/usr/bin/env bash\n", b"#!/usr/bin/env bash\n# Generated from client.sh.in and helper.py by build_client.py.\n", 1)
    client = HERE / "beehive-sync.sh"
    digest = f"{hashlib.sha256(bundle).hexdigest()}  {client.name}\n".encode()
    for path, content in ((client, bundle), (client.with_suffix(".sh.sha256"), digest)):
        if args.check:
            if not path.exists() or path.read_bytes() != content:
                parser.exit(1, f"{path.name} is out of date. Run python3 restore/build_client.py.\n")
        else:
            path.write_bytes(content)


if __name__ == "__main__":
    main()
