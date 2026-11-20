"""Make a crash-consistent LevelDB view of the nonsigning replica without stopping it."""
import os
from pathlib import Path
import re
import stat
import time


TABLE = re.compile(r"[0-9]+\.(?:ldb|sst)\Z")
WAL = re.compile(r"[0-9]+\.log\Z")
MANIFEST = re.compile(r"MANIFEST-[0-9]+\Z")


def identity(path):
    s = path.stat(follow_symlinks=False)
    return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns


def inventory(source):
    # The directory generation also detects creation and rotation during readdir.
    start = identity(source)
    with open(source / "CURRENT", "rb") as stream:
        current = stream.read(128)
    name = current.decode("ascii").rstrip("\n")
    if not MANIFEST.fullmatch(name) or current != (name + "\n").encode():
        raise RuntimeError("Unexpected LevelDB CURRENT")
    files = os.listdir(source)
    tables = {n for n in files if TABLE.fullmatch(n)}
    wals = sorted(n for n in files if WAL.fullmatch(n))
    if not tables or not wals:
        raise RuntimeError("Replica has no LevelDB tables or active WAL")
    metadata = {n: identity(source / n) for n in ["CURRENT", name, *wals]}
    if start != identity(source):
        raise BlockingIOError("LevelDB file inventory moved")
    return start, tables, metadata


def pin(source, frozen, names, pinned):
    # SST names are unique and their committed contents immutable. Only create
    # links in our directory; never chmod, chown or write the linked files.
    count = 0
    for name in names - pinned:
        try:
            os.link(source / name, frozen / name, follow_symlinks=False)
        except FileNotFoundError:
            continue  # Compaction. A complete, stable inventory is required below.
        except FileExistsError:
            if identity(source / name)[:2] != identity(frozen / name)[:2]:
                raise RuntimeError("A pinned table name changed inode")
        pinned.add(name)
        count += 1
        if count % 512 == 0:
            time.sleep(0.05)


def freeze(source: Path, frozen: Path):
    if (source.resolve() != source or frozen.resolve() != frozen or source == frozen or
            source in frozen.parents or frozen in source.parents):
        raise ValueError("Invalid capture paths")
    frozen.mkdir(mode=0o700, parents=True, exist_ok=True)
    if os.listdir(frozen):
        raise ValueError("Capture requires a fresh private directory")
    pinned = set()
    # Seed the bulk of the view without requiring the replica to be idle.
    pin(source, frozen, {n for n in os.listdir(source) if TABLE.fullmatch(n)}, pinned)
    for _ in range(30):
        try:
            start, tables, metadata = inventory(source)
            pin(source, frozen, tables, pinned)
            if not tables <= pinned:
                raise BlockingIOError("Compaction replaced an unpinned table")
            contents = {}
            for name, signature in metadata.items():
                if signature[2] > 512 * 1024**2:
                    raise RuntimeError("Unusually large LevelDB journal or manifest")
                with (source / name).open("rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise RuntimeError("Nonregular LevelDB metadata")
                    contents[name] = stream.read(signature[2] + 1)
                if len(contents[name]) != signature[2]:
                    raise BlockingIOError("LevelDB journal changed during copy")
            if (start, tables, metadata) != inventory(source):
                raise BlockingIOError("LevelDB generation changed during copy")
        except (FileNotFoundError, BlockingIOError):
            time.sleep(0.8)
            continue
        # Exclude stale WALs from an interrupted capture. The independent reader
        # opens only this private LOCK and replays only the frozen WAL copies.
        for name in os.listdir(frozen):
            if WAL.fullmatch(name) or MANIFEST.fullmatch(name) or name == "CURRENT":
                (frozen / name).unlink()
        for name, content in contents.items():
            with (frozen / name).open("xb") as out:
                out.write(content)
                out.flush()
                os.fsync(out.fileno())
        (frozen / "LOCK").touch(mode=0o600, exist_ok=True)
        return {"table_count": len(tables), "wal_count": sum(bool(WAL.fullmatch(n)) for n in metadata),
                "manifest": next(n for n in metadata if MANIFEST.fullmatch(n))}
    raise RuntimeError("No stable LevelDB capture window; retry at the next timer tick")
