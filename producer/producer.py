#!/usr/bin/env python3
"""GenesisL1 Native snapshot builder and atomic publisher. Linux, Python 3.11+."""
import argparse
from contextlib import contextmanager
from datetime import timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import stat
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

import archive
from archive import require
from capture import TABLE, freeze
from chain import CHAIN, VERSION, WITNESSES, agree, now, parse_time, replica

GIB = 1024**3
JOB = "current-build"
MARKER = ".beehive-native-v1"


def read(path):
    return json.loads(path.read_text())


def syncdir(path):
    fd = os.open(path, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def put(path, data, mode=0o640):
    if not isinstance(data, str):
        data = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".native-tmp-")
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w") as out:
            out.write(data)
            os.fchmod(out.fileno(), mode)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, path)
        syncdir(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def log(stage, **fields):
    print(json.dumps({"utc": now().isoformat(), "stage": stage, **fields}), flush=True)


class Config:
    def __init__(self, path):
        d = read(path)
        self.rpc = d["replica_rpc"].rstrip("/")
        require(bool(re.fullmatch(r"http://127\.0\.0\.1:[1-9][0-9]{0,4}", self.rpc)) and
                0 < urlsplit(self.rpc).port < 65536, "Use the replica's local CometBFT RPC")
        self.node_id = d["replica_node_id"].lower()
        require(bool(re.fullmatch("[a-f0-9]{40}", self.node_id)), "Set the dedicated replica's node ID")
        paths = [Path(d[k]) for k in ("node_home", "work_directory", "output_directory", "exporter")]
        require(all(p.is_absolute() and p.resolve() == p and p != Path("/") for p in paths),
                "Use absolute paths without symlinks")
        self.home, self.work, self.public, self.exporter = paths
        for p in paths[:3]:
            for q in paths[:3]:
                require(p not in q.parents, "Node home, working and publication trees must be separate")
        require(len(set(paths[:3])) == 3, "Overlapping publication and source paths")
        self.db = self.home / "data/application.db"
        self.base = d["public_base_url"].rstrip("/")
        u = urlsplit(self.base)
        require(u.scheme == "https" and u.hostname and not (u.username or u.password or u.query or u.fragment),
                "Set the public HTTPS base URL for the Native directory")
        self.operator = d["operator"]
        require(isinstance(self.operator, str) and 0 < len(self.operator) <= 120, "Set the operator name")
        self.exporter_sha = d["exporter_sha256"].lower()
        require(bool(re.fullmatch("[0-9a-f]{64}", self.exporter_sha)), "Set the locally verified exporter SHA-256")
        self.reserve = int(d.get("disk_reserve_gib", 150)) * GIB
        require(self.reserve >= 20 * GIB, "Keep at least 20 GiB of disk reserve")
        self.cache = int(d.get("cache_mib", 8192))
        self.workers = int(d.get("export_workers", 16))
        self.files = int(d.get("open_files", 180000))
        require(256 <= self.cache <= 65536 and 1 <= self.workers <= 64 and 128 <= self.files <= 512000,
                "Exporter settings out of range")
        self.job = self.work / JOB
        self.ready = self.job / "ready.json"
        self.latest = self.public / "latest.json"
        self.rebuild = self.public / ".beehive-native-rebuild.json"

    def space(self, path, additional=0):
        free = shutil.disk_usage(path).free
        needed = self.reserve + additional
        require(free >= needed, f"Insufficient space at {path}: {free} bytes free, {needed} required including reserve")

    def latest_manifest(self):
        if not self.latest.exists():
            return None
        m = read(self.latest)
        require(m["chain_id"] == CHAIN and m["snapshot_format"] == 3, "Output contains a different catalog")
        require(m["file"] == filename(m["snapshot_height"]), "Invalid archive name in current catalog")
        return m

    def due(self, manifest):
        if manifest is None:
            return True
        path = self.public / manifest["file"]
        if not path.is_file() or path.stat().st_size != manifest["bytes"]:
            return True
        return now() >= min(parse_time(manifest["snapshot_block_time_utc"]) + timedelta(hours=48),
                            parse_time(manifest["fixed_trust_anchor"]["expires_at_utc"]) - timedelta(hours=96))


def filename(height):
    require(type(height) is int and 0 < height < 2**53, "Invalid publication height")
    return f"{CHAIN}-native-{height}-format3.tar.gz"


def check_build_space(c, latest):
    # Live SSTs are immutable but compaction can unlink them during this estimate.
    current = 0
    with os.scandir(c.db) as entries:
        for entry in entries:
            if TABLE.fullmatch(entry.name):
                try:
                    current += entry.stat(follow_symlinks=False).st_size
                except FileNotFoundError:
                    continue
    require(current > 0, "No LevelDB tables available for the space estimate")
    estimate = current
    if latest:
        previous = latest.get("compressed_chunk_bytes") or latest["bytes"]
        require(type(previous) is int and previous > 0, "Invalid previous snapshot size")
        previous_tables = latest.get("source_table_bytes")
        if previous_tables is None:
            estimate = previous
        else:
            require(type(previous_tables) is int and previous_tables > 0, "Invalid previous source table size")
            estimate = max(previous, (previous * current + previous_tables - 1) // previous_tables)
    # Allow 25% growth. Chunks and the packaged archive coexist until publication.
    budget = (5 * estimate + 3) // 4
    if c.work.stat().st_dev == c.public.stat().st_dev:
        c.space(c.work, 2 * budget)
    else:
        c.space(c.work, budget)
        c.space(c.public, budget)
    log("space_estimate", source_table_bytes=current, bytes_per_copy=budget)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def marker_value(c):
    return {"chain": CHAIN, "replica": c.node_id, "home": str(c.home), "output": str(c.public)}


@contextmanager
def locked(c, builder):
    if builder:
        c.work.mkdir(mode=0o750, exist_ok=True)
        require(c.work.stat().st_uid == os.getuid(), "The builder must own its private work directory")
    lockpath = c.work / ".lock"
    if builder and not lockpath.exists():
        try:
            with lockpath.open("xb") as f:
                os.fchmod(f.fileno(), 0o640)
        except FileExistsError:
            pass
    with lockpath.open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        marker = c.work / MARKER
        if builder and not marker.exists():
            require(set(os.listdir(c.work)) == {".lock"}, "New work directory must be empty")
            put(marker, marker_value(c))
        require(read(marker) == marker_value(c), "Work directory belongs to a different source or output")
        yield


def discard(c, path):
    # Only delete our private staging data after verifying its ownership marker.
    require(read(c.work / MARKER) == marker_value(c) and path in
            (c.job, c.job / "pin", c.job / "chunks") and path.resolve() == path, "Unsafe cleanup path")
    if path.exists():
        db = c.job / "pin/application.db"
        if path in (c.job, c.job/"pin") and db.is_dir():
            require(db.resolve() == db, "Invalid cleanup directory")
            for index, entry in enumerate(db.iterdir(), 1):
                entry.unlink()
                if index % 256 == 0:
                    time.sleep(0.08)
        shutil.rmtree(path)


def opened(path):
    source, snapshot = None, None
    if path.is_file():
        with path.open() as stream:
            for line in stream:
                if '"source_opened"' in line or '"snapshot_created"' in line:
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        if not line.endswith("\n"):
                            continue
                        raise
                    if row.get("event") == "source_opened":
                        source = row
                    elif row.get("event") == "snapshot_created":
                        snapshot = row["snapshot"]
    return source, snapshot


def export_monitored(c, frozen, dest, out):
    cmd = [str(c.exporter), "--source", str(frozen), "--output", str(dest), "--cache-mib",
           str(c.cache), "--open-files", str(c.files), "--export-workers", str(c.workers)]
    proc = subprocess.Popen(cmd, stdout=out)
    try:
        while True:
            try:
                if proc.wait(timeout=30):
                    raise RuntimeError(f"Exporter exited with code {proc.returncode}")
                break
            except subprocess.TimeoutExpired:
                c.space(c.work)
                mem = re.search(r"(?m)^MemAvailable:\s+(\d+)", Path("/proc/meminfo").read_text())
                require(mem and int(mem[1])*1024 > 4*GIB, "Host has less than 4 GiB available memory")
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def mount_id(path):
    fd = os.open(path, os.O_DIRECTORY)
    try:
        return re.search(r"(?m)^mnt_id:\s*(\d+)$", Path(f"/proc/self/fdinfo/{fd}").read_text())[1]
    finally:
        os.close(fd)


def source_check(c):
    require(os.geteuid() != 0, "Run the builder as the dedicated nonsigning replica's OS account")
    require(c.db.is_dir() and c.db.resolve() == c.db, "Expected a real GoLevelDB application.db")
    require(c.work.is_dir() and c.public.is_dir(), "Create the dedicated working and output directories")
    require(mount_id(c.db) == mount_id(c.work), "Working directory must share the same mount as application.db")
    limit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    require(limit == resource.RLIM_INFINITY or limit >= c.files + 256,
            "Raise the open-file limit to at least open_files + 256 (systemd LimitNOFILE=524288)")
    require(sha(c.exporter) == c.exporter_sha, "Exporter checksum mismatch")
    return replica(c)


def build(c):
    with locked(c, True):
        latest = c.latest_manifest()
        if c.ready.exists():
            ready = read(c.ready)
            h = ready["snapshot"]["height"]
            rejected = read(c.rebuild) if c.rebuild.exists() else {}
            if (latest and latest["snapshot_height"] >= h) or rejected.get("height") == h:
                discard(c, c.job)
            elif parse_time(ready["proof"]["fixed_trust_anchor"]["expires_at_utc"]) > now() + timedelta(hours=48):
                log("ready_to_publish", height=h)
                return
            else:
                discard(c, c.job)
        if not c.due(latest):
            log("current", height=latest["snapshot_height"])
            return
        current = source_check(c)
        c.job.mkdir(mode=0o750, exist_ok=True)
        c.job.chmod(0o750)
        frozen = c.job / "pin/application.db"
        record = c.job / "source.json"
        if record.exists() and parse_time(read(record)["proof"]["fixed_trust_anchor"]["expires_at_utc"]) <= now()+timedelta(hours=48):
            discard(c, c.job)
            c.job.mkdir(mode=0o750)
        dest, exportlog = c.job / "chunks", c.job / "export.jsonl"
        result, snapshot = opened(exportlog) if record.is_file() else (None, None)
        export_complete = bool(snapshot and (dest / "snapshot.pb").is_file())
        if not export_complete:
            discard(c, dest)
            check_build_space(c, latest)
        if not record.exists():
            discard(c, c.job / "pin")
            capture = freeze(c.db, frozen)
            require(replica(c)["pid"] == current["pid"], "Replica restarted during capture")
            inspected = subprocess.check_output([str(c.exporter), "--source", str(frozen), "--inspect",
                                                 "--cache-mib", "256", "--open-files", "1024"], text=True, timeout=300)
            info = json.loads(inspected)
            h, app = info["height"], info["app_hash"]
            require(info["event"] == "source_opened" and info["read_only"] and info["format"] == 3,
                    "Unexpected frozen application state")
            require(current["height"]-2 <= h <= replica(c)["height"]+1 and
                    (not latest or h > latest["snapshot_height"]), "Replica checkpoint failed the fresh-height check")
            proof = agree(h, app, wait=True)
            put(record, {"height": h, "app_hash": app, "capture": capture, "proof": proof})
        source = read(record)
        h = source["height"]
        if not export_complete:
            log("exporting", height=h, app_hash=source["app_hash"], tables=source["capture"]["table_count"])
            with exportlog.open("w") as out:
                export_monitored(c, frozen, dest, out)
                out.flush()
                os.fsync(out.fileno())
            result, snapshot = opened(exportlog)
        require(result and snapshot and result["height"] == h == snapshot["height"]
                and result["app_hash"] == source["app_hash"], "Export changed source height/AppHash")
        pb, _, _ = archive.sdk_metadata(snapshot)
        require((dest / "snapshot.pb").read_bytes() == pb, "Stored SDK protobuf differs from export")
        for directory in (dest, dest / str(h), dest / str(h) / "3"):
            directory.chmod(0o750)
        for chunk in (dest / str(h) / "3").iterdir():
            chunk.chmod(0o640)
        (dest / "snapshot.pb").chmod(0o640)
        ready = {"proof": source["proof"], "snapshot": snapshot}
        if "table_bytes" in source["capture"]:
            ready["source_table_bytes"] = source["capture"]["table_bytes"]
        put(c.ready, ready)
        discard(c, c.job / "pin")
        log("ready_to_publish", height=h, chunks=snapshot["chunks"])


def file_version(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


def verify_retained_files(public, manifest, owner):
    name = manifest["file"]
    checksum = (manifest["sha256"] + "  " + name + "\n").encode()
    archive_path = public / name
    versions = {}
    # Do not follow a replacement symlink or block on a FIFO.
    for path, size in ((public / (name + ".sha256"), len(checksum)), (archive_path, manifest["bytes"])):
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == owner and info.st_size == size,
                f"Unexpected type, owner or size for {path.name}")
        versions[path] = file_version(info)
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            require(file_version(os.fstat(fd)) == versions[path], f"{path.name} changed before verification")
            if path == archive_path:
                require(hashlib.file_digest(stream, "sha256").hexdigest() == manifest["sha256"],
                        f"Archive SHA-256 mismatch for {name}")
            else:
                require(stream.read(len(checksum) + 1) == checksum, f"Checksum file mismatch for {name}")
    return versions


def retire(public, height):
    # Only a previous versioned manifest written by this automated publisher
    # authorizes removing its own archive. Always keep the newest two editions.
    candidates = []
    owner = os.getuid()
    for path in public.glob(f"{CHAIN}-native-*-format3.json"):
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != owner:
            continue
        try:
            manifest = read(path)
            if not isinstance(manifest, dict) or manifest.get("publisher_schema") != "beehive-native-v1":
                continue
            h = manifest["snapshot_height"]
            name = filename(h)
            if (h > height or manifest.get("chain_id") != CHAIN or manifest.get("snapshot_format") != 3
                    or manifest.get("file") != name
                    or path.name != name.removesuffix(".tar.gz") + ".json"):
                continue
            if (type(manifest.get("bytes")) is not int or manifest["bytes"] <= 0
                    or not re.fullmatch("[0-9a-f]{64}", manifest.get("sha256", ""))):
                continue
            expires = parse_time(manifest["fixed_trust_anchor"]["expires_at_utc"])
            published = parse_time(manifest["published_at_utc"])
            deadline = max(published + timedelta(days=7), expires + timedelta(days=1))
        except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
            continue
        candidates.append((h, path, manifest, deadline, file_version(info)))

    candidates.sort(key=lambda row: row[0], reverse=True)
    cutoff = now()
    expired = [row for row in candidates[2:] if row[3] <= cutoff]
    if not expired:
        return
    if candidates[0][0] != height:
        log("retention_deferred", height=height, reason="current manifest unavailable")
        return
    versions = {}
    try:
        for _, path, manifest, _, version in candidates[:2]:
            versions[path] = version
            versions.update(verify_retained_files(public, manifest, owner))
        # Reading large archives takes time. Recheck both complete editions
        # together, while holding the publisher lock, before the first unlink.
        for path, version in versions.items():
            require(file_version(path.lstat()) == version, f"{path.name} changed during verification")
    except (OSError, ValueError) as exc:
        log("retention_deferred", height=height, reason=str(exc))
        return
    for _, path, manifest, _, _ in expired:
        name = manifest["file"]
        files = [public / name, public / (name + ".sha256"), path]
        # A partial earlier cleanup may have left just the manifest. A foreign
        # file or a link at any of these names keeps the whole edition intact.
        for item in files:
            try:
                info = item.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_uid != owner:
                break
        else:
            for item in files:
                item.unlink(missing_ok=True)


def publish(c):
    if not c.ready.exists():
        return
    with locked(c, False):
        if not c.ready.exists():
            return
        r = read(c.ready)
        snap, app = r["snapshot"], r["proof"]["snapshot_app_hash"]
        h = snap["height"]
        name = filename(h)
        old = c.latest_manifest()
        if old and old["snapshot_height"] >= h:
            return
        proof = agree(h, app)
        stage = c.public / ".beehive-native-stage"
        stage.mkdir(mode=0o700, exist_ok=True)
        require(stage.resolve() == stage and stage.stat().st_uid == os.getuid(), "Unsafe publication staging directory")
        temp = stage / "archive.partial"
        temp.unlink(missing_ok=True)

        def progress(label, **fields):
            c.space(c.public)
            log(label, height=h, **fields)

        try:
            try:
                details = archive.package(snap, c.job / "chunks", temp, progress,
                                          check_space=lambda needed: c.space(c.public, needed))
            except (archive.SnapshotDataError, FileNotFoundError) as exc:
                put(c.rebuild, {"height": h, "detected_at_utc": now().isoformat()}, 0o644)
                raise RuntimeError("Fresh export requested after failed SDK input verification") from exc
            # Recheck after disk-intensive work; a delayed run must never move
            # the public pointer after exhausting its trust window.
            proof = agree(h, app)
            archive_path = c.public / name
            temp.chmod(0o644)
            if archive_path.exists():
                require(sha(archive_path) == details["sha256"], "Collision with immutable published filename")
            else:
                os.link(temp, archive_path)
            manifest = {"publisher_schema": "beehive-native-v1", "chain_id": CHAIN, "operator": c.operator,
                        "version": VERSION, "snapshot_height": h, "snapshot_format": 3,
                        "artifact_type": "Cosmos SDK portable native snapshot archive",
                        "file": name, "url": c.base+"/"+name, "checksum_url": c.base+"/"+name+".sha256",
                        "published_at_utc": now().isoformat(), **proof, **details,
                        "verification": {"sdk_protobuf_checked": True, "all_chunk_hashes_rechecked_while_packaging": True,
                                         "combined_chunk_hash_checked": True, "archive_sha256_read_back": True,
                                         "consensus_witnesses": list(WITNESSES),
                                         "full_restore_of_this_edition": "not tested by the automatic publisher"}}
            if "source_table_bytes" in r:
                manifest["source_table_bytes"] = r["source_table_bytes"]
            put(c.public / (name+".sha256"), details["sha256"]+"  "+name+"\n", 0o644)
            put(c.public / name.replace(".tar.gz", ".json"), manifest, 0o644)
            put(c.latest, manifest, 0o644)
            c.rebuild.unlink(missing_ok=True)
            log("published", height=h, sha256=details["sha256"], bytes=details["bytes"])
            retire(c.public, h)
        finally:
            temp.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["check", "build", "publish", "run"])
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    c = Config(args.config)
    if args.action == "check":
        status = source_check(c)
        latest = c.latest_manifest()
        check_build_space(c, latest)
        log("preflight_passed", due=c.due(latest), **status)
        return
    if args.action in ("run", "build"):
        build(c)
    if args.action in ("run", "publish"):
        publish(c)


if __name__ == "__main__":
    main()
