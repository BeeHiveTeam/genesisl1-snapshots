"""Produce the same _snapshot + numbered chunks format as genesisd snapshots dump."""
import base64
import gzip
import hashlib
import io
import os
from pathlib import Path
import stat
import tarfile


def require(condition, message):
    if not condition:
        raise ValueError(message)


class SnapshotDataError(ValueError):
    """Corrupt/incomplete builder input; publication needs a freshly exported checkpoint."""


def check_input(condition, message):
    if not condition:
        raise SnapshotDataError(message)


def varint(value):
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def field(tag, value):
    return varint(tag) + varint(len(value)) + value


def sdk_metadata(snapshot):
    height, fmt, count = (snapshot[k] for k in ("height", "format", "chunks"))
    require(type(height) is int and 0 < height < 2**53 and fmt == 3, "Unexpected snapshot height/format")
    hashes = snapshot["metadata"]["chunk_hashes"]
    require(type(count) is int and 0 < count == len(hashes) <= 100000, "Invalid SDK chunk count")
    hashes = [base64.b64decode(h, validate=True) for h in hashes]
    combined = base64.b64decode(snapshot["hash"], validate=True)
    require(len(combined) == 32 and all(len(h) == 32 for h in hashes), "Invalid SDK SHA-256")
    metadata = b"".join(field(10, digest) for digest in hashes)
    # Snapshot { height=1, format=2, chunks=3, hash=4, metadata=5 }.
    encoded = b"\x08" + varint(height) + b"\x10" + varint(fmt) + b"\x18" + varint(count)
    return encoded + field(34, combined) + field(42, metadata), hashes, combined


def header(name, size):
    info = tarfile.TarInfo(name)
    info.size, info.mode, info.mtime = size, 0o644, 0
    return info


class HashedOutput:
    def __init__(self, stream):
        self.stream, self.sha = stream, hashlib.sha256()

    def write(self, data):
        self.sha.update(data)
        return self.stream.write(data)

    def flush(self):
        self.stream.flush()


class HashedChunk:
    def __init__(self, stream, total):
        self.stream, self.total, self.sha = stream, total, hashlib.sha256()

    def read(self, size):
        data = self.stream.read(size)
        self.total.update(data)
        self.sha.update(data)
        return data


def package(snapshot, output_dir: Path, destination: Path, report):
    meta, hashes, expected = sdk_metadata(snapshot)
    check_input((output_dir / "snapshot.pb").read_bytes() == meta, "SDK protobuf and exporter report disagree")
    chunk_dir = output_dir / str(snapshot["height"]) / "3"
    check_input({str(i) for i in range(len(hashes))} == set(os.listdir(chunk_dir)), "Incomplete SDK chunk directory")
    payload_bytes, combined = 0, hashlib.sha256()
    with destination.open("xb") as raw:
        writer = HashedOutput(raw)
        with gzip.GzipFile(filename="", mode="wb", fileobj=writer, compresslevel=0, mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode="w|", format=tarfile.USTAR_FORMAT,
                              bufsize=1024**2, copybufsize=1024**2) as archive:
                archive.addfile(header("_snapshot", len(meta)), io.BytesIO(meta))
                for index, expected_chunk in enumerate(hashes):
                    path = chunk_dir / str(index)
                    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                    with os.fdopen(descriptor, "rb") as stream:
                        info = os.fstat(descriptor)
                        check_input(stat.S_ISREG(info.st_mode) and info.st_size > 0, "Invalid chunk file")
                        reader = HashedChunk(stream, combined)
                        archive.addfile(header(str(index), info.st_size), reader)
                        check_input(reader.sha.digest() == expected_chunk, f"SDK chunk checksum mismatch at {index}")
                        payload_bytes += info.st_size
                    if (index + 1) % 256 == 0:
                        report("packaging", chunks=index + 1, total=len(hashes))
        check_input(combined.digest() == expected, "Combined SDK snapshot checksum mismatch")
        raw.flush()
        os.fsync(raw.fileno())
    report("verifying_archive", bytes=destination.stat().st_size)
    with destination.open("rb") as stored:
        require(hashlib.file_digest(stored, "sha256").digest() == writer.sha.digest(), "Archive read-back checksum mismatch")
    return {"bytes": destination.stat().st_size, "sha256": writer.sha.hexdigest(),
            "chunk_count": len(hashes), "compressed_chunk_bytes": payload_bytes,
            "native_snapshot_hash": expected.hex().upper()}
