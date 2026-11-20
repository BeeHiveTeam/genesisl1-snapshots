import concurrent.futures
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from archive import require

CHAIN = "genesis_29-2"
VERSION = "v1.6.2"
WITNESSES = ("https://26657.genesisl1.org", "https://genesisl1.rpc.m.anode.team")
PARAMS = "https://1317.genesisl1.org/cosmos/staking/v1beta1/params"
TRUST = timedelta(hours=144)


def now():
    return datetime.now(timezone.utc)


def parse_time(text):
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    require(value.tzinfo is not None, "Timestamp lacks timezone")
    return value


def get(url):
    req = Request(url, headers={"User-Agent": "BeeHive-GenesisL1-Native/1.0"})
    with urlopen(req, timeout=15) as response:
        require(not url.startswith("https://") or response.geturl().startswith("https://"), "HTTPS downgrade from RPC")
        data = response.read(2 * 1024**2 + 1)
    require(len(data) <= 2 * 1024**2, "RPC response too large")
    return json.loads(data)


def rpc(endpoint, method):
    return get(endpoint + "/" + method)["result"]


def local_pid(db, port):
    # Match the live application's actual LevelDB lock to the socket serving
    # this RPC. A different replica's URL must not authorize using this DB.
    st = (db / "LOCK").stat()
    key = (os.major(st.st_dev), os.minor(st.st_dev), st.st_ino)
    owners = set()
    for line in Path("/proc/locks").read_text().splitlines():
        row = line.split()
        if len(row) < 8 or row[1:4] != ["FLOCK", "ADVISORY", "WRITE"]:
            continue
        major, minor, inode = row[5].split(":")
        if (int(major, 16), int(minor, 16), int(inode)) == key:
            owners.add(int(row[4]))
    require(len(owners) == 1, "Expected one running GoLevelDB replica owning this database")
    pid = owners.pop()
    require(Path(f"/proc/{pid}").stat().st_uid == os.geteuid(), "Run as the dedicated replica's OS user")
    listens = set()
    for proc in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
        if not proc.exists():
            continue
        for line in proc.read_text().splitlines()[1:]:
            row = line.split()
            addr, number = row[1].split(":")
            if (int(number, 16) == port and row[3] == "0A" and addr in
                    {"0100007F", "00000000", "0"*32, "0000000000000000FFFF00000100007F"}):
                listens.add("socket:[" + row[9] + "]")
    for file in Path(f"/proc/{pid}/fd").iterdir():
        try:
            if os.readlink(file) in listens:
                return pid
        except FileNotFoundError:
            continue
    raise ValueError("The configured RPC listener and application.db belong to different processes")


def replica(config):
    pid = local_pid(config.db, urlsplit(config.rpc).port)
    data = rpc(config.rpc, "status")
    sync = data["sync_info"]
    require(data["node_info"]["network"] == CHAIN and data["node_info"]["id"] == config.node_id,
            "Wrong chain or configured replica ID")
    require(int(data["validator_info"]["voting_power"]) == 0, "Refusing a voting node")
    require(not sync["catching_up"] and -10 < (now()-parse_time(sync["latest_block_time"])).total_seconds() < 120,
            "Replica is catching up or its head is stale")
    require(rpc(config.rpc, "abci_info")["response"]["version"] == VERSION, "Unsupported GenesisL1 application version")
    return {"pid": pid, "height": int(sync["latest_block_height"]), "node_id": config.node_id}


def agree(height, app_hash, wait=False):
    def headers(endpoint):
        rows = []
        for h in (height-1, height, height+1, height+2):
            signed = rpc(endpoint, "commit?height=" + str(h))["signed_header"]
            header, commit = signed["header"], signed["commit"]
            require(header["chain_id"] == CHAIN and int(header["height"]) == h == int(commit["height"]),
                    "Unexpected witness chain or height")
            block, app = commit["block_id"]["hash"], header["app_hash"]
            require(all(re.fullmatch("[0-9A-F]{64}", s) for s in (block, app)), "Invalid consensus digest")
            rows.append((block, app, header["time"]))
        return rows

    deadline = time.monotonic() + (220 if wait else 0)
    while True:
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                first, second = pool.map(headers, WITNESSES)
            break
        except (OSError, KeyError):
            if time.monotonic() >= deadline:
                raise
            time.sleep(8)
    require(first == second and first[2][1] == app_hash, "Independent commits or frozen AppHash disagree")
    duration = get(PARAMS)["params"]["unbonding_time"]
    require(bool(re.fullmatch(r"[0-9]+(?:\.[0-9]+)?s", duration))
            and float(duration[:-1]) > TRUST.total_seconds(), "Unbonding period no longer supports 144h trust")
    expiry = parse_time(first[0][2]) + TRUST
    require(expiry > now() + timedelta(hours=48), "Less than 48 hours of trust remaining at publication")
    return {"snapshot_block_time_utc": first[1][2], "block_hash": first[1][0], "snapshot_app_hash": app_hash,
            "fixed_trust_anchor": {"height": height-1, "hash": first[0][0], "trust_period": "144h",
                                  "current_unbonding_period": duration, "expires_at_utc": expiry.isoformat(),
                                  "preparation_refuses_with_less_than_one_hour_remaining": True}}
