#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

banner() {
    cat <<'BEEHIVE'
  ____  _____ _____ _   _ _____     _______
 | __ )| ____| ____| | | |_ _\ \   / / ____|
 |  _ \|  _| |  _| | |_| || | \ \ / /|  _|
 | |_) | |___| |___|  _  || |  \ V / | |___
 |____/|_____|_____|_| |_|___|  \_/  |_____|
BEEHIVE
}


# TOML parsing and streaming tar validation use the Python standard library.
support() {
    python3 - "$@" <<'PYTHON'
import concurrent.futures
import datetime as dt
import hashlib
from html.parser import HTMLParser
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import time
import tomllib
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen

CHAIN = "genesis_29-2"
BASE = "https://l1-snapshots.bee-hive.work:8545/"
RPCS = ("https://26657.genesisl1.org", "https://rpc.genesisl1.firsttensor.com")
LC_FILE = "data_13985500.tar.lz4"
LC_URL = "http://85.122.195.176:31246/" + LC_FILE
LC_BYTES = 701371435944
LC_SHA = "24aaaf3bfdce38eef451503386457bbe15c5892a9227644510858d034e2aff8b"
LC_MIRROR = "https://ftp.basementnodes.ca/snapshots/gl1/data.tar.lz4"
GIB = 1024**3


def require(ok, message):
    if not ok:
        raise ValueError(message)


def read_url(url, limit=4*1024**2):
    with urlopen(Request(url, headers={"User-Agent": "BeeHive-GenesisL1-Sync/1"}), timeout=12) as r:
        require(not url.startswith("https:") or r.url.startswith("https:"), "HTTPS downgrade")
        body = r.read(limit + 1)
        require(len(body) <= limit, "response exceeds metadata size limit")
        return body


def rpc(server, method):
    return json.loads(read_url(server + "/" + method))["result"]


def head(url):
    with urlopen(Request(url, method="HEAD"), timeout=10) as r:
        require(not url.startswith("https:") or r.url.startswith("https:"), "HTTPS downgrade")
        return int(r.headers["Content-Length"])


def digest(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Fa-f0-9]{64}", value), "invalid 32-byte hash")
    return value.upper()


def provider_url(url, provider):
    u = urlsplit(url)
    require(u.scheme == "https" and u.hostname and not u.username and not u.password and not u.fragment,
            "a public HTTPS manifest is required")
    suffixes = {"firsttensor": ("firsttensor.com",), "lcserve": ("lcserve.net", "lcserve.com", "lcserve.zip")}
    if provider in suffixes:
        require(any(u.hostname == d or u.hostname.endswith("." + d) for d in suffixes[provider]),
                "manifest must be hosted on the named operator's domain")


def artifact(source, manifest):
    provider_url(manifest, source)
    m = json.loads(read_url(manifest, 128*1024))
    require(m["chain_id"] == CHAIN and m["version"].lstrip("v") == "1.6.2", "incompatible chain or binary")
    filename = m["file"]
    require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", filename), "invalid archive filename")
    native = filename.endswith(".tar.gz") and m.get("snapshot_format") == 3
    require(native or (filename.endswith(".tar.lz4") and m.get("memiavl_enabled") is False
                       and m.get("versiondb_enabled") is False), "unsupported archive format or database backend")
    if source in ("beehive-native", "beehive-full"):
        require(native == (source == "beehive-native"), "catalog entry changed format")
    url = urljoin(manifest, filename)
    if source in ("firsttensor", "lcserve") and m.get("url"):
        url = urljoin(manifest, m["url"])
        u = urlsplit(url)
        require(u.scheme == "https" and u.hostname and not u.username and not u.password, "invalid HTTPS archive URL")
    h = m["snapshot_height" if native else "height"]
    size = m["bytes"]
    require(type(h) is int and 0 < h < 2**53 and type(size) is int and 0 < size <= 8*1024**4, "invalid height/size")
    row = dict(id=source, kind="native" if native else "full", file=filename, url=url, bytes=size,
               height=h, sha256=digest(m["sha256"]).lower(), app_hash=digest(m["snapshot_app_hash" if native else "app_hash"]))
    if native:
        a = m["fixed_trust_anchor"]
        hours = re.fullmatch(r"([1-9]\d*)h(?:0m0s)?", a["trust_period"])
        require(hours and int(hours[1]) <= 144 and 0 < a["height"] <= h, "unsupported trust checkpoint")
        remaining = dt.datetime.fromisoformat(a["expires_at_utc"].replace("Z","+00:00")).timestamp() - time.time()
        require(remaining > 3600, "trust checkpoint expired or has less than one hour left")
        require(type(m["chunk_count"]) is int and m["chunk_count"] > 0, "invalid chunk count")
        row.update(trust_height=int(a["height"]), trust_hash=digest(a["hash"]), hours=int(hours[1]), chunks=m["chunk_count"])
    else:
        row["block_hash"] = digest(m["block_hash"])
    require(head(url) == size, "remote size disagrees with manifest")
    return row


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links=[]

    def handle_starttag(self, tag, attrs):
        if tag in ("a","link") and (url:=dict(attrs).get("href")):
            self.links.append(url)


def firsttensor():
    page="https://firsttensor.com/networks/genesisl1"
    parser=Links()
    parser.feed(read_url(page,2*1024**2).decode("utf-8",errors="replace"))
    for link in parser.links:
        url=urljoin(page,link)
        if "snapshot" in url.lower() and urlsplit(url).path.lower().endswith(".json"):
            try:
                return artifact("firsttensor",url)
            except (KeyError,ValueError,OSError):
                continue
    raise ValueError("no downloadable snapshot manifest in FirstTensor's service list; RPC/gRPC available")


def checksum(url, filename):
    cols=read_url(url+".sha256",1024).decode("ascii").strip().split()
    require(len(cols)==2 and cols[1].lstrip("*")==filename,"unexpected checksum filename")
    return digest(cols[0]).lower()


def lcserve():
    # A legacy HTTP listing can name a candidate. Authenticate new files against the project's HTTPS mirror.
    try:
        with urlopen("https://lcserve.zip",timeout=10) as r:
            base=r.url
            text=r.read(256*1024+1)
        require(len(text)<=256*1024,"LCserve listing too large")
        parser=Links(); parser.feed(text.decode("utf-8",errors="replace"))
        heights=[int(m[1]) for href in parser.links if (m:=re.fullmatch(r"(?:\./)?data_(\d{1,12})\.tar\.lz4",href))]
        if heights:
            h=max(heights); name=f"data_{h}.tar.lz4"; url=urljoin(base,name)
            require(url.startswith("https:") or url.startswith(LC_URL.rsplit("/",1)[0]+"/"),"unknown HTTP snapshot origin")
            size=head(url); sha=checksum(url,name)
            require(0<size<=8*1024**4,"invalid LCserve archive size")
            if not url.startswith("https:"):
                if head(LC_MIRROR)==size and checksum(LC_MIRROR,"data.tar.lz4")==sha:
                    url=LC_MIRROR
                else:
                    require((name,size,sha)==(LC_FILE,LC_BYTES,LC_SHA),"no independent checksum for this HTTP archive")
            return dict(id="lcserve",kind="full",file=name,url=url,bytes=size,height=h,sha256=sha,app_hash="",block_hash="")
    except (OSError,KeyError,ValueError):
        pass
    # This earlier LCserve release was restored; only reuse the exact pinned artifact.
    for url in (LC_URL, LC_MIRROR):
        try:
            if head(url) != LC_BYTES:
                continue
            if url == LC_URL and checksum(url,LC_FILE) != LC_SHA:
                continue
            return dict(id="lcserve", kind="full", file=LC_FILE, url=url, bytes=LC_BYTES, height=13985500,
                        sha256=LC_SHA, app_hash="", block_hash="")
        except (OSError, ValueError, KeyError, TypeError):
            pass
    raise ValueError("known LCserve archive is offline or its mirror has moved to a different snapshot")


def catalogue(custom_source, custom_url):
    sources = {"beehive-native": BASE+"native/latest.json", "beehive-full": BASE+"latest.json",
               "lcserve": None, "firsttensor": None}
    if custom_url:
        require(custom_source in ("firsttensor","lcserve"), "--manifest requires --source firsttensor or lcserve")
        sources[custom_source] = custom_url
    def check(item):
        name, url = item
        try:
            if url:
                return artifact(name, url)
            return lcserve() if name=="lcserve" else firsttensor()
        except Exception as e:
            return {"id":name, "error":" ".join(str(e).split())[:250]}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(check, sources.items()))


def inspect(home):
    cfg = home/"config/config.toml"
    if not cfg.is_file():
        print("No CometBFT configuration at", home)
        return
    c = tomllib.loads(cfg.read_text())
    a = tomllib.loads((home/"config/app.toml").read_text()) if (home/"config/app.toml").is_file() else {}
    print("Inspected home:", home)
    print("P2P state sync flag:", c.get("statesync",{}).get("enable",False),
          "| block sync:", c.get("blocksync",{}).get("version","default"),
          "| MemIAVL:", a.get("memiavl",{}).get("enable",False))
    print("Local database:", "present" if any((home/"data"/n).exists() for n in ("application.db","memiavl.db","state.db")) else "not initialized")
    print("These settings describe the configuration; the original bootstrap method may be unknown.")
    if (home/".beehive-created-identity").is_file() and (home.parent/"plan.json").is_file():
        plan=json.loads((home.parent/"plan.json").read_text())
        print("Recorded bootstrap:", plan["id"], plan["kind"], "snapshot height",plan["height"])
    addr = urlsplit(c.get("rpc",{}).get("laddr",""))
    if addr.hostname in ("127.0.0.1","0.0.0.0","localhost","::","::1") and addr.port:
        try:
            host = "[::1]" if ":" in addr.hostname else "127.0.0.1"
            with urlopen(f"http://{host}:{addr.port}/status",timeout=2) as r:
                st = json.loads(r.read(262144))["result"]
            info = st["sync_info"]
            print("Local RPC chain:", st["node_info"]["network"], "| height:", info["latest_block_height"],
                  "| catching up:", info["catching_up"], "| voting power:", st["validator_info"]["voting_power"])
        except (OSError, ValueError, KeyError) as exc:
            print("Local RPC status unavailable:", str(exc))


def check_target(target, observed):
    require(target.is_absolute() and target.resolve() == target, "target must have no symlink components")
    require(target != Path("/") and target != observed and observed not in target.parents and target not in observed.parents,
            "select a target separate from the inspected home")
    for parent in [target, *target.parents]:
        require(not (parent/"config/config.toml").exists() and not (parent/"config/priv_validator_key.json").exists(),
                "target overlaps an existing node home")
    require(target.parent.is_dir(), "create the target's parent directory first")
    if target.exists():
        require(target.is_dir() and target.stat().st_uid == os.geteuid(), "target must be your own directory")
        if any(target.iterdir()):
            marker = target/".beehive-genesisl1-v1"
            require(marker.is_file() and not marker.is_symlink() and marker.read_text() == CHAIN+"\n",
                    "target already contains unrelated files")
            require(not target.stat().st_mode & 0o077, "managed target must be private (mode 0700)")
            for name in ("plan.json","phase","node","node/config","node/data","node.log","account","unpack","fresh-data"):
                require(not (target/name).is_symlink(), "unexpected symlink in managed target: "+name)


def ports(base):
    require(1024 <= base <= 65533, "invalid port base")
    sockets = []
    try:
        for port in range(base,base+3):
            s=socket.socket()
            sockets.append(s)
            s.bind(("127.0.0.1",port))
    finally:
        for s in sockets:
            s.close()


def identity(home):
    cfg=tomllib.loads((home/"config/config.toml").read_text())
    require(cfg.get("priv_validator_laddr","")=="" and cfg.get("priv_validator_key_file")=="config/priv_validator_key.json"
            and cfg.get("priv_validator_state_file")=="data/priv_validator_state.json" and cfg.get("db_dir")=="data",
            "unexpected signing or database configuration")
    key=home/"config/priv_validator_key.json"
    require(key.resolve()==key, "external signer key path")
    address=json.loads(key.read_text())["address"].upper()
    require(re.fullmatch(r"[0-9A-F]{40}",address), "invalid new node identity")
    return dict(address=address,config_hashes={n:hashlib.sha256((home/"config"/n).read_bytes()).hexdigest()
                for n in ("config.toml","app.toml","genesis.json")})


def commits(height):
    out=[]
    for server in RPCS:
        signed = rpc(server, "commit?height="+str(height))["signed_header"]
        h, c = signed["header"], signed["commit"]
        require(h["chain_id"] == CHAIN and int(h["height"]) == height == int(c["height"]), "wrong chain or height from "+server)
        out.append((digest(c["block_id"]["hash"]), digest(h["app_hash"]), h["time"]))
    require(out[0] == out[1], "independent RPCs disagree at height "+str(height))
    return out[0]


def anchor(row):
    h = row["height"]
    block, app = commits(h)[0], commits(h+1)[1]
    for key, v in (("block_hash",block),("app_hash",app)):
        require(not row.get(key) or digest(row[key]) == v, "manifest disagrees with chain: "+key)
        row[key] = v
    if row["kind"] == "native":
        trust_hash, _, stamp = commits(row["trust_height"])
        require(trust_hash == row["trust_hash"], "independent trust checkpoint mismatch")
        expires = dt.datetime.fromisoformat(stamp.replace("Z","+00:00")).timestamp() + row["hours"]*3600
        require(time.time()+3600 < expires, "verified checkpoint expired or has less than one hour left")
        # Cosmos LCD returns protobuf Duration as seconds.
        params=json.loads(read_url("https://1317.genesisl1.org/cosmos/staking/v1beta1/params"))["params"]
        duration = re.fullmatch(r"(\d+(?:\.\d+)?)s", params["unbonding_time"])
        require(duration and row["hours"]*3600 < float(duration[1]), "trust period must be shorter than unbonding time")
        commits(h+2)
    return row


def change_toml(path, changes):
    section, seen, lines = "", set(), []
    for line in path.read_text().splitlines():
        m = re.fullmatch(r"\s*\[([\w.-]+)\]\s*(?:#.*)?",line)
        if m: section=m[1]
        key = re.match(r"^\s*([\w-]+)\s*=",line)
        entry=(section,key[1]) if key else None
        if entry in changes:
            line=entry[1]+" = "+json.dumps(changes[entry])
            seen.add(entry)
        lines.append(line)
    require(seen == set(changes), "unexpected genesisd configuration template: "+str(set(changes)-seen))
    text="\n".join(lines)+"\n"
    tomllib.loads(text)
    tmp=path.with_suffix(".new")
    tmp.write_text(text)
    tmp.replace(path)


def configure(home, plan, base):
    peers = read_url("https://raw.githubusercontent.com/GenesisL1/genesis-parameters/main/genesis_29-2/peers.txt",65536).decode().strip().strip('"').split(",")
    def good_peer(peer):
        return re.fullmatch(r"[0-9a-f]{40}@[A-Za-z0-9.\[\]:-]+:\d{1,5}", peer) is not None
    require(peers and all(map(good_peer,peers)), "invalid official peers list")
    # Add live outgoing peers to the official list. Their chain must match.
    for witness in RPCS:
        try:
            live=[]
            for p in rpc(witness,"net_info")["peers"]:
                n=p["node_info"]
                ip=ipaddress.ip_address(p["remote_ip"])
                port=int(n["listen_addr"].rsplit(":",1)[1])
                if p["is_outbound"] and ip.version == 4 and ip.is_global and n["network"]==CHAIN and 0<port<65536:
                    entry=f'{n["id"]}@{ip}:{port}'
                    if good_peer(entry): live.append(entry)
            peers=live[:6]+peers
        except (KeyError,ValueError,OSError):
            continue
    c={("","proxy_app"):f"tcp://127.0.0.1:{base+2}",("rpc","laddr"):f"tcp://127.0.0.1:{base+1}",
       ("rpc","pprof_laddr"):"",("rpc","grpc_laddr"):"",("p2p","laddr"):f"tcp://127.0.0.1:{base}",
       ("p2p","persistent_peers"):",".join(dict((p.split("@")[0],p) for p in peers).values()),
       ("p2p","max_packet_msg_payload_size"):65536,("statesync","enable"):False,
       ("consensus","timeout_commit"):"14s",("instrumentation","prometheus"):False}
    native=plan["kind"]=="native"
    if native:
        c.update({("statesync","rpc_servers"):",".join(RPCS),("statesync","trust_height"):plan["trust_height"],
                  ("statesync","trust_hash"):plan["trust_hash"],("statesync","trust_period"):str(plan["hours"])+"h"})
    change_toml(home/"config/config.toml",c)
    change_toml(home/"config/app.toml",{("","minimum-gas-prices"):"50000000000el1",("","iavl-disable-fastnode"):True,
        ("api","enable"):False,("grpc","enable"):False,("grpc-web","enable"):False,("json-rpc","enable"):False,
        ("memiavl","enable"):native,("memiavl","snapshot-interval"):1000000,("versiondb","enable"):False,
        ("state-sync","snapshot-interval"):0})


def extract(archive, dest, budget):
    require(dest.is_dir() and not any(dest.iterdir()), "extraction directory must be empty")
    private={"priv_validator_state.json","priv_validator_key.json","node_key.json"}
    written=count=0
    pointer=None
    def relative(name):
        path=PurePosixPath(name)
        require(path.parts and path.parts[0]=="data" and ".." not in path.parts
                and not any(ord(c)<32 for c in name), "invalid archive path")
        return path
    proc=subprocess.Popen(["lz4","-dc",str(archive)],stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=proc.stdout,mode="r|",bufsize=512*1024) as tf:
            for member in tf:
                rel=relative(member.name)
                count+=1
                require(count<=4000000 and member.size>=0, "archive member limit exceeded")
                if any(p in private for p in rel.parts):
                    continue
                out=dest/rel
                if member.isdir():
                    out.mkdir(mode=0o700,parents=True,exist_ok=True)
                elif member.isreg() and not member.issparse():
                    written+=member.size
                    require(written<=budget, "not enough space for the expanded database")
                    with tf.extractfile(member) as src, out.open("xb") as target:
                        shutil.copyfileobj(src,target,1024*1024)
                elif member.islnk():
                    link=relative(member.linkname)
                    target=dest/link
                    require(not set(link.parts)&private and target.is_file() and target.resolve()==target, "unsafe hard link")
                    os.link(target,out,follow_symlinks=False)
                elif member.issym() and rel.as_posix()=="data/memiavl.db/current":
                    require(pointer is None and re.fullmatch(r"snapshot-[0-9]{20}",member.linkname), "unsafe database link")
                    pointer=(out,member.linkname)
                else:
                    raise ValueError("unsupported archive member: "+member.name)
                tf.members.clear()
        while proc.stdout.read(1024*1024): pass
        require(proc.wait()==0, "LZ4 decompression failed")
        if pointer:
            out,link=pointer
            require((out.parent/link).is_dir(), "missing MemIAVL link target")
            out.symlink_to(link,target_is_directory=True)
        for name in ("application.db","blockstore.db","state.db"):
            require((dest/"data"/name).is_dir(),"missing archive database "+name)
    finally:
        proc.stdout.close()
        if proc.poll() is None: proc.terminate()
        proc.wait()


def check_memiavl(home, height, expected):
    from hashlib import sha256
    # This release records Cosmos SDK CommitInfo in multi-tree __metadata.
    root=home/"data/memiavl.db"
    snapshot=(root/"current").resolve()
    require(snapshot.parent==root and re.fullmatch(r"snapshot-\d+",snapshot.name),"unexpected MemIAVL root")
    meta=snapshot/"__metadata"
    require(meta.resolve()==meta and 0<meta.stat().st_size<=65536,"invalid snapshot metadata")
    def varint(data, pos):
        val=0
        for shift in range(0,70,7):
            b=data[pos]; pos+=1
            val|=(b&127)<<shift
            if b<128: return val,pos
        raise ValueError("invalid protobuf integer")
    def fields(data):
        out={}; pos=0
        while pos<len(data):
            tag,pos=varint(data,pos)
            if tag&7==0: value,pos=varint(data,pos)
            elif tag&7==2:
                size,pos=varint(data,pos)
                require(pos+size<=len(data),"short protobuf field")
                value=data[pos:pos+size]; pos+=size
            else: raise ValueError("unsupported CommitInfo encoding")
            out.setdefault(tag>>3,[]).append(value)
        return out
    def tree(leaves):
        if len(leaves)==1: return sha256(b"\x00"+leaves[0]).digest()
        i=1<<((len(leaves)-1).bit_length()-1)
        return sha256(b"\x01"+tree(leaves[:i])+tree(leaves[i:])).digest()
    messages=[v for group in fields(meta.read_bytes()).values() for v in group if isinstance(v,bytes)]
    require(len(messages)==1,"invalid CommitInfo wrapper")
    info=fields(messages[0]); stores={}
    require(info[1]==[height],"wrong restored height")
    for raw in info[2]:
        entry=fields(raw); key=entry[1][0]; commit=fields(entry[2][0]); val=commit[2][0]
        require(key not in stores and 0<len(key)<128 and commit[1]==[height] and len(val)==32,
                "unexpected store commitment")
        stores[key]=bytes([len(key)])+key+b"\x20"+sha256(val).digest()
    require(stores and tree([stores[k] for k in sorted(stores)]).hex().upper()==expected,"restored AppHash mismatch")
    print("Restored AppHash verified across",len(stores),"SDK stores at height",height)


def main():
    action, *args = sys.argv[1:]
    if action=="catalog": print(json.dumps(catalogue(*args)))
    elif action=="inspect": inspect(Path(args[0]))
    elif action=="target": check_target(Path(args[0]),Path(args[1]).resolve())
    elif action=="anchor": print(json.dumps(anchor(json.loads(args[0]))))
    elif action=="ports": ports(int(args[0]))
    elif action=="pick-port":
        for port in range(36656,65534,100):
            try:
                ports(port)
            except OSError:
                continue
            print(port)
            break
        else: raise ValueError("no free loopback port group; select --port-base")
    elif action=="identity": print(json.dumps(identity(Path(args[0])),sort_keys=True))
    elif action=="config": configure(Path(args[0]),json.loads(Path(args[1]).read_text()),int(args[2]))
    elif action=="extract": extract(Path(args[0]),Path(args[1]),int(args[2]))
    elif action=="memiavl": check_memiavl(Path(args[0]),int(args[1]),args[2])
    elif action=="head-verify":
        local,height=args[0],int(args[1])
        signed=rpc(local,"commit?height="+str(height))["signed_header"]
        require(signed["header"]["chain_id"]==CHAIN and int(signed["header"]["height"])==height, "wrong local block")
        block,app,_=commits(height)
        require(digest(signed["commit"]["block_id"]["hash"])==block and digest(signed["header"]["app_hash"])==app,
                "local block or application hash differs from independent witnesses")
        for server in RPCS:
            remote=rpc(server,"status")
            require(remote["node_info"]["network"]==CHAIN,"wrong witness chain")
            if abs(int(remote["sync_info"]["latest_block_height"])-height)>5:
                sys.exit(75)
        print("Verified mainnet block and application hash at",height)
    else: raise ValueError("unknown helper action")

try:
    main()
except (OSError,KeyError) as exc:
    if sys.argv[1]=="head-verify":
        print("Witness data temporarily unavailable:",exc,file=sys.stderr)
        sys.exit(75)
    sys.exit("BeeHive: "+str(exc))
except (ValueError,TypeError,IndexError) as exc:
    sys.exit("BeeHive: "+str(exc))
PYTHON
}

usage() {
    cat <<'USAGE'
BeeHive GenesisL1 snapshot sync
  bash beehive-sync.sh                       inspect, choose a source, restore and follow mainnet
  bash beehive-sync.sh --check               inspect the existing home and its local RPC
  bash beehive-sync.sh --list                check snapshot providers and display archive sizes
  bash beehive-sync.sh --plan                inspect, verify the selected checkpoint and show the plan
  bash beehive-sync.sh --source beehive-native --target /mnt/data/l1-sync --yes

Options
  --inspect-home DIR   existing home to inspect (default ~/.genesis)
  --target DIR         new private working directory (default ~/.beehive-genesisl1)
  --source NAME        auto, beehive-native, beehive-full, lcserve, or firsttensor
  --manifest URL       LCserve or FirstTensor HTTPS manifest in BeeHive format
                       (see https://l1-snapshots.bee-hive.work:8545/native/latest.json)
  --port-base PORT     three loopback ports (automatic selection starts at 36656)
  --bin PATH           use this genesisd v1.6.2, otherwise use PATH or the official release
  --yes                accept the displayed plan without a terminal prompt

Run in tmux for long transfers. Repeat the same command to resume. Use Ctrl+C to stop
this foreground node. It has its own home, keys and ports. Existing services keep running.
Requires Linux, Bash, Python 3.11+, curl, jq, flock, sha256sum, tar and lz4 for full snapshots.
The automatic binary download supports Linux x86-64. No system installation is performed.
USAGE
}

die() { printf 'BeeHive: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null || die "Required command is missing: $1"; }
field() { jq -er ".$1" <<<"$plan"; }
mark() { printf '%s\n' "$1" >"$job/phase.next"; mv -T -- "$job/phase.next" "$job/phase"; phase=$1; }
space() { df -B1 --output=avail -- "$job" | tail -1 | tr -d ' '; }
check_sha() {
    local sum
    sum=$(sha256sum -- "$1")
    [[ ${sum%% *} == "$2" ]]
}
https_get() {
    curl --fail --show-error --silent --location --proto '=https' --proto-redir '=https' \
        --connect-timeout 12 --max-time 180 --max-filesize "$3" --output "$2" "$1"
}
show_sources() {
    jq -r '.[] | if .error then "  \(.id)   unavailable — \(.error)" else
        "  \(.id)   \(.kind)  \((.bytes/1000000000*100|round)/100) GB  height \(.height)" end' <<<"$catalog"
}

choose() {
    catalog=$(support catalog "$source" "$manifest") || die "Cannot read source catalogue"
    show_sources >&2
    if [[ $action == run && $source == auto && $yes == 0 && -t 0 ]]; then
        read -r -p 'Source name [auto selects the smallest available archive]: ' answer || exit 1
        source=${answer:-auto}
    fi
    if [[ $source == auto ]]; then
        plan=$(jq -ce '[.[]|select(.error==null)]|min_by(.bytes)|select(.!=null)' <<<"$catalog") || die "No usable snapshot"
    else
        plan=$(jq -ce --arg id "$source" '.[]|select(.id==$id and .error==null)' <<<"$catalog") || die "Requested snapshot is unavailable: $source"
    fi
    plan=$(support anchor "$plan") || die "Snapshot could not be verified against the independent RPCs"
}

get_binary() {
    if [[ -n $bin_arg ]]; then
        binary=$(realpath -e -- "$bin_arg") || die "genesisd binary not found"
    elif command -v genesisd >/dev/null; then
        binary=$(command -v genesisd)
    else
        binary="$job/genesisd"
        if [[ ! -e $binary ]]; then
            [[ $(uname -sm) == "Linux x86_64" ]] || die "Supply your platform's genesisd with --bin"
            printf '%s\n' 'Downloading the official GenesisL1 v1.6.2 binary.'
            https_get https://github.com/GenesisL1/genesis-crypto/releases/download/v1.6.2/genesis_1.6.2_Linux_x86_64.tar.gz \
                "$job/release.part" 150000000
            check_sha "$job/release.part" 1ee5232c372851aab81d95ee8f4cb08606c3efbb4aadcee227482a3cb46b75af || die "Release SHA-256 mismatch"
            tar xOzf "$job/release.part" ./bin/genesisd >"$job/genesisd.part"
            check_sha "$job/genesisd.part" ff126ed1cd19e3d604f9a822302b920ee20c6b68868796d2aab3c4f2d8d0f1f0 || die "Binary SHA-256 mismatch"
            chmod 700 -- "$job/genesisd.part"
            mv -T -- "$job/genesisd.part" "$binary"
        fi
        check_sha "$binary" ff126ed1cd19e3d604f9a822302b920ee20c6b68868796d2aab3c4f2d8d0f1f0 || die "Cached binary SHA-256 mismatch"
    fi
    local version
    # This release also opens ~/.genesis while constructing its CLI, even for "version".
    # Give just the child process its own real home as well as an explicit --home.
    mkdir -p -- "$job/account"
    daemon=(env "HOME=$job/account" "$binary")
    version=$("${daemon[@]}" version) || die "Cannot run genesisd"
    [[ ${version#v} == 1.6.2 ]] || die "This release supports genesisd v1.6.2; found $version"
}

prepare_home() {
    local staging
    if [[ ! -d $node ]]; then
        staging=$(mktemp -d "$job/init.XXXXXXXX")
        "${daemon[@]}" init BeeHive-Snapshot-Node --chain-id genesis_29-2 --home "$staging" >"$job/init.log" 2>&1
        https_get https://raw.githubusercontent.com/GenesisL1/genesis-parameters/main/genesis_29-2/genesis.json \
            "$staging/config/genesis.part" 33554432
        check_sha "$staging/config/genesis.part" b8f6939f3bfaeec666c7efb94a683e9295e40705ef08af14d55014e4fa11757f || die "Wrong genesis file"
        mv -T -- "$staging/config/genesis.part" "$staging/config/genesis.json"
        support config "$staging" "$job/plan.json" "$port"
        support identity "$staging" >"$staging/.beehive-created-identity"
        if [[ $kind == native ]]; then
            # Validate the checkpoint and bootstrap while it is still inside its trust period.
            support anchor "$plan" >/dev/null
            if ! "${daemon[@]}" comet bootstrap-state --height "$height" --home "$staging" >"$job/bootstrap.log" 2>&1; then
                die "Light-client bootstrap failed. See $job/bootstrap.log"
            fi
            printf 'CometBFT light-client bootstrap verified at height %s.\n' "$height"
            mkdir -- "$staging/bootstrap-backup"
            local db
            for db in state.db blockstore.db evidence.db; do
                [[ ! -d "$staging/data/$db" ]] || cp -a -- "$staging/data/$db" "$staging/bootstrap-backup/"
            done
            [[ -d "$staging/bootstrap-backup/state.db" ]] || die "CometBFT bootstrap is incomplete"
        fi
        mv -T -- "$staging" "$node"
    fi
    mark ready
}

download() {
    local bytes url hash part protocol current
    bytes=$(field bytes); url=$(field url); hash=$(field sha256)
    part="$archive.part"
    protocol='=https'
    [[ $url != http://85.122.195.176:31246/data_13985500.tar.lz4 ]] || protocol='=http,https'
    if [[ ! -f $archive ]]; then
        current=0; [[ ! -f $part ]] || current=$(stat -c %s -- "$part")
        (( current <= bytes )) || die "The partial download is longer than the pinned archive"
        if (( current < bytes )); then
            printf 'Downloading %s, starting at byte %s of %s.\n' "$(field file)" "$current" "$bytes"
            curl --fail --location --show-error --progress-bar --proto "$protocol" --proto-redir '=https' \
                --connect-timeout 15 --speed-limit 1024 --speed-time 180 --retry 3 \
                --max-filesize "$bytes" --continue-at - --output "$part" "$url"
        fi
        [[ $(stat -c %s -- "$part") == "$bytes" ]] || die "Incomplete download. Run the same command to resume."
        if ! check_sha "$part" "$hash"; then
            mv -T -- "$part" "$part.rejected.$(date +%s)"
            die "Archive SHA-256 mismatch. Rejected data was set aside; run again after checking the source."
        fi
        mv -T -- "$part" "$archive"
    else
        [[ $(stat -c %s -- "$archive") == "$bytes" ]] && check_sha "$archive" "$hash" || die "Cached archive SHA-256 mismatch"
    fi
    printf 'Verified SHA-256 %s\n' "$hash"
    mark downloaded
}

# Move aside only interrupted, script-created paths before the node's first start.
# Preserving these files lets the operator inspect them and recover the disk space.
set_aside() {
    local path=$1 backup
    [[ ! -e $path ]] && return 0
    backup=$(mktemp -d "$job/interrupted.XXXXXXXX")
    mv -T -- "$path" "$backup/$(basename -- "$path")"
    printf 'Preserved incomplete work in %s\n' "$backup" >&2
}

install_full() {
    local staging="$job/unpack" spare budget
    if [[ ! -e "$job/full-extracted" ]]; then
        set_aside "$staging"
        mkdir -- "$staging"
        spare=$(space); budget=$((spare-10*1024*1024*1024))
        (( budget > 0 )) || die "Insufficient free space for extraction"
        support extract "$archive" "$staging" "$budget"
        touch "$job/full-extracted"
    fi
    if [[ ! -d "$job/fresh-data" ]]; then
        [[ $(find "$node/data" -mindepth 1 -maxdepth 1 -printf '%f\n') == priv_validator_state.json ]] || die "Unexpected existing node database"
        jq -e '(.height|tonumber)==0' "$node/data/priv_validator_state.json" >/dev/null || die "The new node has signing history"
        mv -T -- "$node/data" "$job/fresh-data"
    fi
    if [[ ! -d "$node/data" ]]; then
        cp -- "$job/fresh-data/priv_validator_state.json" "$staging/data/priv_validator_state.json"
        mv -T -- "$staging/data" "$node/data"
    fi
    mark restored
}

native_restore() {
    local chunks
    chunks=$(field chunks)
    if [[ $phase == downloaded ]]; then mark loading; fi
    if [[ $phase == loading ]]; then
        if ! "${daemon[@]}" snapshots list --home "$node" 2>&1 | grep -F "height: $height format: 3 chunks: $chunks" >/dev/null; then
            set_aside "$node/data/snapshots"
            "${daemon[@]}" snapshots load "$archive" --home "$node"
            "${daemon[@]}" snapshots list --home "$node" 2>&1 | grep -F "height: $height format: 3 chunks: $chunks" >/dev/null || die "SDK chunk count or format differs"
        fi
        mark loaded
    fi
    if [[ $phase == restoring ]]; then
        set_aside "$node/data/application.db"
        set_aside "$node/data/memiavl.db"
    fi
    mark restoring
    "${daemon[@]}" snapshots restore "$height" 3 --home "$node"
    support memiavl "$node" "$height" "$(field app_hash)"
    mark restored
}

recover_checkpoint() {
    local db path backup="$job/before-first-block" checkpoint="$node/bootstrap-backup"
    [[ $kind == native && ! -e "$job/snapshot-proof" && ! -e "$job/checkpoint-replayed" ]] || return 1
    [[ -d "$checkpoint/state.db" && ! -L $checkpoint && ! -L $node/data ]] || return 1
    jq -e '(.height|tonumber)==0' "$node/data/priv_validator_state.json" >/dev/null || return 1
    if [[ ! -e "$job/replay-pending" ]]; then
        tail -c 65536 "$job/node.log" | grep -F "state ($height) and store (0) height mismatch" >/dev/null || return 1
        [[ ! -e $backup ]] || return 1
        touch "$job/replay-pending" || return 1
    fi
    mkdir -p -- "$backup" || return 1
    for db in state.db blockstore.db evidence.db; do
        path="$node/data/$db"
        [[ ! -L $path && ! -L "$checkpoint/$db" ]] || return 1
        if [[ ! -e "$backup/moved-$db" ]]; then
            if [[ -d $path && ! -e "$backup/$db" ]]; then mv -T -- "$path" "$backup/$db" || return 1; fi
            touch "$backup/moved-$db" || return 1
        fi
        if [[ -d "$checkpoint/$db" && ! -e $path ]]; then
            local temp; temp=$(mktemp -d "$job/checkpoint.XXXXXXXX") || return 1
            cp -a -- "$checkpoint/$db" "$temp/$db" && mv -T -- "$temp/$db" "$path" || return 1
        fi
    done
    mv -T -- "$job/replay-pending" "$job/checkpoint-replayed" || return 1
    printf '%s\n' 'Recovered the saved pre-first-block CometBFT checkpoint.'
}

follow() {
    local pid rpc_url stat current caught ancestor after snapshot_seen=0 verified=0 rc=0
    rpc_url="http://127.0.0.1:$((port+1))"
    if [[ -e "$job/replay-pending" ]]; then recover_checkpoint || die "Could not finish checkpoint recovery"; fi
    if [[ -f "$job/snapshot-proof" && $(cat -- "$job/snapshot-proof") == "$(field app_hash)" ]]; then snapshot_seen=1; fi
    support ports "$port"
    "${daemon[@]}" start --home "$node" --log_level warn >>"$job/node.log" 2>&1 &
    pid=$!
    trap 'kill -TERM "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true' EXIT
    trap 'exit 130' INT TERM
    printf 'Following mainnet. Local RPC %s  •  Node log %s/node.log\n' "$rpc_url" "$job"
    while kill -0 "$pid" 2>/dev/null; do
        if stat=$(curl --noproxy '*' -fsS --max-time 3 "$rpc_url/status" 2>/dev/null) &&
            current=$(jq -er '.result | select(.node_info.network=="genesis_29-2")|.sync_info.latest_block_height|tonumber' <<<"$stat"); then
            jq -e --arg addr "$(jq -r .address <<<"$expected_identity")" \
                '.result.validator_info|.address==$addr and (.voting_power|tonumber)==0' <<<"$stat" >/dev/null || die "Unexpected node signer or voting power"
            caught=$(jq -r '.result.sync_info.catching_up' <<<"$stat")
            if (( current >= height+1 && ! snapshot_seen )); then
                if after=$(curl --noproxy '*' -fsS --max-time 6 "$rpc_url/commit?height=$((height+1))" | jq -er '.result.signed_header.header.app_hash'); then
                    [[ ${after^^} == "$(field app_hash)" ]] || die "Local AppHash at H+1 disagrees with the pinned snapshot"
                    snapshot_seen=1
                    printf '%s\n' "$after" >"$job/snapshot-proof"
                fi
            fi
            if [[ $caught == false ]] && (( snapshot_seen && ! verified )); then
                ancestor=$((current-1))
                if support head-verify "$rpc_url" "$ancestor"; then
                    printf 'Caught up to GenesisL1 at height %s. The node will continue running.\n' "$current"
                    verified=1
                    printf '%s\n' "$current" >"$job/verified-height"
                else
                    rc=$?
                    (( rc == 75 )) || die "Witness verification failed; the new node has been stopped"
                fi
            fi
            if (( ! verified )); then printf 'Height %s  •  catching_up=%s\n' "$current" "$caught"; fi
        fi
        sleep 10
    done
    rc=0
    wait "$pid" || rc=$?
    trap - EXIT INT TERM
    if (( rc )); then
        if recover_checkpoint; then follow; return; fi
        tail -n 12 -- "$job/node.log" >&2
    fi
    return "$rc"
}

main() {
    banner >&2
    local home_root=${HOME:-} observed target source=auto manifest='' action=run yes=0 port=36656 port_set=0 bin_arg='' answer
    if [[ -z $home_root ]]; then home_root=$(getent passwd "$(id -u)" | cut -d: -f6); fi
    observed="$home_root/.genesis"; target="$home_root/.beehive-genesisl1"
    while (($#)); do
        case "$1" in
            --help|-h) usage; return;;
            --check) action=check;;
            --list) action=list;;
            --plan) action=plan;;
            --yes) yes=1;;
            --source|--manifest|--inspect-home|--target|--port-base|--bin)
                (($#>=2)) && [[ -n $2 && $2 != --* ]] || die "Value required for $1"
                case "$1" in
                    --source) source=$2;; --manifest) manifest=$2;; --inspect-home) observed=$2;; --target) target=$2;;
                    --port-base) port=$2; port_set=1;; --bin) bin_arg=$2;;
                esac
                shift;;
            *) die "Unknown option: $1 (see --help)";;
        esac
        shift
    done
    case "$source" in auto|beehive-native|beehive-full|lcserve|firsttensor) ;; *) die "Unknown source: $source";; esac
    [[ -z $manifest || $source == lcserve || $source == firsttensor ]] || die "Use --manifest with --source lcserve or firsttensor"
    need python3; python3 -c 'import tomllib' 2>/dev/null || die "Python 3.11+ is required"
    if [[ $action != list ]]; then support inspect "$observed"; fi
    [[ $action != check ]] || return 0
    need jq
    local catalog plan job node archive phase kind height binary room needed size existing filename
    local -a daemon
    if [[ $action == list ]]; then catalog=$(support catalog "$source" "$manifest"); show_sources; return; fi
    [[ $port =~ ^[1-9][0-9]{3,4}$ ]] || die "Invalid port number"
    for cmd in curl sha256sum tar flock realpath; do need "$cmd"; done
    job=$(realpath -ms -- "$target"); observed=$(realpath -m -- "$observed")
    support target "$job" "$observed"
    local previous=0
    if [[ -f "$job/.beehive-genesisl1-v1" && -f "$job/plan.json" ]]; then
        plan=$(jq -ce . "$job/plan.json")
        [[ $source == auto || $(field id) == "$source" ]] || die "This target already has a pinned source; use a new target"
        local saved_port; saved_port=$(field port)
        (( ! port_set || port == saved_port )) || die "This target already has a different port base"
        port=$saved_port; previous=1
    else
        choose
        if (( ! port_set )); then port=$(support pick-port); fi
    fi
    filename=$(field file); height=$(field height); kind=$(field kind); size=$(field bytes)
    [[ $filename =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ && $height =~ ^[1-9][0-9]{0,14}$ && $size =~ ^[1-9][0-9]{0,13}$ ]] || die "Invalid pinned snapshot metadata"
    [[ $kind == full || $kind == native ]] || die "Unknown snapshot format"
    node="$job/node"; archive="$job/$filename"
    [[ ! -L $archive && ! -L $archive.part ]] || die "Unexpected symlink in archive cache"
    [[ $kind != full ]] || need lz4
    printf 'Selected %s, %s, height %s, %s bytes.\nTarget %s   RPC 127.0.0.1:%s\n' "$(field id)" "$kind" "$height" "$size" "$job" "$((port+1))"
    if [[ $action == plan ]]; then
        if (( previous )); then printf '%s\n' 'Showing the existing pinned job.'; fi
        return 0
    fi
    if (( ! yes )); then
        [[ -t 0 ]] || die "An interactive terminal or --yes is required to start"
        read -r -p 'Start or resume this independent node? [y/N] ' answer || exit 1
        [[ $answer == [yY] || $answer == [yY][eE][sS] ]] || return 0
    fi
    support ports "$port"
    if [[ ! -e $job ]]; then mkdir --mode=0700 -- "$job"; fi
    exec 8<"$job"
    flock -n 8 || die "Another run owns this target"
    support target "$job" "$observed"
    if [[ ! -f "$job/.beehive-genesisl1-v1" ]]; then
        chmod 700 -- "$job"
        printf '%s\n' genesis_29-2 >"$job/.beehive-genesisl1-v1"
    fi
    if (( ! previous )); then
        [[ ! -e "$job/plan.json" ]] || die "Another run selected a source; rerun to resume its pinned plan"
        jq --argjson port "$port" '.+{port:$port}' <<<"$plan" >"$job/plan.next"
        mv -T -- "$job/plan.next" "$job/plan.json"
    fi
    phase=new; [[ ! -e "$job/phase" ]] || read -r phase <"$job/phase"
    case "$phase" in new|ready|downloaded|loading|loaded|restoring|restored) ;; *) die "Unknown saved phase: $phase";; esac
    if [[ $phase == new || $phase == ready ]]; then
        room=$(space); existing=0
        if [[ -f $archive ]]; then existing=$(stat -c %s -- "$archive"); elif [[ -f $archive.part ]]; then existing=$(stat -c %s -- "$archive.part"); fi
        if [[ $kind == native ]]; then needed=$((size + 380*1024*1024*1024)); else needed=$((size+size*115/100+10*1024*1024*1024)); fi
        (( room+existing >= needed )) || die "Available space is below the estimate of $needed bytes including the download and restored database"
    fi
    get_binary
    if [[ $phase == new ]]; then prepare_home; fi
    local expected_identity live_identity
    expected_identity=$(cat -- "$node/.beehive-created-identity")
    live_identity=$(support identity "$node")
    [[ $live_identity == "$expected_identity" ]] || die "Configuration or signer differs from the new node created by this run"
    if [[ $phase == ready ]]; then download; fi
    if [[ $phase != restored ]]; then
        if [[ $kind == full ]]; then install_full; else native_restore; fi
    fi
    follow
}

if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
