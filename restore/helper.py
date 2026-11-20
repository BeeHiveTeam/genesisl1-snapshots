"""GenesisL1 restore catalog, checks and filesystem operations."""
import concurrent.futures
import datetime as dt
import hashlib
from html.parser import HTMLParser
from http.client import HTTPException
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
from urllib.request import HTTPRedirectHandler, Request, build_opener, urlopen

CHAIN = "genesis_29-2"
BASE = "https://l1-snapshots.bee-hive.work:8545/"
RPCS = ("https://26657.genesisl1.org", "https://rpc.genesisl1.firsttensor.com")
LC_HEIGHT = 13985500
LC_CATALOG = "https://lcserve.zip/"
LC_BYTES = 701371435944
LC_SHA = "24aaaf3bfdce38eef451503386457bbe15c5892a9227644510858d034e2aff8b"
LC_MIRROR = "https://ftp.basementnodes.ca/snapshots/gl1/data.tar.lz4"
GIB = 1024**3
HTTP_HEADERS = {"User-Agent": os.environ.get("BEEHIVE_SYNC_AGENT", "BeeHive-GenesisL1-Sync/1")}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def read_url(url, limit=4*1024**2):
    with urlopen(Request(url, headers=HTTP_HEADERS), timeout=12) as r:
        require(not url.startswith("https:") or r.url.startswith("https:"), "HTTPS downgrade")
        body = r.read(limit + 1)
        require(len(body) <= limit, "response exceeds metadata size limit")
        return body


def rpc(server, method):
    return json.loads(read_url(server + "/" + method))["result"]


def head(url):
    with urlopen(Request(url, method="HEAD", headers=HTTP_HEADERS), timeout=10) as r:
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


class LCServeRedirects(HTTPRedirectHandler):
    def __init__(self, directory):
        self.directory = directory

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlsplit(newurl)
        require(urlsplit(req.full_url).scheme == "https", "redirect from an HTTP storage endpoint")
        require(target.hostname and target.username is None and target.password is None and not target.fragment,
                "invalid LCserve redirect")
        if target.scheme == "http":
            require(self.directory, "HTTPS archive metadata downgrade")
            provider_url(req.full_url, "lcserve")
            ip = ipaddress.ip_address(target.hostname)
            require(ip.is_global and not ip.is_multicast and not ip.is_reserved and
                    target.port is not None and 25000 <= target.port <= 64000, "invalid LCserve storage endpoint")
        elif self.directory:
            provider_url(newurl, "lcserve")
        else:
            require(target.scheme == "https", "HTTPS archive metadata required")
        redirect = super().redirect_request(req, fp, code, msg, headers, newurl)
        if req.get_method() == "HEAD":
            redirect.method = "HEAD"
        return redirect


def lcserve_read(url, method="GET", *, directory=False):
    opener = build_opener(LCServeRedirects(directory))
    return opener.open(Request(url, method=method, headers=HTTP_HEADERS), timeout=10)


def lcserve_listing():
    with lcserve_read(LC_CATALOG, directory=True) as response:
        base = response.url
        page = response.read(256 * 1024 + 1)
    require(len(page) <= 256 * 1024, "LCserve listing too large")
    parser = Links()
    parser.feed(page.decode("utf-8", errors="replace"))
    heights = {int(m[1]) for href in parser.links
               if (m := re.fullmatch(r"(?:\./)?data_([1-9]\d{0,11})\.tar\.lz4", href))}
    require(heights, "no snapshots listed by LCserve")
    return base, heights


def lcserve_metadata(url, filename):
    with lcserve_read(url, "HEAD") as response:
        size = int(response.headers.get("Content-Length", "0"))
    require(0 < size <= 8 * 1024**4, "invalid LCserve archive size")
    with lcserve_read(url + ".sha256") as response:
        body = response.read(1025)
    require(len(body) <= 1024, "LCserve checksum file too large")
    fields = body.decode("ascii").split()
    require(len(fields) == 2 and fields[1].removeprefix("*") == filename, "unexpected checksum filename")
    return size, digest(fields[0]).lower()


def lcserve(pinned=None):
    # A saved plan or this previously verified release can authenticate direct HTTP storage.
    height = pinned["height"] if pinned else LC_HEIGHT
    expected = (pinned["bytes"], pinned["sha256"]) if pinned else (LC_BYTES, LC_SHA)
    filename = f"data_{height}.tar.lz4"
    if pinned:
        require(pinned["kind"] == "full" and pinned["file"] == filename and
                type(height) is int and 0 < height < 10**12, "invalid pinned LCserve archive")

    def row(h, name, url, size, sha):
        return dict(id="lcserve", kind="full", file=name, url=url, bytes=size, height=h,
                    sha256=sha, app_hash="", block_hash="", lcserve_catalog=True)

    errors = (OSError, ValueError, KeyError, HTTPException)
    try:
        base, available = lcserve_listing()
    except errors:
        base, available = "", set()
    candidates = [height] if pinned or not available else list(dict.fromkeys((max(available), height)))
    for h in candidates:
        if h not in available:
            continue
        name = f"data_{h}.tar.lz4"
        url = urljoin(base, name)
        try:
            size, sha = lcserve_metadata(url, name)
            if h == height:
                require((size, sha) == expected, "LCserve snapshot differs from its pin")
            elif urlsplit(url).scheme == "http":
                require(lcserve_metadata(LC_MIRROR, "data.tar.lz4") == (size, sha),
                        "no independent checksum for this HTTP archive")
                url = LC_MIRROR
            return row(h, name, url, size, sha)
        except errors:
            continue
    try:
        if lcserve_metadata(LC_MIRROR, "data.tar.lz4") == expected:
            return row(height, filename, LC_MIRROR, *expected)
    except errors:
        pass
    raise ValueError("LCserve archive is offline or does not match the trusted size and SHA-256")


def download_url(plan):
    url = plan["url"]
    scheme = urlsplit(url).scheme
    # Older plans stored only the original HTTP address or the official mirror URL.
    if plan.get("id") == "lcserve" and plan.get("lcserve_catalog", scheme == "http" or url == LC_MIRROR):
        return lcserve(pinned=plan)["url"]
    require(scheme == "https" and urlsplit(url).hostname, "HTTPS archive URL required")
    return url


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


def verify_head(local, height):
    signed = rpc(local, "commit?height="+str(height))["signed_header"]
    header, commit = signed["header"], signed["commit"]
    require(header["chain_id"] == CHAIN and int(header["height"]) == height == int(commit["height"]),
            "wrong local block")
    block, app, _ = commits(height)
    require(digest(commit["block_id"]["hash"]) == block and digest(header["app_hash"]) == app,
            "local block or application hash differs from independent witnesses")
    for server in RPCS:
        remote = rpc(server, "status")
        require(remote["node_info"]["network"] == CHAIN, "wrong witness chain")
        if abs(int(remote["sync_info"]["latest_block_height"])-height) > 5:
            sys.exit(75)
    print("Verified mainnet block and application hash at", height)


def main():
    action, *args = sys.argv[1:]
    if action=="catalog": print(json.dumps(catalogue(*args)))
    elif action=="download-url": print(download_url(json.loads(args[0])))
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
    elif action=="head-verify": verify_head(args[0],int(args[1]))
    else: raise ValueError("unknown helper action")


if __name__ == "__main__":
    try:
        main()
    except (OSError,KeyError) as exc:
        if sys.argv[1]=="head-verify":
            print("Witness data temporarily unavailable:",exc,file=sys.stderr)
            sys.exit(75)
        sys.exit("BeeHive: "+str(exc))
    except (ValueError,TypeError,IndexError) as exc:
        sys.exit("BeeHive: "+str(exc))
