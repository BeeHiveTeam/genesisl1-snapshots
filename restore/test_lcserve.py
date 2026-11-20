"""LCserve discovery and resumable downloads, using in-memory storage endpoints."""
from contextlib import ExitStack
from email.message import Message
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import HTTPHandler, HTTPSHandler
from urllib.response import addinfourl

import helper

HERE = Path(__file__).resolve().parent
STORE = "http://93.184.216.34"


class CatalogFixture:
    def __init__(self):
        self.base = STORE + ":35001/"
        self.archives = {helper.LC_HEIGHT: (helper.LC_BYTES, helper.LC_SHA)}
        self.mirror = None
        self.requests = []
        self.extra_links = ""
        self.redirects = {}

    def install(self, stack):
        def serve(handler, request):
            return self.respond(request)
        for cls, method in ((HTTPHandler, "http_open"), (HTTPSHandler, "https_open")):
            stack.enter_context(patch.object(cls, method, serve))

    def respond(self, request):
        url, method = request.full_url, request.get_method()
        self.requests.append((method, url))
        if request.get_header("User-agent") != helper.HTTP_HEADERS["User-Agent"]:
            raise AssertionError("missing client User-Agent")
        headers = Message()
        body, status = b"", 200
        if url == helper.LC_CATALOG or url in self.redirects:
            headers["Location"] = self.redirects.get(url, self.base)
            status = 302
        elif url == self.base:
            body = ("".join(f'<a href="./data_{h}.tar.lz4">data</a>' for h in self.archives)
                    + self.extra_links).encode()
        else:
            records = {self.base + f"data_{h}.tar.lz4": values for h, values in self.archives.items()}
            if self.mirror is not None:
                records[helper.LC_MIRROR] = self.mirror
            archive = url.removesuffix(".sha256")
            if archive not in records:
                raise OSError("fixture endpoint unavailable")
            size, sha = records[archive]
            if method == "HEAD" and url == archive:
                headers["Content-Length"] = str(size)
            elif method == "GET" and url == archive + ".sha256":
                body = f"{sha}  {archive.rsplit('/', 1)[-1]}\n".encode()
            else:
                raise AssertionError("discovery must only request the listing, HEAD and checksum")
        response = addinfourl(io.BytesIO(body), headers, url, status)
        response.msg = "Found" if status == 302 else "OK"
        return response


class LCServeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = CatalogFixture()
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.fixture.install(stack)

    def test_known_release_follows_current_port_even_when_mirror_has_changed(self):
        self.fixture.mirror = (helper.LC_BYTES, "ff" * 32)
        old = helper.lcserve()
        self.assertEqual(old["url"], self.fixture.base + old["file"])
        self.fixture.base = STORE + ":48002/"
        self.fixture.archives[helper.LC_HEIGHT + 10] = (100, "ef" * 32)
        self.assertEqual(helper.download_url(old), self.fixture.base + old["file"])
        del old["lcserve_catalog"]  # A plan saved by the previous release.
        self.assertEqual(helper.download_url(old), self.fixture.base + old["file"])
        old["url"] = helper.LC_MIRROR
        self.assertEqual(helper.download_url(old), self.fixture.base + old["file"])
        self.assertNotIn(("HEAD", old["url"]), self.fixture.requests)

    def test_new_http_release_requires_matching_independent_https_metadata(self):
        current = helper.LC_HEIGHT + 99
        self.fixture.archives = {current: (125, "12" * 32)}
        for mirror in (None, (124, "12" * 32), (125, "34" * 32)):
            self.fixture.mirror = mirror
            with self.subTest(mirror=mirror), self.assertRaisesRegex(ValueError, "trusted"):
                helper.lcserve()
        self.fixture.mirror = self.fixture.archives[current]
        result = helper.lcserve()
        self.assertEqual((result["url"], result["height"]), (helper.LC_MIRROR, current))

    def test_saved_release_can_use_only_an_exact_match(self):
        old = helper.lcserve()
        self.fixture.base = STORE + ":48002/"
        for modified in ((helper.LC_BYTES, "dc" * 32), (helper.LC_BYTES + 1, helper.LC_SHA)):
            self.fixture.archives[helper.LC_HEIGHT] = modified
            self.fixture.mirror = modified
            with self.subTest(modified=modified), self.assertRaisesRegex(ValueError, "trusted"):
                helper.download_url(old)
        self.fixture.archives.clear()
        self.fixture.mirror = (helper.LC_BYTES, helper.LC_SHA)
        self.assertEqual(helper.download_url(old), helper.LC_MIRROR)
        with self.assertRaisesRegex(ValueError, "invalid pinned"):
            helper.download_url(dict(old, file="new.tar.lz4"))

    def test_only_the_https_operator_can_advertise_the_http_endpoint(self):
        for bad in ("http://127.0.0.1:35001/", STORE + ":25/", STORE + ":65001/",
                    "http://user@93.184.216.34:35001/", "https://unrelated.example/"):
            self.fixture.redirects = {helper.LC_CATALOG: bad}
            with self.subTest(url=bad), self.assertRaises(ValueError):
                helper.lcserve_listing()
            self.assertNotIn(("GET", bad), self.fixture.requests)
        evil = STORE + ":60000/"
        self.fixture.redirects = {self.fixture.base: evil}
        with self.assertRaisesRegex(ValueError, "redirect from an HTTP"):
            helper.lcserve_listing()
        self.assertNotIn(("GET", evil), self.fixture.requests)
        self.fixture.redirects = {helper.LC_MIRROR: evil}
        with self.assertRaisesRegex(ValueError, "downgrade"):
            helper.lcserve_metadata(helper.LC_MIRROR, "data.tar.lz4")
        self.assertNotIn(("GET", evil), self.fixture.requests)

    def test_https_storage_can_publish_a_new_snapshot_and_redirect_a_head_request(self):
        self.fixture.base = "https://storage.lcserve.zip/snapshots/"
        self.fixture.archives = {123: (654, "bc" * 32)}
        archive = self.fixture.base + "data_123.tar.lz4"
        self.fixture.redirects[archive] = helper.LC_MIRROR
        self.fixture.mirror = self.fixture.archives[123]
        result = helper.lcserve()
        self.assertEqual((result["height"], result["url"], result["sha256"]), (123, archive, "bc" * 32))
        self.assertIn(("HEAD", helper.LC_MIRROR), self.fixture.requests)
        self.assertNotIn(("GET", helper.LC_MIRROR), self.fixture.requests)

    def test_ignores_offsite_and_traversing_archive_links(self):
        self.fixture.extra_links = ('<a href="http://example.org/data_999999999.tar.lz4">a</a>'
                                    '<a href="../data_999999999.tar.lz4">b</a>')
        self.assertEqual(helper.lcserve_listing(), (self.fixture.base, {helper.LC_HEIGHT}))

    def test_explicit_https_manifest_and_other_sources_keep_their_url(self):
        url = "https://snapshots.lcserve.zip/files/custom.tar.lz4"
        self.assertEqual(helper.download_url({"id": "lcserve", "url": url}), url)
        with self.assertRaisesRegex(ValueError, "HTTPS"):
            helper.download_url({"id": "beehive-full", "url": self.fixture.base + "file"})
        self.assertEqual(self.fixture.requests, [])


class BundledDownloadTest(unittest.TestCase):
    def test_complete_partial_file_is_verified_offline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            partial = root / "archive.part"
            partial.write_bytes(b"verified")
            plan = dict(id="lcserve", bytes=8, sha256=hashlib.sha256(partial.read_bytes()).hexdigest())
            command = ('source "$1"; job=$2; archive="$job/archive"; plan=$3; '
                       'support() { exit 9; }; curl() { exit 9; }; download')
            result = subprocess.run(["bash", "-c", command, "test", str(HERE / "beehive-sync.sh"),
                                     tmp, json.dumps(plan)], capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual((root / "archive").read_bytes(), b"verified")
            self.assertEqual((root / "phase").read_text(), "downloaded\n")

    def test_rotation_during_download_resumes_bytes_and_rejects_a_replaced_archive(self):
        payload = b"abcdefghij"
        digest = hashlib.sha256(payload).hexdigest()
        pinned = dict(id="lcserve", kind="full", height=321, file="data_321.tar.lz4",
                      bytes=len(payload), sha256=digest, url=STORE + ":30000/data_321.tar.lz4")
        for replaced in (False, True):
            with self.subTest(replaced=replaced), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                # Every fresh helper process sees the provider's current published port.
                (root / "sitecustomize.py").write_text(
                    "from contextlib import ExitStack\nfrom pathlib import Path\n"
                    "from test_lcserve import CatalogFixture, STORE\n"
                    "stack = ExitStack()\nf = CatalogFixture()\n"
                    f"rotated = Path({str(root / 'transfers')!r}).exists()\n"
                    "f.base = STORE + (':48002/' if rotated else ':35001/')\n"
                    f"f.archives = {{321: (10, 'ff' * 32 if rotated and {replaced!r} else {digest!r})}}\n"
                    "f.install(stack)\n")
                (root / "archive.part").write_bytes(payload[:2])
                run = r'''source "$1"
job=$2; archive="$job/archive"; plan=$3
curl() {
    local out='' proto='' resume='' redirects='' url
    while (($#)); do
        case $1 in
            --proto) proto=$2; shift;;
            --max-redirs) redirects=$2; shift;;
            --continue-at) resume=$2; shift;;
            --output) out=$2; shift;;
            --) shift; break;;
        esac
        shift
    done
    url=$1
    [[ $proto == '=http' && $redirects == 0 && $resume == - && -f $out ]] || return 9
    printf '%s %s\n' "$url" "$(stat -c %s "$out")" >> "$job/transfers"
    case $url in
        *:35001/data_321.tar.lz4) printf cde >> "$out"; return 18;;
        *:48002/data_321.tar.lz4) printf fghij >> "$out";;
        *) return 9;;
    esac
}
download'''
                result = subprocess.run(["bash", "-c", run, "test", str(HERE / "beehive-sync.sh"),
                                         str(root), json.dumps(pinned)], capture_output=True, text=True,
                                        env=dict(os.environ, PYTHONPATH=os.pathsep.join((tmp, str(HERE))),
                                                 PYTHONDONTWRITEBYTECODE="1"), timeout=15)
                transfers = (root / "transfers").read_text().splitlines()
                first = STORE + ":35001/data_321.tar.lz4 2"
                if replaced:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(transfers, [first])
                    self.assertEqual((root / "archive.part").read_bytes(), payload[:5])
                    self.assertFalse((root / "phase").exists())
                else:
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(transfers, [first, STORE + ":48002/data_321.tar.lz4 5"])
                    self.assertEqual((root / "archive").read_bytes(), payload)
                    self.assertEqual((root / "phase").read_text(), "downloaded\n")
