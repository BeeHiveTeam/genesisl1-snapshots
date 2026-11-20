"""Run the public client's follow loop with a controlled child and offline RPC replies."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

from test_restore import SCRIPT


FAKE_NODE = r"""#!/usr/bin/env bash
job=$1
result=$2
if [[ $result == replay && ! -f $job/checkpoint-replayed ]]; then
    echo 'state (100) and store (0) height mismatch'
    exit 42
fi
if [[ $result == interrupt || $result == terminate ]]; then
    trap 'touch "$job/terminated"; exit 0' TERM
fi
for i in {1..1000}; do
    if [[ -f $job/polled ]]; then
        case "$result" in
            interrupt|terminate)
                if [[ ! -f $job/signalled ]]; then
                    touch "$job/signalled"
                    if [[ $result == interrupt ]]; then kill -INT "$PPID"; else kill -TERM "$PPID"; fi
                fi ;;
            signal) kill -TERM "$$" ;;
            replay) exit 0 ;;
            *) exit "$result" ;;
        esac
    fi
    /bin/sleep .01
done
exit 97
"""

FOLLOW = r'''
source "$1"
job=$2; result=$3; witness_status=$4
node="$job/node"; port=36656; height=100; kind=native
plan='{"app_hash":"ABCD"}'; expected_identity='{"address":"fixture"}'
daemon=("$job/fake-node" "$job" "$result")
curl() {
    if [[ $result == replay && ! -f $job/checkpoint-replayed ]]; then return 7; fi
    case "${@: -1}" in
        */status) [[ -f $job/status.json ]] && cat -- "$job/status.json" ;;
        */commit?height=101) echo '{"result":{"signed_header":{"header":{"app_hash":"ABCD"}}}}' ;;
        *) return 7 ;;
    esac
}
support() {
    case "$1" in
        ports) [[ $2 == "$port" ]] ;;
        head-verify)
            [[ $2 == 'http://127.0.0.1:36657' && $3 == 123 ]] || return 2
            echo "$3" >> "$job/head-requests"
            return "$witness_status" ;;
        *) return 2 ;;
    esac
}
sleep() {
    if [[ $result != replay || -f $job/checkpoint-replayed ]]; then touch "$job/polled"; fi
    /bin/sleep .02
}
follow
'''


class FollowTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.job = Path(temp.name)
        (self.job / "node").mkdir()
        daemon = self.job / "fake-node"
        daemon.write_text(FAKE_NODE)
        daemon.chmod(0o700)
        self.verified = self.job / "verified-height"

    def set_status(self, *, catching_up=False, voting_power="0"):
        status = {"node_info": {"network": "genesis_29-2"},
                  "validator_info": {"address": "fixture", "voting_power": voting_power},
                  "sync_info": {"latest_block_height": "124", "catching_up": catching_up}}
        (self.job / "status.json").write_text(json.dumps({"result": status}))

    def follow(self, result=0, witness_status=0):
        return subprocess.run(
            ["bash", "-c", FOLLOW, "test", SCRIPT, self.job, str(result), str(witness_status)],
            capture_output=True, text=True, timeout=20)

    def assert_early_exit(self, run):
        self.assertEqual(run.returncode, 1, run.stderr)
        self.assertIn("before mainnet catch-up was verified", run.stderr)
        self.assertNotIn("Caught up", run.stdout)
        self.assertFalse(self.verified.exists())

    def test_clean_exit_without_status(self):
        self.assert_early_exit(self.follow())

    def test_clean_exit_while_catching_up(self):
        self.set_status(catching_up=True)
        self.assert_early_exit(self.follow())
        self.assertEqual((self.job / "snapshot-proof").read_text(), "ABCD\n")
        self.assertFalse((self.job / "head-requests").exists())

    def test_resume_requires_new_verification_when_witness_is_behind(self):
        self.verified.write_text("112\n")
        (self.job / "snapshot-proof").write_text("ABCD\n")
        self.set_status()
        self.assert_early_exit(self.follow(witness_status=75))
        self.assertIn("123", (self.job / "head-requests").read_text().splitlines())

    def test_verified_node_failure_preserves_exit_status(self):
        self.set_status()
        run = self.follow(result=42)
        self.assertEqual(run.returncode, 42, run.stderr)
        self.assertEqual(self.verified.read_text(), "124\n")

    def test_signal_ending_node_preserves_exit_status(self):
        run = self.follow(result="signal")
        self.assertEqual(run.returncode, 143, run.stderr)
        self.assertFalse(self.verified.exists())

    def test_interrupted_client_stops_child(self):
        for result, status in (("interrupt", 130), ("terminate", 143)):
            with self.subTest(signal=result):
                for path in ("polled", "signalled", "terminated"):
                    (self.job / path).unlink(missing_ok=True)
                run = self.follow(result=result)
                self.assertEqual(run.returncode, status, run.stderr)
                self.assertTrue((self.job / "terminated").is_file())
                self.assertFalse(self.verified.exists())

    def test_checkpoint_replay_can_still_reach_verified_height(self):
        node = self.job / "node"
        (node / "data").mkdir()
        (node / "data/priv_validator_state.json").write_text('{"height":"0"}')
        db = node / "bootstrap-backup/state.db"
        db.mkdir(parents=True)
        (db / "CURRENT").write_text("fixture")
        self.set_status()
        run = self.follow(result="replay")
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertTrue((self.job / "checkpoint-replayed").is_file())
        self.assertEqual((node / "data/state.db/CURRENT").read_text(), "fixture")
        self.assertEqual(self.verified.read_text(), "124\n")

    def test_nonzero_voting_power_stops_before_verification(self):
        self.set_status(voting_power="1")
        run = self.follow()
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("Unexpected node signer or voting power", run.stderr)
        self.assertFalse(self.verified.exists())
        self.assertFalse((self.job / "head-requests").exists())
