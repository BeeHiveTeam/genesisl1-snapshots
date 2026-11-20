# Create and publish GenesisL1 Native snapshots

This procedure exports mainnet `genesis_29-2` from an already synchronized, dedicated, **nonsigning** `genesisd v1.6.2` replica using classic IAVL and GoLevelDB. It works while the replica is running. Its `snapshot-interval` can stay at zero. MemIAVL, Pebble and remote gRPC donors require another export method.

## 1. Check your replica and disk

Use Linux, Python 3.11 or newer, Bash, Git, Go, a C compiler, `patch`, `curl` and `sha256sum`. We tested the exporter with Go 1.23.12 on Linux amd64. The commands below use a dedicated service account named `genesis-replica`. Substitute your nonsigning replica's actual account and paths. Run the producer under that account.

```bash
curl -fsS http://127.0.0.1:26657/status | python3 -c 'import json,sys; r=json.load(sys.stdin)["result"]; print(r["node_info"]["network"], r["node_info"]["id"], r["validator_info"]["voting_power"], r["sync_info"]["catching_up"])'
findmnt -T /var/lib/genesisl1-replica/node/data/application.db
df -h /var/lib/genesisl1-replica /srv
```

Expect `genesis_29-2`, a 40-character peer ID, voting power `0` and `False`. Record the peer ID. The port in this example is the replica's local **CometBFT RPC**. It must be owned by the same process that holds the database lock.

Put the empty work directory on the **same mount** as `application.db`. It pins immutable SST files with hard links and privately copies the changing manifest and write-ahead logs. The output directory can be on another filesystem. The initial free-space check requires the configured reserve plus 115 GiB in the workspace. Publication requires the reserve plus 65 GiB on the output filesystem. The default reserve is 150 GiB. Allow additional space for changing pinned files and retained archives. Our first compressed archive was 51.09 GB. Archive size, export time and I/O depend on the state and hardware.

## 2. Build and verify the exporter

Clone the source and build as an ordinary Linux user. The commands install the finished executable under the checkout without giving the builder write access to the installed scripts.

```bash
sudo git clone https://github.com/BeeHiveTeam/genesisl1-snapshots.git /opt/genesisl1-snapshots
cd /opt/genesisl1-snapshots
sha256sum -c SHA256SUMS
bash producer/build-exporter.sh "$HOME/native-export"
sudo install -d -m 0755 /opt/genesisl1-snapshots/bin
sudo install -m 0755 "$HOME/native-export" /opt/genesisl1-snapshots/bin/native-export
sha256sum /opt/genesisl1-snapshots/bin/native-export
```

The first build needs access to Go module dependencies. The build checks the pinned IAVL v1.2.6 module, applies the ordered parallel-read patch and tests a small export and restore against the stock SDK. Record the checksum printed for **your own executable**. Toolchain and platform can change it. The IAVL patch is distributed under the upstream Apache license in `producer/exporter/IAVL-LICENSE`.

## 3. Configure the producer

```bash
sudo install -d -m 0750 -o genesis-replica -g genesis-replica /var/lib/genesisl1-replica/native-work
sudo install -d -m 0755 -o genesis-replica -g genesis-replica /srv/genesisl1-native
sudo install -m 0644 producer/config.example.json /etc/genesisl1-native-producer.json
sudoedit /etc/genesisl1-native-producer.json
```

Set `replica_rpc`, `replica_node_id`, `node_home`, `work_directory`, `output_directory`, `exporter`, `exporter_sha256`, `operator` and `public_base_url`. The base URL is the HTTPS directory where **your** web server will serve `latest.json`. Use absolute paths without symlinks. The node home, workspace and publication directory must be separate directory trees. Give the producer its own initially empty workspace.

```bash
sudo -u genesis-replica /opt/genesisl1-snapshots/producer/create-native.sh check --config /etc/genesisl1-native-producer.json
```

Expected output includes `preflight_passed` and a recent height. If the open-file limit is too low, configure the service limit below or raise that account's hard limit to at least `open_files + 256`. If the replica, filesystem or checksum test fails, correct the reported setting before starting.

## 4. Start the first build and schedule new editions

The sample units use the account and installation paths above. Adjust their `User`, `Group`, resource limits and paths to your host. The sample caps CPU at four cores and memory at 32 GiB. For shared storage, add I/O bandwidth limits for your actual block device.

```bash
cd /opt/genesisl1-snapshots
sudo install -m 0644 systemd/genesisl1-native-producer.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now genesisl1-native-producer.timer
sudo systemctl start --no-block genesisl1-native-producer.service
systemctl list-timers genesisl1-native-producer.timer
journalctl -u genesisl1-native-producer.service -f
```

For a single manual run, use `sudo -u genesis-replica /opt/genesisl1-snapshots/producer/create-native.sh run --config /etc/genesisl1-native-producer.json`. A lock serializes builds and publication. During export, progress is written to `current-build/export.jsonl` under your workspace. The first BeeHive export took 21 h 34 min. It is separate from packaging, read-back and node restoration.

The timer checks hourly, including after downtime. A new build is due when the previous checkpoint reaches 48 hours of age or its remaining trust window falls to 96 hours. An interrupted export restarts from its retained, still-usable checkpoint. Completed chunks awaiting publication can be retried independently. The previous manifest stays available until the new archive passes verification. An outage that outlasts the trust window can leave a gap.

## 5. Serve and check the files

Configure your HTTPS static file server to serve the output directory at `public_base_url`. Enable byte-range downloads and restrict access to dot-prefixed paths, including private staging. Public files are world-readable. Expose only the published output directory.

```bash
base=https://snapshots.example.org/native
curl -fsS "$base/latest.json" | python3 -m json.tool
file=$(curl -fsS "$base/latest.json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["file"])')
curl -fIs "$base/$file"
curl -fsS "$base/$file.sha256"
```

Each edition contains an SDK `.tar.gz` archive, a SHA-256 file and a height-specific JSON manifest. `latest.json` switches last. The publisher checks SDK metadata, each chunk hash, the combined snapshot hash and the archive's read-back SHA-256. It checks the AppHash for height H in the H+1 header and the trust hash at H−1 against two independent consensus RPCs. The fixed 144-hour trust period must fit the current staking unbonding period. Publication requires at least 48 hours of trust remaining.

The manifest records these checks. For an end-to-end test of a particular edition, restore it on a separate node and compare its height, block hash and AppHash with the network. The publisher keeps at least the newest two editions. Older automatically created archives become eligible for deletion seven days after publication and at least one day after trust expires. [GenesisL1 restore guide](restore.en.md)
