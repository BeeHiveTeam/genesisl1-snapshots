# Restore a separate GenesisL1 node

Use a Linux host with Bash, Python 3.11+, `curl`, `jq`, `flock`, `sha256sum`, `tar` and `lz4`. Choose an empty target on NVMe with space for both the archive and the restored database. BeeHive's first test used an archive of 51.09 GB. For disk planning allow up to 1 TB of free space for the archive, state and compaction. The automatic binary download supports Linux amd64.

## Choosing directories

You choose which node to inspect and which disk to restore to. For each path option you omit, the script uses the default shown below, under the home directory of the user running the command. The `--source auto` option selects the snapshot source.

| Option | Purpose | Default |
| --- | --- | --- |
| `--inspect-home` | Existing node home. The script reads its configuration and checks its local RPC. You can omit this option on a fresh host. | `~/.genesis` |
| `--target` | Separate workspace for the archive and restore. The new node home is the `node/` subdirectory, with its database in `node/data/`. | `~/.beehive-genesisl1` |

If your existing node uses a custom directory, pass its path explicitly. For example, with a running node in `/srv/genesis/mainnet` and a target of `/mnt/data/l1-receiver`, the restored node home will be **`/mnt/data/l1-receiver/node`**. The parent directory `/mnt/data` must exist and be writable. For the first run, choose a new or empty target. The script rejects the existing node home and its subdirectories as a target.

From the repository root, verify the restoration script, list the sources and check the plan without downloading chain data. Substitute your own paths in both restore commands.

```bash
(cd restore && sha256sum -c beehive-sync.sh.sha256)
bash restore/beehive-sync.sh --list
bash restore/beehive-sync.sh --plan --source beehive-native \
  --inspect-home /srv/genesis/mainnet --target /mnt/data/l1-receiver
```

When the plan shows the expected chain, height, archive and target, run it in `tmux`.

```bash
bash restore/beehive-sync.sh --source beehive-native \
  --inspect-home /srv/genesis/mainnet --target /mnt/data/l1-receiver --yes
```

Repeat the same command with the same target after an interruption. The script operates on a separate node home with its own keys and ports. Check that the receiver matches witnesses and catches up to the changing mainnet head. The Native checkpoint must have more than one hour of trust remaining for a new plan. Other source choices and the interactive `auto` option are described by `bash restore/beehive-sync.sh --help`. [Current Native manifest](https://l1-snapshots.bee-hive.work:8545/native/latest.json)

The restore script checks the checkpoint and final block against the CometBFT RPCs at `https://26657.genesisl1.org` and `https://rpc.genesisl1.firsttensor.com`. The producer uses the same first witness and anode.team as its second witness at publication.

Each time the receiver starts, the script clears the previous `verified-height` in the target directory. Once the receiver reports that it has caught up at height H and both witnesses confirm the block at H−1, the script records H in that file and keeps the node running. If the node exits normally before this check succeeds, the script returns exit code 1. Read `node.log` in the same directory and repeat the command to resume.

The downloadable Bash script includes its Python helper and can run by itself. To change the client, edit `restore/client.sh.in` or `restore/helper.py` and regenerate the script and its checksum with `python3 restore/build_client.py`. CI checks that both generated files match their sources.
