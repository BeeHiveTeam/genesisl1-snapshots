# Restore a separate GenesisL1 node

Use a Linux host with Bash, Python 3.11+, `curl`, `jq`, `flock`, `sha256sum`, `tar` and `lz4`. Choose an empty target on NVMe with space for both the archive and the restored database. BeeHive's first test used an archive of 51.09 GB. For disk planning allow up to 1 TB of free space for the archive, state and compaction. The automatic binary download supports Linux amd64.

From the repository root, verify the published restoration script, list the sources and check the plan without downloading chain data.

```bash
(cd restore && sha256sum -c beehive-sync.sh.sha256)
bash restore/beehive-sync.sh --list
bash restore/beehive-sync.sh --plan --source beehive-native --target /mnt/data/l1-receiver
```

When the plan shows the expected chain, height, archive and target, run it in `tmux`.

```bash
bash restore/beehive-sync.sh --source beehive-native --target /mnt/data/l1-receiver --yes
```

Repeat the same command with the same target after an interruption. The script operates on a separate node home with its own keys and ports. Check that the receiver matches witnesses and catches up to the changing mainnet head. The Native checkpoint must have more than one hour of trust remaining for a new plan. Other source choices and the interactive `auto` option are described by `bash restore/beehive-sync.sh --help`. [Current Native manifest](https://l1-snapshots.bee-hive.work:8545/native/latest.json)
