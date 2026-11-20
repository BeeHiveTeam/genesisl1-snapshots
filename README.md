# BeeHive GenesisL1 snapshots

Tools for creating a portable Cosmos SDK Native snapshot of GenesisL1 mainnet and restoring a new node from a published snapshot. The supported chain is `genesis_29-2`, with `genesisd v1.6.2` and SDK snapshot format 3.

| Task | English | Русский |
| --- | --- | --- |
| Create snapshots, verify them and schedule updates | [Producer guide](docs/create-native.en.md) | [Создание снимков](docs/create-native.ru.md) |
| Download a snapshot and restore a separate node | [Restore guide](docs/restore.en.md) | [Восстановление ноды](docs/restore.ru.md) |

BeeHive serves the archives over HTTPS. The Native and Full manifests list the current height, size, SHA-256 and download URL. [Native manifest](https://l1-snapshots.bee-hive.work:8545/native/latest.json) · [Full manifest](https://l1-snapshots.bee-hive.work:8545/latest.json)

The producer takes a consistent view of a running nonsigning GoLevelDB replica, exports the SDK chunks and verifies each chunk and the archive. Two independent consensus RPCs must agree before the publisher switches `latest.json`. The default schedule checks hourly and starts a new build when the source checkpoint is 48 hours old.

Native archives use `.tar.gz` for compatibility with `genesisd snapshots load`. The SDK has already compressed the chunks. The outer gzip uses level 0, so the download size closely follows the total chunk size.

| Stage | CometBFT RPC witnesses |
| --- | --- |
| Create and publish | `https://26657.genesisl1.org` and `https://genesisl1.rpc.m.anode.team` |
| Restore and verify | `https://26657.genesisl1.org` and `https://rpc.genesisl1.firsttensor.com` |

Each stage requires its two witnesses to agree. The archive server supplies the files.

The first BeeHive Native archive was 51,088,788,193 bytes. Exporting that checkpoint took 21 h 34 min. On 20 November 2026, a node on a separate test server restored it after three forced interruptions and reached independent mainnet verification in 1 h 44 min 32 s from the start of the run. These measurements are specific to that archive and those machines.

To check your copy of the scripts, guides and pinned exporter sources, run `sha256sum -c SHA256SUMS` from the repository root. The Go build also checks the upstream IAVL module and tests the export against the stock SDK before it installs the local binary.
