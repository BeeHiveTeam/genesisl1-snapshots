# Восстановление отдельной ноды GenesisL1

Нужен Linux с Bash, Python 3.11+, `curl`, `jq`, `flock`, `sha256sum`, `tar` и `lz4`. Укажите пустой каталог на NVMe. Для архива, восстановленной базы и уплотнения таблиц ориентируйтесь на запас до 1 ТБ. Первый архив BeeHive занимал при скачивании 51,09 ГБ. Автоматическая установка бинарника поддерживает Linux amd64.

Из корня репозитория проверьте сумму скрипта, получите список источников и проверьте план без скачивания архива.

```bash
(cd restore && sha256sum -c beehive-sync.sh.sha256)
bash restore/beehive-sync.sh --list
bash restore/beehive-sync.sh --plan --source beehive-native --target /mnt/data/l1-receiver
```

Когда в плане появятся нужные сеть, высота, архив и каталог, запустите восстановление в `tmux`.

```bash
bash restore/beehive-sync.sh --source beehive-native --target /mnt/data/l1-receiver --yes
```

После обрыва повторите ту же команду с тем же `--target`. Скрипт использует отдельную ноду со своими ключами и портами. Проверьте сверку с независимыми RPC и догон текущей высоты mainnet. При создании нового плана до конца доверия контрольной точки должно оставаться больше часа. Другие источники и интерактивный выбор `auto` описаны в `bash restore/beehive-sync.sh --help`. [Актуальный манифест Native](https://l1-snapshots.bee-hive.work:8545/native/latest.json)
