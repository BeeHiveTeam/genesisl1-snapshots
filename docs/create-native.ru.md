# Как создавать и публиковать Native-снимки GenesisL1

Понадобится отдельная синхронизированная **несигнирующая** реплика mainnet `genesis_29-2` с `genesisd v1.6.2`, классическим IAVL и GoLevelDB. Во время экспорта реплика продолжает работать. Её `snapshot-interval` может оставаться равным нулю. Для MemIAVL, Pebble и удалённого gRPC нужен другой метод экспорта.

## 1. Проверить реплику и диск

Нужны Linux, Python 3.11 или новее, Bash, Git, Go, компилятор C, `patch`, `curl` и `sha256sum`. Мы проверили экспортёр на Linux amd64 с Go 1.23.12. В примере реплика работает от пользователя `genesis-replica`. Подставьте системного пользователя и пути своей несигнирующей реплики. Скрипт запускается от её имени.

```bash
curl -fsS http://127.0.0.1:26657/status | python3 -c 'import json,sys; r=json.load(sys.stdin)["result"]; print(r["node_info"]["network"], r["node_info"]["id"], r["validator_info"]["voting_power"], r["sync_info"]["catching_up"])'
findmnt -T /var/lib/genesisl1-replica/node/data/application.db
df -h /var/lib/genesisl1-replica /srv
```

Ожидаемый вывод содержит `genesis_29-2`, идентификатор пира из 40 символов, силу голоса `0` и `False`. Сохраните идентификатор. Укажите порт локального **CometBFT RPC** этой реплики. Скрипт проверит, что сокет RPC и блокировка базы принадлежат одному процессу.

Пустой рабочий каталог должен находиться **на той же точке монтирования**, что и `application.db`. Скрипт создаёт жёсткие ссылки на неизменяемые таблицы и отдельно копирует манифест и журналы записи между изменениями базы. Готовые архивы можно публиковать с другого диска. При первой сборке скрипт оценивает объём чанков по размеру SST-файлов реплики. При следующих сборках он берёт размер предыдущего экспорта и учитывает рост таблиц, если их прежний размер записан в манифесте. Для старых манифестов исходной оценкой служит размер архива. Скрипт добавляет 25% к оценке каждой копии. Если чанки и архив лежат на одном диске, нужны место для обеих копий и резерв (по умолчанию 150 ГиБ). Во время уплотнения базы закреплённые таблицы могут занять дополнительное место. Перед упаковкой скрипт измеряет готовые чанки и учитывает выравнивание tar, накладные расходы gzip и метаданные. Первый архив занимал 51,09 ГБ.

## 2. Собрать и проверить экспортёр

Сборку выполняет обычный пользователь Linux. Готовый файл устанавливается отдельно.

```bash
sudo git clone https://github.com/BeeHiveTeam/genesisl1-snapshots.git /opt/genesisl1-snapshots
cd /opt/genesisl1-snapshots
sha256sum -c SHA256SUMS
bash producer/build-exporter.sh "$HOME/native-export"
sudo install -d -m 0755 /opt/genesisl1-snapshots/bin
sudo install -m 0755 "$HOME/native-export" /opt/genesisl1-snapshots/bin/native-export
sha256sum /opt/genesisl1-snapshots/bin/native-export
```

При первой сборке нужен доступ к модулям Go. Скрипт проверит контрольную сумму исходного IAVL v1.2.6, применит патч параллельного чтения и проверит небольшой экспорт и восстановление штатным SDK. Запишите SHA-256 **своего** бинарника. Она зависит от версии компилятора и платформы. Лицензия исходного IAVL находится в `producer/exporter/IAVL-LICENSE`.

## 3. Заполнить конфигурацию

```bash
sudo install -d -m 0750 -o genesis-replica -g genesis-replica /var/lib/genesisl1-replica/native-work
sudo install -d -m 0755 -o genesis-replica -g genesis-replica /srv/genesisl1-native
sudo install -m 0644 producer/config.example.json /etc/genesisl1-native-producer.json
sudoedit /etc/genesisl1-native-producer.json
```

Заполните `replica_rpc`, `replica_node_id`, `node_home`, `work_directory`, `output_directory`, `exporter`, `exporter_sha256`, `operator` и `public_base_url`. Последнее поле — публичный HTTPS-каталог **вашего** сервера, в котором будет доступен `latest.json`. Укажите абсолютные пути без символических ссылок. Каталог ноды, рабочий каталог и каталог публикации должны быть отдельными деревьями. При первом запуске рабочий каталог должен быть пустым.

```bash
sudo -u genesis-replica /opt/genesisl1-snapshots/producer/create-native.sh check --config /etc/genesisl1-native-producer.json
```

При успехе появятся `preflight_passed` и свежая высота. Если лимит открытых файлов слишком мал, используйте лимит systemd из следующего шага или поднимите жёсткий лимит пользователя хотя бы до `open_files + 256`. При ошибке реплики, диска или SHA-256 исправьте указанный параметр перед запуском.

## 4. Запустить создание и включить расписание

Проверьте `User`, `Group`, пути и ресурсы в примерах systemd. В образце службе выделено до четырёх ядер и 32 ГиБ памяти. Если диск общий, добавьте лимиты чтения и записи для реального блочного устройства.

```bash
cd /opt/genesisl1-snapshots
sudo install -m 0644 systemd/genesisl1-native-producer.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now genesisl1-native-producer.timer
sudo systemctl start --no-block genesisl1-native-producer.service
systemctl list-timers genesisl1-native-producer.timer
journalctl -u genesisl1-native-producer.service -f
```

Для однократного запуска выполните `sudo -u genesis-replica /opt/genesisl1-snapshots/producer/create-native.sh run --config /etc/genesisl1-native-producer.json`. Блокировка предотвращает одновременный экспорт и публикацию. Ход экспорта записывается в `current-build/export.jsonl` внутри рабочего каталога. Первый экспорт BeeHive занял 21 ч 34 мин. Упаковка, повторное чтение архива и восстановление ноды измеряются отдельно.

Таймер проверяет манифест каждый час и учитывает перезагрузку хоста. Новый экспорт начинается, когда контрольной точке предыдущего снимка исполнилось 48 часов или осталось 96 часов доверия. После прерывания экспорта скрипт может начать экспорт заново с сохранённой актуальной копии. Если готовы все части, повторяется только публикация. Ссылка `latest.json` меняется после проверок. При длительном отключении сервера срок доверия предыдущего снимка может закончиться.

## 5. Открыть скачивание и проверить результат

Настройте свой статический HTTPS-сервер на раздачу `output_directory` по адресу `public_base_url`. Включите скачивание диапазонами и закройте внешнее чтение скрытых путей с точкой, включая временные файлы упаковки. Для раздачи достаточно каталога готовых файлов.

```bash
base=https://snapshots.example.org/native
curl -fsS "$base/latest.json" | python3 -m json.tool
file=$(curl -fsS "$base/latest.json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["file"])')
curl -fIs "$base/$file"
curl -fsS "$base/$file.sha256"
```

У каждого выпуска есть архив `.tar.gz`, SHA-256 и отдельный манифест с высотой в имени. Перед заменой `latest.json` скрипт проверяет метаданные SDK, хеш каждой части, общий хеш снимка и SHA-256 повторно прочитанного архива. Для двух независимых RPC должны совпасть контрольная точка H−1 и AppHash состояния H в заголовке H+1. Период доверия в 144 часа должен быть меньше текущего периода анбондинга. При публикации требуется не меньше 48 часов оставшегося доверия.

Эти результаты попадут в манифест. Для полного испытания конкретного выпуска восстановите его на отдельной ноде и сверьте высоту, хеш блока и AppHash с сетью. Всегда сохраняются минимум два последних автоматических выпуска. Более старые можно удалить автоматически только спустя семь дней после публикации и минимум сутки после истечения доверия. [Инструкция по восстановлению](restore.ru.md)

При публикации заголовки сверяются через `https://26657.genesisl1.org` и `https://genesisl1.rpc.m.anode.team`. Скрипт восстановления независимо проверяет их через `https://26657.genesisl1.org` и `https://rpc.genesisl1.firsttensor.com`. SDK уже сжал чанки, поэтому внешний контейнер gzip использует уровень 0.
