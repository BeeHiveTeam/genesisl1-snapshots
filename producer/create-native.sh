#!/usr/bin/env bash
set -euo pipefail
umask 027
cat <<'BEEHIVE'
  ____  _____ _____ _   _ _____     _______
 | __ )| ____| ____| | | |_ _\ \   / / ____|
 |  _ \|  _| |  _| | |_| || | \ \ / /|  _|
 | |_) | |___| |___|  _  || |  \ V / | |___
 |____/|_____|_____|_| |_|___|  \_/  |_____|
BEEHIVE
if [[ $# == 0 || $1 == --help || $1 == -h ]]; then
    cat <<'HELP'
BeeHive GenesisL1 Native producer
  ./create-native.sh check --config /path/to/producer.json
  ./create-native.sh run   --config /path/to/producer.json

check verifies the running nonsigning replica, binary, disk and file limit.
run creates a checkpoint when due and publishes a verified portable archive.
The build and publish subcommands support installations with separate accounts.
HELP
    exit 0
fi
dir="$(cd -- "$(dirname -- "$0")" && pwd)"
ulimit -Sn "$(ulimit -Hn)"
export GOMAXPROCS="${GOMAXPROCS:-4}" GOMEMLIMIT="${GOMEMLIMIT:-24GiB}"
exec python3 -B "$dir/producer.py" "$@"
