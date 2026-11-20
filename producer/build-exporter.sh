#!/usr/bin/env bash
set -euo pipefail
umask 022
here="$(cd -- "$(dirname -- "$0")" && pwd)"
target="${1:-$here/../bin/native-export}"
target="$(realpath -m -- "$target")"
scratch="$(mktemp -d)"
trap 'rm -rf -- "$scratch"' EXIT
export GOWORK=off CGO_ENABLED=1
mkdir "$scratch/module"
cd "$scratch"
go mod download -json github.com/cosmos/iavl@v1.2.6 > upstream.json
upstream="$(python3 -c 'import json,sys; from pathlib import Path; d=json.loads(Path("upstream.json").read_text()); d["Sum"]=="h1:Hs3LndJbkIB+rEvToKJFXZvKo6Vy0Ex1SJ54hhtioIs=" or sys.exit("Unexpected IAVL v1.2.6 module checksum"); print(d["Dir"])')"
cp -- "$here"/exporter/{go.mod,go.sum,main.go,main_test.go} "$scratch/module/"
cp -R -- "$upstream" "$scratch/module/iavl-parallel"
chmod -R u+w "$scratch/module/iavl-parallel"
patch -s --fuzz=0 -d "$scratch/module/iavl-parallel" -p1 < "$here/exporter/iavl-v1.2.6.patch"
cd "$scratch/module"
go test -mod=readonly -p 2 -count=1 .
go build -mod=readonly -p 2 -trimpath -o "$scratch/native-export" .
mkdir -p -- "$(dirname -- "$target")"
install -m 0755 -- "$scratch/native-export" "$target"
sha256sum -- "$target"
