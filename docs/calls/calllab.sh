#!/usr/bin/env bash
# Build and run the calllab prototype in podman.
#   docs/calls/calllab.sh build
#   docs/calls/calllab.sh run -mode listen -allow 57XXXXXXXXXX [-echo=true]
#   docs/calls/calllab.sh run -mode call -allow 57XXXXXXXXXX -target 57XXXXXXXXXX -play /data/test.wav
# Expects the meowcaller integration checkout next to this repo (../meowcaller).
# DATA_DIR holds the linked-device store, the QR PNG and the recordings (mounted at /data).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
MEOW=${MEOW:-$(cd "$ROOT/../meowcaller" && pwd)}
DATA_DIR=${DATA_DIR:-$ROOT/tmp/calllab}
GOIMAGE=${GOIMAGE:-docker.io/library/golang:1.26.10-bookworm}
cmd=${1:-}; shift || true
case "$cmd" in
build)
  podman run --rm \
    -v "$ROOT:/w/gows-plus-calls:Z" -v "$MEOW:/w/meowcaller:z,ro" \
    -v "${GOMODCACHE:-$HOME/go/pkg/mod}:/go/pkg/mod:z" \
    -e GOFLAGS=-mod=mod -e GONOSUMDB=github.com/devlikeapro \
    -w /w/gows-plus-calls/src "$GOIMAGE" \
    go build -buildvcs=false -o ../bin/calllab ./cmd/calllab
  ;;
run)
  mkdir -p "$DATA_DIR"
  exec podman run --rm --name "${NAME:-calllab}" \
    -v "$ROOT/bin:/app:z,ro" -v "$DATA_DIR:/data:Z" \
    "$GOIMAGE" /app/calllab -data /data "$@"
  ;;
*) echo "usage: $0 build | run <calllab flags>" >&2; exit 2 ;;
esac
