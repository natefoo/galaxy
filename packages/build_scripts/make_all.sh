#!/bin/sh
set -eu

PACKAGES="$(dirname "$0")/.."

cd "$PACKAGES"

for dir in *; do
    [ ! -d "$dir" -o ! -f "${dir}/setup.py" ] && continue
    make -C "$dir" "$@"
done
