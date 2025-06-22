#!/bin/bash
pip uninstall rainbowbatch -y
pip install -e . --no-build-isolation --config-settings editable_mode=compat

# Resolve script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

SRC_DIR="$SCRIPT_DIR/secrets/pywikibot"
DEST_DIR="$HOME/pywikibot"

mkdir -p "$DEST_DIR"
cp -r "$SRC_DIR/"* "$DEST_DIR/"

echo "Copied Pywikibot config from $SRC_DIR to $DEST_DIR"