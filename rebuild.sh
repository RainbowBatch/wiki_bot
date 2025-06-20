#!/bin/bash
pip uninstall rainbowbatch -y
pip install -e . --no-build-isolation --config-settings editable_mode=compat
