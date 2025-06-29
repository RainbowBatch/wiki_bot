#!/bin/bash
pip uninstall rainbowbatch -y
pip install -e . --no-build-isolation --config-settings editable_mode=compat
python -m spacy download en_core_web_sm