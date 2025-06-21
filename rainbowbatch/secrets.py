from pathlib import Path
import rainbowbatch.kfio as kfio

SECRET_DIR = kfio.TOP_LEVEL_DIR / 'secrets'

def secret_file(fname):
    return SECRET_DIR / fname
