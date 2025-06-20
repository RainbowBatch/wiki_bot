from pathlib import Path

# TODO: Switch to a more robust secret management system.

# TODO: Make this more robust.
# TODO: Support for tmp directories, other airflow helpers.
TOP_LEVEL_DIR = Path(
    __file__
).parent.parent.absolute(
)

SECRET_DIR = TOP_LEVEL_DIR / 'secrets'

def secret_file(fname):
    return SECRET_DIR / fname
