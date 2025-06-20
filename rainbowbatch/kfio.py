import json
import maya
import pandas as pd
import requests
import time

from string_processing import splits
from pathlib import Path

# TODO: Make this more robust.
# TODO: Support for tmp directories, other airflow helpers.
TOP_LEVEL_DIR = Path(
    __file__
).parent.parent.absolute(
)

print(TOP_LEVEL_DIR)

TRANSCRIPT_DIR = TOP_LEVEL_DIR / 'transcripts'
AUDIO_FILES_DIR = TOP_LEVEL_DIR / 'audio_files'
DATA_DIR = TOP_LEVEL_DIR / 'data'


def save(df, fname):
    with open(TOP_LEVEL_DIR / fname, "w", encoding='utf-8') as json_file:
        json_file.write(
            json.dumps(
                json.loads(
                    df.to_json(orient='records')
                ),
                indent=2, sort_keys=True,
            )
        )

def save_json(obj, fname):
    with open(TOP_LEVEL_DIR / fname, "w", encoding='utf-8') as json_file:
        json_file.write(
            json.dumps(
                obj,
                indent=2, sort_keys=True,
            )
        )


def serialize_without_nulls(df):
    return [
        {
            key: value
            for key, value in row.items()
            if isinstance(value, list) or pd.notnull(value)
        }
        for row in json.loads(df.to_json(orient='records'))
    ]


def save_without_nulls(df, fname):
    with open(TOP_LEVEL_DIR / fname, "w", encoding='utf-8') as json_file:
        json_file.write(
            json.dumps(
                serialize_without_nulls(df),
                indent=2, sort_keys=True,
            )
        )


def load(fname):
    return pd.read_json(
        TOP_LEVEL_DIR / fname,
        orient='records',
    )


def download(url):
    print("requesting", url)
    page = None
    while page is None or page.status_code != 200:
        time.sleep(2)
        print('.')
        page = requests.get(url)
    return page


def load_category_remapping(fname):
    category_remapping_df = load(TOP_LEVEL_DIR / fname)

    category_remapping_df.new_categories = category_remapping_df.new_categories.apply(
        splits)
    category_remapping_df.people = category_remapping_df.people.apply(splits)

    return category_remapping_df


def load_citations_table(fname):
    citations_df = load(TOP_LEVEL_DIR / fname)
    citations_df.citations_start_date = citations_df.citations_start_date.apply(
        lambda dt: maya.parse(dt) if not pd.isna(dt) else None)
    return citations_df
