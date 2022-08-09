import io
import math
import maya
import pandas as pd
import pandoc
import re
import wikitextparser
import kfio

from episode_processor import process_ep_record
from jinja2 import Template
from pprint import pprint
from slugify import slugify


merged_df = kfio.load('data/merged.json')

citations_df = kfio.load_citations_table('data/citations.json')

category_remapping_df = kfio.load_category_remapping('data/categories_remapping.json')

RECORDS = merged_df.to_dict(orient='records')
NEW_RECORDS = []
for raw_record in RECORDS:
    record = process_ep_record(raw_record, citations_df, category_remapping_df)
    NEW_RECORDS.append({
        k: record[k]
        for k in ['title', 'episode_number', 'coverage_start_date', 'coverage_end_date', 'coverage_date']
    })
    pprint(NEW_RECORDS[-1])

df = pd.DataFrame.from_records(NEW_RECORDS)

kfio.save(df, 'data/date_listing.json')
