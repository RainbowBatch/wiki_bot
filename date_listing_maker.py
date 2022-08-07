import io
import math
import maya
import pandas as pd
import pandoc
import re
import wikitextparser

from episode_processor import load_category_remapping
from episode_processor import load_citations_table
from episode_processor import process_ep_record
from jinja2 import Template
from pprint import pprint
from slugify import slugify


merged_df = pd.read_csv('merged.csv')

citations_df = load_citations_table('citations.csv')

category_remapping_df = load_category_remapping('categories_remapping.csv')

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

with open("date_listing.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))
