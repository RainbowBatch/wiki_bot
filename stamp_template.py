import io
import math
import maya
import pandas as pd
import pandoc
import re

from date_lookup import canonicalize_date
from episode_processor import load_category_remapping
from episode_processor import load_citations_table
from episode_processor import process_ep_record
from jinja2 import Template
from pprint import pprint
from slugify import slugify
from wiki_cleaner import simple_format

merged_df = pd.read_csv('merged.csv')

citations_df = load_citations_table('citations.csv')

category_remapping_df = load_category_remapping('categories_remapping.csv')

with open('episode.wiki.template') as episode_template_f:
    template = Template(episode_template_f.read())

RECORDS = merged_df.to_dict(orient='records')
for raw_record in RECORDS:
    record = process_ep_record(raw_record, citations_df, category_remapping_df)

    raw = template.render(**record)
    pretty = simple_format(raw)

    slug = slugify(record['title'], separator='_')

    with io.open('sample_pages/%s.wiki' % slug, mode="w", encoding="utf-8") as f:
        f.write(pretty)

# STILL NEEDED
# Automatic Links
# Episode type categories?
