import io
import math
import maya
import pandas as pd
import pandoc
import re
import kfio

from date_lookup import canonicalize_date
from episode_processor import process_ep_record
from jinja2 import Template
from pprint import pprint
from slugify import slugify
from wiki_cleaner import simple_format

merged_df = kfio.load('data/merged.json')

citations_df = kfio.load_citations_table('data/citations.json')

category_remapping_df = kfio.load_category_remapping('data/categories_remapping.json')

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

# ??? Automatic Links
