import io
import math
import maya
import pandas as pd
import pandoc
import re
import wikitextparser

from date_lookup import canonicalize_date
from episode_processor import process_ep_record, load_category_remapping
from jinja2 import Template
from pprint import pprint
from slugify import slugify

merged_df = pd.read_csv('merged.csv')

citations_df = pd.read_csv('citations.csv', encoding='latin1')

category_remapping_df = load_category_remapping('categories_remapping.csv')

with open('episode.wiki.template') as episode_template_f:
    template = Template(episode_template_f.read())


def prettify_mediawiki(raw):
    p1 = wikitextparser.parse(raw).pformat()

    # Remove lines that match the following patterns:
    # Topics covered:
    # [https://knowledgefight.com/research/2022/3/3/episode-655-july-18-2003 Citations]
    # [https://www.gofundme.com/f/dreamycreamysummer The Dreamy Creamy Fundraiser]

    # <0x92> -> '? Other strange encodings?

    # Remove blank lines in bulleted lists.

    p2 = re.sub(r'(\n\s*)+\n+', '\n\n', p1)

    return p2


RECORDS = merged_df.to_dict(orient='records')
for raw_record in RECORDS:
    record = process_ep_record(raw_record, citations_df, category_remapping_df)

    raw = template.render(**record)
    pretty = prettify_mediawiki(raw)

    #print("===== RAW ======\n\n\n\n")
    # print(raw)

    #print("===== PRETTY ======\n\n\n\n")
    # print(pretty)

    #print("===== RECORD ======\n\n\n\n")
    # pprint(record)

    slug = slugify(record['title'], separator='_')

    with io.open('sample_pages/%s.wiki' % slug, mode="w", encoding="utf-8") as f:
        f.write(pretty)

# STILL NEEDED
# Automatic Links
# Episode type categories?
