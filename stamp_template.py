from jinja2 import Template
import pandas as pd
from pprint import pprint
import math
import pandoc
import maya
import re
import wikitextparser
from slugify import slugify
import io

merged_df = pd.read_csv('merged.csv')

citations_df = pd.read_csv('citations.csv', encoding ='latin1')

with open('episode.wiki.template') as episode_template_f:
    template = Template(episode_template_f.read())


def cleans(s):
    if isinstance(s, float) and math.isnan(s):
        return None
    return s.strip()

def cleantitle(s):
    if isinstance(s, float) and math.isnan(s):
        return None
    s = s.strip()
    if s[0] =='#':
        s = s[1:].strip()
    return s

def agressive_splits(s):
    if isinstance(s, float) and math.isnan(s):
        return []

    l1 = re.split(',|;', s)
    l2 = [x.strip() for x in l1]
    return [x for x in l2 if len(x) > 0]

def splits(s):
    if isinstance(s, float) and math.isnan(s):
        return []

    l1 = s.split(";")
    l2 = [x.strip() for x in l1]
    return [x for x in l2 if len(x) > 0]

TIMEZONE = 'US/Eastern'
DATE_FORMAT = "%B %#d, %Y"

def process_ep_record(ep_record):
    ep_record['title'] = cleantitle(ep_record['title'])
    ep_record['prev_title'] = cleantitle(ep_record['prev_title'])
    ep_record['next_title'] = cleantitle(ep_record['next_title'])

    ep_record['clean_title'] = ep_record['title'].split(':')[-1].strip()

    ep_record['ooc_drop'] = cleans(ep_record['ooc_drop'])
    ep_record['beverage'] = cleans(ep_record['beverage'])
    ep_record['people'] = agressive_splits(ep_record['people'])
    ep_record['sources'] = agressive_splits(ep_record['sources'])
    ep_record['themes'] = splits(ep_record['themes'])
    ep_record['notable_bits'] = splits(ep_record['notable_bits'])
    ep_record['mediawiki_description'] = pandoc.write(
        pandoc.read(ep_record['details_html'], format="html-native_divs-native_spans"),
        format="mediawiki"
    )

    # This is a bit hacky, but we need to pick up the correct citations from the external table.
    ep_record['mediawiki_citations'] = []
    relevant_citations = citations_df[citations_df.citations_episode_number == ep_record['episode_number']]
    if len(relevant_citations) > 0:
        print(ep_record['title'], 'has %d citations' % len(relevant_citations))
        for relevant_citation in relevant_citations.to_dict(orient='records'):
            ep_record['mediawiki_citations'].append(relevant_citation['citations_mediawiki'])

    ep_record['coverage_start_date'] =  cleans(ep_record['coverage_start_date'])
    if ep_record['coverage_start_date'] is not None:
        ep_record['coverage_start_date'] = maya.when(ep_record['coverage_start_date'], timezone=TIMEZONE).datetime().strftime(DATE_FORMAT)

    ep_record['coverage_end_date'] =  cleans(ep_record['coverage_end_date'])
    if ep_record['coverage_end_date'] is not None:
        ep_record['coverage_end_date'] = maya.when(ep_record['coverage_end_date'], timezone=TIMEZONE).datetime().strftime(DATE_FORMAT)

    if ep_record['coverage_start_date'] == ep_record['coverage_end_date']:
        ep_record['coverage_date'] = ep_record['coverage_start_date']
    else:
        ep_record['coverage_date'] = None

    # Clean up obsolete fields
    del ep_record['release_date_x']
    del ep_record['release_date_y']
    del ep_record['details_html']
    del ep_record['description']
    del ep_record['embed_player_url']

    return ep_record

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
    record = process_ep_record(raw_record)

    raw = template.render(**record)
    pretty = prettify_mediawiki(raw)

    #print("===== RAW ======\n\n\n\n")
    #print(raw)

    #print("===== PRETTY ======\n\n\n\n")
    #print(pretty)

    #print("===== RECORD ======\n\n\n\n")
    #pprint(record)

    slug = slugify(record['title'], separator='_')

    with io.open('sample_pages/%s.wiki' % slug, mode="w", encoding="utf-8") as f:
        f.write(pretty)

# STILL NEEDED
# Automatic Links
# Episode type categories?
