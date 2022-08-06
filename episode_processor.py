import io
import math
import maya
import pandas as pd
import pandoc
import re
import wikitextparser

from box import Box
from date_lookup import canonicalize_date
from pprint import pprint


def cleans(s):
    if isinstance(s, float) and math.isnan(s):
        return None
    return s.strip()


def cleantitle(s):
    if isinstance(s, float) and math.isnan(s):
        return None
    s = s.strip()
    if s[0] == '#':
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


def process_ep_record(ep_record, citations_df, category_remapping_df):
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
        pandoc.read(ep_record['details_html'],
                    format="html-native_divs-native_spans"),
        format="mediawiki"
    )

    episode_type_row = category_remapping_df[category_remapping_df.original_category == ep_record['episode_type']]
    assert len(episode_type_row) == 1, ep_record['episode_type']
    episode_type_row = Box(episode_type_row.to_dict(orient='records')[0])

    # Include people who aren't already included.
    for person in episode_type_row.people:
        if person not in ep_record['people']:
            ep_record['people'].append(person)

    assert len(episode_type_row.new_categories) > 0
    ep_record['categories'] = episode_type_row.new_categories

    # This is a bit hacky, but we need to pick up the correct citations from the external table.
    ep_record['mediawiki_citations'] = []
    relevant_citations = citations_df[citations_df.citations_episode_number ==
                                      ep_record['episode_number']]
    if len(relevant_citations) > 0:
        # print(ep_record['title'], 'has %d citations' % len(relevant_citations))
        for relevant_citation in relevant_citations.to_dict(orient='records'):
            ep_record['mediawiki_citations'].append(
                relevant_citation['citations_mediawiki'])

    ep_record['coverage_start_date'] = cleans(ep_record['coverage_start_date'])
    if ep_record['coverage_start_date'] is not None:
        ep_record['coverage_start_date'] = canonicalize_date(
            ep_record['coverage_start_date'])

    ep_record['coverage_end_date'] = cleans(ep_record['coverage_end_date'])
    if ep_record['coverage_end_date'] is not None:
        ep_record['coverage_end_date'] = canonicalize_date(
            ep_record['coverage_end_date'])

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

    return Box(ep_record)


def load_category_remapping(fname):
    category_remapping_df = pd.read_csv('categories_remapping.csv')

    category_remapping_df.new_categories = category_remapping_df.new_categories.apply(splits)
    category_remapping_df.people = category_remapping_df.people.apply(splits)

    return category_remapping_df


if __name__ == '__main__':
    merged_df = pd.read_csv('merged.csv')

    citations_df = pd.read_csv('citations.csv', encoding='latin1')

    category_remapping_df = load_category_remapping('categories_remapping.csv')

    print(category_remapping_df)

    RECORDS = merged_df.to_dict(orient='records')
    for raw_record in RECORDS:
        record = process_ep_record(
            raw_record, citations_df, category_remapping_df)

        # pprint(record)
