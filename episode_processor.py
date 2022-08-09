import io
import kfio
import math
import maya
import numpy as np
import pandas as pd
import pandoc
import re
import wikitextparser

from box import Box
from date_lookup import canonicalize_date
from date_lookup import extract_date_from_string
from date_lookup import format_date
from pprint import pprint
from string_processing import agressive_splits
from string_processing import cleans
from string_processing import cleantitle
from string_processing import splits
from wiki_cleaner import format_citation_block
from wiki_cleaner import simple_format


def process_ep_record(ep_record, citations_df, category_remapping_df):
    potentially_missing_columns = set([
        'ooc_drop',
        'beverage',
        'people',
        'sources',
        'themes',
        'notable_bits',
        'episode_type',
        'coverage_start_date',
        'coverage_end_date',
        'release_date_x',
        'release_date_y',
        'description',
    ])

    # Fill in columns.
    for colname in potentially_missing_columns:
        if colname not in ep_record:
            ep_record[colname] = np.nan

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

    if pd.isna(ep_record['details_html']):
        ep_record['mediawiki_description'] = ''
    else:
        ep_record['mediawiki_description'] = simple_format(
            pandoc.write(
                pandoc.read(ep_record['details_html'],
                            format="html-native_divs-native_spans"),
                format="mediawiki"
            )
        )

    if not pd.isna(ep_record['episode_type']):
        episode_type_row = category_remapping_df[category_remapping_df.original_category ==
                                                 ep_record['episode_type']]
        assert len(episode_type_row) == 1, ep_record['episode_type']
        episode_type_row = Box(episode_type_row.to_dict(orient='records')[0])

        # Include people who aren't already included.
        for person in episode_type_row.people:
            if person not in ep_record['people']:
                ep_record['people'].append(person)

        assert len(episode_type_row.new_categories) > 0
        ep_record['categories'] = episode_type_row.new_categories
    else:
        ep_record['categories'] = []

    # This is a bit hacky, but we need to pick up the correct citations from the external table.
    ep_record['mediawiki_citations'] = []
    relevant_citations = citations_df[citations_df.citations_episode_number ==
                                      ep_record['episode_number']].sort_values(by=['citations_date'])
    if len(relevant_citations) > 0:
        for relevant_citation in relevant_citations.to_dict(orient='records'):
            ep_record['mediawiki_citations'].append(
                format_citation_block(
                    relevant_citation['citations_mediawiki'],
                    relevant_citation['citations_url'],
                    relevant_citation['citations_title'],
                )
            )

    title_based_coverage_start_date, title_based_coverage_end_date = extract_date_from_string(
        ep_record['title'])

    if pd.isna(ep_record['coverage_start_date']) and title_based_coverage_start_date is not None:
        ep_record['coverage_start_date'] = format_date(
            title_based_coverage_start_date)

    if pd.isna(ep_record['coverage_end_date']) and title_based_coverage_end_date is not None:
        ep_record['coverage_end_date'] = format_date(
            title_based_coverage_end_date)

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


if __name__ == '__main__':
    merged_df = kfio.load('data/merged.json')

    citations_df = kfio.load_citations_table('data/citations.json')

    category_remapping_df = kfio.load_category_remapping('data/categories_remapping.json')

    RECORDS = merged_df.to_dict(orient='records')
    NEW_RECORDS = []
    for raw_record in RECORDS:
        record = process_ep_record(
            raw_record, citations_df, category_remapping_df)

        NEW_RECORDS.append(record)
        # pprint(record)

    df = pd.DataFrame.from_records(NEW_RECORDS)

    kfio.save(df, 'data/final.json')