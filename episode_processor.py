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


def sortkey(clean_title, episode_number, prev_episode_number, letter_code=None, max_digits=4):
    episode_number = episode_number
    prev_episode_number = prev_episode_number

    if episode_number.startswith('S'):
        assert prev_episode_number is not None
        return sortkey(clean_title, prev_episode_number, None, letter_code='S', max_digits=max_digits)
    if episode_number.endswith(('A', 'B', 'C', 'D', 'E', 'F', 'G')):
        assert letter_code is None
        letter_code = episode_number[-1]
        episode_number = episode_number[:-1]

    if letter_code is None:
        letter_code = ''

    return '#_EPISODE_%s%s:%s' % (str(int(episode_number)).zfill(max_digits), letter_code, clean_title)


def canonicalize_title(title):
    return title.replace(' ', '_').replace('#', '').replace('"', '{{QUOTE}}').replace("/", "{{FORWARD_SLASH}}").replace(":", "{{COLON}}").replace("?", "{{QUESTION_MARK}}")


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
        'release_date',
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

    # TODO: These are probably wrong.
    ep_record['wiki_link'] = "https://knowledge-fight.fandom.com/wiki/%s" % ep_record['title'].replace(' ', '_')
    ep_record['wiki_transcript_link'] = "https://knowledge-fight.fandom.com/wiki/Transcript/%s" % ep_record['title'].replace(' ', '_')

    ep_record['slug'] = canonicalize_title(ep_record['title'])

    ep_record['ofile'] = 'kf_wiki_content/%s.wiki' % ep_record['slug']

    # Transcript columns
    # ep_record['transcript_ofile'] = 'kf_wiki_content/Transcript{{FORWARD_SLASH}}%s.wiki' % ep_record['slug']

    # TODO: Handle "Repost:" special case...
    if ep_record['title'].startswith("Repost:"):
        ep_record['clean_title'] = ep_record['title']
    else:
        ep_record['clean_title'] = ep_record['title'].split(':')[-1].strip()

    ep_record['sortkey'] = sortkey(
        ep_record['clean_title'], ep_record['episode_number'], ep_record['prev_episode_number'])

    ep_record['safe_title'] = ep_record['title'].replace('#', '')
    ep_record['safe_clean_title'] = ep_record['clean_title'].replace('#', '')
    if ep_record['prev_title'] is not None:
        ep_record['safe_prev_title'] = ep_record['prev_title'].replace('#', '')
    if ep_record['next_title'] is not None:
        ep_record['safe_next_title'] = ep_record['next_title'].replace('#', '')

    ep_record['ooc_drop'] = cleans(ep_record['ooc_drop'])
    ep_record['beverage'] = cleans(ep_record['beverage'])
    ep_record['people'] = agressive_splits(ep_record['people'])
    ep_record['sources'] = agressive_splits(ep_record['sources'])
    ep_record['themes'] = splits(ep_record['themes'])
    ep_record['notable_bits'] = splits(ep_record['notable_bits'])

    if pd.isna(ep_record['details_html']):
        ep_record['mediawiki_description'] = ''
        ep_record['plaintext_description'] = ''
    else:
        ep_record['mediawiki_description'] = simple_format(
            pandoc.write(
                pandoc.read(ep_record['details_html'],
                            format="html-native_divs-native_spans"),
                format="mediawiki"
            )
        )
        pre_plaintext_description = pandoc.write(
            pandoc.read(ep_record['mediawiki_description'],
                        format="mediawiki"),
            format="plain"
        ).replace('\n-  ', '\n')
        ep_record['plaintext_description'] = ' '.join([
            line.strip()
            for line in pre_plaintext_description.split('\n')
            if len(line.strip()) > 0
        ])

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
    ep_record['citations_links'] = []
    relevant_citations = citations_df[citations_df.citations_episode_number ==
                                      ep_record['episode_number']].sort_values(by=['citations_start_date'])
    if len(relevant_citations) > 0:
        for relevant_citation in relevant_citations.to_dict(orient='records'):
            ep_record['mediawiki_citations'].append(
                format_citation_block(
                    relevant_citation['citations_mediawiki'],
                    relevant_citation['citations_url'],
                    relevant_citation['citations_title'],
                )
            )

            ep_record['citations_links'].append(
                relevant_citation['citations_url']
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

    # TODO: Nicer formatting here
    if ep_record['coverage_date'] is None:
        ep_record['coverage_dates_string'] = "%s - %s" % (ep_record['coverage_start_date'], ep_record['coverage_end_date'])
    else:
        ep_record['coverage_dates_string'] = ep_record['coverage_date']

    formatted_release_date_0 = canonicalize_date(ep_record['release_date'])
    formatted_release_date_x = canonicalize_date(ep_record['release_date_x'])
    formatted_release_date_y = canonicalize_date(ep_record['release_date_y'])
    if formatted_release_date_0 is not None:
        ep_record['release_date'] = formatted_release_date_0
    elif formatted_release_date_x is not None:
        ep_record['release_date'] = formatted_release_date_x
    elif formatted_release_date_y is not None:
        ep_record['release_date'] = formatted_release_date_y
    else:
        ep_record['release_date'] = None

    if ep_record['release_date'] == None:
        print(ep_record['release_date'],
              ep_record['release_date_x'], ep_record['release_date_y'])
        print(formatted_release_date_0,
              formatted_release_date_x, formatted_release_date_y)

    # Clean up obsolete fields
    # These are reflected in release_date
    del ep_record['release_date_x']
    del ep_record['release_date_y']
    # Redundant with details_html
    del ep_record['description']

    return Box(ep_record)
