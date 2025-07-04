import io
import math
import maya
import numpy as np
import pandas as pd
import pypandoc
import rainbowbatch.kfio as kfio
import re
import wikitextparser

from box import Box
from rainbowbatch.remap.date_lookup import canonicalize_date
from rainbowbatch.remap.date_lookup import extract_date_from_string
from rainbowbatch.remap.date_lookup import format_date
from rainbowbatch.remap.date_lookup import format_daterange
from rainbowbatch.remap.string_processing import agressive_splits
from rainbowbatch.remap.string_processing import cleans
from rainbowbatch.remap.string_processing import cleantitle
from rainbowbatch.remap.string_processing import splits
from rainbowbatch.remap.wiki_cleaner import format_citation_block
from rainbowbatch.remap.wiki_cleaner import simple_format


def sortkey(clean_title, episode_number, prev_nonspecial_episode_number, count_since_nonspecial_episode_number, letter_code=None, max_digits=4):
    if episode_number.startswith('S'):
        assert letter_code is None
        letter_code = 'S'
        episode_number = prev_nonspecial_episode_number
    if episode_number.endswith(('A', 'B', 'C', 'D', 'E', 'F', 'G')):
        assert letter_code is None
        letter_code = episode_number[-1]
        episode_number = episode_number[:-1]

    if letter_code is None:
        letter_code = ''

    if letter_code == 'S':
        letter_code += str(count_since_nonspecial_episode_number)

    return '#_EPISODE_%s%s:%s' % (str(int(episode_number)).zfill(max_digits), letter_code, clean_title)

# TODO: This belongs in rainbowbatch/remap
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
    ep_record['transcript_title'] = 'Transcript/' + ep_record['title']
    ep_record['prev_title'] = cleantitle(ep_record['prev_title'])
    ep_record['next_title'] = cleantitle(ep_record['next_title'])

    # TODO: These are probably wrong.
    ep_record['wiki_link'] = "https://knowledge-fight.fandom.com/wiki/%s" % ep_record['title'].replace(
        ' ', '_')
    ep_record['wiki_transcript_link'] = "https://knowledge-fight.fandom.com/wiki/Transcript/%s" % ep_record['title'].replace(
        ' ', '_')

    ep_record['slug'] = canonicalize_title(ep_record['title'])
    ep_record['transcript_slug'] = 'Transcript{{FORWARD_SLASH}}' + \
        ep_record['slug']

    ep_record['ofile'] = 'kf_wiki_content/%s.wiki' % ep_record['slug']
    ep_record['transcript_ofile'] = 'kf_wiki_content/%s.wiki' % ep_record['transcript_slug']

    # TODO: Handle "Repost:" special case...
    if ep_record['title'].startswith("Repost:"):
        ep_record['clean_title'] = ep_record['title']
    else:
        ep_record['clean_title'] = ep_record['title'].split(':')[-1].strip()
    ep_record['transcript_clean_title'] = 'Transcript/' + \
        ep_record['clean_title']

    ep_record['sortkey'] = sortkey(
        ep_record['clean_title'],
        ep_record['episode_number'],
        ep_record['prev_nonspecial_episode_number'],
        ep_record['count_since_nonspecial_episode_number'],
    )
    ep_record['transcript_sortkey'] = ep_record['sortkey'].replace(
        "EPISODE", "TRANSCRIPT")

    ep_record['safe_title'] = ep_record['title'].replace('#', '')
    ep_record['safe_clean_title'] = ep_record['clean_title'].replace('#', '')
    ep_record['transcript_safe_title'] = ep_record['transcript_title'].replace(
        '#', '')
    ep_record['transcript_safe_clean_title'] = ep_record['transcript_clean_title'].replace(
        '#', '')

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
            pypandoc.convert_text(
                ep_record['details_html'],
                to='mediawiki',
                format='html-native_divs-native_spans'
            )
        )
        pre_plaintext_description = pypandoc.convert_text(
            ep_record['mediawiki_description'],
            to='plain',
            format='mediawiki'
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
        ep_record['coverage_dates_string'] = format_daterange(
            ep_record['coverage_start_date'], ep_record['coverage_end_date'])
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

    # Redundant with title fields.
    del ep_record['twitch_title']

    return Box(ep_record)
