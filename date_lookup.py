import box
import kfio
import math
import maya
import pandas as pd
import re

TIMEZONE = 'US/Eastern'
DATE_FORMAT = "%B %#d, %Y"


def parse_date_range(date_range_string):
    # TODO: Use extract_date_from_string instead.
    if '-' not in date_range_string:
        # Singleton date
        return maya.when(date_range_string, timezone=TIMEZONE)
    return maya.when(date_range_string, timezone=TIMEZONE)


def mayafy_date(datestring):
    if pd.isna(datestring) or len(datestring.strip()) == 0:
        return None
    return maya.when(datestring, timezone=TIMEZONE)


def canonicalize_date(datestring):
    if pd.isna(datestring) or len(datestring.strip()) == 0:
        return None
    return format_date(mayafy_date(datestring))


def format_date(dt):
    if isinstance(dt, str):
        return canonicalize_date(dt)
    return dt.datetime().strftime(DATE_FORMAT)


date_listing_df = kfio.load(
    'data/final.json')[['title', 'episode_number', 'coverage_start_date', 'coverage_end_date', 'coverage_date']]

date_listing_df.coverage_date = date_listing_df.coverage_date.apply(
    canonicalize_date)
date_listing_df['coverage_start_date_maya'] = date_listing_df.coverage_start_date.apply(
    mayafy_date)
date_listing_df['coverage_end_date_maya'] = date_listing_df.coverage_end_date.apply(
    mayafy_date)


def lookup_by_epnum(episode_number):
    df_view = date_listing_df[date_listing_df.episode_number == episode_number]

    if len(df_view) > 0:
        # Assert only one result?

        return box.Box(df_view.to_dict(orient='records')[0])

    return None


def lookup_by_maya_date(dt):

    df_view = date_listing_df[
        (date_listing_df.coverage_start_date_maya <= dt)
        & (date_listing_df.coverage_end_date_maya >= dt)
    ]

    if len(df_view) > 0:
        # Assert only one result?

        return box.Box(df_view.to_dict(orient='records')[0])

    return None


date_match_regex = re.compile(
    "(?P<month>\w+) (?P<start_date>\d+)(-(?P<end_date>\d+))?, (?P<year>\d+)")

date2_match_regex = re.compile(
    "(?P<start_month>\w+) (?P<start_date>\d+)(-(?P<end_month>\w+) (?P<end_date>\d+))?, (?P<year>\d+)")


def extract_date_from_string(raw_string):
    match = date_match_regex.search(raw_string)
    match2 = date2_match_regex.search(raw_string)

    if match2 is not None:
        start_month = match2.group('start_month')
        end_month = match2.group('end_month')
        start_date = match2.group('start_date')
        end_date = match2.group('end_date')
        year = match2.group('year')
    elif match is not None:
        start_month = match.group('month')
        end_month = start_month
        start_date = match.group('start_date')
        end_date = match.group('end_date')
        year = match.group('year')
    else:
        return None, None

    start_date = mayafy_date('%s %s, %s' % (start_month, start_date, year))
    if end_date is None:
        return start_date, start_date
    else:
        end_date = mayafy_date('%s %s, %s' % (end_month, end_date, year))
        return start_date, end_date

    return None, None


if __name__ == '__main__':
    citations_df = kfio.load_citations_table('data/citations.json')

    for title in citations_df.citations_title:
        match = extract_date_from_string(title)
        print(match)

    match = extract_date_from_string("July 31-August 1, 2003")
    print(match)
