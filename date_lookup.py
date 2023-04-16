import box
import kfio
import math
import maya
import pandas as pd
import re

TIMEZONE = 'US/Eastern'
DATE_FORMAT = "%B %#d, %Y"


def mayafy_date(datestring):
    if pd.isna(datestring) or len(datestring.strip()) == 0:
        return None
    return maya.parse(datestring, timezone=TIMEZONE)


def canonicalize_date(datestring):
    if pd.isna(datestring) or len(datestring.strip()) == 0:
        return None
    return format_date(mayafy_date(datestring))

def format_daterange(start_dt, end_dt):
    if isinstance(start_dt, str):
        start_dt = mayafy_date(start_dt)
    if isinstance(end_dt, str):
        end_dt = mayafy_date(end_dt)

    if start_dt is None or end_dt is None:
        if start_dt is None and end_dt is None:
            return None
        print(start_dt, end_dt)
        return "Error, why is only one date set..." # TODO
    if start_dt == end_dt:
        return format_date(start_dt)


    if start_dt.month == end_dt.month and start_dt.year == end_dt.year:
        return "%s %s-%s, %s" % (
            start_dt.datetime().strftime("%B"),
            start_dt.datetime().strftime("%#d"),
            end_dt.datetime().strftime("%#d"),
            start_dt.datetime().strftime("%Y"),
        )
    if start_dt.year == end_dt.year:
        return "%s %s-%s %s, %s" % (
            start_dt.datetime().strftime("%B"),
            start_dt.datetime().strftime("%#d"),
            end_dt.datetime().strftime("%B"),
            end_dt.datetime().strftime("%#d"),
            start_dt.datetime().strftime("%Y"),
        )
    return "%s-%s" % (format_date(start_dt), format_date(end_dt))


def format_date(dt):
    if isinstance(dt, str):
        return canonicalize_date(dt)
    if dt is None:
        return None
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
    "(?P<month>\w+\.?) (?P<start_date>\d+)(\s*-\s*(?P<end_date>\d+))?, (?P<year>\d+)")

date2_match_regex = re.compile(
    "(?P<start_month>\w+\.?) (?P<start_date>\d+)(\s*-\s*(?P<end_month>\w+\.?) (?P<end_date>\d+))?, (?P<year>\d+)")


def extract_date_from_string(raw_string):
    raw_string = raw_string.replace(" and ", "-").replace("&", "-")
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
        print("===")
        print(title)
        print(format_daterange(*match))
        print(match)

    match = extract_date_from_string("July 31-August 1, 2003")
    print(match)
