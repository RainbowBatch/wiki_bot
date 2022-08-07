import box
import math
import maya
import pandas as pd
import re

TIMEZONE = 'US/Eastern'
DATE_FORMAT = "%B %#d, %Y"


def parse_date_range(date_range_string):
    # TODO(woursler): Use extract_date_from_string instead.
    if '-' not in date_range_string:
        # Singleton date
        return maya.when(date_range_string, timezone=TIMEZONE)
    return maya.when(date_range_string, timezone=TIMEZONE)


def mayafy_date(datestring):
    if isinstance(datestring, float) and math.isnan(datestring):
        return None
    return maya.when(datestring, timezone=TIMEZONE)


def canonicalize_date(datestring):
    if isinstance(datestring, float) and math.isnan(datestring):
        return None
    return format_date(mayafy_date(datestring))


def format_date(dt):
    if isinstance(dt, str):
        return canonicalize_date(dt)
    return dt.datetime().strftime(DATE_FORMAT)


date_listing_df = pd.read_csv('date_listing.csv')

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


def lookup_date(datestring):
    df_view = date_listing_df[date_listing_df.coverage_date ==
                              canonicalize_date(datestring)]

    if len(df_view) == 0:
        # No Exact match. Try for a date range match.

        maya_date = mayafy_date(datestring)

        df_view = date_listing_df[
            (date_listing_df.coverage_start_date_maya <= maya_date)
            & (date_listing_df.coverage_end_date_maya >= maya_date)
        ]

    if len(df_view) > 0:
        # Assert only one result?

        return box.Box(df_view.to_dict(orient='records')[0])

    return None


date_match_regex = re.compile(
    "(?P<month>\w+) (?P<start_date>\d+)(-(?P<end_date>\d+))?, (?P<year>\d+)")


def extract_date_from_string(raw_string):
    match = date_match_regex.search(raw_string)

    if match is None:
        return None, None

    month = match.group('month')
    start_date = match.group('start_date')
    end_date = match.group('end_date')
    year = match.group('year')

    start_date = mayafy_date('%s %s, %s' % (month, start_date, year))
    if end_date is None:
        return start_date, start_date
    else:
        end_date = mayafy_date('%s %s, %s' % (month, end_date, year))
        return start_date, end_date

    return None, None


if __name__ == '__main__':
    for date in [
        'June 17, 2015',
    ]:
        print("===")
        print(date, "->\n\t", lookup_date(date))

    citations_df = pd.read_csv('citations.csv', encoding='latin1')

    for title in citations_df.citations_title:
        match = extract_date_from_string(title)
        print(match)
