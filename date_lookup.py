import pandas as pd
import maya
import box
import math

TIMEZONE = 'US/Eastern'
DATE_FORMAT = "%B %#d, %Y"

def mayafy_date(datestring):
    if isinstance(datestring, float) and math.isnan(datestring):
        return None
    return maya.when(datestring, timezone=TIMEZONE)

def canonicalize_date(datestring):
    if isinstance(datestring, float) and math.isnan(datestring):
        return None
    return mayafy_date(datestring).datetime().strftime(DATE_FORMAT)

date_listing_df = pd.read_csv('date_listing.csv')

date_listing_df.coverage_date = date_listing_df.coverage_date.apply(canonicalize_date)
date_listing_df['coverage_start_date_maya'] = date_listing_df.coverage_start_date.apply(mayafy_date)
date_listing_df['coverage_end_date_maya'] = date_listing_df.coverage_end_date.apply(mayafy_date)


def lookup_date(datestring):
    df_view = date_listing_df[date_listing_df.coverage_date == canonicalize_date(datestring)]

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


if __name__ == '__main__':
    for date in [
            'February 11, 2022',
            'July 10, 2015',
            'February 10, 2020',
            'February 11, 2020',
            'February 12, 2020',
            'February 13, 2020',
            'February 14, 2020',
            'February 15, 2020',
            'February 16, 2020',
            'February 17, 2020',
            'February 18, 2020',
            'February 19, 2020',
            'February 20, 2020',
            'February 21, 2020',
            'February 22, 2020',
            'February 23, 2020',
            'February 24, 2020',
            'February 25, 2020',
            'February 26, 2020',
            'February 27, 2020',
            'February 28, 2020',
        ]:
        print("===")
        print(date, "->\n\t", lookup_date(date))
