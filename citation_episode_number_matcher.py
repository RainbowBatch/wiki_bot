import kfio
import pandas as pd
import re

from date_lookup import extract_date_from_string
from date_lookup import lookup_by_maya_date
from pprint import pprint

explicit_episode_regex = re.compile(r"Episode\s+#?(?P<episode_number>\d+):")


MANUAL_OVERRIDE = {
    # These appear to be just wrong on the explicit ep number.
    # Possibly an old numbering system?
    "Episode 314: January 17-18, 2013": "297",
    "Episode 315: January 20-23, 2013": "298",
    "Episode 316: May 17-20, 2019": "299",
    "Episode 317: May 21-22, 2019": "300",
    "Episode 320: January 29-31, 2013": "303",
    "Episode 321: May 30-31, 2019": "304",
    "Episode 322: February 1, 2013": "305",
    "Episode 335: July 5, 2019": "318",
    'Citations For Dennis Montgomery Episode': '25',
    # These are just dates that are slightly off the usual format.
    'Citations for March 31st, 2017': '27',
    'Citations for April 14th, 2017': '30',
    # Covered twice due to a special episode...
    'August 9, 2015': '56',
    # Technically all 230, but....
    'Obama Deception Citations': '230A',
    # These are not citations. They seem like guest lists?
    'July 2015': None,
    'March 2009': None,
    'February 2009': None,
    'June 2015': None,
    'January 2009': None,
}


def guess_associated_episode(episode_title):
    print(episode_title)

    if episode_title in MANUAL_OVERRIDE:
        return MANUAL_OVERRIDE[episode_title]

    guesses = set()

    explicit_match = explicit_episode_regex.search(episode_title)
    if explicit_match is not None:
        guesses.add(explicit_match.group('episode_number'))
    start_date, end_date = extract_date_from_string(episode_title)

    start_ep = lookup_by_maya_date(start_date)
    end_ep = lookup_by_maya_date(end_date)

    if start_ep is not None:
        guesses.add(start_ep['episode_number'])

    if end_ep is not None:
        guesses.add(end_ep['episode_number'])

    if len(guesses) == 0:
        return None

    assert len(guesses) == 1, guesses
    return list(guesses)[0]


if __name__ == '__main__':
    citations_df = kfio.load('data/citations.json')

    citations_df['new_episode_number'] = citations_df.citations_title.apply(
        guess_associated_episode)

    pd.set_option('display.max_rows', None)
    print(citations_df[citations_df.new_episode_number != citations_df.citations_episode_number][[
          'new_episode_number', 'citations_episode_number', 'citations_title']])
