import pandas as pd
import maya
import re
from date_lookup import lookup_date

TIMEZONE = 'US/Eastern'
DATE_FORMAT = "%B %#d, %Y"

HARDCODED = {
    'Citations For Dennis Montgomery Episode': '25',
    'Obama Deception Citations': '230A',  # Technically all 230, but....
    # These are not citations. They seem like guest lists?
    'July 2015': None,
    'March 2009': None,
    'February 2009': None,
    'June 2015': None,
    'January 2009': None,
}


date_match_regex = re.compile("^\w+ \d+, \d+$")


def parse_date_range(date_range_string):
    if '-' not in date_range_string:
        # Singleton date
        return maya.when(date_range_string, timezone=TIMEZONE)
    return maya.when(date_range_string, timezone=TIMEZONE)


def extract_episode_number(title):
    if title in HARDCODED:
        return HARDCODED[title]

    if ":" in title:
        fragment = title.split(':')[0].strip()
        if '#' in fragment:
            return fragment.split('#')[-1].strip()
        if fragment.startswith("Episode "):
            return fragment[7:].strip()
        return fragment

    if title.startswith("Citations for"):
        guessed_ep_details = lookup_date(title[14:])

        if guessed_ep_details is not None:
            return guessed_ep_details.episode_number

    if date_match_regex.match(title):
        guessed_ep_details = lookup_date(title)

        if guessed_ep_details is not None:
            return guessed_ep_details.episode_number
        else:
            print("Unable to match", title)

    print("WEIRD TITLE", title)

    return None


if __name__ == '__main__':
    title_table = pd.read_csv('citations.csv', encoding='latin1')

    titles = title_table.citations_title.to_list()

    print([
        extract_episode_number(title)
        for title in titles
    ])
