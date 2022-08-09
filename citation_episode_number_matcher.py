import kfio
import pandas as pd
import re

from date_lookup import lookup_by_epnum
from date_lookup import lookup_date
from date_lookup import parse_date_range
from raw_entity_extraction import extract_episode_number

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


def guess_episode(title):
    if title in HARDCODED:
        return lookup_by_epnum(HARDCODED[title])

    if ":" in title:
        fragment = title.split(':')[0].strip()
        if '#' in fragment:
            return lookup_by_epnum(fragment.split('#')[-1].strip())
        if fragment.startswith("Episode "):
            return lookup_by_epnum(fragment[7:].strip())
        return lookup_by_epnum(fragment.strip())

    if title.startswith("Citations for"):
        return lookup_date(title[14:])

    if date_match_regex.match(title):
        guessed_ep_details = lookup_date(title)

        if guessed_ep_details is not None:
            return guessed_ep_details
        else:
            print("Unable to match", title)

    print("WEIRD TITLE", title)

    return None


if __name__ == '__main__':
    title_table = kfio.load_citations_table('data/citations.json')

    titles = title_table.citations_title.to_list()

    episode_numbers = [
        extract_episode_number(title)
        for title in titles
    ]

    print(episode_numbers)

    from collections import Counter

    print(Counter(episode_numbers))
