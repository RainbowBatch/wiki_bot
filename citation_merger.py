import pandas as pd
import maya

from date_lookup import lookup_date

title_table = pd.read_csv('citations.csv', encoding ='latin1')

titles = title_table.citations_title.to_list()

TIMEZONE = 'US/Eastern'
DATE_FORMAT = "%B %#d, %Y"

HARDCODED = {
	'Citations For Dennis Montgomery Episode': '25',
	'Obama Deception Citations': '230A', # Technically all 230, but....
}

# TODO(woursler): This probably doesn't work for the ones under /the-past/
def parse_date_range(date_range_string):
	if '-' not in date_range_string:
		# Singleton date
		return maya.when(date_range_string, timezone=TIMEZONE)
	return maya.when(date_range_string, timezone=TIMEZONE)

def extract_episode_number(title):
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

	if title in HARDCODED:
		return HARDCODED[title]

	print("WEIRD TITLE", title)

	return None

print([
	extract_episode_number(title)
	for title in titles
])

