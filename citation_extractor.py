import pandas as pd
import pandoc
import re
import requests
import time
import wikitextparser
import math

from bs4 import BeautifulSoup
from citation_episode_number_matcher import guess_episode
from date_lookup import extract_date_from_string
from date_lookup import format_date
from pprint import pprint


def request_with_retries(url):
    print("requesting", url)
    page = None
    while page is None or page.status_code != 200:
        # SquareSpace is super senstive to too many requests.
        time.sleep(2)
        print('.')
        page = requests.get(url)
    return page


sitemap_page = request_with_retries('https://knowledgefight.com/sitemap.xml')
sitemap_soup = BeautifulSoup(sitemap_page.text, 'html.parser')
sitemap = [
    loc.text
    for loc in sitemap_soup.find_all("loc")
]


citation_url_regex = re.compile(
    "^https://knowledgefight.com/((the-past)|(research))/\d+/\d+/\d+/.*$")
citation_urls = [s for s in sitemap if citation_url_regex.match(s)]

print("Located", len(citation_urls), "citation urls.")

header = [
    "citations_url",
    "citations_title",
    "citations_episode_number",
    "citations_date",
    "citations_tags",
    "citations_html",
    "citations_mediawiki",
]
rows = []

for citations_url in citation_urls:
    citations_page = request_with_retries(citations_url)
    citations_soup = BeautifulSoup(citations_page.text, 'html.parser')
    citations_html = citations_soup.find("div", class_="entry-content")

    title_html = citations_soup.find("h1", class_="entry-title")
    citations_title = title_html.text

    # TODO(woursler): Something about this is wrong. It associates April 1st, 2008 with 122 (April 1st, 2013)
    associated_episode = guess_episode(citations_title)

    if associated_episode is None:
        citations_episode_number is None
    else:
        citations_episode_number = associated_episode.episode_number

    start_date, end_date = extract_date_from_string(citations_title)

    if start_date is None and associated_episode is not None:
        start_date = associated_episode.coverage_start_date

    citations_date = None
    if start_date is not None and not pd.isnan(start_date):
        citations_date = format_date(start_date)
    print(citations_date)

    citations_tags = []
    tags_container_html = citations_soup.find("div", class_="tags")
    if tags_container_html is not None:
        for tag_html in tags_container_html.find_all("a"):
            citations_tags.append(tag_html.text)

    citations_mediawiki = pandoc.write(
        pandoc.read(citations_html, format="html-native_divs-native_spans"),
        format="mediawiki"
    )

    print("Finished", citations_url)

    rows.append([
        citations_url,
        citations_title,
        citations_episode_number,
        citations_date,
        ';'.join(citations_tags),
        citations_html,
        citations_mediawiki,
    ])

df = pd.DataFrame(rows, columns=header)

with open("citations.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))
