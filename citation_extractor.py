import kfio
import math
import pandas as pd
import pandoc
import re
import wikitextparser

from bs4 import BeautifulSoup
from citation_episode_number_matcher import guess_associated_episode
from date_lookup import extract_date_from_string
from date_lookup import format_date
from pprint import pprint

sitemap_page = kfio.download('https://knowledgefight.com/sitemap.xml')
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
    "citations_start_date",
    "citations_tags",
    "citations_html",
    "citations_mediawiki",
]
rows = []

for citations_url in citation_urls:
    citations_page = kfio.download(citations_url)
    citations_soup = BeautifulSoup(citations_page.text, 'html.parser')
    citations_html = citations_soup.find(
        "div",
        class_="entry-content",
    ).encode("utf-8")

    title_html = citations_soup.find("h1", class_="entry-title")
    citations_title = title_html.text

    citations_episode_number = guess_associated_episode(citations_title)

    start_date, end_date = extract_date_from_string(citations_title)

    citations_start_date = None
    if start_date is not None:
        citations_start_date = format_date(start_date)

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
        citations_start_date,
        citations_tags,
        citations_html,
        citations_mediawiki,
    ])

df = pd.DataFrame(rows, columns=header)

kfio.save(df, 'data/citations.json')
