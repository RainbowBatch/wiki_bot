import pandas as pd
import requests
import time
import pandoc
import wikitextparser
import re
from pprint import pprint

from bs4 import BeautifulSoup


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


citation_url_regex = re.compile("^https://knowledgefight.com/((the-past)|(research))/\d+/\d+/\d+/.*$")
citation_urls = [ s for s in sitemap if citation_url_regex.match(s) ]

print("Located", len(citation_urls), "citation urls.")

header = [
    "citations_url",
    "citations_title",
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
        ';'.join(citations_tags),
        citations_html,
        citations_mediawiki,
    ])

df = pd.DataFrame(rows, columns=header)

with open("citations.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))