import pandas as pd
import requests
import time
import pandoc
import wikitextparser

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


LISTING_QUEUE = [
    "https://knowledgefight.com/citations",
    "https://knowledgefight.com/research/",
]
VISITED_LISTINGS = set()

URLS = set()

# Also look for any links from libsyn. Should all be dupes, but worth checking.
details_table = pd.read_csv('libsyn_details.csv', encoding='latin1')

for ep_record in details_table.to_dict(orient='records'):

    ep_record['mediawiki_description'] = pandoc.write(
        pandoc.read(ep_record['details_html'],
                    format="html-native_divs-native_spans"),
        format="mediawiki"
    )

    parsed_description = wikitextparser.parse(
        ep_record['mediawiki_description'])

    if len(parsed_description.external_links) > 0:
        for link in parsed_description.external_links:
            link_url = link.url
            if 'knowledgefight.com/research/' in link_url:
                URLS.add(link_url)


while len(LISTING_QUEUE) > 0:
    listing_url = LISTING_QUEUE.pop()
    listing_page = request_with_retries(listing_url)
    VISITED_LISTINGS.add(listing_url)
    listing_soup = BeautifulSoup(listing_page.text, 'html.parser')

    for link_html in listing_soup.find_all("a"):
        link = link_html['href']
        if link.startswith("/research/"):
            link = "https://knowledgefight.com" + link

        if not link.startswith('https://knowledgefight.com/research/'):
            continue

        if '#comments' in link or '/category/' in link or '/tag/' in link:
            continue

        URLS.add(link)

    pagination_html = listing_soup.find("nav", class_="pagination")
    if pagination_html is not None:
        for link_html in pagination_html.find_all("a"):
            link = link_html['href']
            assert link.startswith('/research?')
            link = "https://knowledgefight.com" + link
            if link in VISITED_LISTINGS:
                continue
            LISTING_QUEUE.append(link)

    # print(listing_soup)

print("Located", len(URLS), "citation urls.")

header = [
    "citations_url",
    "citations_title",
    "citations_html",
    "citations_mediawiki",
]
rows = []


for citations_url in URLS:
    citations_page = None
    while citations_page is None or citations_page.status_code != 200:
        # SquareSpace is super senstive to too many requests.
        time.sleep(2)
        print('.')
        citations_page = requests.get(citations_url)
    citations_soup = BeautifulSoup(citations_page.text, 'html.parser')
    citations_html = citations_soup.find("div", class_="entry-content")

    title_html = citations_soup.find("h1", class_="entry-title")

    citations_mediawiki = pandoc.write(
        pandoc.read(citations_html, format="html-native_divs-native_spans"),
        format="mediawiki"
    )

    print("Finished", citations_url)
    citations_title = title_html.text

    rows.append([
        citations_url,
        citations_title,
        citations_html,
        citations_mediawiki,
    ])

df = pd.DataFrame(rows, columns=header)

with open("citations.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))
