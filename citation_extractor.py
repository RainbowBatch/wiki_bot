import pandas as pd
import requests
import time
import pandoc
import wikitextparser

from bs4 import BeautifulSoup

details_table = pd.read_csv('libsyn_details.csv', encoding ='latin1')


URLS = []
for ep_record in details_table.to_dict(orient='records'):

    ep_record['mediawiki_description'] = pandoc.write(
        pandoc.read(ep_record['details_html'], format="html-native_divs-native_spans"),
        format="mediawiki"
    )

    parsed_description = wikitextparser.parse(ep_record['mediawiki_description'])

    if len(parsed_description.external_links) > 0:
        for link in parsed_description.external_links:
            link_url = link.url
            if 'knowledgefight.com/research/' in link_url:
                URLS.append(link_url)

print("Located", len(URLS), "citation urls.")

header = [
    "citations_url",
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


    citations_mediawiki = pandoc.write(
        pandoc.read(citations_html, format="html-native_divs-native_spans"),
        format="mediawiki"
    )

    print("Finished", citations_url)
    # print(citations_mediawiki)

    rows.append([
        citations_url,
        citations_html,
        citations_mediawiki,
    ])

df = pd.DataFrame(rows, columns=header)

with open("citations.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))
