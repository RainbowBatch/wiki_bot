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


def reprocess_citation_episodes():
    '''Returns true if we manage to identify any new associations.'''
    dirty_flag = False
    dirty_list = []

    citations_table = kfio.load('data/citations.json')

    null_citations_table = citations_table[citations_table['citations_episode_number'].isnull(
    )]

    for record in null_citations_table.to_dict(orient='records'):
        citations_episode_number = guess_associated_episode(
            record['citations_title'])

        if citations_episode_number is not None:
            dirty_flag = True
            dirty_list.append(record['citations_url'])
            for index in citations_table.index[citations_table.citations_url == record['citations_url']].tolist():
                citations_table.at[index,
                                   'citations_episode_number'] = citations_episode_number

    kfio.save(citations_table, 'data/citations.json')

    if dirty_flag:
        print("Updated citation <-> episode mapping for %d URLs." %
              len(dirty_list))

    return dirty_flag


def download_citations():
    sitemap_page = kfio.download('https://knowledgefight.com/sitemap.xml')
    sitemap_soup = BeautifulSoup(sitemap_page.text, features="xml")
    sitemap = [
        loc.text
        for loc in sitemap_soup.find_all("loc")
    ]

    citation_url_regex = re.compile(
        "^https://knowledgefight.com/((the-past)|(research))/\d+/\d+/\d+/.*$")
    citation_urls = [s for s in sitemap if citation_url_regex.match(s)]

    print("Located", len(citation_urls), "citation urls.")

    existing_citations_table = kfio.load('data/citations.json')

    existing_citations_urls = existing_citations_table.citations_url.to_list()

    new_citation_urls = [
        x for x in citation_urls if x not in existing_citations_urls]

    if len(new_citation_urls) == 0:
        print("We already have data for all citation URLs!")
        return

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

    print("Downloading html for %d citations." % len(new_citation_urls))

    for citations_url in new_citation_urls:
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
            pandoc.read(citations_html,
                        format="html-native_divs-native_spans"),
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

    df = existing_citations_table.append(
        pd.DataFrame(rows, columns=header),
        ignore_index=True,
    )

    kfio.save(df, 'data/citations.json')


if __name__ == '__main__':
    download_citations()
