import kfio
import pandas as pd

from bs4 import BeautifulSoup

title_table = kfio.load('data/titles.json')

existing_details_table = kfio.load('data/libsyn_details.json')

existing_urls = existing_details_table.libsyn_page.to_list()
all_urls = title_table.libsyn_page.to_list()

new_urls = [x for x in all_urls if x not in existing_urls]

if len(new_urls) == 0:
    print("We already have data for all URLs! Exiting.")
    quit()

print("Downloading details for %d episodes." % len(new_urls))

header = [
    "libsyn_page",
    "embed_player_url",
    "details_html",
    "episode_length",
]
rows = []

for details_url in new_urls:
    details_page = kfio.download(details_url)
    details_soup = BeautifulSoup(details_page.text, 'html.parser')

    player_iframe = details_soup.find("iframe")
    player_url = 'https:'+player_iframe['src']
    player_page = kfio.download(player_url)
    player_soup = BeautifulSoup(player_page.text, 'html.parser')

    description = details_soup.find(
        "div", class_="libsyn-item-body").encode("utf-8")
    episode_length = ''.join(player_soup.find(
        "span", class_="static-duration").contents).strip().split(' ')[-1]

    print("Found description", description)

    rows.append([
        details_url,
        player_url,
        description,
        episode_length,
    ])
    print(details_url, episode_length)


df = existing_details_table.append(
    pd.DataFrame(rows, columns=header),
    ignore_index=True,
)

kfio.save(df, 'data/libsyn_details.json')
