import kfio
import pandas as pd

from bs4 import BeautifulSoup

title_table = kfio.load('data/titles.json')

header = [
    "libsyn_page",
    "embed_player_url",
    "details_html",
    "episode_length",
]
rows = []

for _, coarse_episode_details in title_table.iterrows():
    details_url = coarse_episode_details.libsyn_page
    details_page = kfio.download(details_url)
    details_soup = BeautifulSoup(details_page.text, 'html.parser')

    player_iframe = details_soup.find("iframe")
    player_url = 'https:'+player_iframe['src']
    player_page = kfio.download(player_url)
    player_soup = BeautifulSoup(player_page.text, 'html.parser')

    description = details_soup.find("div", class_="libsyn-item-body").encode("utf-8")
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


df = pd.DataFrame(rows, columns=header)

kfio.save(df, 'data/libsyn_details.json')
