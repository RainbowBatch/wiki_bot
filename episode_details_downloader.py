import pandas as pd
import requests
import time

from bs4 import BeautifulSoup

title_table = pd.read_csv('titles.csv')

header = [
    "libsyn_page",
    "embed_player_url",
    "details_html",
    "episode_length",
]
rows = []

for _, coarse_episode_details in title_table.iterrows():
    details_url = coarse_episode_details.libsyn_page
    details_page = requests.get(details_url)
    details_soup = BeautifulSoup(details_page.text, 'html.parser')

    player_iframe = details_soup.find("iframe")
    player_url = 'https:'+player_iframe['src']
    player_page = requests.get(player_url)
    player_soup = BeautifulSoup(player_page.text, 'html.parser')

    description = details_soup.find("div", class_="libsyn-item-body")
    episode_length = ''.join(player_soup.find(
        "span", class_="static-duration").contents).strip().split(' ')[-1]

    rows.append([
        details_url,
        player_url,
        description,
        episode_length,
    ])
    print(details_url, episode_length)
    # Wait a bit between episodes so that we don't get rate limited.
    time.sleep(0.5)


df = pd.DataFrame(rows, columns=header)

with open("libsyn_details.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))
