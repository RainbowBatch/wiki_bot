import pandas as pd
import requests
import kfio

from bs4 import BeautifulSoup

r = requests.get('https://knowledgefight.libsyn.com/page/1/size/1000')

soup = BeautifulSoup(r.text, 'html.parser')

header = [
    "title",
    "release_date",
    "libsyn_page",
    "download_link",
]
rows = []

for item in soup.find_all("div", class_="libsyn-item"):
    title_link = item.find("div", class_="libsyn-item-title").find("a")
    release_date = ''.join(
        item.find("div", class_="libsyn-item-release-date")).strip()
    download_link = item.find(
        "ul", class_="libsyn-item-free").find("a")['href']
    title = ''.join(title_link.contents).strip()
    libsyn_page = title_link['href']
    rows.append([
        title,
        release_date,
        libsyn_page,
        download_link,
    ])

df = pd.DataFrame(reversed(rows), columns=header)

kfio.save(df, 'data/titles.json')
