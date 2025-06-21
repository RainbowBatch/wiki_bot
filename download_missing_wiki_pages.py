import rainbowbatch.kfio as kfio
import pandas as pd

from bs4 import BeautifulSoup

# While there are more than 1000 missing pages, Fandom only tracks the first thousand.
missing_page = kfio.download(
    "https://knowledgefight.wiki/index.php?title=Special:WantedPages&limit=5000")
missing_soup = BeautifulSoup(missing_page.text, 'html.parser')

missing_list = missing_soup.find("ol", class_="special")

header = [
    "title",
    "references",
]
rows = []

for missing_item in missing_list.find_all("li"):
    if missing_item.find("a", class_="newcategory") is not None:
        continue  # We don't care about new categories right now.
    if missing_item.find('del') is not None:
        continue  # These ones have been created recently.
    links = missing_item.find_all("a")
    rows.append((
        links[0].text,
        int(links[1].text.split()[0])
    ))

df = pd.DataFrame(rows, columns=header)

kfio.save(df, 'data/missing_pages.json')
