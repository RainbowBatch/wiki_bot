import kfio
import pandas as pd

from bs4 import BeautifulSoup

# While there are more than 1000 missing pages, Fandom only tracks the first thousand.
missing_page = kfio.download("https://knowledge-fight.fandom.com/wiki/Special:WantedPages?limit=1000")
missing_soup = BeautifulSoup(missing_page.text, 'html.parser')

missing_list = missing_soup.find("ol", class_="special")

header = [
    "title",
    "references",
]
rows = []

for missing_item in missing_list.find_all("li"):
	if missing_item.find("a", class_= "newcategory") is not None:
		continue # We don't care about new categories right now.
	rows.append((
		missing_item.find("span").text,
		int(missing_item.find("a").text.split()[0])
	))

df = pd.DataFrame(rows, columns=header)

print(df)

kfio.save(df, 'data/missing_pages.json')