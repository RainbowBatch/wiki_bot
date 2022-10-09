import io
import kfio
import page_listing
import pandas as pd

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from pygit2 import Repository
from tqdm import tqdm
from wiki_cleaner import simple_format

env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape()
)

template = env.get_template('episode.wiki.template')


def stamp_templates():
    episodes_df = kfio.load('data/final.json')

    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'bot_raw', "Please checkout bot_raw! Currently on %s." % git_branch

    PAGE_RECORDS = []
    print("Stamping episodes.")
    for record in tqdm(episodes_df.to_dict(orient='records')):
        raw = template.render(**record)
        pretty = simple_format(raw)
        with io.open(record['ofile'], mode="w", encoding="utf-8") as f:
            f.write(pretty)

        PAGE_RECORDS.append({
            'title': record['safe_title'],
            'slug': record['slug'],
            'oldid': None,
        })

    page_listing.add_all(pd.DataFrame.from_records(
        PAGE_RECORDS
    ))

    page_listing.save()


if __name__ == '__main__':
    stamp_templates()
