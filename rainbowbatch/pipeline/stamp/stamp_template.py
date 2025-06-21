import io
import numpy as np
import pandas as pd
import rainbowbatch.kfio as kfio
import rainbowbatch.wiki.page_listing as page_listing

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from rainbowbatch.git import check_git_branch
from rainbowbatch.remap.wiki_cleaner import simple_format
from tqdm import tqdm

env = Environment(
    loader=FileSystemLoader(kfio.TOP_LEVEL_DIR/"templates"),
    autoescape=select_autoescape()
)

template = env.get_template('episode.wiki.template')


def stamp_templates():
    episodes_df = kfio.load('data/final.json')

    episodes_df = episodes_df.replace({np.nan: None})

    assert check_git_branch('bot_raw'), "Please checkout bot_raw! Currently on %s." % git_branch

    PAGE_RECORDS = []
    print("Stamping episodes.")
    for record in tqdm(episodes_df.to_dict(orient='records')):
        raw = template.render(**record)
        pretty = simple_format(raw)
        with io.open(kfio.TOP_LEVEL_DIR/record['ofile'], mode="w", encoding="utf-8") as f:
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
