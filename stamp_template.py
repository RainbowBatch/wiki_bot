import io
import kfio
import pandas as pd

from jinja2 import Template
from pygit2 import Repository
from wiki_cleaner import simple_format


def stamp_templates():
    episodes_df = kfio.load('data/final.json')
    with open('episode.wiki.template') as episode_template_f:
        template = Template(episode_template_f.read())

    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'bot_raw', "Please checkout bot_raw! Currently on %s." % git_branch

    PAGE_RECORDS = []
    for record in episodes_df.to_dict(orient='records'):
        raw = template.render(**record)
        pretty = simple_format(raw)
        with io.open(record['ofile'], mode="w", encoding="utf-8") as f:
            f.write(pretty)

        PAGE_RECORDS.append({
            'title': record['title'],
            'slug': record['slug'],
            #'oldid': None,
        })

    page_records_df = pd.DataFrame.from_records(PAGE_RECORDS).sort_values('title')

    kfio.save(page_records_df, 'kf_wiki_content/page_listing.json')

if __name__ == '__main__':
    stamp_templates()
