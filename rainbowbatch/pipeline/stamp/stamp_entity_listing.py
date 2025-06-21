import box
import io
import maya
import pandas as pd
import parse
import rainbowbatch.kfio as kfio
import rainbowbatch.wiki.page_listing as page_listing

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from natsort import natsorted
from rainbowbatch.git import check_git_branch
from rainbowbatch.pipeline.episode_processor import canonicalize_title
from rainbowbatch.remap.wiki_cleaner import simple_format

entities_df = kfio.load('data/raw_entities.json')
episodes_df = kfio.load('data/final.json')
wiki_df = kfio.load('data/scraped_page_data.json')

env = Environment(
    loader=FileSystemLoader(kfio.TOP_LEVEL_DIR/"templates"),
    autoescape=select_autoescape()
)


def pretty_entity_sourcetext(value):
    return repr(value)


env.filters["pretty_entity_sourcetext"] = pretty_entity_sourcetext


def stamp_entity_listing():
    assert check_git_branch('bot_raw'), "Please checkout bot_raw! Currently on %s." % git_branch

    template = env.get_template('entity_listing.wiki.template')

    # We can't render all the entities because the resulting page is too large.
    # Instead we split them into groups based on their first letter.
    grouped_entities_df = entities_df.groupby('starting_char')

    for starting_letter, _ in grouped_entities_df:
        fragment_df = grouped_entities_df.get_group(starting_letter)

        print(starting_letter)
        print(fragment_df)

        fragment_title = 'RainbowBatch Entities/%s' % starting_letter
        fragment_slug = canonicalize_title(fragment_title)
        fragment_fname = kfio.TOP_LEVEL_DIR/'kf_wiki_content/%s.wiki' % fragment_slug

        raw = template.render(
            entities=fragment_df.to_dict(orient='records')
        )

        pretty = simple_format(raw)

        with io.open(fragment_fname, mode="w", encoding="utf-8") as f:
            f.write(pretty)
            page_listing.add(fragment_title, fragment_slug)
            page_listing.repair()
            page_listing.save()


if __name__ == '__main__':
    stamp_entity_listing()
