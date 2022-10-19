import io
import kfio
import maya
import page_listing
import pandas as pd
import parse
import box

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from pprint import pprint
from pygit2 import Repository
from wiki_cleaner import simple_format

entities_df = kfio.load('data/raw_entities.json')
episodes_df = kfio.load('data/final.json')

def wiki_link(link_text, link_dest):
    if link_text == link_dest:
        return "[[%s]]" % link_dest
    else:
        return "[[%s|%s]]" % (link_dest, link_text)

def lookup_by_epnum(episode_number):
    df_view = episodes_df[episodes_df.episode_number == episode_number]

    if len(df_view) > 0:
        # Assert only one result?

        return box.Box(df_view.to_dict(orient='records')[0])

    return None

env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape()
)

FANCY_NAMES = {
    'autosub': "AutoSub",
    'otter': "otter.ai",
    'welder': "Welder",
    'manual': "Manually-created"
}

def pretty_entity_origin(value):
    if value.startswith('transcripts'):
        parse_result = parse.parse(r"transcripts\{}.{}.{}", value)
        episode_number, transcript_type, _ = parse_result

        episode_record = lookup_by_epnum(episode_number)
        if episode_record is not None:
            return "%s transcript for %s" % (FANCY_NAMES[transcript_type], wiki_link(episode_record.next_title, episode_record.safe_next_title))
        return "%s transcript for %s" % (FANCY_NAMES[transcript_type], episode_number)
    # TODO(woursler): Ensure the link will exist!
    return "[[%s]]" % value

def pretty_entity_sourcetext(value):
    return repr(value)

env.filters["pretty_entity_origin"] = pretty_entity_origin
env.filters["pretty_entity_sourcetext"] = pretty_entity_sourcetext

template = env.get_template('entity_listing.wiki.template')


def stamp_entity_listing():
    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'bot_raw', "Please checkout bot_raw! Currently on %s." % git_branch

    raw = template.render(
        entities=entities_df.to_dict(orient='records')
    )

    pretty = simple_format(raw)

    with io.open('kf_wiki_content/RainbowBatch_Generated_Entity_Listing.wiki', mode="w", encoding="utf-8") as f:
        f.write(pretty)
        page_listing.add('RainbowBatch Generated Entity Listing', 'RainbowBatch_Generated_Entity_Listing')
        page_listing.repair()
        page_listing.save()


if __name__ == '__main__':
    stamp_entity_listing()
