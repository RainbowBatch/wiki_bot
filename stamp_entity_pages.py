import io
import kfio
import maya
import page_listing
import pandas as pd

from box import Box
from episode_processor import canonicalize_title
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import select_autoescape
from pprint import pprint
from pygit2 import Repository
from stamp_episode_listing import mayafy_date
from wiki_cleaner import simple_format

git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'bot_raw', "Please checkout bot_raw! Currently on %s." % git_branch

env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape()
)

template = env.get_template('entity.wiki.template')

raw_entities = kfio.load('data/raw_entities.json')

episodes_df = kfio.load('data/final.json')
episodes_df['maya_release_date'] = episodes_df.release_date.apply(
    mayafy_date)

for entity_record in raw_entities[raw_entities.is_existing & ~raw_entities.is_redirect].to_dict(orient='records'):
    entity_record = Box(entity_record)
    origin_episode_numbers = set([o.split('__')[0]
                                  for o in entity_record.entity_origin])
    origin_episode_numbers.discard('None')

    # Don't talk about entities that appear in a majority of episodes.
    if len(origin_episode_numbers) <= 0 or len(origin_episode_numbers) >= 0.5 * len(episodes_df):
        continue

    relevant_episodes = episodes_df[episodes_df.episode_number.isin(origin_episode_numbers)].sort_values(
        ['maya_release_date', 'episode_number']).to_dict(orient='records')

    # TODO: Ensure we don't clobber anything important.q
    raw = template.render(
        relevant_episodes=relevant_episodes,
    )

    pretty = simple_format(raw)


    page_title = entity_record.entity_name
    page_slug = canonicalize_title(page_title)
    page_fname = 'kf_wiki_content/%s.wiki' % page_slug

    with io.open(page_fname, mode="w", encoding="utf-8") as f:
        f.write(pretty)
        page_listing.add(page_title, page_slug)
        page_listing.repair()
        page_listing.save()
