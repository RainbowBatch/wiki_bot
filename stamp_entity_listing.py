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
from episode_processor import canonicalize_title

entities_df = kfio.load('data/raw_entities.json')
episodes_df = kfio.load('data/final.json')
wiki_df = kfio.load('data/scraped_page_data.json')


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


def lookup_wiki_page(page_title):
    df_view = wiki_df[wiki_df.title == page_title]

    if len(df_view) > 0:
        # Assert only one result?

        return box.Box(df_view.to_dict(orient='records')[0])

    return None


env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape()
)

REFERENCE_TYPES = {
    'autosub': "AutoSub",
    'otter': "otter.ai",
    'welder': "Welder",
    'manual': "Manually-created",
    'whisper': "OpenAI Whisper",
    'wiki': "Wiki Page",
}


def parse_entity_orgin(value):
    if value.startswith('transcripts'):
        parse_result = parse.parse(r"transcripts\{}.{}.{}", value)
        episode_number, transcript_type, _ = parse_result

        episode_record = lookup_by_epnum(episode_number)

        if episode_record is not None:
            return (episode_number, transcript_type, episode_record.safe_title)
        return (episode_number, transcript_type, None)

    wiki_page = lookup_wiki_page(value)

    if wiki_page is not None and not pd.isna(wiki_page.episodeNumber):
        return (wiki_page.episodeNumber, 'wiki', value)
    return (None, 'wiki', value)


def pretty_entity_origin(value):
    if value.startswith('transcripts'):
        parse_result = parse.parse(r"transcripts\{}.{}.{}", value)
        episode_number, transcript_type, _ = parse_result

        episode_record = lookup_by_epnum(episode_number)
        if episode_record is not None:
            return "%s transcript for %s" % (REFERENCE_TYPES[transcript_type], wiki_link(episode_record.title, episode_record.safe_title))
        return "%s transcript for %s" % (REFERENCE_TYPES[transcript_type], episode_number)
    # TODO(woursler): Ensure the link will exist!
    return "[[%s]]" % value


def pretty_entity_sourcetext(value):
    return repr(value)


env.filters["pretty_entity_origin"] = pretty_entity_origin
env.filters["pretty_entity_sourcetext"] = pretty_entity_sourcetext

template = env.get_template('entity_listing.wiki.template')


def create_entity_origin_list_mw(entities_origins):
    eo_rows = [parse_entity_orgin(eo) for eo in entities_origins]

    eo_df = pd.DataFrame(
        eo_rows, columns=['episode_number', 'reference_type', 'title'])

    grouped_eo_df = eo_df.groupby('episode_number')

    formatted_entity_strings = []

    for non_episode_entry in eo_df[eo_df['episode_number'].isna()].to_dict(orient='records'):
        formatted_entity_strings.append("%s (via %s)" % (
            non_episode_entry['title'], REFERENCE_TYPES[non_episode_entry['reference_type']]))

    for episode_number, _ in grouped_eo_df:
        group_eo_df = grouped_eo_df.get_group(episode_number)
        reference_types = set(group_eo_df.reference_type.to_list())

        episode_record = lookup_by_epnum(episode_number)

        if episode_record is None:
            slug = episode_number
        else:
            slug = wiki_link(episode_record.title, episode_record.safe_title)

        formatted_entity_strings.append("%s (via %s)" % (
            slug, ', '.join([REFERENCE_TYPES[rt] for rt in reference_types])))

    assert len(formatted_entity_strings) > 0

    return formatted_entity_strings


def stamp_entity_listing():
    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'bot_raw', "Please checkout bot_raw! Currently on %s." % git_branch

    # TODO(woursler): Move these upstream into entity generation.
    entities_df['grouped_entity_origin'] = entities_df.entity_origin.apply(
        create_entity_origin_list_mw)

    entities_df['starting_char'] = entities_df.entity_name.apply(
        lambda name: name[0].upper())

    # We can't render all the entities because the resulting page is too large.
    # Instead we split them into groups based on their first letter.
    grouped_entities_df = entities_df.groupby('starting_char')

    for starting_letter, _ in grouped_entities_df:
        fragment_df = grouped_entities_df.get_group(starting_letter)

        print(starting_letter)
        print(fragment_df)

        fragment_title = 'RainbowBatch Entities/%s' % starting_letter
        fragment_slug = canonicalize_title(fragment_title)
        fragment_fname = 'kf_wiki_content/%s.wiki' % fragment_slug

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
