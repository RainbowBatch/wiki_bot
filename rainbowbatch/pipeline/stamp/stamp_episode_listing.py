import io
import maya
import pandas as pd
import rainbowbatch.kfio as kfio

from collections import OrderedDict
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from rainbowbatch.git import check_git_branch
from rainbowbatch.remap.wiki_cleaner import simple_format

env = Environment(
    loader=FileSystemLoader(kfio.TOP_LEVEL_DIR/"templates"),
    autoescape=select_autoescape()
)

# TODO: These should be moved into remap
def extract_year(dts):
    if dts is None:
        return None
    return int(dts.split(',')[-1].strip())


def mayafy_date(dt):
    return maya.parse(dt) if not pd.isna(dt) else None


def date_midpoint(dt1, dt2):
    return dt1  # TODO: Actually do this.


def stamp_episode_listing():
    assert check_git_branch('bot_raw'), "Please checkout bot_raw! Currently on %s." % git_branch

    template = env.get_template('episode_listing.wiki.template')

    episodes_df = kfio.load('data/final.json')

    episodes_df['release_year'] = episodes_df.release_date.apply(extract_year)

    episodes_df['maya_release_date'] = episodes_df.release_date.apply(
        mayafy_date)
    episodes_df['maya_coverage_start_date'] = episodes_df.coverage_start_date.apply(
        mayafy_date)
    episodes_df['maya_coverage_end_date'] = episodes_df.coverage_end_date.apply(
        mayafy_date)
    # TODO: Actually use this to generate coverage_year
    episodes_df['maya_coverage_mid_date'] = episodes_df.apply(lambda x: date_midpoint(
        x.maya_coverage_start_date, x.maya_coverage_end_date), axis=1)

    episodes_df['coverage_year'] = episodes_df.coverage_start_date.apply(
        extract_year)

    categories = sorted({
        category
        for row in episodes_df.categories.dropna()
        if isinstance(row, list)
        for category in row
        if category != 'Present Day'
    })

    release_years = list(sorted(set(episodes_df.release_year.to_list())))
    coverage_years = sorted(
        {int(y) for y in episodes_df.coverage_year.dropna()}
    )

    release_year_shards = OrderedDict(
        (
            release_year,
            episodes_df[episodes_df.release_year == release_year]
            .sort_values(['maya_release_date', 'episode_number'])
            .to_dict(orient='records')
        )
        for release_year in release_years
    )

    coverage_year_shards = OrderedDict(
        (
            int(coverage_year),
            episodes_df[episodes_df.coverage_year == coverage_year]
            .sort_values(['maya_coverage_mid_date', 'maya_release_date', 'episode_number'])
            .to_dict(orient='records')
        )
        for coverage_year in coverage_years
        if not pd.isna(coverage_year)
    )

    category_shards = OrderedDict(
        (
            category,
            episodes_df[episodes_df.categories.map(set([category]).issubset)]
            .sort_values(['maya_release_date', 'episode_number'])
            .to_dict(orient='records')
        )
        for category in categories
    )

    raw = template.render(
        release_year_shards=release_year_shards,
        coverage_year_shards=coverage_year_shards,
        category_shards=category_shards,
    )

    pretty = simple_format(raw)

    with io.open(kfio.TOP_LEVEL_DIR/'kf_wiki_content/List_of_Knowledge_Fight_episodes.wiki', mode="w", encoding="utf-8") as f:
        f.write(pretty)


if __name__ == '__main__':
    stamp_episode_listing()
