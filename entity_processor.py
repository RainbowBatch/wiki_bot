import kfio
import pandas as pd
import pathlib
import re

from collections import Counter
from collections import defaultdict
from entity import create_entity_origin_list_mw
from entity import restore_capitalization
from entity import simplify_entity
from pprint import pprint
from pprint import pprint
from pygit2 import Repository
from tqdm import tqdm

PROTO_ENTITIES_PATH = pathlib.Path('data/proto_entities.json')

grouped_proto_entities = defaultdict(list)

print("Re-shuffling entities.")
for _, origin, proto_entities in tqdm(list(kfio.load(PROTO_ENTITIES_PATH).itertuples())):
    origin = tuple(origin)
    for proto_entity in proto_entities:
        e_key = simplify_entity(proto_entity['entity_name'])
        grouped_proto_entities[e_key].append(proto_entity)


def sum_vector_dicts(v_dicts):
    result = defaultdict(lambda: 0)
    for vd in v_dicts:
        for k, v in vd.items():
            result[k] += v
    return dict(result)


def group_proto_entities(proto_entities):
    return {
        'entity_count': sum([e['entity_count'] for e in proto_entities]),
        'entity_origin': sum_vector_dicts([e['entity_origin'] for e in proto_entities]),
        'entity_sourcetexts': sum_vector_dicts([e['entity_sourcetexts'] for e in proto_entities]),
        'entity_type': sum_vector_dicts([e['entity_type'] for e in proto_entities]),
    }


BANNED_TYPES = [
    'DATE',
    'MONEY',
    'TIME',
    'CARDINAL',
    'QUANTITY',
    'PERCENT',
]


def banned_type_fraction(entity_type):
    num = 0
    denom = 0
    for t, c in entity_type.items():
        denom += c
        if t in BANNED_TYPES:
            num += c
    return num / denom


def has_diverse_origins(entity_origins):
    origin_counter = Counter([
        origin.split('__')[0]
        for origin in entity_origins
    ])

    return origin_counter['None'] > 1 or len(origin_counter) > 1


print("Grouping and filtering entities.")
entities_records = []

for e_key, proto_entities in tqdm(grouped_proto_entities.items()):
    if len(e_key) < 10:
        continue

    if '-->' in e_key or 'alex -' in e_key or ' - ' in e_key:
        continue

    if re.search(r"\d{2}:\d{2}:\d{2},\d{3}", e_key, re.IGNORECASE):
        continue

    z = group_proto_entities(proto_entities)


    if banned_type_fraction(z['entity_type']) > 0.8:
        continue

    # There's little value in entities that only appear in one episode.
    if not has_diverse_origins(set(z['entity_origin'].keys())):
        continue

    z['entity_name'] = restore_capitalization(
        e_key, z['entity_sourcetexts'].keys())

    entities_records.append(z)

entities_df = pd.DataFrame.from_records(entities_records)

print("Augmenting final dataframe.")


git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'latest_edits', "Please checkout latest_edits! Currently on %s." % git_branch

page_listing = kfio.load('kf_wiki_content/page_listing.json')
known_missing_pages = kfio.load('data/missing_pages.json')
redirects = kfio.load('data/wiki_redirects.json')

# Only include things we don't have existing knowledge of...
entities_df['is_existing'] = entities_df.entity_name.isin(map(lambda x: x.lower(
), page_listing.title.to_list())) | entities_df.entity_name.isin(page_listing.title.to_list())
entities_df['is_known_missing'] = entities_df.entity_name.isin(map(lambda x: x.lower(
), known_missing_pages.title.to_list())) | entities_df.entity_name.isin(known_missing_pages.title.to_list())

# TODO: Why doesn't this work
entities_df['is_redirect'] = entities_df.entity_name.isin(map(lambda x: x.lower(
), redirects['from'].to_list()))

entities_df['grouped_entity_origin'] = entities_df.entity_origin.apply(
    create_entity_origin_list_mw)

entities_df['starting_char'] = entities_df.entity_name.apply(
    lambda name: name[0].upper())

print("Sorting final result.")
entities_df = entities_df.sort_values(
    'entity_name', key=lambda col: col.str.lower())

print(entities_df)

kfio.save(entities_df, 'data/raw_entities.json')
