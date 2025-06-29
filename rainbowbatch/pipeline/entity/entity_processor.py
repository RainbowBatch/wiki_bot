import rainbowbatch.kfio as kfio
import pandas as pd
import pathlib
import re

from collections import Counter
from collections import defaultdict
from rainbowbatch.entity.entity import LIKELY_PEOPLE
from rainbowbatch.entity.entity import NOT_RELEVANT_PEOPLE
from rainbowbatch.entity.entity import create_entity_origin_list_mw
from rainbowbatch.entity.entity import restore_capitalization
from rainbowbatch.entity.entity import simplify_entity
from natsort import natsorted
from rainbowbatch.git import check_git_branch
from tqdm import tqdm


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


PROTO_ENTITIES_PATH = kfio.TOP_LEVEL_DIR / 'data/proto_entities.json'

grouped_proto_entities = defaultdict(list)

print("Re-shuffling entities.")
for _, origin, proto_entities in tqdm(list(kfio.load(PROTO_ENTITIES_PATH).itertuples())):
    origin = tuple(origin)
    for proto_entity in proto_entities:
        e_key = simplify_entity(proto_entity['entity_name'])
        grouped_proto_entities[e_key].append(proto_entity)

print("Grouping and filtering entities.")
entities_records = []

finalized_entity_names = dict()

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

    z['raw_entity_name'] = e_key
    z['entity_name'] = restore_capitalization(
        e_key, z['entity_sourcetexts'].keys())

    finalized_entity_names[e_key] = z['entity_name']

    entities_records.append(z)

entities_df = pd.DataFrame.from_records(entities_records)

print("Augmenting final dataframe.")

assert check_git_branch('latest_edits'), "Please checkout latest_edits! Currently on %s." % git_branch

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


grouped_by_episode = defaultdict(lambda: defaultdict(list))

print("Re-shuffling entities by episode.")
for _, origin, proto_entities in tqdm(list(kfio.load(PROTO_ENTITIES_PATH).itertuples())):
    origin = tuple(origin.split('__'))
    for proto_entity in proto_entities:
        episode_number = origin[0]
        if episode_number is None or episode_number == 'None':
            continue
        entity_name = simplify_entity(proto_entity['entity_name'])

        if entity_name not in finalized_entity_names:
            continue

        grouped_by_episode[episode_number][entity_name].append(proto_entity)

per_episode_results = dict()

for episode_number, entity_map in grouped_by_episode.items():
    per_episode_results[episode_number] = dict()
    per_episode_results[episode_number]['raw_entities'] = list()
    per_episode_results[episode_number]['people'] = list()
    for entity_name, proto_entities in entity_map.items():
        z = group_proto_entities(proto_entities)
        z['entity_name'] = finalized_entity_names[entity_name]

        per_episode_results[episode_number]['raw_entities'].append(z)

        clean_entity_name = z['entity_name']

        if clean_entity_name in NOT_RELEVANT_PEOPLE:
            continue
        if clean_entity_name not in LIKELY_PEOPLE:
            continue
        if 'PERSON' not in z['entity_type']:
            continue
        if max(z['entity_origin'].values()) < 3:
            continue

        per_episode_results[episode_number]['people'].append(z['entity_name'])

kfio.save_json(per_episode_results, 'data/raw_entities_per_episode.json')

# TODO: A ton of people are missing from this for some reason.
nlp_guests = [
    {
        "episode_number": episode_number,
        "people": results['people']
    }
    for episode_number, results in per_episode_results.items()
]

nlp_guests = natsorted(nlp_guests, key=lambda x: x["episode_number"])

kfio.save_json(nlp_guests, 'data/nlp_guests.json')
