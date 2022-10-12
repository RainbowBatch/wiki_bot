import kfio
import pandas as pd
import pandoc
import parse
import re
from box import Box
from collections import Counter
from collections import defaultdict
from entity import extract_entities
from entity import simplify_entity
from pprint import pprint
from pygit2 import Repository
from glob import glob
from tqdm import tqdm

git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'latest_edits', "Please checkout latest_edits! Currently on %s." % git_branch

entities = []

page_listing = kfio.load('kf_wiki_content/page_listing.json')
known_missing_pages = kfio.load('data/missing_pages.json')

print("Processing Wiki Pages")
for page_record in tqdm(page_listing.to_dict(orient='records')):
    page_record = Box(page_record)

    fname = 'kf_wiki_content/%s.wiki' % page_record.slug
    try:

        with open(fname, encoding='utf-8') as f:
            S = f.read()

        if '#redirect' in S:
            # Don't process redirects.
            continue

        # Strip out existing links.
        S = pandoc.write(
            pandoc.read(S, format="mediawiki"),
            format="plain"
        )

        entities.extend(extract_entities(S, page_record.title))
    except Exception as e:
        print(e)
        print("Error Processing", fname)

print("Processing TXT Transcripts")
for transcript_fname in tqdm(glob('transcripts/*.txt')):
    try:
        with open(transcript_fname, encoding='utf-8') as f:
            S = f.read()

        entities.extend(extract_entities(S, transcript_fname))

    except Exception as e:
        print(e)
        print("Error Processing", transcript_fname)

print("Processing SRT Transcripts")
for transcript_fname in tqdm(glob('transcripts/*.srt')):
    try:
        with open(transcript_fname, encoding='utf-8') as f:
            S = f.read()

        entities.extend(extract_entities(S, transcript_fname))

    except Exception as e:
        print(e)
        print("Error Processing", transcript_fname)

counter = Counter()
types = defaultdict(Counter)
origins = defaultdict(set)
sourcetexts = defaultdict(set)

header = [
    "entity_name",
    "entity_count",
    "entity_type",
    "entity_origin",
    "entity_sourcetexts",
]
rows = []

print("Constructing entity counter.")
for s, t, o in tqdm(entities):

    s1 = simplify_entity(s)

    if len(s1) < 10:
        continue

    if '-->' in s1 or 'Alex -' in s1 or ' - ' in s1:
        continue

    if re.search(r"\d{2}:\d{2}:\d{2},\d{3}", s1, re.IGNORECASE):
        continue

    counter.update([s1])
    types[s1].update([t])
    origins[s1].add(o)
    sourcetexts[s1].add(s)


BANNED_TYPES = [
    'DATE',
    'MONEY',
    'TIME',
    'CARDINAL',
    'QUANTITY',
    'PERCENT',
]

print("Constructing final rows")
for s, count in tqdm(counter.most_common()):
    ts = [t for t, _ in types[s].most_common()]
    os = sorted(origins[s])
    sts = sorted(sourcetexts[s])

    # There's little value in entities that only appear in one episode.
    if len(os) <= 1:
        continue

    banned_flag = False
    for banned_t in BANNED_TYPES:
        if banned_t in ts:
            banned_flag = True
    if banned_flag:
        continue

    rows.append([s, count, ts, os, sts])

print("Finalizing and saving data.")

df = pd.DataFrame(rows, columns=header)

# Only include things we don't have existing knowledge of...
df['is_existing'] = df.entity_name.isin(map(lambda x: x.lower(), page_listing.title.to_list()))
df['is_known_missing'] = df.entity_name.isin(map(lambda x: x.lower(), known_missing_pages.title.to_list()))
print("Starting sort.")
df = df.sort_values('entity_name')

print(df)

kfio.save(df, 'data/raw_entities.json')
