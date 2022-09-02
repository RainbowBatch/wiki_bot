import kfio
import pandas as pd
import pandoc

from box import Box
from collections import Counter
from collections import defaultdict
from entity import extract_entities
from entity import simplify_entity
from pprint import pprint
from pygit2 import Repository

git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'latest_edits', "Please checkout latest_edits! Currently on %s." % git_branch

entities = []

page_listing = kfio.load('kf_wiki_content/page_listing.json')
known_missing_pages = kfio.load('data/missing_pages.json')

recognized_entities = page_listing.title.to_list(
) + known_missing_pages.title.to_list()

for page_record in page_listing.to_dict(orient='records'):
    page_record = Box(page_record)

    fname = 'kf_wiki_content/%s.wiki' % page_record.slug
    try:

        with open(fname, encoding='utf-8') as f:
            S = f.read()

        if '#redirect' in S:
            print(fname, "is a redirect")
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

print("Constructing counter.")

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

for s, t, o in entities:
    print("processing 1", s, len(counter))

    s1 = simplify_entity(s)

    if len(s1) < 10:
        continue
    counter.update([s1])
    types[s1].update([t])
    origins[s1].add(o)
    sourcetexts[s1].add(s)

print("Done constructing counter.")

BANNED_TYPES = [
    'DATE',
    'MONEY',
    'TIME',
    'CARDINAL',
]

print("Constructing rows.")

for s, count in counter.most_common():
    print("processing 2", s)
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

print("Done making rows. Making df.")

df = pd.DataFrame(rows, columns=header)

# Only include things we don't have existing knowledge of...
df = df[~df.entity_name.isin(recognized_entities)]
print("Starting sort.")
df = df.sort_values('entity_name')

print(df)

kfio.save(df, 'data/raw_entities.json')
