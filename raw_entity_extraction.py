import en_core_web_sm
import glob
import kfio
import pandas as pd
import pandoc
import spacy

from collections import Counter
from collections import defaultdict
from pprint import pprint
from spacy import displacy

nlp = en_core_web_sm.load()

entities = []

for fname in glob.glob('sample_pages/*.wiki', recursive=False):
    try:

        with open(fname) as f:
            S = f.read()

        # Strip out existing links.
        S = pandoc.write(
            pandoc.read(S, format="mediawiki"),
            format="plain"
        )

        doc = nlp(S)

        entities.extend([(X.text, X.label_, fname) for X in doc.ents])
    except:
        print("Error Processing", fname)


def simplify(s):
    s = ' '.join(s.split())
    if s.endswith("'s"):
        return s[:-2]
    return s


def extract_episode_number(s):
    return s.split('\\')[-1].split('_')[0]


counter = Counter()
types = defaultdict(Counter)
origins = defaultdict(set)

header = [
    "entity_name",
    "entity_count",
    "entity_type",
    "entity_origin",
]
rows = []

for s, t, o in entities:
    s = simplify(s)

    if '-' in s:
        continue
    if len(s) < 10:
        continue
    counter.update([s])
    types[s].update([t])
    origins[s].add(extract_episode_number(o))

BANNED_TYPES = [
    'DATE',
    'MONEY',
    'TIME',
    'CARDINAL',
]

for s, count in counter.most_common():
    ts = [t for t, _ in types[s].most_common()]
    os = origins[s]

    # There's little value in entities that only appear in one episode.
    if len(os) <= 1:
        continue

    banned_flag = False
    for banned_t in BANNED_TYPES:
        if banned_t in ts:
            banned_flag = True
    if banned_flag:
        continue

    rows.append([s, count, ts, os])

df = pd.DataFrame(rows, columns=header)

print(df)

kfio.save(df, 'data/raw_entities.json')
