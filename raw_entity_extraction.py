import en_core_web_sm
import kfio
import pandas as pd
import pandoc
import spacy

from box import Box
from collections import Counter
from collections import defaultdict
from pprint import pprint
from pygit2 import Repository
from spacy import displacy

git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'latest_edits', "Please checkout latest_edits! Currently on %s." % git_branch

nlp = en_core_web_sm.load()

entities = []

page_listing = kfio.load('kf_wiki_content/page_listing.json')
known_missing_pages = kfio.load('data/missing_pages.json')

recognized_entities = page_listing.title.to_list() + known_missing_pages.title.to_list()

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

        doc = nlp(S)

        entities.extend([(X.text, X.label_, page_record.title)
                         for X in doc.ents])
    except Exception as e:
        print(e)
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
    os = sorted(origins[s])

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

# Only include things we don't have existing knowledge of...
df=df[~df.entity_name.isin(recognized_entities)]

print(df)

kfio.save(df, 'data/raw_entities.json')
