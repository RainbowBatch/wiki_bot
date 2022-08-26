import en_core_web_sm
import kfio
import pandas as pd
import pandoc
import spacy
import re
from box import Box
from collections import Counter
from collections import defaultdict
from pprint import pprint
from pygit2 import Repository
from spacy import displacy
from glob import glob
import parse

nlp = en_core_web_sm.load()

entities = []

page_listing = kfio.load('kf_wiki_content/page_listing.json')

scraped_pages = kfio.load('data/scraped_page_data.json')

S = set(['People'])
redirects = scraped_pages[~scraped_pages.redirect.isna()]
existing_people = scraped_pages[scraped_pages.redirect.isna() & scraped_pages.wiki_categories.map(S.issubset)]

missing_people = kfio.load('data/missing_pages.json')

REMAPPING = {
    'Donald Trump\u200f\u200e': 'Donald Trump',
    'Rhonda Santas': 'Ron DeSantis',
    'Owen Troyer': 'Owen Schroyer',
    'Meghan Kelly': 'Megyn Kelly',
    'Alex E. Jones': 'Alex Jones',
    'Wolfgang Halbeck': 'Wolfgang Halbig',
    'Howard Stearn': 'Howard Stern',
    'Dan Bodandy': 'Dan Bidondi',
    'Dan Vedandi': 'Dan Bidondi',
    'Dan Vidanti': 'Dan Bidondi',
    'Dan Bodandi': 'Dan Bidondi',
    'Neil Hesleyn': 'Neil Heslin',
    'Alexander Dugan': 'Alexander Dugin',
    'Bobby Barnes': 'Robert Barnes',
}

OVERUSED = [
    'Alex Jones',
    'Jordan Holmes',
    'Dan Friesen',
]

for redirect in redirects.to_dict(orient='records'):
    REMAPPING[redirect['title']] = redirect['redirect']

# These are names that have non-misspelling redirects.
del REMAPPING['Neil Heslin']
del REMAPPING['Scarlett Lewis']
del REMAPPING['Adam Lanza']

valid_outputs = set(
    existing_people.title.to_list() + missing_people.title.to_list() + [
        'Alex Lee Moyer',
    ]
)

def simplify(s):
    s=s.strip()
    s = ' '.join(s.split())
    if s.endswith("'s"):
        s = s[:-2]
    if s in REMAPPING:
        s = REMAPPING[s]
    return s


table_header = ['episode_number', 'guests']
transcript_guest_names = []

for transcript_fname in glob('transcripts/*.txt'):
    episode_number = parse.parse("transcripts\\{}.txt", transcript_fname)[0]

    try:
        with open(transcript_fname, encoding='utf-8') as f:
            S = f.read()

        if '#redirect' in S:
            print(transcript_fname, "is a redirect")
            continue

        doc = nlp(S)

        counter = Counter()
        types = defaultdict(Counter)

        for s, t in [(X.text, X.label_) for X in doc.ents]:
            s = simplify(s)

            if '-' in s:
                continue

            if len(s) < 10:
                continue

            counter.update([s])
            types[s].update([t])

        BANNED_TYPES = [
            'DATE',
            'MONEY',
            'TIME',
            'CARDINAL',
            'PERCENT',
        ]

        rows = []
        for s, count in counter.most_common():
            ts = [t for t, _ in types[s].most_common()]

            banned_flag = False
            for banned_t in BANNED_TYPES:
                if banned_t in ts:
                    banned_flag = True
            if banned_flag:
                continue

            if count < 2:
                continue

            rows.append([s, count, ts, s in valid_outputs])

        pprint(rows)
        likely_guests = [name for name, _, _, is_valid in rows if is_valid and name not in OVERUSED]
        transcript_guest_names.append([episode_number, likely_guests])

    except Exception as e:
        print(e)
        print("Error Processing", transcript_fname)


df = pd.DataFrame(transcript_guest_names, columns=table_header)

kfio.save(df, 'data/nlp_guests.json')