import kfio
import pandas as pd
import parse

from collections import Counter
from collections import defaultdict
from entity import LIKELY_PEOPLE
from entity import extract_entities
from entity import simplify_entity
from glob import glob
from pprint import pprint


table_header = ['episode_number', 'guests']
transcript_guest_names = []

OVERUSED = [
    'Alex Jones',
    'Jordan Holmes',
    'Dan Friesen',
]

for transcript_fname in glob('transcripts/*.txt'):
    episode_number = parse.parse("transcripts\\{}.txt", transcript_fname)[0]

    print(episode_number)

    try:
        with open(transcript_fname, encoding='utf-8') as f:
            S = f.read()

        if '#redirect' in S:
            print(transcript_fname, "is a redirect")
            continue

        counter = Counter()
        types = defaultdict(Counter)

        for s, t, _ in extract_entities(S, None):
            s = simplify_entity(s)

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

            rows.append([simplify_entity(s), count, ts, s in LIKELY_PEOPLE])

        pprint(rows)
        likely_guests = [name for name, _, _,
                         is_valid in rows if is_valid and name not in OVERUSED]
        transcript_guest_names.append([episode_number, likely_guests])

    except Exception as e:
        print(e)
        print("Error Processing", transcript_fname)


df = pd.DataFrame(transcript_guest_names, columns=table_header)

kfio.save(df, 'data/nlp_guests.json')
