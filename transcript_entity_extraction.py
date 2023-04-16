import kfio
import pandas as pd
import parse
import natsort

from collections import Counter
from collections import defaultdict
from entity import LIKELY_PEOPLE
from entity import NOT_RELEVANT_PEOPLE
from entity import extract_entities
from entity import simplify_entity
from glob import glob
from pprint import pprint
from pathlib import Path
from natsort import natsorted

table_header = ['episode_number', 'people']
transcript_guest_names = []

OVERUSED = [
    'Alex Jones',
    'Jordan Holmes',
    'Dan Friesen',
]

for transcript_fname in natsorted(glob(str(kfio.TRANSCRIPT_DIR / '*.otter.txt'))):
    episode_number = parse.parse(
        "{}.otter.txt", Path(transcript_fname).name)[0]

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

            s_is_person = s in LIKELY_PEOPLE

            if 'PERSON' in ts and count > 3:
                s = s.title()
                s_is_person = True

            if s in NOT_RELEVANT_PEOPLE:
                s_is_person = False

            rows.append([s, count, ts, s_is_person])

        pprint(rows)
        likely_guests = [name for name, _, _,
                         is_valid in rows if is_valid and name not in OVERUSED]
        transcript_guest_names.append([episode_number, likely_guests])

    except Exception as e:
        print(e)
        print("Error Processing", transcript_fname)


df = pd.DataFrame(transcript_guest_names, columns=table_header)
df = df.sort_values(by=['episode_number'], key=natsort.natsort_keygen())

kfio.save(df, kfio.DATA_DIR / 'nlp_guests.json')
