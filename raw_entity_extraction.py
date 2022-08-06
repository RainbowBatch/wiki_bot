import spacy
from spacy import displacy
from collections import Counter, defaultdict
import en_core_web_sm
from pprint import pprint
import pandoc
import glob
import pandas as pd

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
    "entity_type_slug",
    "entity_origin_slug",
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

	t_slug = ';'.join(ts)
	o_slug = ';'.join(os)
	rows.append([s, count, t_slug, o_slug])

df = pd.DataFrame(rows, columns=header)

print(df)

with open("raw_entities.csv", "w") as csv_file:
    csv_file.write(df.to_csv(index=False, line_terminator='\n'))