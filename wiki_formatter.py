import en_core_web_sm
import rainbowbatch.kfio as kfio
import pandas as pd
import spacy

from box import Box
from collections import Counter
from collections import defaultdict
from pprint import pprint
from pygit2 import Repository
from spacy import displacy
from rainbowbatch.remap.wiki_cleaner import simple_format
from tqdm import tqdm

# TODO: Breaks "The Dreamy Creamy Summer" article?

git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'pre-upload', "Please checkout pre-upload! Currently on %s." % git_branch

nlp = en_core_web_sm.load()

entities = []

page_listing = kfio.load('kf_wiki_content/page_listing.json')

for page_record in tqdm(page_listing.to_dict(orient='records')):
    page_record = Box(page_record)

    if 'Dreamy Creamy Summer' in page_record.title:
        continue

    if 'Transcript' in page_record.title:
        continue

    fname = 'kf_wiki_content/%s.wiki' % page_record.slug
    try:
        with open(fname, encoding='utf-8') as f:
            page_text = f.read()

        if '#redirect' in page_text.lower():
            formatted_page_text = page_text.replace(u'\u200f', '').replace(u'\u200e', '').strip()
            assert len(formatted_page_text.split('\n')) == 1
        else:
            formatted_page_text = simple_format(page_text)

        if page_text.strip() != formatted_page_text.strip():
            with open(fname, "w", encoding='utf-8') as f:
                f.write(formatted_page_text)
    except:
        print("Problem formatting", fname)
