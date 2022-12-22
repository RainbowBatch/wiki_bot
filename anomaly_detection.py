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
from wiki_cleaner import simple_format

# TODO(woursler): Breaks "The Dreamy Creamy Summer" article?

git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'pre-upload', "Please checkout pre-upload! Currently on %s." % git_branch

nlp = en_core_web_sm.load()

entities = []

page_listing = kfio.load('kf_wiki_content/page_listing.json')
known_missing_pages = kfio.load('data/missing_pages.json')

recognized_entities = page_listing.title.to_list() + known_missing_pages.title.to_list()

for page_record in page_listing.to_dict(orient='records'):
    page_record = Box(page_record)

    if 'Dreamy Creamy Summer' in page_record.title:
        continue

    fname = 'kf_wiki_content/%s.wiki' % page_record.slug
    #try:
    with open(fname, encoding='utf-8') as f:
        page_text = f.read()


    if '#redirect' in page_text.lower():
        if len(page_text.strip().split('\n')) > 1:
            print(fname, "is a problematic redirect")

    #except:
    #    print("Problem processing", fname)
