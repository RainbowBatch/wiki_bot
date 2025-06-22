import io
import json
import mwclient
import pandas as pd
import rainbowbatch.kfio as kfio
import re
import time
import urllib.parse

from box import Box
from datetime import datetime
from io import StringIO
from pygit2 import Repository
from rainbowbatch.external.kfwiki import make_kfwiki_client, ensure_logged_in
from rainbowbatch.git import check_git_branch
from tqdm import tqdm

dry_run = False
force_upload = True


assert check_git_branch(
    'pre-upload'), "Please checkout pre-upload! Currently on wrong branch"


site = make_kfwiki_client()

# Load page listing
page_listing = pd.read_json(kfio.TOP_LEVEL_DIR /
                            'kf_wiki_content/page_listing.json', orient='records')

for page_record in tqdm(page_listing.to_dict(orient='records')):
    page_record = Box(page_record)
    page_record.oldid = page_record.get('oldid') or None

    with open(kfio.TOP_LEVEL_DIR / f"kf_wiki_content/{page_record.slug}.wiki", encoding='utf-8') as f:
        new_page_text = f.read().strip()

    page = site.pages[page_record.title]

    if page.exists:
        existing_text = page.text().strip()
        if existing_text == new_page_text:
            continue

        revisions = list(page.revisions(limit=1, dir='older'))
        if revisions and revisions[0]['revid'] != page_record.oldid:
            print(f"Conflict on '{page_record.title}'! Expected oldid={page_record.oldid}, but got {revisions[0]['revid']}.")
            continue
    else:
        if page_record.oldid and not force_upload:
            print(f"Trying to update '{page_record.title}', but it doesn't exist! Probably deleted?")
            continue

    if page_record.title.lower() in ("infowars", "ron desantis"):
        print(f"SKIPPING {page_record.title} BECAUSE OF CASE ISSUE")
        continue

    print(f"Updating '{page_record.title}'")

    if not dry_run:
        ensure_logged_in(site)
        page.save(
            new_page_text, summary="RainbowBatch update" if page.exists else "RainbowBatch generated stub.")
