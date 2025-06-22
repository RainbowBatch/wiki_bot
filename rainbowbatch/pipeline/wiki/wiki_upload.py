import io
import json
import maya
import pandas as pd
import pywikibot
import re
import urllib.parse

from box import Box
from io import StringIO
from pygit2 import Repository
from tqdm import tqdm

# TODO: This is using pywikibot, but I need to migrate it to mwclient

dry_run = False
force_upload = True


def clean_summary(summary):
    return summary[9:-9]


def canonicalize_title(title):
    return title.replace(' ', '_').replace('#', '').replace('"', '{{QUOTE}}').replace("/", "{{FORWARD_SLASH}}").replace(":", "{{COLON}}").replace("?", "{{QUESTION_MARK}}")


def parse_history_table(page):
    wiki = page.getVersionHistoryTable()
    data = re.sub("^\|.|^!.", "", wiki.replace("|-\n", ""), flags=re.MULTILINE)
    data = '\n'.join(
        list(StringIO(data))[1::2]
    )

    history_table = pd.read_csv(StringIO(data), delimiter="\|\||!!")

    history_table = history_table.rename(columns=lambda x: x.strip())

    history_table['username'] = history_table['username'].apply(
        lambda x: x.strip())

    history_table['time'] = history_table['date/time'].apply(maya.parse)

    history_table['summary'] = history_table['edit summary'].apply(
        clean_summary)

    return history_table.drop(columns=['date/time', 'edit summary'])


git_branch = Repository('../kf_wiki_content/').head.shorthand.strip()

assert git_branch == 'pre-upload', "Please checkout pre-upload! Currently on %s." % git_branch

site = pywikibot.Site()

# Ensure the user is logged in
if not site.user():
    print("User not logged in. Attempting to log in...")
    site.login()
    if not site.user():
        raise Exception(
            "Login failed. Please check your credentials or configuration.")
else:
    print(f"Logged in as: {site.user()}")

page_listing = pd.read_json(
    '../kf_wiki_content/page_listing.json',
    orient='records',
)

for page_record in tqdm(page_listing.to_dict(orient='records')):
    if 'oldid' not in page_record:
        page_record['oldid'] = None
    if pd.isna(page_record['oldid']):
        page_record['oldid'] = None
    page_record = Box(page_record)

    # if page_record.slug  == 'RainbowBatch_Generated_Entity_Listing':
    #    continue # This page is too large right now.

    with open('../kf_wiki_content/%s.wiki' % page_record.slug, encoding='utf-8') as f:
        new_page_text = f.read()

    page = pywikibot.Page(site, page_record.title)

    if page.exists():
        if page.text.strip() == new_page_text.strip():
            continue
        history_table = parse_history_table(page)

        if history_table.oldid.iloc[0] != page_record.oldid:
            print("Conflict on '%s'! Expected to see oldid=%s, but the actual state is oldid=%s. Re-merge and try again." % (
                page_record.title,
                page_record.oldid,
                history_table.oldid.iloc[0],
            ))
            continue
    else:
        if page_record.oldid is not None and not force_upload:
            print("Trying to update '%s', but it doesn't exist! Probably deleted?" %
                  page_record.title)
            continue  # TODO(woursler): Message!

    if page_record.title.lower() == "infowars" or page_record.title.lower() == "ron desantis":
        print("SKIPPING %s BECAUSE OF CASE ISSUE" % page_record.title)
        continue

    print("Updating '%s'." % page_record.title)

    if not dry_run:
        page.text = new_page_text
        page.save("RainbowBatch update" if page.exists()
                  else "RainbowBatch generated stub.")
