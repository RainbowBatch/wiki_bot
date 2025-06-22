import io
import json
import maya
import natsort
import pandas as pd
import re
import time
import urllib.parse

from io import StringIO
from tqdm import tqdm

import rainbowbatch.kfio as kfio
from rainbowbatch.external.kfwiki import make_kfwiki_client, hard_reset_site
from rainbowbatch.git import check_git_branch, check_has_uncommitted_git_changes


# TODO: Some of this should be in remap!
def clean_summary(summary):
    return summary[9:-9]

# TODO: Unify this with the instance in rainbowbatch/pipeline/episode_processor.py


def canonicalize_title(title):
    return title.replace(' ', '_').replace('#', '').replace('"', '{{QUOTE}}').replace("/", "{{FORWARD_SLASH}}").replace(":", "{{COLON}}").replace("?", "{{QUESTION_MARK}}")


# TODO: This should maybe go in it's own file.
def parse_history_table(page):
    wiki = page.getVersionHistoryTable()
    data = re.sub(r"^\|.|^!.", "", wiki.replace("|-\n", ""), flags=re.MULTILINE)
    data = '\n'.join(
        list(StringIO(data))[1::2]
    )

    history_table = pd.read_csv(
        StringIO(data), delimiter=r"\|\||!!", engine="python")

    history_table = history_table.rename(columns=lambda x: x.strip())

    history_table['username'] = history_table['username'].apply(
        lambda x: x.strip())

    history_table['time'] = history_table['date/time'].apply(maya.parse)

    history_table['summary'] = history_table['edit summary'].apply(
        clean_summary)

    return history_table.drop(columns=['date/time', 'edit summary'])

site = make_kfwiki_client()

assert check_git_branch(
    'latest_edits'), "Please checkout latest_edits! Currently on %s." % git_branch
# TODO: This doesn't seem to work?
assert check_has_uncommitted_git_changes(
), "Please commit! Uncommitted changes may be overwritten."

PAGE_RECORDS = []

for page in tqdm(site.allpages()):
    for attempt in range(3):  # retry up to 3 times
        try:
            title = page.title()

            canonicalized_title = urllib.parse.unquote(
                page.full_url().split("/")[-1])
            text = page.text
            # process page.title() and page.editTime()
            assert canonicalize_title(canonicalized_title) == canonicalize_title(
                title), (title, canonicalized_title)

            history_table = parse_history_table(page)
            super_canonicalized_title = canonicalize_title(canonicalized_title)

            PAGE_RECORDS.append({
                'title': title,
                'slug': super_canonicalized_title,
                'oldid': history_table.oldid.iloc[0]
            })

            with io.open(kfio.TOP_LEVEL_DIR / 'kf_wiki_content' / ("%s.wiki" % super_canonicalized_title), mode="w", encoding="utf-8") as f:
                f.write(text.strip() + '\n')

            kfio.save(history_table, "kf_wiki_content/%s.history.json" %
                      super_canonicalized_title)
            break
        except:
            print(f"[Retry {attempt+1}] Login/session failed — attempting relogin")
            site = hard_reset_site(site)
            time.sleep(2)



page_records_df = pd.DataFrame.from_records(PAGE_RECORDS)


page_records_df = page_records_df.sort_values(
    by=['title'],
    key=natsort.natsort_keygen(),
)

kfio.save(page_records_df,  'kf_wiki_content/page_listing.json')
