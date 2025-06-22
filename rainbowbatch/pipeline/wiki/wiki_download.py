import io
import mwclient
import natsort
import pandas as pd
import rainbowbatch.kfio as kfio
import time
import urllib.parse

from datetime import datetime
from rainbowbatch.external.kfwiki import make_kfwiki_client
from rainbowbatch.git import check_git_branch
from rainbowbatch.git import check_has_uncommitted_git_changes
from tqdm import tqdm


def canonicalize_title(title):
    return title.replace(' ', '_').replace('#', '').replace('"', '{{QUOTE}}')\
                .replace("/", "{{FORWARD_SLASH}}").replace(":", "{{COLON}}")\
                .replace("?", "{{QUESTION_MARK}}")


def parse_history_table(page):
    history = []
    for rev in page.revisions():
        history.append({
            'oldid': rev['revid'],
            'username': rev['user'],
            'summary': rev.get('comment', ''),
            'time': datetime.fromtimestamp(time.mktime(rev['timestamp'])).isoformat()
        })

    return pd.DataFrame(history)


site = make_kfwiki_client()

assert check_git_branch(
    'latest_edits'), "Please checkout latest_edits! Currently on wrong branch"
assert check_has_uncommitted_git_changes(
), "Please commit! Uncommitted changes may be overwritten."

PAGE_RECORDS = []
for mw_page in tqdm(site.allpages()):
    title = mw_page.name
    text = mw_page.text()

    canonicalized_title = urllib.parse.unquote(mw_page.name)
    assert canonicalize_title(canonicalized_title) == canonicalize_title(
        title), (title, canonicalized_title)

    history_table = parse_history_table(mw_page)
    super_canonicalized_title = canonicalize_title(canonicalized_title)

    PAGE_RECORDS.append({
        'title': title,
        'slug': super_canonicalized_title,
        'oldid': history_table.oldid.iloc[0] if not history_table.empty else None,
    })

    with io.open(kfio.TOP_LEVEL_DIR / 'kf_wiki_content' / f"{super_canonicalized_title}.wiki", mode="w", encoding="utf-8") as f:
        f.write(text.strip() + '\n')

    kfio.save(history_table, f"kf_wiki_content/{super_canonicalized_title}.history.json")

# Sort and save the index
page_records_df = pd.DataFrame.from_records(PAGE_RECORDS)

page_records_df = page_records_df.sort_values(
    by=['title'],
    key=natsort.natsort_keygen(),
)

kfio.save(page_records_df, 'kf_wiki_content/page_listing.json')
