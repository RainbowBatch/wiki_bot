import rainbowbatch.kfio as kfio
import natsort
import numpy as np
import pandas as pd

page_listing = kfio.load('kf_wiki_content/page_listing.json')
page_listing.oldid = page_listing.oldid.astype('Int64')


def lookup(slug):
    for _, row in page_listing.iterrows():
        if row['slug'] == slug:
            return row
    return None


def add(title, slug):
    global page_listing

    # Use pd.concat instead of append
    new_row = pd.DataFrame([{'title': title, 'slug': slug}])
    page_listing = pd.concat([page_listing, new_row], ignore_index=True)
    page_listing = page_listing.drop_duplicates(subset='slug', keep="last")


def add_all(other_page_listing):
    global page_listing

    page_listing = pd.concat(
        [page_listing, other_page_listing], ignore_index=True)
    page_listing = page_listing.drop_duplicates(subset='slug', keep="last")


def save():
    global page_listing
    page_listing = page_listing.sort_values(
        by=['title'],
        key=natsort.natsort_keygen(),
    )

    print(page_listing)

    kfio.save(page_listing, 'kf_wiki_content/page_listing.json')


def repair():
    global page_listing
    page_listing = page_listing.sort_values(
        ['title', 'oldid'],
        ascending=[True, True],
        na_position='first',
    )
    page_listing = page_listing.drop_duplicates(subset='slug', keep="last")


if __name__ == '__main__':
    repair()
    save()
