import kfio
import pandas as pd
import numpy as np

page_listing = kfio.load('kf_wiki_content/page_listing.json')
page_listing.oldid = page_listing.oldid.astype('Int64')


def add(title, slug):
    global page_listing

    page_listing = page_listing.append(
        {'title': title, 'slug': slug}, ignore_index=True)
    page_listing = page_listing.drop_duplicates(subset='slug', keep="last")

def add_all(other_page_listing):
    global page_listing
    page_listing = pd.concat([page_listing, other_page_listing])

    page_listing = page_listing.drop_duplicates(subset='slug', keep="last")


def save():
    global page_listing
    page_listing = page_listing.sort_values('title')

    print(page_listing)

    kfio.save(page_listing, 'kf_wiki_content/page_listing.json')

def repair():
    global page_listing
    page_listing = page_listing.sort_values(['title', 'oldid'], ascending = [True, True], na_position='first')
    page_listing = page_listing.drop_duplicates(subset='slug', keep="last")

if __name__=='__main__':
    repair()
    save()

