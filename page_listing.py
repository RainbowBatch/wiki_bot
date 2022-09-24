import kfio
import pandas as pd

page_listing = kfio.load('kf_wiki_content/page_listing.json')


def add(title, slug):
    global page_listing
    print("Adding ", title, slug)

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
