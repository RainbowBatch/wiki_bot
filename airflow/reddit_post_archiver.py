# Migrated from reddit_post_downloader
# TODO: Confirm working.
# TODO: Move meaty parts of this back into rainbowbatch.

import json
import logging
import natsort
import pandas as pd
import praw
import rainbowbatch.kfio as kfio
import requests
import time
import urllib.parse

from airflow import DAG
from airflow.operators.python import PythonOperator
from box import Box
from datetime import datetime
from rainbowbatch.external.reddit import make_reddit_client

# TODO: Use tqdm_loggable and swap back to using tqdm.

# TODO: Update to airflow variables or other similar things.
EXISTING_POSTS_FILE = "sensitive/all_reddit_posts.json"
DISCUSSIONS_FILE = "data/reddit_episode_discussions.json"
FINAL_DATA_FILE = "data/final.json"

# /tmp/ files.
# TODO: I'd prefer not to have these, or make them more robust.
MOST_RECENT_POSTS_FILE = "/tmp/most_recent_posts.json"
GOOGLE_POST_IDS_FILE = "/tmp/google_post_ids.json"
SEARCH_BASED_POSTS_FILE = "/tmp/search_based_posts.json"
USER_BASED_POSTS_FILE = "/tmp/user_based_posts.json"
LIBSYN_LINKS_FILE = "/tmp/libsyn_links.json"


def download_recent_posts(**context):
    reddit = make_reddit_client()
    existing_df = kfio.load(EXISTING_POSTS_FILE)
    existing_post_ids = set(existing_df.PostIds)

    subreddit = reddit.subreddit('KnowledgeFight')

    titles, links, timestamps, post_ids2 = [], [], [], []

    logging.info("Downloading recent posts.")
    for post in subreddit.new(limit=1000):
        titles.append(post.title)
        timestamps.append(post.created_utc)
        post_ids2.append(post.id)
        links.append(None if post.is_self else post.url)

    data = {'Title': titles, 'Link': links,
            "Timestamps": timestamps, 'PostIds': post_ids2}
    most_recent_posts_df = pd.DataFrame(data)
    most_recent_posts_df.to_json(
        MOST_RECENT_POSTS_FILE, orient="records")


def scrape_google_posts(**context):
    from bs4 import BeautifulSoup

    discussion_post_df = kfio.load(DISCUSSIONS_FILE)
    known_discussed_episodes = set(discussion_post_df.episode_number)
    max_episode = max([int(x)
                       for x in known_discussed_episodes if x.isdigit()])

    result_urls = set()
    logging.info("Scraping Google for posts.")
    for n in range(1, max_episode + 1):
        n_str = str(n)
        if n_str in known_discussed_episodes:
            continue
        logging.info("Sending request for episode %d" % n)
        response = requests.get(
            "https://www.google.com/search?q=knowledge+fight+reddit+%23" + n_str)
        soup = BeautifulSoup(response.content, "html.parser")
        results = soup.find_all("h3")
        for result in results:
            url = result.find_parent('a')['href']
            query_string = urllib.parse.parse_qs(
                urllib.parse.urlparse(url).query)['q'][0]
            result_urls.add(query_string)

    post_ids = [url.split(
        '/')[6] for url in result_urls if 'https://www.reddit.com/r/KnowledgeFight/comments/' in url]
    with open(GOOGLE_POST_IDS_FILE, "w") as f:
        json.dump(post_ids, f)


def process_google_posts(**context):
    reddit = make_reddit_client()
    existing_df = kfio.load(EXISTING_POSTS_FILE)
    existing_post_ids = set(existing_df.PostIds)

    with open(GOOGLE_POST_IDS_FILE) as f:
        post_ids = json.load(f)

    titles, links, timestamps, post_ids2 = [], [], [], []

    logging.info("Processing scraped google results.")
    for post_id in post_ids:
        if post_id in existing_post_ids:
            continue
        post = reddit.submission(id=post_id)
        titles.append(post.title)
        timestamps.append(post.created_utc)
        post_ids2.append(post.id)
        links.append(None if post.is_self else post.url)

    data = {'Title': titles, 'Link': links,
            "Timestamps": timestamps, 'PostIds': post_ids2}
    search_based_df = pd.DataFrame(data)
    search_based_df.to_json(SEARCH_BASED_POSTS_FILE, orient="records")


def user_post_download(**context):
    reddit = make_reddit_client()
    existing_df = kfio.load(EXISTING_POSTS_FILE)
    existing_post_ids = set(existing_df.PostIds)

    # Load dataframes
    search_based_df = pd.read_json(SEARCH_BASED_POSTS_FILE)
    most_recent_posts_df = pd.read_json(MOST_RECENT_POSTS_FILE)

    posters = set()
    combined_post_ids = pd.concat(
        [search_based_df, most_recent_posts_df, existing_df]).PostIds
    logging.info("Identifying posters.")
    for post_id in combined_post_ids:
        if post_id in existing_post_ids:
            continue
        time.sleep(1)
        post = reddit.submission(id=post_id)
        posters.add(str(post.author))

    ids = []
    logging.info("Looking up posts by subreddit users.")
    for poster in posters:
        try:
            user = reddit.redditor(poster)
            for post in user.submissions.new(limit=None):
                if post.subreddit != 'KnowledgeFight':
                    continue
                ids.append(post.id)
        except Exception:
            continue

    titles, links, timestamps, post_ids2 = [], [], [], []

    logging.info("Processing user posts.")
    for post_id in ids:
        if post_id in existing_post_ids:
            continue
        time.sleep(1)
        post = reddit.submission(id=post_id)
        titles.append(post.title)
        timestamps.append(post.created_utc)
        post_ids2.append(post.id)
        links.append(None if post.is_self else post.url)

    data = {'Title': titles, 'Link': links,
            "Timestamps": timestamps, 'PostIds': post_ids2}
    user_based_df = pd.DataFrame(data)
    user_based_df.to_json(USER_BASED_POSTS_FILE, orient="records")


def merge_and_save(**context):
    existing_df = kfio.load(EXISTING_POSTS_FILE)
    search_based_df = pd.read_json(SEARCH_BASED_POSTS_FILE)
    most_recent_posts_df = pd.read_json(MOST_RECENT_POSTS_FILE)
    user_based_df = pd.read_json(USER_BASED_POSTS_FILE)

    merged_df = pd.concat([search_based_df, most_recent_posts_df, user_based_df, existing_df])\
        .drop_duplicates(subset=['PostIds'])\
        .sort_values(by=['PostIds'])

    kfio.save(merged_df, EXISTING_POSTS_FILE)


def process_libsyn_links(**context):
    merged_df = kfio.load(EXISTING_POSTS_FILE)
    libsyn_links = merged_df[merged_df['Link'].str.contains(
        'knowledgefight.libsyn.com').fillna(False)]

    def link_fixer(link):
        return link.replace("http://", "https://").replace("/podcast/", "/")

    libsyn_links['Link'] = libsyn_links['Link'].apply(link_fixer)
    libsyn_links.to_json(LIBSYN_LINKS_FILE, orient="records")


def resolve_reddit_ids(**context):
    reddit = make_reddit_client()
    libsyn_links = pd.read_json(LIBSYN_LINKS_FILE)
    final_data = kfio.load(FINAL_DATA_FILE)

    def resolve_reddit_id(rid):
        submission = reddit.submission(rid)
        return 'https://www.reddit.com' + submission.permalink

    episode_discussions = pd.merge(
        libsyn_links, final_data,
        left_on='Link', right_on='libsyn_page'
    )[['episode_number', 'PostIds', 'Link']]

    episode_discussions = episode_discussions.sort_values(
        by=['episode_number'], key=natsort.natsort_keygen()
    )

    episode_discussions['permalink'] = episode_discussions.PostIds.apply(
        resolve_reddit_id)
    kfio.save(episode_discussions, DISCUSSIONS_FILE)


with DAG(
    "reddit_post_archiver",
    start_date=datetime(2025, 6, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    download_recent_posts_task = PythonOperator(
        task_id="download_recent_posts",
        python_callable=download_recent_posts
    )

    scrape_google_posts_task = PythonOperator(
        task_id="scrape_google_search_results",
        python_callable=scrape_google_posts
    )

    process_google_posts_task = PythonOperator(
        task_id="process_google_scraped_posts",
        python_callable=process_google_posts
    )

    user_post_download_task = PythonOperator(
        task_id="download_posts_by_users",
        python_callable=user_post_download
    )

    merge_and_save_task = PythonOperator(
        task_id="merge_and_save_all_posts",
        python_callable=merge_and_save
    )

    process_libsyn_links_task = PythonOperator(
        task_id="process_libsyn_links",
        python_callable=process_libsyn_links
    )

    resolve_reddit_ids_task = PythonOperator(
        task_id="resolve_reddit_post_permalinks",
        python_callable=resolve_reddit_ids
    )

    scrape_google_posts_task >> process_google_posts_task
    [download_recent_posts_task, process_google_posts_task] >> user_post_download_task
    user_post_download_task >> merge_and_save_task >> process_libsyn_links_task >> resolve_reddit_ids_task
