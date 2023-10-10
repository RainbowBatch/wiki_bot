from box import Box
from bs4 import BeautifulSoup
import json
import kfio
import natsort
import pandas as pd
import praw
import requests
import urllib.parse
import time
from tqdm import tqdm

with open("secrets/reddit.json") as secrets_f:
    secrets = Box(json.load(secrets_f))

    reddit = praw.Reddit(
        user_agent='RainbowBatch',
        client_id=secrets.client_id,
        client_secret=secrets.client_secret,
        username='RainbowBatch',
        password=secrets.bot_password,
    )


existing_df = kfio.load('sensitive/all_reddit_posts.json')
existing_post_ids = set(existing_df.PostIds)

discussion_post_df = kfio.load('sensitive/reddit_episode_discussions.json')
known_discussed_episodes = set(discussion_post_df.episode_number)

# Define the subreddit you want to scrape
subreddit_name = 'KnowledgeFight'

# Get the subreddit
subreddit = reddit.subreddit(subreddit_name)

# Initialize lists to store titles and links
titles = []
links = []
timestamps = []
post_ids2 = []

# Iterate through the subreddit posts and extract titles and links
for post in tqdm(subreddit.new(limit=None), total=1000, desc="Downloading recent posts."):
    titles.append(post.title)
    timestamps.append(post.created_utc)
    post_ids2.append(post.id)

    if post.is_self:  # Check if it's a text post
        links.append(None)
    else:  # Link post
        links.append(post.url)

# Create a dataframe from the lists
data = {'Title': titles, 'Link': links, "Timestamps": timestamps, 'PostIds': post_ids2}
most_recent_posts_df = pd.DataFrame(data)


max_episode = max([int(x) for x in known_discussed_episodes if x.isdigit()])
result_urls = set()
for n in tqdm(range(1, max_episode+1), desc="Scraping Google for posts."):
    n = str(n)

    if n in known_discussed_episodes:
        continue

    # Make a request to Google Search
    response = requests.get("https://www.google.com/search?q=knowledge+fight+reddit+%23" + n)

    # Parse the response as HTML
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the search results
    results = soup.find_all("h3")

    for result in results:
      url = result.find_parent('a')['href']
      query_string = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)['q'][0]
      result_urls.add(query_string)

post_ids = [
    url.split('/')[6]
    for url in result_urls
    if 'https://www.reddit.com/r/KnowledgeFight/comments/' in url
]


# Initialize lists to store titles and links
titles = []
links = []
timestamps = []
post_ids2 = []

# Iterate through the subreddit posts and extract titles and links
for post_id in tqdm(post_ids, desc="Processing scraped google results."):
    if post_id in existing_post_ids:
        continue
    post = reddit.submission(id=post_id)
    titles.append(post.title)
    timestamps.append(post.created_utc)
    post_ids2.append(post.id)

    if post.is_self:  # Check if it's a text post
        links.append(None)
    else:  # Link post
        links.append(post.url)

# Create a dataframe from the lists
data = {'Title': titles, 'Link': links, "Timestamps": timestamps, 'PostIds': post_ids2}
search_based_df = pd.DataFrame(data)

ids = []


download_by_author = True
if download_by_author:
    posters = set()

    for post_id in tqdm(pd.concat([search_based_df, most_recent_posts_df, existing_df]).PostIds, desc="Identifying posters."):
        # Already processed, most likely.
        if post_id in existing_post_ids:
            continue
        time.sleep(1)
        post = reddit.submission(id=post_id)
        posters.add(str(post.author))

    for poster in tqdm(posters, desc="Looking up posts by subreddit users."):
        try:
            user = reddit.redditor(poster)
            for post in user.submissions.new(limit=None):
                if post.subreddit != 'KnowledgeFight':
                    continue
                ids.append(post.id)
        except:
            continue

    # Initialize lists to store titles and links
    titles = []
    links = []
    timestamps = []
    post_ids2 = []

    for post_id in tqdm(ids, desc="Processing user posts."):
        if post_id in existing_post_ids:
            continue
        time.sleep(1)
        post = reddit.submission(id=post_id)
        titles.append(post.title)
        timestamps.append(post.created_utc)
        post_ids2.append(post.id)

        if post.is_self:  # Check if it's a text post
            links.append(None)
        else:  # Link post
            links.append(post.url)

    # Create a dataframe from the lists
    data = {'Title': titles, 'Link': links, "Timestamps": timestamps, 'PostIds': post_ids2}
    user_based_df = pd.DataFrame({'Title': [], 'Link': [], "Timestamps": [], 'PostIds': []})
else:
    user_based_df = pd.DataFrame(data)

merged_df = pd.concat([search_based_df, most_recent_posts_df, user_based_df, existing_df]).drop_duplicates(subset=['PostIds']).sort_values(by=['PostIds'])

kfio.save(merged_df, 'sensitive/all_reddit_posts.json')



print("Processing libsyn links.")

libsyn_links = merged_df[merged_df['Link'].str.contains('knowledgefight.libsyn.com').fillna(False)]
def link_fixer(link):
    print(link)
    return link.replace("http://", "https://").replace("/podcast/", "/")
libsyn_links['Link'] = libsyn_links['Link'].apply(link_fixer)

print(libsyn_links)
final_data = kfio.load('data/final.json')


episode_discussions = pd.merge(libsyn_links, final_data, left_on='Link', right_on='libsyn_page')[['episode_number', 'PostIds', 'Link']].sort_values(by=['episode_number'], key=natsort.natsort_keygen())
print(episode_discussions)
kfio.save(episode_discussions, 'sensitive/reddit_episode_discussions.json')
print(set(libsyn_links['Link']) - set(final_data['libsyn_page']))