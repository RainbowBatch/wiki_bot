import json
import praw
import random
import re

from box import Box
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from search_transcripts import search_transcripts
from transcripts import format_timestamp


def escape_ansi(line):
    ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', line)

env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape()
)

env.filters["format_speaker"] = lambda speaker: "Unknown" if speaker is None else speaker
env.filters["format_timestamp"] = format_timestamp
env.filters["escape_ansi"] = escape_ansi

reply_template = env.get_template('reddit_bot_reply.md.template')


with open("secrets/reddit.json") as secrets_f:
    secrets = Box(json.load(secrets_f))

    reddit = praw.Reddit(
        user_agent='RainbowBatch',
        client_id=secrets.client_id,
        client_secret=secrets.client_secret,
        username='RainbowBatch',
        password=secrets.bot_password,
    )

# TODO(woursler): Switch this over to knowledgefight once the bot is working.
subreddit = reddit.subreddit("pythonforengineers+KnowledgeFight")


def already_replied(comment):
    for reply in comment.replies:
        if reply.author == "RainbowBatch":
            return True
    return False


print("Listening for new comments!")

for comment in subreddit.stream.comments():
    if re.search("u/RainbowBatch", comment.body, re.IGNORECASE):
        comment.refresh()
        if already_replied(comment):
           continue

        if comment.author not in ["CelestAI", "SauceCupAficionado"]:
            continue # For now, restrict who can summon the bot.

        print("Responding to '%s" % comment.body)

        # TODO: Do something smarter, possibly using GPT?
        quoted_strings = re.findall('"([^"]*)"', comment.body)

        # TODO: Do something less brittle, recover from errors.
        assert len(quoted_strings) == 1

        search_term = quoted_strings[0]

        print("Searching for '%s'" % search_term)
        search_results = search_transcripts(search_term, remove_overlaps=True, max_results=100)

        print("Done. Replying.")

        comment.reply(
            reply_template.render(
                search_term=search_term,
                search_results=search_results,
            ).strip()
        )
