import json
import praw
import random
import re
import traceback

from box import Box
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from search_transcripts import search_transcripts
from transcripts import format_timestamp
import logging

# Configure logging.
logging.basicConfig(
    filename='reddit_bot.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(name)s %(message)s',
)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("parse").setLevel(logging.WARNING)
logging.getLogger("prawcore").setLevel(logging.WARNING)
stderr_logger = logging.StreamHandler()
stderr_logger.setLevel(logging.DEBUG)
stderr_logger.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s %(message)s'))
logging.getLogger().addHandler(stderr_logger)


env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape()
)


def format_speaker(speaker):
    '''Shortens common names to save space.'''
    if speaker is None:
        return ''
    if speaker == "Alex Jones":
        return "Alex"
    return speaker


env.filters["format_speaker"] = format_speaker
env.filters["format_timestamp"] = lambda ts: format_timestamp(ts, shorten=True)

reply_template = env.get_template('reddit_bot_reply.md.template')
error_reply_template = env.get_template('reddit_bot_error.md.template')


with open("secrets/reddit.json") as secrets_f:
    secrets = Box(json.load(secrets_f))

    reddit = praw.Reddit(
        user_agent='RainbowBatch',
        client_id=secrets.client_id,
        client_secret=secrets.client_secret,
        username='RainbowBatch',
        password=secrets.bot_password,
    )

# Only interact with specific subreddits.
subreddit = reddit.subreddit("pythonforengineers+KnowledgeFight")


def already_replied(comment):
    for reply in comment.replies:
        if reply.author == "RainbowBatch":
            return True
    return False


logging.info("Listening for new comments!")


def clean_reply(raw_reply_markdown):
    # TODO: Remove other raw things, e.g. \n\n\n => \n\n
    return raw_reply_markdown.strip()


def formulate_search_reply(search_term):
    logging.info("Searching for '%s'" % search_term)

    search_results = search_transcripts(
        search_term,
        remove_overlaps=True,
        max_results=100,
        highlight_f=lambda s: "**%s**" % s,
    )

    raw_reply = reply_template.render(
        search_term=search_term,
        search_results=search_results,
        results_truncated=len(search_results) >= 100
    )

    reply = clean_reply(raw_reply)

    logging.info("Formulated response.")

    return reply


for comment in subreddit.stream.comments():
    if re.search("u/RainbowBatch", comment.body, re.IGNORECASE):
        comment.refresh()
        if already_replied(comment):
            continue

        if comment.author not in ["CelestAI", "SauceCupAficionado"]:
            continue  # For now, restrict who can summon the bot.

        logging.info("Responding to %s -- '%s'" %
                     (comment.permalink, comment.body))

        try:
            # TODO: Do something smarter, possibly using GPT?
            quoted_strings = re.findall('"([^"]*)"', comment.body)

            assert len(quoted_strings) == 1
            search_term = quoted_strings[0]

            reply_text = formulate_search_reply(search_term)

            logging.info(
                "Full reply text: %s" % reply_text
            )

            logging.info("Reply length: %d" % len(reply_text))

            reply_comment = comment.reply(reply_text)

            logging.info(
                "Reply details: {ID: %s, permalink: %s }" % (
                    reply_comment.id,
                    reply_comment.permalink,
                ),
            )

        except:
            # printing stack trace
            logging.exception(
                "Failed to serve response. Replying with an error.")
            print("Failed to handle '%s'" % comment.body)
            traceback.print_exc()
            reply_comment = comment.reply(
                clean_reply(error_reply_template.render()))
            logging.error(
                "Error reply details: {ID: %s, permalink: %s }" % (
                    reply_comment.id,
                    reply_comment.permalink,
                ),
            )
