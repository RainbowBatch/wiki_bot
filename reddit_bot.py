import json
import logging
import os
import praw
import rainbowbatch.kfio as kfio
import random
import re
import traceback

from attr import attr
from attr import attrs
from box import Box
from itertools import tee
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from rainbowbatch.secrets import secret_file
from rainbowbatch.transcripts import format_timestamp
from reddit_bot.nlp_command_parser import fuzzy_parse_bot_command
from sensitive.redactions import is_sensitive
from sensitive.transcript_search import search_transcripts
from thefuzz import process as fuzzy_process
from typing import Optional


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

COMMAND_MAPPING = {
    'details': 'episode_details',
    'episode': 'episode_details',
    'episode_details': 'episode_details',
    'help': 'help',
    'quitquitquit': 'quitquitquit',
    'search': 'transcript_search',
    'search_transcripts': 'transcript_search',
    'shutdown': 'quitquitquit',
    'transcript_search': 'transcript_search',
}


def format_speaker(speaker):
    '''Shortens common names to save space.'''
    if speaker is None:
        return ''
    if speaker == "Alex Jones":
        return "Alex"
    return speaker


def format_redacted(n_redacted):
    if n_redacted < 5:
        return "A few"
    if n_redacted < 10:
        return "Several"
    if n_redacted < 20:
        return "Around a dozen"
    return "Many"


env.filters["format_redacted"] = format_redacted
env.filters["format_speaker"] = format_speaker
env.filters["format_timestamp"] = lambda ts: format_timestamp(ts, shorten=True)

episode_details_reply_template = env.get_template(
    'reddit_bot_episode_details.md.template')
error_reply_template = env.get_template('reddit_bot_error.md.template')
generic_reply_template = env.get_template(
    'reddit_bot_generic_reply.md.template')
help_reply_template = env.get_template('reddit_bot_help.md.template')
transcript_search_reply_template = env.get_template(
    'reddit_bot_reply.md.template')

episodes_df = kfio.load('data/final.json')

with open(secret_file("reddit.json")) as secrets_f:
    secrets = Box(json.load(secrets_f))

    reddit = praw.Reddit(
        user_agent='RainbowBatch',
        client_id=secrets.client_id,
        client_secret=secrets.client_secret,
        username='RainbowBatch',
        password=secrets.bot_password,
    )


# Only interact with specific subreddits.
subreddit = reddit.subreddit("KnowledgeFight+KnowledgeFightBots")


def already_replied(comment):
    for reply in comment.replies:
        if reply.author == "RainbowBatch":
            return True
    return False


logging.info("Listening for new comments!")


def clean_reply(raw_reply_markdown):
    return re.sub(r'\n\n+', '\n\n', raw_reply_markdown).strip()


def split_list_on_condition(l, condition):
    l1, l2 = tee((condition(item), item) for item in l)
    return [i for p, i in l1 if p], [i for p, i in l2 if not p]


def formulate_search_reply(search_term):
    if len(search_term) > 500:
        logging.info("Rejected search for length: '%s'" % search_term)
        return clean_reply(
            generic_reply_template.render(
                reply="Please enter a shorter search term. Search terms must be less than 500 char -- yours is currently %d char." % len(
                    search_term),
            ),
        )

    logging.info("Searching for '%s'" % search_term)

    search_results = search_transcripts(
        search_term,
        remove_overlaps=True,
        max_results=100,
        highlight_f=lambda s: "**%s**" % s,
    )

    redacted_results, safe_results = split_list_on_condition(
        search_results, is_sensitive)

    # Note: This is the number of redactions
    # it's safe to inform the user about.
    n_redactions = len(redacted_results)

    # If there's exactly one redaction, that suggests someone could potentially
    # be fishing to see what's in the dataset.
    if n_redactions == 1:
        n_redactions = 0
    # If all the redactions come from a single episode, same thing.
    # TODO: Might be good to enforce a time range as well.
    elif len(set([result.episode_number for result in redacted_results])) <= 1:
        n_redactions = 0

    # TODO: Add redaction logic for clusters.

    raw_reply = transcript_search_reply_template.render(
        search_term=search_term,
        search_results=safe_results,
        results_truncated=len(search_results) >= 100,
        n_redactions=n_redactions,
    )

    reply = clean_reply(raw_reply)

    logging.info("Formulated response.")

    return reply


def formulate_episode_details_reply(episode_number):
    logging.info("Getting details for '%s'" % episode_number)

    idxs = episodes_df.index[episodes_df.episode_number ==
                             episode_number].tolist()
    assert len(idxs) == 1
    idx = idxs[0]

    episode_details = episodes_df.iloc[idx]

    raw_reply = episode_details_reply_template.render(
        episode_details=episode_details,
    )

    reply = clean_reply(raw_reply)

    logging.info("Formulated response.")

    return reply


for comment in subreddit.stream.comments():
    # Only respond to comments that explicitly invoke RainbowBatch
    if not re.search("u/RainbowBatch", comment.body, re.IGNORECASE):
        continue

    # Don't respond to the same comment multiple times.
    try:
        comment.refresh()
    except:
        continue

    if already_replied(comment):
        continue

    # Only a few people can interact with the bot (for now).
    if comment.author not in ["CelestAI", "SauceCupAficionado", "KnowledgeFightBots"]:
        continue  # For now, restrict who can summon the bot.

    logging.info("Responding to {ID: %s, permalink: %s }" % (
        comment.id,
        comment.permalink,
    ))

    try:
        # TODO: This does the right thing but it's a mess.
        for line in comment.body.split('\n'):
            if not re.search("u/RainbowBatch", line, re.IGNORECASE):
                continue
            command = fuzzy_parse_bot_command(line)
            break

        logging.info(
            "Handling command: %s" % repr(command)
        )

        if command.command == 'quitquitquit':
            authorized_users = ['RainbowBatch', 'CelestAI', 'KnowledgeFightBots'] + [
                redditor.name
                for redditor in reddit.subreddit("KnowledgeFight").moderator()
            ]

            if comment.author in authorized_users:
                logging.info(
                    "Remote shutdown requested by %s" % comment.author
                )

                reply_comment = comment.reply(
                    "!quitquitquit acknowledged. Shutting down now.")

                logging.info(
                    "Reply details: {ID: %s, permalink: %s }" % (
                        reply_comment.id,
                        reply_comment.permalink,
                    ),
                )

                os._exit(1)
            else:
                raise ValueError("Non-authorized user asked for shutdown.")
        elif command.command == 'transcript_search':
            search_term = command.argument

            reply_text = formulate_search_reply(search_term)

            logging.info(
                "Full reply text: \n\n%s\n\n" % reply_text
            )

            logging.info("Reply length: %d" % len(reply_text))

            reply_comment = comment.reply(reply_text)

            logging.info(
                "Reply details: {ID: %s, permalink: %s }" % (
                    reply_comment.id,
                    reply_comment.permalink,
                ),
            )
        elif command.command == 'episode_details':
            reply_text = formulate_episode_details_reply(command.argument)

            logging.info(
                "Full reply text: \n\n%s\n\n" % reply_text
            )

            logging.info("Reply length: %d" % len(reply_text))

            reply_comment = comment.reply(reply_text)

            logging.info(
                "Reply details: {ID: %s, permalink: %s }" % (
                    reply_comment.id,
                    reply_comment.permalink,
                ),
            )
        elif command.command == 'help':
            reply_text = clean_reply(help_reply_template.render())

            reply_comment = comment.reply(reply_text)

            logging.info(
                "Reply details: {ID: %s, permalink: %s }" % (
                    reply_comment.id,
                    reply_comment.permalink,
                ),
            )
        else:
            raise ValueError("Unhandled command.")

    except:
        # printing stack trace
        logging.exception(
            "Failed to serve response. Replying with an error.")
        print("Failed to handle '%s'" % comment.permalink)
        traceback.print_exc()
        reply_comment = comment.reply(
            clean_reply(error_reply_template.render()))
        logging.error(
            "Error reply details: {ID: %s, permalink: %s }" % (
                reply_comment.id,
                reply_comment.permalink,
            ),
        )
