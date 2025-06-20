import json
import praw

from box import Box

from rainbowbatch.secrets import secret_file

def make_reddit_client():
    with open(secret_file("reddit.json")) as secrets_f:
        secrets = Box(json.load(secrets_f))

        # TODO: We should probably keep this all in the secrets so other reddit users are simple to use.
        client = praw.Reddit(
            user_agent='RainbowBatch',
            client_id=secrets.client_id,
            client_secret=secrets.client_secret,
            username='RainbowBatch',
            password=secrets.bot_password,
        )
    return client
