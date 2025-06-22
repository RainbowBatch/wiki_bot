import json
import twitch

from box import Box
from rainbowbatch.secrets import secret_file


def make_twitch_client():
    with open(secret_file("twitch.json")) as secrets_f:
        secrets = Box(json.load(secrets_f))

        twitch_api = twitch.Helix(
            client_id=secrets.client_id,
            client_secret=secrets.client_secret,
        )
    return twitch_api
