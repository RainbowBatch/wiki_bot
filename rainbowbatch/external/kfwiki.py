import json
import mwclient

from box import Box
from rainbowbatch.secrets import secret_file


def make_kfwiki_client():
    site = mwclient.Site('knowledgefight.wiki', path='/')

    with open(secret_file("kfwiki.json")) as secrets_f:
        secrets = Box(json.load(secrets_f))

    full_username = f"{secrets.username_user}@{secrets.username_bot}"
    site.login(full_username, secrets.bot_password)
    return site


if __name__ == '__main__':
    site = make_kfwiki_client()
    username = site.get('query', meta='userinfo')['query']['userinfo']['name']
    print(f"Logged in as: {username}")
