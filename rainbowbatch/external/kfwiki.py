import os

from pathlib import Path

# NOTE: In order for this to work, we need to place the files from secret/pywikibot
# somewhere in the unix file system. This workaround assumed they've copied into
# ~/pywikibot. rebuild.sh contains a script to do this automatically.
pywikibot_dir = Path.home() / 'pywikibot'
os.environ['PYWIKIBOT_DIR'] = str(pywikibot_dir)

import pywikibot

from pywikibot import config
from pywikibot import site


def make_kfwiki_client():
    site = pywikibot.Site()

    site.login()
    if not site.user():
        raise Exception("Login failed — check your credentials.")
    return site


def hard_reset_site(site):
    # Clear cached site objects so we get a fresh login context
    site._apisite._sites.clear()

    # Force the correct username if not already set
    config.usernames['knowledgefight']['en'] = 'RainbowBatch'

    new_site = pywikibot.Site('en', 'knowledgefight')
    new_site.login()
    return new_site

if __name__ == '__main__':
    site = make_kfwiki_client()
    print(f"Logged in as: {site.user()}")
