from setuptools import find_packages
from setuptools import setup

setup(
    name="rainbowbatch",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "beautifulsoup4",
        "google-api-python-client",
        "google-auth",
        "google-auth-oauthlib",
        "lxml",
        "maya",
        "natsort",
        "pandas",
        "praw",
        "pygit2",
        "pypandoc",
        "python-box",
        "pywikibot",
        "requests",
        "spotipy",
        "tqdm",
        "twitch-python",
        "wikitextparser",
    ],
)
