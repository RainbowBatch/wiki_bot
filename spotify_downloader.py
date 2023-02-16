import json
import kfio
import natsort
import pandas as pd
import spotipy

from box import Box
from episode_number_util import extract_episode_number
from pprint import pprint


def download_spotify_details():
    PODCAST_ID = "6hK78c5u6Bscdz0HCDeFLn"

    with open("secrets/spotify.json") as secrets_f:
        secrets = Box(json.load(secrets_f))

        client_credentials_manager = spotipy.oauth2.SpotifyClientCredentials(
            client_id=secrets.client_id,
            client_secret=secrets.client_secret,
        )
        spotify = spotipy.Spotify(
            client_credentials_manager=client_credentials_manager)

    BATCH_SIZE = 50
    offset = 0

    header = [
        "episode_number",
        "spotify_page",
    ]
    rows = []

    while True:

        episodes = spotify.show_episodes(
            PODCAST_ID, market="US", limit=BATCH_SIZE, offset=offset)
        offset += BATCH_SIZE

        if len(episodes['items']) == 0:
            break

        for episode in episodes['items']:
            rows.append([
                extract_episode_number(episode['name']),
                episode['external_urls']['spotify'],
            ])

    df = pd.DataFrame(rows, columns=header)
    df = df.sort_values(by=['episode_number'], key=natsort.natsort_keygen())
    kfio.save(df, 'data/spotify_details.json')


if __name__ == '__main__':
    download_spotify_details()
