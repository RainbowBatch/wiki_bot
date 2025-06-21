import natsort
import pandas as pd
import rainbowbatch.kfio as kfio

from rainbowbatch.external.spotify import make_spotify_client
from rainbowbatch.remap.episode_number_util import extract_episode_number


def download_spotify_details():
    PODCAST_ID = "spotify:show:6hK78c5u6Bscdz0HCDeFLn"

    spotify = make_spotify_client()

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
            if episode is None:
                continue
            rows.append([
                extract_episode_number(episode['name']),
                episode['external_urls']['spotify'],
            ])

    df = pd.DataFrame(rows, columns=header)
    df = df.sort_values(by=['episode_number'], key=natsort.natsort_keygen())
    kfio.save(df, 'data/spotify_details.json')


if __name__ == '__main__':
    download_spotify_details()
