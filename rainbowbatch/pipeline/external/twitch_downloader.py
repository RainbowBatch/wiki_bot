import natsort
import pandas as pd
import rainbowbatch.kfio as kfio

from rainbowbatch.external.twitch import make_twitch_client
from rainbowbatch.remap.date_lookup import extract_date_from_string
from rainbowbatch.remap.date_lookup import lookup_by_maya_date
from rainbowbatch.remap.date_lookup import mayafy_date
from rainbowbatch.remap.episode_number_util import extract_episode_number


def match_episode_number(twitch_title):
    mod_title = twitch_title.removeprefix("Knowledge Fight:").strip()

    dt_start, dt_end = extract_date_from_string(mod_title)

    likely_episode = lookup_by_maya_date(dt_start)

    if likely_episode is not None:
        return likely_episode['episode_number']
    return None


def download_twitch_details():
    PODCAST_ID = "6hK78c5u6Bscdz0HCDeFLn"

    twitch_api = make_twitch_client()

    header = [
        "episode_number",
        "twitch_title",
        "twitch_url",
    ]
    rows = []

    for video in twitch_api.user('knowledgefight').videos():
        rows.append([
            match_episode_number(video.title),
            video.title,
            video.url,
        ])

    df = pd.DataFrame(rows, columns=header)
    df = df.sort_values(by=['episode_number'], key=natsort.natsort_keygen())
    kfio.save(df, 'data/twitch_details.json')


if __name__ == '__main__':
    download_twitch_details()
