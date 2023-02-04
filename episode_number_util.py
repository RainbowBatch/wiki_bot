import re

EPISODE_NUMBER_PATTERN = re.compile("^[A-Z]?\d+[A-Z]?$")

MANUAL_EPISODE_NUMBER_REMAPPING = {
    '122 LIVE': '122',

    'InfoWars Roulette #1': 'S001',
    'Spiritual Correction': 'S003',
    'TWTWYTT Special': 'S004',
    'The Halloween Story': 'S005',
    'InfoWars Roulette #2': 'S006',

    "We'll Be Back On Monday": 'S009',

    'No One Is Mad At The Crew': 'S010',

    'Repost 25': 'S011',
    'Repost: Episode #25': 'S011',

    "Alex's Breaky Minisode": 'S012',

    "Lost Episode": 'S013', # TODO: This may not be correct.
}


def extract_episode_number(title):
    if title[0] == "#":
        return clean_episode_number(title[1:].split(":")[0])
    return clean_episode_number(title)


def clean_episode_number(episode_number):
    episode_number = episode_number.strip()
    if episode_number in MANUAL_EPISODE_NUMBER_REMAPPING:
        return MANUAL_EPISODE_NUMBER_REMAPPING[episode_number]
    assert EPISODE_NUMBER_PATTERN.match(episode_number), episode_number
    return episode_number
