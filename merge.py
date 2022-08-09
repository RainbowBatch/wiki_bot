import json
import kfio
import pandas as pd

title_table = kfio.load('data/titles.json')
details_table = kfio.load('data/libsyn_details.json')

# Annotate next / previous from the libsyn dataset.
# TODO(woursler): Do something different?
title_table['next_title'] = title_table.title.shift(-1)
title_table['prev_title'] = title_table.title.shift(1)

tracker_table = kfio.load('data/tracker.json')

def split_out_episode_number(title):
    if title[0] == "#":
        return clean_episode_number(title[1:].split(":")[0])
    return clean_episode_number(title)

manual_episode_number_remapping = {
    'Repost 25': '25(Repost)',
    'Repost: Episode #25': '25(Repost)',
    '122 LIVE': '122',

    'InfoWars Roulette #1': 'S001',
    'Spiritual Correction': 'S003',
    'TWTWYTT Special': 'S004',
    'The Halloween Story': 'S005',
    'InfoWars Roulette #2': 'S006',

    # ??? "We'll Be Back On Monday": 'S007',

    'No One Is Mad At The Crew': 'S010',
}


def clean_episode_number(title):
    title = title.strip()
    if title in manual_episode_number_remapping:
        return manual_episode_number_remapping[title]
    return title


title_table['episode_number'] = title_table.title.apply(
    split_out_episode_number)

tracker_table['episode_number'] = tracker_table.episode_number.apply(
    clean_episode_number)


# TODO(woursler): Ensure we're not dropping anything here?
augmented_title_table = pd.merge(
    title_table,
    details_table,
    how='inner',
    on='libsyn_page'
)

merged = pd.merge(
    augmented_title_table,
    tracker_table,
    how='inner',  # TODO(woursler): Outer join?
    on='episode_number'
)

print(merged)

title_keys = title_table.episode_number.to_list()
tracker_keys = tracker_table.episode_number.to_list()
merged_keys = merged.episode_number.to_list()

title_unique_keys = [x for x in title_keys if x not in merged_keys]
tracker_unique_keys = [x for x in tracker_keys if x not in merged_keys]


print("Title Unique", title_unique_keys)
print("Tracker Unique", tracker_unique_keys)

title_table_view = augmented_title_table[augmented_title_table.episode_number.isin(
    title_unique_keys)]

print(title_table_view)

kfio.save(merged, 'data/merged.json')
kfio.save(title_table_view, 'data/tracker_unique_rows.json')
