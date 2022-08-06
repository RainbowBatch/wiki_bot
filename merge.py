import pandas as pd

title_table = pd.read_csv('titles.csv')

# Annotate next / previous from the libsyn dataset.
title_table['next_title'] = title_table.title.shift(-1)
title_table['prev_title'] = title_table.title.shift(1)

tracker_table = pd.read_csv('tracker.csv')

details_table = pd.read_csv('libsyn_details.csv', encoding ='latin1')

def split_out_episode_number(title):
    if title[0] == "#":
        return clean_episode_number(title[1:].split(":")[0])
    return clean_episode_number(title)


#Title Unique ['InfoWars Roulette #1', 'Spiritual Correction', 'TWTWYTT Special', 'The Halloween Story', '122 LIVE', 'InfoWars Roulette #2', "We'll Be Back On Monday", 'Repost: Episode #25', 'No One Is Mad At The Crew', '656', '657', '658', '659', '660', '661', '662', '663', '664', '665', '666', '667', '668', '669', '670', '671', '672', '673', '674', '675', '676', '677', '678', '679', '680', '681', '682', '683', '684', '685', '686', '687', '688', '689', '690', '691', '692', "Alex's Breaky Minisode", '693', '694', '695', '696', '697', '698', '699', '700', '701', '702', '703', '704', '705', '706', '707', '708', '709', '710', '711']
#Tracker Unique ['S001', 'S002', 'S003', 'S004', 'S005', '122', 'S006', '235', 'S007', 'S008', 'S009', 'Lost Episode', 'Repost 25', 'S010']

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

merged = pd.merge(
    # TODO(woursler): Ensure we're not dropping anything here?
    pd.merge(
        title_table,
        details_table,
        how='inner',
        on='libsyn_page'
    ),
    tracker_table,
    how='inner',
    on='episode_number'
)

print(merged)

title_keys = title_table.episode_number.to_list()
tracker_keys = tracker_table.episode_number.to_list()
merged_keys = merged.episode_number.to_list()

print("Title Unique", [x for x in title_keys if x not in merged_keys])
print("Tracker Unique", [x for x in tracker_keys if x not in merged_keys])

with open("merged.csv", "w", encoding='utf-8') as csv_file:
    csv_file.write(merged.to_csv(index=False, line_terminator='\n'))
