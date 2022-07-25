import pandas as pd

title_table = pd.read_csv('titles.csv')

# Annotate next / previous from the libsyn dataset.
title_table['next_title'] = title_table.title.shift(-1)
title_table['prev_title'] = title_table.title.shift(1)

tracker_table = pd.read_csv('tracker.csv')

details_table = pd.read_csv('libsyn_details.csv', encoding ='latin1')

def split_out_episode_number(title):
    if title[0] == "#":
        return title[1:].split(":")[0]
    return title


title_table['episode_number'] = title_table.title.apply(
    split_out_episode_number)

merged = pd.merge(
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
