import json
import kfio
import pandas as pd
from tqdm import tqdm
from episode_processor import process_ep_record
from episode_number_util import extract_episode_number, clean_episode_number


def merge_records():

    title_table = kfio.load('data/titles.json')
    libsyn_details_table = kfio.load('data/libsyn_details.json')
    spotify_details_table = kfio.load('data/spotify_details.json')
    overlay_table = kfio.load('data/overlay.json')

    # Annotate next / previous from the libsyn dataset.
    # TODO: Do something different?
    title_table['next_title'] = title_table.title.shift(-1)
    title_table['prev_title'] = title_table.title.shift(1)

    tracker_table = kfio.load('data/tracker.json')

    title_table['episode_number'] = title_table.title.apply(
        extract_episode_number)

    title_table['next_episode_number'] = title_table.episode_number.shift(-1)
    title_table['prev_episode_number'] = title_table.episode_number.shift(1)

    tracker_table['episode_number'] = tracker_table.episode_number.apply(
        clean_episode_number)


    # TODO: Ensure we're not dropping anything here?
    augmented_title_table = pd.merge(
        title_table,
        libsyn_details_table,
        how='inner',
        on='libsyn_page'
    )

    augmented_title_table = pd.merge(
        augmented_title_table,
        spotify_details_table,
        how='inner',
        on='episode_number'
    )

    merged = pd.merge(
        augmented_title_table,
        tracker_table,
        how='inner',  # TODO: Outer join?
        on='episode_number'
    )

    title_keys = title_table.episode_number.to_list()
    tracker_keys = tracker_table.episode_number.to_list()
    merged_keys = merged.episode_number.to_list()

    title_unique_keys = [x for x in title_keys if x not in merged_keys]
    tracker_unique_keys = [x for x in tracker_keys if x not in merged_keys]

    print("Unable to match the following episodes (Only in tracker)",
          tracker_unique_keys)

    title_table_view = augmented_title_table[augmented_title_table.episode_number.isin(
        title_unique_keys)]

    citations_df = kfio.load_citations_table('data/citations.json')

    category_remapping_df = kfio.load_category_remapping(
        'data/categories_remapping.json')

    NEW_RECORDS = []
    print("Processing raw episode records.")
    for raw_record in tqdm(merged.to_dict(orient='records') + title_table_view.to_dict(orient='records')):
        record = process_ep_record(
            raw_record, citations_df, category_remapping_df)

        NEW_RECORDS.append(record)

    processed_records = pd.DataFrame.from_records(NEW_RECORDS)

    print("Adding overlays.")
    for overlay_record in tqdm(overlay_table.to_dict(orient='records')):
        assert 'episode_number' in overlay_record
        idxs = processed_records.index[processed_records.episode_number ==
                                       overlay_record['episode_number']].tolist()
        assert len(idxs) == 1
        idx = idxs[0]

        for field, value in overlay_record.items():
            if field == 'episode_number':
                continue
            if not isinstance(value, list) and pd.isna(value):
                continue
            if value == '##REMOVE##':
                processed_records.at[idx, field] = None
            else:
                processed_records.at[idx, field] = value

    kfio.save(processed_records, 'data/final.json')


if __name__ == '__main__':
    merge_records()
