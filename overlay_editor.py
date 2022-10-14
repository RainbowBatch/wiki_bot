import kfio
import box
from box import Box
from collections import defaultdict
import pandas as pd
from date_lookup import mayafy_date

scraped_data = kfio.load('data/scraped_page_data.json')

final_data = kfio.load('data/final.json')

valid_categories = set([
    x
    for y in kfio.load('data/categories_remapping.json').new_categories.to_list()
    for x in y.split(';')
])

new_overlay = defaultdict(Box)
existing_overlay = kfio.load('data/overlay.json')
for overlay_entry in existing_overlay.to_dict(orient='records'):
    overlay_entry = Box(overlay_entry)
    new_overlay[overlay_entry.episode_number] = overlay_entry

for page_record in scraped_data.to_dict(orient='records'):
    page_record = Box(page_record)
    wiki_categories = page_record.wiki_categories

    if 'Episodes' not in wiki_categories:
        continue

    matching_episodes = final_data[final_data.episode_number ==
                                   page_record.episodeNumber]

    assert len(matching_episodes) == 1

    matching_episode_record = Box(
        matching_episodes.to_dict(orient='records')[0])

    matching_overlays = existing_overlay[existing_overlay.episode_number == page_record.episodeNumber]
    if len(matching_overlays) > 0:
        current_overlay = Box(matching_overlays.to_dict(orient='records')[0])
    else:
        current_overlay = None


    overlay_categories = set() if current_overlay is None else current_overlay.categories
    overlay_categories = (set() if isinstance(overlay_categories, float) else set(overlay_categories))

    inferred_categories = (set(wiki_categories) | set(overlay_categories)).intersection(valid_categories)

    if matching_episode_record.coverage_start_date is not None and '2003' in matching_episode_record.coverage_start_date:
        inferred_categories.add('2003 Investigation')
        inferred_categories.discard('Time Travel')

    if len(matching_episode_record.categories) == 0:
        if matching_episode_record.coverage_start_date is not None:
            delay_days = (
                mayafy_date(matching_episode_record.release_date)
                - mayafy_date(matching_episode_record.coverage_start_date)
            ).days
            if delay_days <= 7:
                inferred_categories.add('Present Day')

    current_categories = set(matching_episode_record.categories)

    if inferred_categories != current_categories:
        new_overlay[page_record.episodeNumber]['episode_number'] = page_record.episodeNumber
        new_overlay[page_record.episodeNumber]['categories'] = inferred_categories

    # TODO(woursler): WTF -- This is wrong.
    current_people = matching_episode_record.people or []
    inferred_people = page_record.appearance or []

    if set(current_people) != set(inferred_people):
        new_overlay[page_record.episodeNumber]['episode_number'] = page_record.episodeNumber
        new_overlay[page_record.episodeNumber]['people'] = inferred_people

    current_start_date = matching_episode_record.coverage_start_date
    current_end_date = matching_episode_record.coverage_end_date
    inferred_start_date = page_record.coverageStartDate or page_record.coverageDate or (None if current_overlay is None else (current_overlay.coverage_start_date or current_overlay.coverage_date))
    inferred_end_date = page_record.coverageEndDate or page_record.coverageDate or (None if current_overlay is None else (current_overlay.coverage_end_date or current_overlay.coverage_date))

    dirty_date = False
    if current_start_date != inferred_start_date:
        new_overlay[page_record.episodeNumber]['episode_number'] = page_record.episodeNumber
        new_overlay[page_record.episodeNumber]['coverage_start_date'] = inferred_start_date
        dirty_date = True

    if current_end_date != inferred_end_date:
        new_overlay[page_record.episodeNumber]['episode_number'] = page_record.episodeNumber
        new_overlay[page_record.episodeNumber]['coverage_end_date'] = inferred_end_date
        dirty_date = True

    if dirty_date:
        if inferred_start_date == inferred_end_date:
            new_overlay[page_record.episodeNumber]['coverage_date'] = inferred_start_date
        elif current_overlay is not None and not pd.isna(current_overlay.coverage_date):
            new_overlay[page_record.episodeNumber]['coverage_date'] = '##REMOVE##'

new_overlay_df = pd.DataFrame.from_records(
    list(new_overlay.values())).sort_values('episode_number')

kfio.save_without_nulls(new_overlay_df, 'data/overlay.json')
