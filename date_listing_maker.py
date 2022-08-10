import kfio

episodes_df = kfio.load('data/final.json')

# TODO(woursler): Probably could just use final.json for the date listing!
df = episodes_df[['title', 'episode_number', 'coverage_start_date', 'coverage_end_date', 'coverage_date']]

kfio.save(df, 'data/date_listing.json')
