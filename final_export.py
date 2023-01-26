import kfio
import pandas as pd

episodes_df = kfio.load('data/final.json')

final_df = pd.DataFrame()

final_df["Episode Number"] = episodes_df["episode_number"]
final_df["Air Date"] = episodes_df["release_date"]
final_df["Coverage Start Date"] = episodes_df["coverage_start_date"]
final_df["Coverage End Date"] = episodes_df["coverage_end_date"]
final_df["Type of Episode"] = episodes_df["episode_type"]
final_df["Novelty Beverage"] = episodes_df["beverage"]
final_df["Refereneced people/ Guests"] = episodes_df["people"].apply(lambda x: '; '.join(x))
# Not reliably back-populated, so skipping.
final_df["Books/ Primary sources"] = ''
final_df["Themes"] = ''
final_df["Out Of Context"] = ''
final_df["Noteable Drops or Bits"] = ''
final_df["Episode Description"] = episodes_df["mediawiki_description"].apply(lambda x: '  '.join(x.split('\n')).strip())

print(final_df)

final_df.to_csv('episodes.csv', index=False)