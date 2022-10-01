import kfio

episodes_df = kfio.load('data/final.json')


episodes_df.to_csv('episodes.csv', index=False)