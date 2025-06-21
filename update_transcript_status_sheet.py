import rainbowbatch.kfio as kfio
import json
import natsort
import uuid
from box import Box
from pprint import pprint
from transcripts import create_best_transcript_listing
from transcripts import parse_transcript
from tqdm import tqdm
import pandas as pd

transcript_listing = create_best_transcript_listing()
episode_listing = kfio.load('data/final.json')

rows = []

for transcript_record in tqdm(transcript_listing.to_dict(orient='records')):
    transcript_record = Box(transcript_record)
    episode_number = transcript_record.episode_number

    possible_episode_records = episode_listing[episode_listing.episode_number == episode_number].to_dict(
        orient='records')

    if len(possible_episode_records) != 1:
        print("Failure on", episode_number)
        continue

    episode_record = Box(possible_episode_records[0])

    rows.append([
        episode_number, transcript_record.transcript_format, transcript_record.transcript_type, episode_record.episode_length
    ])

headers = ['episode_number', 'origin_format', 'type', 'episode_length']


df = pd.DataFrame(rows, columns=headers)

df = df.sort_values(by=['episode_number'], key=natsort.natsort_keygen())

print(df)

df.to_csv('data/transcript_status.csv', index=False)