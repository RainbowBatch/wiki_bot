import kfio
import json

import uuid
from box import Box
from pprint import pprint
from transcripts import create_best_transcript_listing
from transcripts import parse_transcript
from tqdm import tqdm

transcript_listing = create_best_transcript_listing()
episode_listing = kfio.load('data/final.json')


KF_TRANSCRIPT_NAMESPACE = uuid.UUID('c43bff5e-c3d0-4ee8-9925-d76901871ef7')


for transcript_record in tqdm(transcript_listing.to_dict(orient='records')):
    episode_number = transcript_record['episode_number']

    possible_episode_records = episode_listing[episode_listing.episode_number == episode_number].to_dict(
        orient='records')

    if len(possible_episode_records) != 1:
        print("Failure on", episode_number)
        continue

    episode_record = Box(possible_episode_records[0])

    transcript = parse_transcript(transcript_record)
    transcript.augment_timestamps()

    obj = {
        "audio_identifier": "KF_%s" % episode_number,
        "audio_url": episode_record.download_link,
        "history": {
            "type": transcript_record['transcript_type'],
            "original_format": transcript_record['transcript_format'],
        },
        "blocks": [
            {
                "id": str(uuid.uuid3(KF_TRANSCRIPT_NAMESPACE, "%s: %s --> %s" % (episode_number, str(block.start_timestamp), str(block.end_timestamp)))),
                "speaker": block.speaker_name,
                "start_timestamp": block.start_timestamp,
                "end_timestamp": block.end_timestamp,
                "text": block.text,
            }
            for block in transcript.blocks
        ]
    }

    with open("transcripts/best/%s.json" % episode_number, "w") as outfile:
        json.dump(
            obj,
            outfile,
            indent=2,
            sort_keys=True,
        )
