import kfio
import json

from box import Box
from pprint import pprint
from transcripts import create_best_transcript_listing
from transcripts import parse_transcript
from uuid import uuid4

transcript_listing = create_best_transcript_listing()
episode_listing = kfio.load('data/final.json')

EP_NUMBER = '25'

transcript_record = Box(
    transcript_listing[transcript_listing.episode_number == EP_NUMBER].to_dict(orient='records')[0])
episode_record = Box(
    episode_listing[episode_listing.episode_number == EP_NUMBER].to_dict(orient='records')[0])

transcript = parse_transcript(transcript_record)
transcript.augment_timestamps()

obj = {
    "audio_identifier": "KF_%s" % EP_NUMBER,
    "audio_url": episode_record.download_link,
    "blocks": [
        {
            "id": str(uuid4()),
            "speaker": block.speaker_name,
            "start_timestamp": block.start_timestamp,
            "end_timestamp": block.end_timestamp,
            "text": block.text,
        }
        for block in transcript.blocks
    ]
}

with open("KF_%s_transcript.json" % EP_NUMBER, "w") as outfile:
    json.dump(
        obj,
        outfile,
        indent=2,
        sort_keys=True,
    )
