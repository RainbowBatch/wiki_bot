import json
import uuid

from box import Box
from rainbowbatch.transcripts import Transcript
from rainbowbatch.transcripts import parse_alt_json_transcript

KF_TRANSCRIPT_NAMESPACE = uuid.UUID('c43bff5e-c3d0-4ee8-9925-d76901871ef7')

episode_number = 1028
ipath = r"C:\Users\wours\Downloads\%s.whisper.json" % episode_number

with open(ipath) as f:
    transcript_blocks = parse_alt_json_transcript(f.read())

transcript = Transcript(blocks=transcript_blocks)

obj = {
    "audio_identifier": "KF_%s" % episode_number,
    # TODO: Fix this in generality.
    "audio_url": "https://traffic.libsyn.com/secure/knowledgefight/M-B2.mp3",
    "history": {
        "type": "whisper",
        "original_format": "json",
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

print("WRITING", "transcripts/%s.whisper.json" % episode_number)
with open("transcripts/%s.whisper.json" % episode_number, "w") as outfile:
    json.dump(
        obj,
        outfile,
        indent=2,
        sort_keys=True,
    )
