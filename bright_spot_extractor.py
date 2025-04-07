import base64
import json
import kfio
import time
import natsort

import pandas as pd

from box import Box
from google import genai
from google.genai import types
from pprint import pprint
from tqdm import tqdm
from transcripts import create_best_transcript_listing
from transcripts import parse_transcript

transcript_listing = create_best_transcript_listing()
episode_listing = kfio.load('data/final.json')


def attempt_bright_spot_extraction(episode_number):
    with open("secrets/gemini.json") as secrets_f:
        secrets = Box(json.load(secrets_f))

    client = genai.Client(
        api_key=secrets.api_key,
    )

    files = [
        # Make the file available in local system working directory
        client.files.upload(
            file="transcript_processing/first_15m/%s.txt" % episode_number),
    ]
    print(files)
    model = "gemini-2.0-flash"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_uri(
                    file_uri=files[0].uri,
                    mime_type=files[0].mime_type,
                ),
                types.Part.from_text(
                    text="""What is Jordan's bright spot? What is Dan's? Respond with a json object structured as follows: {"jordan": ["bright spots here"], "dan": ["bright spots here"]}. Bright spot text should be nicely formatted, concise, and not in first person (e.g. Playing Minecraft, Eating interesting mustards)."""),
            ],
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        temperature=1,
        top_p=0.95,
        top_k=40,
        max_output_tokens=8192,  # TODO: This can likely be much shorter.
        response_mime_type="text/plain",
    )

    response = ""
    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        response += chunk.text

    # Extract and load the JSON
    response_data = json.loads(
        response[response.find('{'):response.rfind('}') + 1])

    return response_data


completed_episode_numbers = set()
with open("data/ai_bright_spots.jsonl") as extracted_brightspots_f:
    for l in extracted_brightspots_f:
        data = json.loads(l)
        completed_episode_numbers.add(data['episode_number'])

with open("data/ai_bright_spots.jsonl", "a") as extracted_brightspots_f:

    for transcript_record in tqdm(transcript_listing.to_dict(orient='records')):
        episode_number = transcript_record['episode_number']

        if episode_number in completed_episode_numbers:
            continue

        possible_episode_records = episode_listing[episode_listing.episode_number == episode_number].to_dict(
            orient='records')

        if len(possible_episode_records) != 1:
            print("Failure on", episode_number)
            continue

        episode_record = Box(possible_episode_records[0])

        transcript = parse_transcript(transcript_record)
        transcript.augment_timestamps()

        textified_transcript = '\n'.join([
            ("" if block.speaker_name is None else "%s: " %
             block.speaker_name) + block.text
            for block in transcript.blocks
            if block.start_timestamp < 15 * 60  # 15 Minutes
        ])

        if "bright spot" not in textified_transcript.lower():
            continue

        print(episode_number, "has a bright spot")

        # print(textified_transcript)

        with open("transcript_processing/first_15m/%s.txt" % episode_number, "w") as outfile:
            outfile.write(textified_transcript)

        result = {
            "episode_number": episode_number,
            **attempt_bright_spot_extraction(episode_number)
        }

        # Write one JSON object per line
        json.dump(result, extracted_brightspots_f)
        extracted_brightspots_f.write("\n")
        extracted_brightspots_f.flush()  # Force the write to disk

        # Wait before the next request
        time.sleep(5)


final_data = []
with open("data/ai_bright_spots.jsonl") as extracted_brightspots_f:
    for l in extracted_brightspots_f:
        data = Box(json.loads(l))
        for bs in data['dan']:
            final_data.append([
                data.episode_number,
                "Dan",
                bs
            ])
        for bs in data['jordan']:
            final_data.append([
                data.episode_number,
                "Jordan",
                bs
            ])


bs_df = pd.DataFrame(final_data, columns=[
                     "episode_number", "host", "bright_spot"])
bs_df.sort_values(by=['episode_number'], key=natsort.natsort_keygen())

kfio.save(bs_df, 'data/ai_bright_spots.json')
bs_df.to_csv('data/ai_bright_spots.csv', index=False)