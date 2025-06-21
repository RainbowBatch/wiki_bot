import base64
import json
import natsort
import pandas as pd
import rainbowbatch.kfio as kfio
import time

from box import Box
from google import genai
from google.genai import types
from rainbowbatch.secrets import secret_file
from rainbowbatch.transcripts import create_best_transcript_listing
from rainbowbatch.transcripts import parse_transcript
from tqdm import tqdm

transcript_listing = create_best_transcript_listing()
episode_listing = kfio.load('data/final.json')


def attempt_ooc_drop_extraction(episode_number):
    with open(secret_file("gemini.json")) as secrets_f:
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
                    text="""What is the text of the Out of Context Drop from this partial transcript? Respond with a json object structured as follows: {"ooc_drop": ["Text Here"]}. Sound drop text should be nicely formatted and concise."""),
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
with open("data/ai_ooc_drops.jsonl") as extracted_ooc_drops_f:
    for l in extracted_ooc_drops_f:
        data = json.loads(l)
        completed_episode_numbers.add(data['episode_number'])

with open("data/ai_ooc_drops.jsonl", "a") as extracted_ooc_drops_f:

    for transcript_record in tqdm(transcript_listing.to_dict(orient='records')):
        episode_number = transcript_record['episode_number']

        if episode_number in completed_episode_numbers:
            continue

        possible_episode_records = episode_listing[episode_listing.episode_number == episode_number].to_dict(
            orient='records')

        # TODO: Elimante ones with bright spots?

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

        if not (("context drop" in textified_transcript.lower()) or ("out of contents" in textified_transcript.lower())):
            continue

        print(episode_number, "has a likely ooc drop")

        # print(textified_transcript)

        with open("transcript_processing/first_15m/%s.txt" % episode_number, "w") as outfile:
            outfile.write(textified_transcript)

        result = {
            "episode_number": episode_number,
            **attempt_ooc_drop_extraction(episode_number)
        }

        # Write one JSON object per line
        json.dump(result, extracted_ooc_drops_f)
        extracted_ooc_drops_f.write("\n")
        extracted_ooc_drops_f.flush()  # Force the write to disk

        # Wait before the next request
        time.sleep(5)

scraped_data = kfio.load('data/scraped_page_data.json')

final_data = []
with open("data/ai_ooc_drops.jsonl") as extracted_ooc_drops_f:
    # TODO: Skip things we already have OOC drops for.
    for l in extracted_ooc_drops_f:
        data = Box(json.loads(l))

        existing_ooc_drop = scraped_data[scraped_data.episodeNumber ==
                                         data.episode_number].oocDrop.isna().any()

        # TODO: Why is this reversed compared to what I expect?
        if existing_ooc_drop:
            final_data.append([
                data.episode_number,
                data.ooc_drop[0]
            ])
        else:
            print("SKIPPING", data.episode_number)


bs_df = pd.DataFrame(final_data, columns=[
                     "episode_number", "ooc_drop"])
bs_df.sort_values(by=['episode_number'], key=natsort.natsort_keygen())
bs_df.to_csv('data/ai_ooc_drops.csv', index=False)
