import pandas as pd
import rainbowbatch.kfio as kfio

from natsort import natsorted
from pprint import pprint
from rainbowbatch.transcripts import create_full_transcript_listing
from rainbowbatch.transcripts import parse_transcript
from search_transcripts import search_transcript
from tqdm import tqdm

# TODO: Move this script into sensitive. It's too exploitable otherwise.

SEARCHTERMS = [
    # Look for the drops that Dan and Jordan play when they mention wonk names.
    "a little breaky for me.",
    "and then we're going to come back and i'm going to start the show over.",
    "few living black people that have been abused by white people as much as i have been abused by black people.",
    "fuck the horse you rode in on and all your shit.",
    "go home to your mother and tell her you're brilliant.",
    "i apologize to the crew and the listeners yesterday that i was legitimately having breakdowns on air",
    "i don't want to hate black people.",
    "i got plenty of words for you, but at the end of the day, fuck you",
    "i have risen above my enemies.",
    "i might quit tomorrow, actually.",
    "i renounce jesus christ",
    "i'm a policy wonk",
    "i'm just going to take a little breakie.",
    "jar jar binks has a caribbean black accent.",
    "loser little titty baby.",
    "maybe i'll just be gone a month, maybe five years.",
    "maybe i'll walk out of here tomorrow and you never see me again.",
    "maybe today should be my last broadcast.",
    "sodomites sent me a bucket of poop.",

    # Other things that tend to indicate wonk names are about to be said.
    "birthday shoutout",
    "say hello to some new wonks",
]

MARGIN = 10  # seconds

transcript_listing = create_full_transcript_listing()

# TODO(woursler): Do this for every transcript related to each episode, rather than each transcript alone.

episode_grouping = transcript_listing.groupby('episode_number')

sensitive_listing = {}

for episode_number in tqdm(natsorted(episode_grouping.groups)):
    episode_sensitive_listing = []
    sensitive_listing[episode_number] = episode_sensitive_listing

    episode_transcript_listing = pd.DataFrame(
        episode_grouping.get_group(episode_number))

    result_records = []

    for record in episode_transcript_listing.to_dict(orient='records'):

        transcript = parse_transcript(record)
        transcript.augment_timestamps()

        for searchterm in SEARCHTERMS:
            result_records.extend(search_transcript(
                record, transcript, searchterm))

    if len(result_records) == 0:
        continue

    result_df = pd.DataFrame.from_records(result_records)

    result_df['mod_start_timestamp'] = result_df['start_timestamp'].apply(
        lambda ts: ts - MARGIN)
    result_df['mod_end_timestamp'] = result_df['end_timestamp'].apply(
        lambda ts: ts + MARGIN)

    result_df = result_df.sort_values(by='mod_start_timestamp')

    # TODO(woursler): Multiple transcripts... grouped = best_result_df.groupby('episode_number')

    result_df['overlap_group'] = (result_df['mod_end_timestamp'].cummax(
    ).shift() <= result_df['mod_start_timestamp']).cumsum()

    sensitive_groups = result_df.groupby('overlap_group')

    for group_id in sensitive_groups.groups:
        group_df = pd.DataFrame(sensitive_groups.get_group(group_id))

        start_timestamp = group_df.mod_start_timestamp.min()
        if start_timestamp < 0:
            start_timestamp = 0
        end_timestamp = group_df.mod_end_timestamp.max()

        episode_sensitive_listing.append({
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp,
            'size': len(group_df),
        })

pprint(sensitive_listing)
kfio.save_json(sensitive_listing, 'sensitive/raw_segments.json')
