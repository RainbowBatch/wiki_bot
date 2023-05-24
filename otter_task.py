import autosub
import click
import pandas as pd
import parse

from box import Box
from natsort import natsorted
from os.path import exists
from tqdm import tqdm
from transcripts import create_best_transcript_listing
from transcripts import create_full_transcript_listing


@click.command()
@click.option('--min-ep-num', default=0, help='Lowest numbered episode to include.')
@click.option('--max-ep-num', default=10**3, help='Highest numbered episode to include.')
def generate_otter_commands(min_ep_num, max_ep_num):

    transcript_listing = create_full_transcript_listing()

    grouped = transcript_listing.groupby('episode_number')

    missing_episode_numbers = []
    for episode_number in natsorted(grouped.groups):
        df = pd.DataFrame(grouped.get_group(episode_number))

        if not 'otter' in set(df.transcript_type):
            missing_episode_numbers.append(episode_number)

    transcript_listing = create_best_transcript_listing()

    worklist = []
    for transcript_record in tqdm(transcript_listing.to_dict(orient='records')):
        transcript_record = Box(transcript_record)

        if transcript_record.episode_number not in missing_episode_numbers:
            continue

        ep_num = int(
            ''.join([s for s in transcript_record.episode_number if s.isdigit()]))

        if ep_num < min_ep_num:
            continue

        if ep_num > max_ep_num:
            continue

        worklist.append(ep_num)

    worklist = natsorted(worklist)

    print("There are %d episodes to transcribe." % len(worklist))

    print(worklist)


if __name__ == '__main__':
    generate_otter_commands()
