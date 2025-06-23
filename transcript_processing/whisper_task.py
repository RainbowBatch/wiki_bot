import click
import pandas as pd
import parse

from box import Box
from natsort import natsorted
from os.path import exists
from rainbowbatch.transcripts import create_best_transcript_listing
from rainbowbatch.transcripts import create_full_transcript_listing
from tqdm import tqdm


@click.command()
@click.option('--n_threads', default=1, help='Number of simultaneous whisper processes your GPU can run.')
@click.option('--min-ep-num', default=0, help='Lowest numbered episode to include.')
@click.option('--max-ep-num', default=2*10**3, help='Highest numbered episode to include.')
def generate_whisper_commands(min_ep_num, max_ep_num, n_threads):

    transcript_listing = create_full_transcript_listing()

    grouped = transcript_listing.groupby('episode_number')

    missing_episode_numbers = []
    for episode_number in natsorted(grouped.groups):
        df = pd.DataFrame(grouped.get_group(episode_number))

        if not 'whisper' in set(df.transcript_type):
            missing_episode_numbers.append(episode_number)

    transcript_listing = create_best_transcript_listing()

    commands = []
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

        commands.append(
            r"whisper --model medium.en --language en --device cuda --output_dir=.\transcripts\ .\audio_files\%s.mp3" % transcript_record.episode_number)

    commands = natsorted(commands)

    print("There are %d episodes to transcribe." % len(commands))

    for i in range(n_threads):
        print('\n\n\n')
        print('; '.join(commands[i::n_threads]))


if __name__ == '__main__':
    generate_whisper_commands()
