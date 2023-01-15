
import autosub
import click
import parse

from box import Box
from glob import glob
from os.path import exists
from tqdm import tqdm
from transcripts import create_best_transcript_listing


@click.command()
@click.option('--n_threads', default=2, help='Number of simultaneous whisper processes your GPU can run.')
@click.option('--min-ep-num', default=0, help='Lowest numbered episode to include.')
@click.option('--max-ep-num', default=10**3, help='Highest numbered episode to include.')
def generate_whisper_commands(min_ep_num, max_ep_num, n_threads):

    transcript_listing = create_best_transcript_listing()
    bad_transcript_listing = transcript_listing[transcript_listing.transcript_type == 'autosub']

    commands = []
    for transcript_record in tqdm(bad_transcript_listing.to_dict(orient='records')):
        transcript_record = Box(transcript_record)

        ep_num = int(
            ''.join([s for s in transcript_record.episode_number if s.isdigit()]))

        if ep_num < min_ep_num:
            continue

        if ep_num > max_ep_num:
            continue

        commands.append(
            r"whisper --model medium.en --language en --device cuda --output_dir=.\transcripts\ .\audio_files\%s.mp3" % transcript_record.episode_number)

    for i in range(n_threads):
        print('\n\n\n')
        print('; '.join(commands[i::n_threads]))


if __name__ == '__main__':
    generate_whisper_commands()
