import click
import io
import maya
import pandas as pd
import parse
import rainbowbatch.kfio as kfio
import rainbowbatch.wiki.page_listing as page_listing

from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from os.path import exists
from rainbowbatch.git import check_git_branch
from rainbowbatch.remap.wiki_cleaner import simple_format
from tqdm import tqdm
from transcripts import create_best_transcript_listing
from transcripts import format_timestamp
from transcripts import parse_transcript


@click.command()
@click.option("--overwrite", is_flag=True, show_default=True, default=False, help="Rewrite existing files")
def stamp_transcripts_cli(overwrite):
    stamp_transcripts(overwrite)

# TODO(woursler): Add ep number range flags.


def stamp_transcripts(overwrite):
    env = Environment(
        loader=FileSystemLoader(kfio.TOP_LEVEL_DIR/"templates"),
        autoescape=select_autoescape()
    )

    env.filters["format_speaker"] = lambda speaker: "Unknown Speaker" if speaker is None else speaker
    env.filters["format_timestamp"] = format_timestamp

    template = env.get_template('transcript.wiki.template')

    episodes_df = kfio.load('data/final.json')

    assert check_git_branch('bot_raw'), "Please checkout bot_raw! Currently on %s." % git_branch

    transcript_listing = create_best_transcript_listing()
    transcript_listing = transcript_listing[transcript_listing.transcript_type != 'autosub']

    for transcript_record in tqdm(transcript_listing.to_dict(orient='records')):
        transcript_fname = transcript_record['transcript_fname']
        episode_number = transcript_record['episode_number']

        idxs = episodes_df.index[episodes_df.episode_number ==
                                 episode_number].tolist()
        # TODO: WTF print(transcript_record, idxs)
        if len(idxs) >= 2:
            continue  # FOR NOW... TODO(woursler): WTF
        assert len(idxs) <= 1
        if len(idxs) == 0:
            continue  # Likely a lost episode.
        idx = idxs[0]

        episode_details = episodes_df.iloc[idx]

        transcript = parse_transcript(transcript_record)

        if exists(episode_details['transcript_ofile']) and page_listing.lookup(episode_details['transcript_slug']) is not None and not overwrite:
            continue

        raw = template.render(
            episode_details=episode_details,
            transcript_blocks=transcript.blocks,
        )

        pretty = simple_format(raw)

        with io.open(episode_details['transcript_ofile'], mode="w", encoding="utf-8") as f:
            f.write(pretty)

        print("page_listing.add",
            episode_details['transcript_safe_title'], episode_details['transcript_slug'])
        page_listing.add(
            title=episode_details['transcript_safe_title'], slug=episode_details['transcript_slug'])

    print("SAVING PAGE LISTING")
    page_listing.save()


if __name__ == '__main__':
    stamp_transcripts_cli()
