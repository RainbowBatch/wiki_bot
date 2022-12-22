import click
import datetime
import kfio
import maya
import pandas as pd
import pandoc
import pathlib

from box import Box
from entity import aggregate_proto_entities
from entity import extract_entities
from entity import parse_entity_orgin
from entity_extraction_util import transcript_extractor
from entity_extraction_util import wikipage_extractor
from pprint import pprint
from pygit2 import Repository
from pytimeparse.timeparse import timeparse as duration_seconds
from tqdm import tqdm
from transcripts import create_full_transcript_listing
from transcripts import parse_transcript


@click.command()
@click.option('--margin', default='7d', help='Duration for which to re-process (e.g. 3d)')
@click.option("--overwrite", is_flag=True, show_default=True, default=False, help="Don't use the contents of the existing proto-entities file.")
def extract_proto_entities_cli(margin, overwrite):
    extract_proto_entities(margin, overwrite)


def modified_time(path):
    if not path.exists():
        return None

    return maya.MayaDT.from_datetime(datetime.datetime.fromtimestamp(
        path.stat().st_mtime, tz=datetime.timezone.utc))


def extract_proto_entities(margin, overwrite):
    PROTO_ENTITIES_PATH = pathlib.Path('data/proto_entities.json')

    last_run_time = modified_time(PROTO_ENTITIES_PATH)

    if last_run_time is None:
        overwrite = True  # Nothing to start with, so we have to overwrite.

    if margin is None or overwrite:
        margin_seconds = None
    else:
        margin_seconds = duration_seconds(margin)

    new_proto_entities = dict()

    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    if git_branch == 'latest_edits':

        page_listing = kfio.load('kf_wiki_content/page_listing.json')

        print("Processing wiki pages into proto entities")
        for page_record in tqdm(page_listing.to_dict(orient='records')):
            page_record = Box(page_record)

            if page_record.slug.startswith('RainbowBatch_Entities') or page_record.slug.startswith('Transcript') or page_record.slug.startswith('List_of_Knowledge_Fight_episodes'):
                continue  # Avoid circular entity inclusion.

            fname = 'kf_wiki_content/%s.wiki' % page_record.slug

            origin = '__'.join(
                ['None' if frag is None else frag for frag in parse_entity_orgin(page_record.title)])

            with open(fname, encoding='utf-8') as f:
                f_contents = f.read()

            proto_entities = aggregate_proto_entities(
                wikipage_extractor(f_contents, origin)).sort_values(
                'entity_name', key=lambda col: col.str.lower())
            new_proto_entities[origin] = kfio.serialize_without_nulls(
                proto_entities)
    else:
        assert not overwrite, "Checkout latest_edits to use overwrite more."
        print("Skipping Wiki pages. Checkout latest_edits to reindex.")

    print("Processing transcripts into proto entities")

    transcript_listing = create_full_transcript_listing()

    transcript_listing['modified_time'] = transcript_listing.transcript_fname.apply(
        lambda fname: modified_time(pathlib.Path(fname)))

    if margin_seconds is None:
        relevant_transcript_listing = transcript_listing
    else:
        cutoff_time = last_run_time.subtract(seconds=margin_seconds)
        relevant_transcript_listing = transcript_listing[transcript_listing.modified_time > cutoff_time]

    for transcript_record in tqdm(relevant_transcript_listing.to_dict(orient='records')):
        transcript = parse_transcript(transcript_record)

        origin = '__'.join(['None' if frag is None else frag for frag in parse_entity_orgin(
            transcript_record['transcript_fname'])])

        proto_entities = aggregate_proto_entities(
            transcript_extractor(transcript, origin)).sort_values(
            'entity_name', key=lambda col: col.str.lower())
        new_proto_entities[origin] = kfio.serialize_without_nulls(
            proto_entities)

    print("Done, saving results.")
    if overwrite:
        kfio.save(pd.DataFrame(new_proto_entities.items(), columns=[
                  'origin', 'proto_entities']).sort_values('origin'), PROTO_ENTITIES_PATH)
    else:
        combined_proto_entities = new_proto_entities
        for _, origin, proto_entities in kfio.load(PROTO_ENTITIES_PATH).itertuples():
            origin = tuple(origin)
            if origin not in combined_proto_entities:
                combined_proto_entities[origin] = proto_entities
        kfio.save(pd.DataFrame(combined_proto_entities.items(), columns=[
                  'origin', 'proto_entities']).sort_values('origin'), PROTO_ENTITIES_PATH)


if __name__ == '__main__':
    extract_proto_entities_cli()
