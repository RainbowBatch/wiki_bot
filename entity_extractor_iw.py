import click
import datetime
import maya
import pathlib
import kfio
import pandas as pd
import pandoc
import glob

from box import Box
from pygit2 import Repository
from entity import aggregate_proto_entities
from entity import extract_entities
from pytimeparse.timeparse import timeparse as duration_seconds
from tqdm import tqdm
from entity import parse_entity_orgin
from pprint import pprint


@click.command()
def extract_proto_entities_cli():
    extract_proto_entities()


def extract_proto_entities():
    PROTO_ENTITIES_PATH = pathlib.Path(
        'transcripts/infowars/proto_entities.json')

    new_proto_entities = dict()

    for f_name in tqdm(glob.glob('transcripts/infowars/*.txt')):
        with open(f_name, 'rb') as f:
            f_byte_contents = f.read()
            try:
                f_contents = f_byte_contents.decode('utf-8')
            except UnicodeDecodeError:
                f_contents = f_byte_contents.decode('utf-16')

        # TODO: Do this from the transcript table.
        origin = f_name.split(r"\\")[-1][:-4]

        try:
            proto_entities = aggregate_proto_entities(
                extract_entities(f_contents, origin)).sort_values(
                'entity_name', key=lambda col: col.str.lower())
            new_proto_entities[origin] = kfio.serialize_without_nulls(
                proto_entities)
        except:
            print("Problem extracting from ", f_name)

    print("Done, saving results.")
    kfio.save(pd.DataFrame(new_proto_entities.items(), columns=[
              'origin', 'proto_entities']).sort_values('origin'), PROTO_ENTITIES_PATH)


if __name__ == '__main__':
    extract_proto_entities_cli()
