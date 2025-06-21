import click
import diff_match_patch as dmp_module
import pandas as pd

from attr import asdict
from attr import attr
from attr import attrs
from box import Box
from natsort import natsorted
from rainbowbatch.transcripts import create_full_transcript_listing
from rainbowbatch.transcripts import format_timestamp
from rainbowbatch.transcripts import parse_transcript
from rainbowbatch.transcripts import type_sorter_index
from sensitive.transcript_search import SearchResult
from sensitive.transcript_search import search_transcripts
from termcolor import colored
from thefuzz import fuzz
from tqdm import tqdm
from typing import List


def default_if_none(v, default_v):
    if v is not None:
        return v
    return default_v


def display_search_results(results: List[SearchResult]):
    for result in results:
        print("Ep%s@[%s --> %s]  %s:  %s" % (
            result.episode_number.rjust(3, '0'),
            format_timestamp(result.start_timestamp),
            default_if_none(format_timestamp(result.end_timestamp), ''),
            default_if_none(result.speaker_name, "Unknown"),
            result.snippet,
        ))


@click.command()
@click.argument('searchterm')
@click.option('--remove-overlaps/--include-overlaps', default=True)
def search_transcripts_cli(searchterm, remove_overlaps):
    results = search_transcripts(
        searchterm, remove_overlaps, highlight_f=lambda s: colored(s, 'cyan'))
    display_search_results(results)


if __name__ == '__main__':
    search_transcripts_cli()
