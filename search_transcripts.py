import click
import diff_match_patch as dmp_module
import pandas as pd

from attr import asdict
from attr import attr
from attr import attrs
from box import Box
from natsort import natsorted
from termcolor import colored
from thefuzz import fuzz
from tqdm import tqdm
from transcripts import create_full_transcript_listing
from transcripts import format_timestamp
from transcripts import parse_transcript
from transcripts import type_sorter_index
from typing import List


@click.command()
@click.argument('searchterm')
@click.option('--remove-overlaps/--include-overlaps', default=True)
def search_transcripts_cli(searchterm, remove_overlaps):
    results = search_transcripts(
        searchterm, remove_overlaps, highlight_f=lambda s: colored(s, 'cyan'))
    display_search_results(results)


def merge(*dict_list):
    result = {}
    for d in dict_list:
        result.update(d)
    return result


def default_if_none(v, default):
    if pd.isna(v):
        return default
    return v


def match_range(diff):
    i = 0
    matched_segments = []
    for t, diff_section in diff:
        if t == -1:
            i += len(diff_section)
        if t == 0:
            matched_segments.append((i, i + len(diff_section)))
            i += len(diff_section)
    return matched_segments


# Used to ensure that highlighted sections never start or end with whitespace.
def split_whitespace(string):
    leading_whitespace = string[:len(string) - len(string.lstrip())]
    trailing_whitespace = string[len(string.rstrip()):]
    middle_text = string.strip()
    if len(middle_text) > 0:
        return (leading_whitespace, middle_text, trailing_whitespace)
    return (leading_whitespace, '', '')


def highlight_snippets(text, ranges, max_snippet_len=80, highlight_f=lambda x: x):
    if len(ranges) == 0:
        return text

    min_range_idx = min([min(r) for r in ranges])
    max_range_idx = max([max(r) for r in ranges])

    ranges = [None] + ranges + [None]

    result = ''
    if len(text) <= max_snippet_len:
        start = 0
        end = len(text)
    else:
        range_idx_span = max_range_idx - min_range_idx
        if range_idx_span >= max_snippet_len:
            start = min_range_idx-1
            end = max_range_idx+1
        else:
            range_slop = (max_snippet_len - range_idx_span) // 2
            start = min_range_idx-range_slop
            end = max_range_idx+range_slop

    if start > 0:
        result += '... '
    for a, b in zip(ranges, ranges[1:]):
        if a is None:
            result += text[start:b[0]]
        elif b is None:
            leading_ws, middle_text, trailing_ws = split_whitespace(
                text[a[0]:a[1]])
            result += leading_ws
            if len(middle_text) > 0:
                result += highlight_f(middle_text)
            result += trailing_ws
            result += text[a[1]:end]
        else:
            leading_ws, middle_text, trailing_ws = split_whitespace(
                text[a[0]:a[1]])
            result += leading_ws
            if len(middle_text) > 0:
                result += highlight_f(middle_text)
            result += trailing_ws
            result += text[a[1]:b[0]]
    if end < len(text):
        result += ' ...'

    return result


@attrs
class SearchResult:
    episode_number = attr()
    start_timestamp = attr()
    end_timestamp = attr()
    speaker_name = attr()
    text = attr()
    snippet = attr()


def display_search_results(results: List[SearchResult]):
    for result in results:
        print("Ep%s@[%s --> %s]  %s:  %s" % (
            result.episode_number.rjust(3, '0'),
            format_timestamp(result.start_timestamp),
            default_if_none(format_timestamp(result.end_timestamp), ''),
            default_if_none(result.speaker_name, "Unknown"),
            result.snippet,
        ))


def search_transcript(transcript_record, transcript, searchterm):
    for block in transcript.blocks:
            match = fuzz.partial_ratio(block.text.lower(), searchterm.lower())

            if match > 80 and len(block.text) / len(searchterm) > .6:
                yield merge(
                    transcript_record,
                    asdict(block),
                    {'match': match}
                )

def search_transcripts(searchterm, remove_overlaps, max_results=1000, highlight_f=lambda x: x) -> List[SearchResult]:
    dmp = dmp_module.diff_match_patch()

    transcript_listing = create_full_transcript_listing()

    result_records = []

    for record in tqdm(transcript_listing.to_dict(orient='records')):
        transcript = parse_transcript(record)
        transcript.augment_timestamps()

        result_records.extend(search_transcript(record, transcript, searchterm))

    if len(result_records) == 0:
        return []

    result_df = pd.DataFrame.from_records(result_records)

    best_result_df = result_df.nlargest(max_results, 'match').sort_values(
            by=['episode_number', 'start_timestamp'])

    grouped = best_result_df.groupby('episode_number')

    results = []

    for episode_number in natsorted(grouped.groups):
        df = pd.DataFrame(grouped.get_group(episode_number))

        # Detect overlapping segments and only keep the best.
        if remove_overlaps:
            df['overlap_group'] = (df['end_timestamp'].cummax(
            ).shift() <= df['start_timestamp']).cumsum()
            df['transcript_type_rank'] = df['transcript_type'].map(
                type_sorter_index)

            df = df.sort_values(
                ['start_timestamp', 'transcript_type_rank'],
                ascending=[True, True],
            ).drop_duplicates('overlap_group', keep='first')

        for _, row in df.iterrows():
            diff = dmp.diff_main(row.text.lower(), searchterm.lower())

            results.append(SearchResult(
                row.episode_number,
                row.start_timestamp,
                row.end_timestamp,
                row.speaker_name,
                row.text,
                highlight_snippets(row.text, match_range(
                    diff), highlight_f=highlight_f),
            ))

    return results


if __name__ == '__main__':
    search_transcripts_cli()
