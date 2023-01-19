import click
import diff_match_patch as dmp_module
import pandas as pd

from attr import asdict
from box import Box
from natsort import natsorted
from termcolor import colored
from thefuzz import fuzz
from tqdm import tqdm
from transcripts import create_full_transcript_listing
from transcripts import format_timestamp
from transcripts import parse_transcript
from transcripts import type_sorter_index


@click.command()
@click.argument('searchterm')
@click.option('--remove-overlaps/--include-overlaps', default=True)
def search_transcripts_cli(searchterm, remove_overlaps):
    search_transcripts(searchterm, remove_overlaps)


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


def highlight_snippets(text, ranges, max_snippet_len=80):
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
            result += colored(text[a[0]:a[1]], 'cyan')
            result += text[a[1]:end]
        else:
            result += colored(text[a[0]:a[1]], 'cyan')
            result += text[a[1]:b[0]]
    if end < len(text):
        result += ' ...'

    return result


def search_transcripts(searchterm, remove_overlaps):
    dmp = dmp_module.diff_match_patch()

    transcript_listing = create_full_transcript_listing()

    result_records = []

    for record in tqdm(transcript_listing.to_dict(orient='records')):
        transcript = parse_transcript(record)
        transcript.augment_timestamps()
        for block in transcript.blocks:
            match = fuzz.partial_ratio(block.text.lower(), searchterm.lower())

            if match > 80 and len(block.text) / len(searchterm) > .6:
                result_records.append(merge(
                    record,
                    asdict(block),
                    {'match': match}
                ))

            # TODO: Two line match?

    result_df = pd.DataFrame.from_records(result_records)

    best_result_df = result_df.nlargest(1000, 'match').sort_values(
        by=['episode_number', 'start_timestamp'])

    grouped = best_result_df.groupby('episode_number')

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

            print("Ep%s@[%s --> %s]  %s:  %s" % (
                row.episode_number.rjust(3, '0'),
                format_timestamp(row.start_timestamp),
                default_if_none(format_timestamp(row.end_timestamp), ''),
                default_if_none(row.speaker_name, "Unknown"),
                highlight_snippets(row.text, match_range(diff)),
            ))


if __name__ == '__main__':
    search_transcripts_cli()
