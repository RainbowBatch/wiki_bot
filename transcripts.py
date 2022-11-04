import datetime
import pandas as pd
import parse

from attr import attr
from attr import attrs
from box import Box
from box import Box
from glob import glob
from tqdm import tqdm


def create_full_transcript_listing():
    header = ['episode_number', 'transcript_type',
              'transcript_format', 'transcript_fname']
    rows = []
    for transcript_fname in glob('transcripts/*.*.*'):
        episode_number, transcript_type, transcript_format = parse.parse(
            "transcripts\\{}.{}.{}", transcript_fname)
        rows.append([episode_number, transcript_type,
                     transcript_format, transcript_fname])

    return pd.DataFrame(rows, columns=header)


def create_best_transcript_listing():
    df = create_full_transcript_listing()

    type_sorter = [
        'autosub',
        'welder',
        'whisper',
        'fek', # TODO(woursler)
        'otter',
        'manual',
    ]
    type_sorter_index = dict(zip(type_sorter, range(len(type_sorter))))
    df['transcript_type_rank'] = df['transcript_type'].map(type_sorter_index)

    df = df.sort_values(
        ['episode_number', 'transcript_type_rank', 'transcript_format'],
        ascending=[True, True, False],
    )

    return df.drop_duplicates('episode_number', keep='last')[['episode_number', 'transcript_type',
                                                              'transcript_format', 'transcript_fname']]


@attrs
class TranscriptBlock:
    speaker_name = attr(default=None)
    start_timestamp = attr(default=None)
    end_timestamp = attr(default=None)
    text = attr(default=None)


@attrs
class Transcript:
    metadata = attr(default=None)
    blocks = attr(default=None)


def parse_timestamp(ts_str):
    ts_str = ts_str.strip()

    pattern = '%M:%S'
    if ts_str.count(':') == 2:
        pattern = '%H:' + pattern
    if ',' in ts_str:
        pattern += ",%f"
    if '.' in ts_str:
        pattern += ".%f"

    ts_dt = datetime.datetime.strptime(ts_str, pattern)
    ts_seconds = datetime.timedelta(hours=ts_dt.hour, minutes=ts_dt.minute,
                                    seconds=ts_dt.second, microseconds=ts_dt.microsecond).total_seconds()

    return ts_seconds


def format_timestamp(ts_seconds):
    hours, remainder = divmod(ts_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    seconds, microseconds = divmod(seconds, 1)
    return '{:02}:{:02}:{:02}.{}'.format(int(hours), int(minutes), int(seconds), str(microseconds).split('.')[-1][:3])


def parse_whisper_txt(transcript_text):
    transcript_text = transcript_text.replace('\x00', '').replace('ÿþ', '')
    blocks = transcript_text.split('\n')

    transcript_blocks = []

    parse_template = "[{} --> {}] {}"

    for block in blocks:
        line = block.strip()
        if len(line) == 0:
            continue
        start_timestamp, end_timestamp, text = parse.parse(
            parse_template, line)

        transcript_blocks.append(TranscriptBlock(
            start_timestamp=parse_timestamp(start_timestamp),
            end_timestamp=parse_timestamp(end_timestamp),
            text=text.strip(),
        ))
    return transcript_blocks


def parse_otter_txt(transcript_text):
    blocks = transcript_text.split('\n\n')

    transcript_blocks = []

    for block in blocks:
        block = block.strip()
        if len(block) == 0:
            continue
        block_lines = block.split('\n')
        assert len(block_lines) == 2, block

        first_line_chunks = block_lines[0].strip().split('  ')
        assert len(first_line_chunks) == 2, block_lines[0]
        name, timestamp = first_line_chunks
        text = block_lines[1].strip()

        transcript_blocks.append(TranscriptBlock(
            speaker_name=name,
            start_timestamp=parse_timestamp(timestamp),
            text=text,
        ))

    return transcript_blocks


def parse_srt(transcript_text):
    blocks = transcript_text.split('\n\n')

    transcript_blocks = []

    for block in blocks:
        block_lines = block.split('\n')
        if block_lines[-1].strip() == '':
            block_lines = block_lines[:-1]
        if len(block_lines) == 0:
            continue
        assert len(block_lines) == 3, block_lines

        start_timestamp, end_timestamp = block_lines[1].split(" --> ")
        text = block_lines[2]

        transcript_blocks.append(TranscriptBlock(
            start_timestamp=parse_timestamp(start_timestamp),
            end_timestamp=parse_timestamp(end_timestamp),
            text=text,
        ))

    return transcript_blocks


FORMAT_PARSERS = {
    ('fek', 'txt'): parse_otter_txt, # FowlEdgeKnight is mostly editing otter transcripts.
    ('otter', 'txt'): parse_otter_txt,
    ('autosub', 'srt'): parse_srt,
    ('welder', 'srt'): parse_srt,
    ('whisper', 'txt'): parse_whisper_txt,
}


def parse_transcript(transcript_record):
    transcript_record = Box(transcript_record)
    format_parser_key = (transcript_record.transcript_type,
                         transcript_record.transcript_format)

    assert format_parser_key in FORMAT_PARSERS, format_parser_key

    with open(transcript_record.transcript_fname) as transcript_file:
        transcript_blocks = FORMAT_PARSERS[format_parser_key](
            transcript_file.read())

    return Transcript(metadata=Box(transcript_record), blocks=transcript_blocks)


if __name__ == '__main__':
    for record in tqdm(create_full_transcript_listing().to_dict(orient='records')):
        z = parse_transcript(record)
