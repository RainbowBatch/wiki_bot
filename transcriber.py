import autosub
import parse

from glob import glob
from os.path import exists
from pathlib import Path

TOP_LEVEL_DIR = Path(
    __file__
).parent.absolute(
)

TRANSCRIPT_DIR = TOP_LEVEL_DIR / 'transcripts'
AUDIO_FILES_DIR = TOP_LEVEL_DIR / 'audio_files'

for audio_fname in glob(str(AUDIO_FILES_DIR / '*.mp3')):
    episode_number = parse.parse(
        str(AUDIO_FILES_DIR / '{}.mp3'), audio_fname)[0]

    if exists(str(AUDIO_FILES_DIR / '%s.srt') % episode_number) or exists(str(TRANSCRIPT_DIR / '%s.autosub.srt') % episode_number):
        print("Skipping", Path(audio_fname).name)
        continue
    print("Processing", Path(audio_fname).name)

    subtitle_file_path = autosub.generate_subtitles(
        source_path=audio_fname,
        concurrency=10,
        src_language='en',
        dst_language='en',
        api_key=None,
        subtitle_file_format='srt',
        output=None,
    )


for fname in glob(str(AUDIO_FILES_DIR / '*.srt')):
    episode_number = fname.split('/')[-1][:-4]

    new_fname = str(TRANSCRIPT_DIR / '%s.autosub.srt') % episode_number

    with open(fname, 'r') as old_file, open(new_fname, 'w') as new_file:
        new_file.write(old_file.read())
