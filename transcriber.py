import autosub
from glob import glob
import parse
from os.path import exists

for audio_fname in glob('audio_files/*.mp3'):
    episode_number = parse.parse("audio_files/{}.mp3", audio_fname)[0]

    if exists('audio_files/%s.srt' % episode_number):
        print("Skipping", audio_fname)
        continue
    print("Processing", audio_fname)

    subtitle_file_path = autosub.generate_subtitles(
        source_path=audio_fname,
        concurrency=10,
        src_language='en',
        dst_language='en',
        api_key=None,
        subtitle_file_format='srt',
        output=None,
    )
