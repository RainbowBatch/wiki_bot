import autosub
from glob import glob
import parse
from os.path import exists

for audio_fname in glob('audio_files/*.mp3'):
    episode_number = parse.parse("audio_files/{}.mp3", audio_fname)[0]

    if exists('audio_files/%s.srt' % episode_number) or exists('transcripts/%s.autosub.srt' % episode_number):
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


for fname in glob('audio_files/*.srt'):
    episode_number = fname.split('/')[-1][:-4]

    new_fname = r'transcripts/%s.autosub.srt' % episode_number

    with open(fname,'r') as old_file, open(new_fname,'w') as new_file:
        new_file.write(old_file.read())