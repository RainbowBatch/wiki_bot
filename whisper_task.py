
import autosub
from glob import glob
import parse
from os.path import exists

i = 0
for audio_fname in glob(r'audio_files\infowars\*.mp3'):
    episode_label = parse.parse(r"audio_files\infowars\{}.mp3", audio_fname)[0]

    if not episode_label.startswith('2013'):
        continue

    if exists(r'transcripts\infowars_tmp\%s.mp3.srt' % episode_label):
        print("Skipping", audio_fname)
        continue
    print("Processing", audio_fname)

    print(
        r"whisper --model medium.en --language en --device cuda --output_dir=.\transcripts\infowars_tmp\ .\audio_files\infowars\%s.mp3 > .\transcripts\infowars_tmp\%s.txt" % (episode_label, episode_label))

    i += 1
    if i >= 4:
        break