
import autosub
from glob import glob
import parse
from os.path import exists

commands = []

N = 100

i = 0
#for audio_fname in glob(r'audio_files\infowars\*.mp3'):
for episode_number in range(500, 600):

    #episode_label = parse.parse(r"audio_files\infowars\{}.mp3", audio_fname)[0]

    #if not episode_label.startswith('2012'):
    #    continue

    #if exists(r'transcripts\infowars_tmp\%s.mp3.srt' % episode_label)  or exists(r'transcripts\infowars\%s.txt' % episode_label):
    #    continue

    commands.append(
#        r"whisper --model medium.en --language en --device cuda --output_dir=.\transcripts\infowars_tmp\ .\audio_files\infowars\%s.mp3" % episode_label)
        r"whisper --model medium.en --language en --device cuda --output_dir=.\transcripts\ .\audio_files\%s.mp3" % str(episode_number))

    i += 1
    if i >= 2*N:
        break

print('\n\n\n')
print('; '.join(commands[0::2]))
print('\n\n\n')
print('; '.join(commands[1::2]))
print('\n\n\n')
