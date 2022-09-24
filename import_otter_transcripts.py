from glob import glob

for fname in glob(r'C:\Users\wours\Dropbox\Apps\Otter\*.txt'):
    episode_number = fname.split('\\')[-1][:-4]

    new_fname = r'transcripts\%s.otter.txt' % episode_number

    with open(fname,'r') as old_file, open(new_fname,'w') as new_file:
        new_file.write(old_file.read())
