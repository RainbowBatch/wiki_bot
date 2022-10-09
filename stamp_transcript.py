import io
import kfio
import maya
import page_listing
import pandas as pd
import parse

from glob import glob
from jinja2 import Environment
from jinja2 import FileSystemLoader
from jinja2 import Template
from jinja2 import select_autoescape
from pprint import pprint
from pygit2 import Repository
from wiki_cleaner import simple_format


env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape()
)

template = env.get_template('transcript.wiki.template')

def process_otter_transcript(transcript_text):
    blocks = transcript_text.split('\n\n')

    transcript_blocks = []

    parse_template = "{}  {:tt}"

    for block in blocks:
        block = block.strip()
        if len(block) == 0:
            continue
        block_lines = block.split('\n')
        assert len(block_lines) == 2, block

        name, timestamp = block_lines[0].strip().split('  ')
        text = block_lines[1].strip()

        transcript_blocks.append({
            'name': name,
            'timestamp': timestamp,
            'text': text,
        })

    return transcript_blocks



def stamp_transcript():
    episodes_df = kfio.load('data/final.json')

    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'bot_raw', "Please checkout bot_raw! Currently on %s." % git_branch

    for transcript_fname in glob('transcripts/*.otter.txt'):
        print("Processing", transcript_fname)
        episode_number = parse.parse(r"transcripts\{}.otter.txt", transcript_fname)[0]

        idxs = episodes_df.index[episodes_df.episode_number == episode_number].tolist()
        assert len(idxs) == 1
        idx = idxs[0]

        episode_details = episodes_df.iloc[idx]

        with open(transcript_fname) as transcript_file:
            transcript_blocks = process_otter_transcript(transcript_file.read())

        raw = template.render(
            episode_details=episode_details,
            transcript_blocks=transcript_blocks,
        )

        pretty = simple_format(raw)

        slug = 'Transcript{{FORWARD_SLASH}}' + episode_details['slug']
        ofile = 'kf_wiki_content/' + slug + '.wiki'
        title = 'Transcript/' + episode_details['title']

        with io.open(ofile, mode="w", encoding="utf-8") as f:
            f.write(pretty)

        page_listing.add(title=title, slug=slug)

    page_listing.save()

if __name__ == '__main__':
    stamp_transcript()
