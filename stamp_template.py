import io
import kfio

from jinja2 import Template
from wiki_cleaner import simple_format

episodes_df = kfio.load('data/final.json')

with open('episode.wiki.template') as episode_template_f:
    template = Template(episode_template_f.read())

for record in episodes_df.to_dict(orient='records'):
    raw = template.render(**record)
    pretty = simple_format(raw)
    with io.open(record['ofile'], mode="w", encoding="utf-8") as f:
        f.write(pretty)

# ??? Automatic Links
