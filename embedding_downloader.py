import os
import openai
import kfio
import pandas as pd
from wiki_cleaner import simple_format
import pandoc

with open("secrets/openaiorg.txt") as openaiorg_f:
   openai.organization = openaiorg_f.read().strip()
with open("secrets/openaikey.txt") as openaikey_f:
   openai.api_key = openaikey_f.read().strip()


def get_embedding(text, model="text-similarity-babbage-001"):
   text = text.replace("\n", " ")
   return openai.Embedding.create(input = [text], model=model)['data'][0]['embedding']


classified_descriptions = kfio.load('data/final.json')[['episode_number', 'mediawiki_description']]

unclassified_descriptions = kfio.load('data/tracker_unique_rows.json')
def f(html):
    if pd.isna(html):
        return None
    else:
        return simple_format(
            pandoc.write(
                pandoc.read(html,
                            format="html-native_divs-native_spans"),
                format="mediawiki"
            )
        )
unclassified_descriptions['mediawiki_description'] = unclassified_descriptions.details_html.apply(f)
unclassified_descriptions = unclassified_descriptions[['episode_number', 'mediawiki_description']]

all_descriptions = classified_descriptions.append(unclassified_descriptions, ignore_index=True)


existing_embeddings = kfio.load('data/description_embeddings.json')
# This is a hack because some subsets are currently interpreted as ints
existing_embeddings['episode_number'] = existing_embeddings['episode_number'].astype(str)

existing_keys = existing_embeddings.episode_number.to_list()
all_keys = all_descriptions.episode_number.to_list()

new_keys = [x for x in all_keys if x not in existing_keys]

if len(new_keys) <= 0:
   print("Didn't find anything new to download.")
   quit()

print('Downloading for %d episodes:' % len(new_keys), new_keys)

new_embeddings = all_descriptions[all_descriptions.episode_number.isin(new_keys)]
new_embeddings['gpt3_ts_babbage_embedding'] = new_embeddings.mediawiki_description.apply(
   lambda x: get_embedding(x, model='text-similarity-babbage-001')
)

all_embeddings = existing_embeddings.append(new_embeddings, ignore_index=True)[['episode_number', 'gpt3_ts_babbage_embedding']]

print(all_embeddings)

kfio.save(all_embeddings, 'data/description_embeddings.json')
