from bs4 import BeautifulSoup
import requests
import random
import time
import rainbowbatch.kfio as kfio
import json
from tqdm import tqdm

HEADERS = {
    'User-agent':
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582"
}

with open('data/entity_spelling_map.txt') as f:
    CURRENT = json.loads("{\n%s\n}" % f.read().strip()[:-1])


for k, v in CURRENT.items():
    if v is not None:
        print('"%s" => "%s"' % (k, v))


# TODO: Add a persistent cache on top of this.

def correct_spelling(s):
    time.sleep(random.uniform(5, 20))

    params = {
      'q': s,
      'hl': 'en',
      'gl': 'us',
    }

    html = requests.get('https://www.google.com/search?q=', headers=HEADERS, params=params).text
    soup = BeautifulSoup(html, 'html.parser')

    corrected = soup.find('a', id='scl')
    if corrected is None:
        return s
    return corrected.text


raw_entity_listing = kfio.load('data/raw_entities.json')

queue = raw_entity_listing.entity_name.to_list()
random.shuffle(queue)
with open('data/entity_spelling_map.txt', 'a') as f:
    for entity_name in tqdm(queue):
        if entity_name in CURRENT:
            continue

        try:
            corrected_entity_name = correct_spelling(entity_name)

            if corrected_entity_name != entity_name:
                f.write('"%s": "%s",\n' % (
                    entity_name,
                    corrected_entity_name,
                ))
                print('"%s" => "%s"' % (entity_name, corrected_entity_name))
            else:
                f.write('"%s": null,\n' % entity_name)
            f.flush()
        except:
            continue