from bs4 import BeautifulSoup
import requests
import random
import time
import kfio

HEADERS = {
    'User-agent':
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582"
}

# TODO(woursler): Add a persistent cache on top of this.

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

for entity_name in raw_entity_listing.entity_name.to_list():
    # print(entity_name)
    corrected_entity_name = correct_spelling(entity_name)

    if corrected_entity_name != entity_name:
        print(entity_name, ':', corrected_entity_name)
    else:
        print('- ', entity_name)

# print(correct_spelling('Neil Hesleyn'))
