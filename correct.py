from bs4 import BeautifulSoup
import requests
import random
import time

HEADERS = {
    'User-agent':
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582"
}

# TODO(woursler): Add a persistent cache on top of this.

def correct_spelling(s):
    time.sleep(random.uniform(5, 60))

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

print(correct_spelling('Neil Hesleyn'))
