import kfio
import requests
from os.path import exists
from tqdm import tqdm

episode_listing = kfio.load('data/final.json')

print(episode_listing[['episode_number', 'download_link']])

for record in tqdm(episode_listing.to_dict(orient='records')):
	audio_fname = 'audio_files/%s.mp3' % record['episode_number']
	url = record['download_link']

	if exists(audio_fname):
		print("Skipping", record['title'])
		continue

	print("Downloading", record['title'])

	response = requests.get(url)

	assert response.status_code == 200

	with open(audio_fname, 'wb') as f:
		f.write(response.content)