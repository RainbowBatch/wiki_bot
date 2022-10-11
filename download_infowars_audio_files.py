import kfio
import requests
import pandas as pd
from os.path import exists
from tqdm import tqdm
from bs4 import BeautifulSoup
import maya
from date_lookup import format_date
from tqdm import tqdm

def download_broadcast_details():
	response = requests.get('http://rss.infowars.com/playlist.xspf')
	soup = BeautifulSoup(response.text)

	header = [
	        "filename",
	        "show",
	        "release_date",
	        "release_dow",
	        "download_link",
	]
	rows = []

	for item in tqdm(soup.find_all("track")):
		ep_url = item.find("location").contents[0]
		ep_title = item.find("title").contents[0]
		ep_details= ep_title[:-4].split('_')
		ep_date, ep_show = ep_details[0], ep_details[-1]
		ep_year, ep_month, ep_day = ep_date[:4], ep_date[4:6], ep_date[6:]
		ep_date = maya.when('%s-%s-%s' % (ep_year, ep_month, ep_day), timezone='US/Eastern')
		ep_dow = ep_date.datetime().strftime('%A')
		ep_date = format_date(ep_date)

		rows.append([
			ep_title,
			ep_show,
			ep_date,
			ep_dow,
			ep_url,
		])

	df = pd.DataFrame(reversed(rows), columns=header)

	kfio.save(df, 'data/infowars_listing.json')

def download_broadcast_audio(year):
	episode_listing = kfio.load('data/infowars_listing.json')
	relevant_episode_listing = episode_listing[episode_listing.release_date.str.contains(year, case=False) & (episode_listing.show == 'Alex')]

	for record in tqdm(relevant_episode_listing.to_dict(orient='records')):
		audio_fname = 'audio_files/infowars/%s' % record['filename']
		url = record['download_link']

		if exists(audio_fname):
			print("\nSkipping", record['filename'])
			continue

		print("\nDownloading", record['filename'])

		response = requests.get(url)

		assert response.status_code == 200

		with open(audio_fname, 'wb') as f:
			f.write(response.content)


if __name__ == '__main__':
	# download_broadcast_details()
	download_broadcast_audio(year='2013')