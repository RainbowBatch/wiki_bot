import json
import spotipy

from box import Box
from rainbowbatch.secrets import secret_file


def make_spotify_client():
    with open(secret_file("spotify.json")) as secrets_f:
        secrets = Box(json.load(secrets_f))

        client_credentials_manager = spotipy.oauth2.SpotifyClientCredentials(
            client_id=secrets.client_id,
            client_secret=secrets.client_secret,
        )
        spotify_client = spotipy.Spotify(
            client_credentials_manager=client_credentials_manager)
    return spotify_client
