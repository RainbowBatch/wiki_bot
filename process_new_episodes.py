from citation_extractor import download_citations
from citation_extractor import reprocess_citation_episodes
from episode_details_downloader import download_episode_details
from spotify_downloader import download_spotify_details
from twitch_downloader import download_twitch_details
from merge import merge_records
from stamp_episode_listing import stamp_episode_listing
from stamp_template import stamp_templates
from title_download import download_titles

if __name__ == '__main__':
    download_titles()
    download_episode_details()
    try:
        download_spotify_details()
    except:
        print("Warning: Failed to download spotify details.")
    download_twitch_details()
    download_citations()
    merge_records()
    # This is a bit clunky, but we need to make sure that we propagate information
    # through the date listing so that citations are properly matched. It's probably
    # usually un-needed, but better to have it then not.
    if reprocess_citation_episodes():
        print("Doing second merge due to recalculated citations.")
        merge_records()  # In case any citations have new associations.
    stamp_templates()
    stamp_episode_listing()

    # regenerate entitles
