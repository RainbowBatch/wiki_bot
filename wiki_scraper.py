import glob
import kfio
from box import Box
import wikitextparser
from pprint import pprint
import re
import pandas as pd

redirect_regex = re.compile(r'#redirect\s*\[\[\s*(?P<link>[^\]]+)\]\]', re.IGNORECASE)

category_regex = re.compile(r'\[\[\s*Category\s*:\s*(?P<category>[^\]]+)\]\]')

page_listing = kfio.load('kf_wiki_content/page_listing.json')
episode_listing = kfio.load('data/final.json')

PAGE_RECORDS = []
for wiki_fname in glob.glob('kf_wiki_content/*.wiki'):
    with open(wiki_fname, encoding='utf-8') as wiki_f:
        page_slug = wiki_fname[16:-5]

        page_metadata = Box(page_listing[
            page_listing.slug == wiki_fname[16:-5]
        ].to_dict(orient='records')[0])

        page_text = wiki_f.read()

        page_parsed = wikitextparser.parse(page_text)

        page_metadata.is_stub = False
        page_metadata.is_external_redirect = False

        page_metadata.wiki_categories = [
            z.strip()
            for z in category_regex.findall(page_text)
        ]

        for template in page_parsed.templates:
            template_name = template.name.strip().lower()
            if template_name == 'stub':
                page_metadata.is_stub = True
                continue
            if template_name == 'externalredirect':
                page_metadata.is_external_redirect = True
                continue
            if template_name in ['notice', 'messagebox', 'quote', 'main', 'topic']:
                continue
            if template_name != 'episode':
                raise NotImplementedError("Unable to handle template named '%s'." % template_name)
            for argument in template.arguments:
                argument_name = argument.name.strip()
                if argument_name in ['title']:
                    continue
                assert argument_name not in page_metadata, argument_name + ' ' + wiki_fname
                page_metadata[argument_name] = argument.value.strip()

        redirect_match = redirect_regex.search(page_text)
        if redirect_match is not None:
            page_metadata.redirect = redirect_match.group('link')

        # Reprocess
        # appearance
        # episodeType
        # nextEpisode
        # previousEpisode
        def process_appearances(appearances):
            appearances = re.split('\[\[|\]\]', appearances)
            appearances = [a.strip() for a in appearances]
            for appearance in appearances:
                if len(appearance) == 0 or appearance in [',', ';']:
                    continue
                yield appearance
        if 'appearance' in page_metadata:
            page_metadata.appearance = list(process_appearances(page_metadata.appearance))
        if 'hosts' in page_metadata:
            page_metadata.hosts = list(process_appearances(page_metadata.hosts))

        def process_episodeType(episode_types):
            episode_types = re.split('\[\[|\]\]', episode_types)
            episode_types = [a.strip() for a in episode_types]
            for episode_type in episode_types:
                if len(episode_type) == 0 or episode_type in [',', ';']:
                    continue
                if episode_type.startswith(':Category:'):
                    yield episode_type.split('|')[0][10:]
                else:
                    yield episode_type
        if 'episodeType' in page_metadata:
            page_metadata.episodeType = list(process_episodeType(page_metadata.episodeType))

        def match_episode(raw_link):
            raw_link = raw_link.strip()
            assert raw_link.startswith('[[')
            assert raw_link.endswith(']]')
            raw_link = raw_link[2:-2]
            matching_episodes = episode_listing[episode_listing.title.isin(raw_link.split('|'))].episode_number.to_list()

            if len(matching_episodes) == 1:
                return matching_episodes[0]

            raise NotImplementedError("No match %s; => %s" % (raw_link, ', '.join(matching_episodes)))

        if 'previousEpisode' in page_metadata:
            page_metadata.previousEpisode = match_episode(page_metadata.previousEpisode)
        if 'nextEpisode' in page_metadata:
            page_metadata.nextEpisode = match_episode(page_metadata.nextEpisode)

        if 'caption' in page_metadata:
            page_metadata.libsyn_link = page_metadata.caption.split()[0][1:]
            del page_metadata['caption']

        PAGE_RECORDS.append(page_metadata)


page_records_df = pd.DataFrame.from_records(PAGE_RECORDS).sort_values('title')

redirect_header = ['from', 'to']
redirect_rows = []
for page_metadata in PAGE_RECORDS:
       if 'redirect' in page_metadata:
            redirect_rows.append({
                'from': page_metadata.title.strip(),
                'to': page_metadata.redirect.strip(),
            })

redirects_df = pd.DataFrame(redirect_rows, columns=redirect_header)

kfio.save(page_records_df, 'data/scraped_page_data.json')
kfio.save(redirects_df, 'data/wiki_redirects.json')