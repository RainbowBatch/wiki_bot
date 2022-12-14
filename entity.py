import _thread as thread
import box
import en_core_web_sm
import kfio
import pandas as pd
import parse
import re
import sys
import threading

from natsort import natsorted
from three_merge import merge
from time import sleep

nlp = en_core_web_sm.load()

episodes_df = kfio.load('data/final.json')

wiki_df = kfio.load('data/scraped_page_data.json')

page_listing = kfio.load('kf_wiki_content/page_listing.json')

scraped_pages = kfio.load('data/scraped_page_data.json')

redirects = scraped_pages[~scraped_pages.redirect.isna()]
# TODO(woursler): Also include external redirects by default?
existing_people = scraped_pages[scraped_pages.redirect.isna(
) & scraped_pages.wiki_categories.map(set(['People']).issubset)]
external_redirects = scraped_pages[~scraped_pages.redirect.isna(
) & scraped_pages.is_external_redirect]

missing_pages = kfio.load('data/missing_pages.json')

hardcoded_people = [
    'Omar al-Faruq',
    'Jim Shepherd',
    'Elizabeth Williamson',
]

LIKELY_PEOPLE = set(
    existing_people.title.to_list() + missing_pages.title.to_list() +
    external_redirects.title.to_list() + hardcoded_people
)

NOT_RELEVANT_PEOPLE = [
    "Cookie Monster",
    "Frankenstein",
    "Illuminati",
    "John Birch Society",
    "New World Order",
    "Project Veritas",
    "Rube Goldberg",
    "The Sandy Hook Elementary Massacre",
    "Zero Hedge",
    "Cinco Demayo",
    "Wolfgang Puck",
    "Arnold Palmer",
    "Wild Wild West",
    "Dan Jordan",
    "Alex Jordan",
    "Beatles Rock Band",
]

REMAPPING = {
    'Alex E. Jones': 'Alex Jones',
    'alex emmerich jones': 'Alex Jones',
    'Alex Jones\'': 'Alex Jones',
    'alex jonesy': 'Alex Jones',
    'Alex Lee Boyer': 'Alex Lee Moyer',
    'Alex': 'Alex Jones',
    'Alex\'s True Story References': 'Alex\'s True Story',
    'Alexander Dugan': 'Alexander Dugin',
    'Bobby Barnes': 'Robert Barnes',
    'Call Schwab': 'Klaus Schwab',
    'Carol Quickly': 'Carroll Quigley',
    'Carol Quigley': 'Carroll Quigley',
    'Carrie Cassidy': 'Kerry Cassidy',
    'Chris Madden': 'Chris Mattei',
    'Chris Maddie': 'Chris Mattei',
    'Chris Matic': 'Chris Mattei',
    'Chris Mattie': 'Chris Mattei',
    'Chris Mehdi': 'Chris Mattei',
    'Clash Schwab': 'Klaus Schwab',
    'Claude Schwab': 'Klaus Schwab',
    'Claus Schwab': 'Klaus Schwab',
    'Clinton Hillary': 'Hillary Clinton',
    'Dan Bodandi': 'Dan Bidondi',
    'Dan Bodandy': 'Dan Bidondi',
    'Dan Vedandi': 'Dan Bidondi',
    'Dan Vidanti': 'Dan Bidondi',
    'Dave Dobbin Meyer': 'Dave Daubenmire',
    'Dave Mustane': 'Dave Mustaine',
    'Don Friesen': 'Dan Friesen',
    'Donald Trump\u200f\u200e': 'Donald Trump',
    'Evan Mcmullen': 'Evan McMullen',
    'Fifth Amendment': '5th Amendment',
    'Fifth Avenue': '5th Avenue',
    'First Amendment': '1st Amendment',
    'Fourth Amendment': '4th Amendment',
    'Gavin McGinnis': 'Gavin McInnes',
    'Howard Stearn': 'Howard Stern',
    'John Rapoport': "John Rappaport",
    'Knowledgebitecom': 'knowledgefight.com',
    'Knowledgebuycom': 'knowledgefight.com',
    'Larry Clayman': 'Larry Klayman',
    'Leann McAdoo': "Lee Ann McAdoo",
    'Leanne McAdoo': "Lee Ann McAdoo",
    'Marty Derosa': 'Marty DeRosa',
    'Meghan Kelly': 'Megyn Kelly',
    'Neil Hesleyn': 'Neil Heslin',
    'Ninth Circuit': '9th Circuit',
    'Norm Pattice': 'Norm Pattis',
    'NRA Wayne LaPierre': 'Wayne LaPierre',
    'Ollie North': 'Oliver North',
    'Omar Alfaruk': 'Omar al-Faruq',
    'Owen Troyer': 'Owen Schroyer',
    'Rhonda Santas': 'Ron DeSantis',
    'Ron Desantis': "Ron DeSantis",
    'Second Amendment': '2nd Amendment',
    'Steve Crowder': 'Steven Crowder',
    'Steve Patentic': 'Steve Pieczenik',
    'Steve Quale': "Steve Quayle",
    'Stewart Road': 'Stewart Rhodes',
    'Stewart Roads': 'Stewart Rhodes',
    'Wolfgang Halbeck': 'Wolfgang Halbig',
    'Zuckerberg': 'Mark Zuckerberg',
}


OVERUSED = [
    'Alex Jones',
    'Jordan Holmes',
    'Dan Friesen',
]

for redirect in redirects.to_dict(orient='records'):
    if redirect['redirect'].startswith('Category:'):
        target = redirect['redirect'][9:]
    elif '#' in redirect['redirect']:
        target = redirect['redirect'].split('#')[0]
    else:
        target = redirect['redirect']

    if redirect['title'] != target:
        REMAPPING[redirect['title']] = target

# These are names that have non-misspelling redirects.
del REMAPPING['Neil Heslin']
del REMAPPING['Scarlett Lewis']
del REMAPPING['Adam Lanza']
del REMAPPING['Y2K']

REMAPPING = {
    k.lower(): v
    for k, v in REMAPPING.items()
}

CAPITALIZATION_REMAPPING = {
    s.lower(): s
    for s in set(
        missing_pages.title.to_list()
        + scraped_pages.title.to_list()
        + list(REMAPPING.values())
        + hardcoded_people
    )
}

_RE_COMBINE_WHITESPACE = re.compile(r"\s+")


def simplify_entity(s):
    s = _RE_COMBINE_WHITESPACE.sub(' ', s)
    s = s.replace(u'\u200f', '').replace(u'\u200e', '')
    s = s.replace('.', '')
    s = s.replace('-', ' ')
    s = s.strip()
    assert u'\u200f\u200e' not in s
    s = ' '.join(s.split())
    if s.endswith("'s"):
        s = s[:-2]
    if s.lower().startswith("a "):
        s = s[2:]
    if s.lower().startswith("an "):
        s = s[3:]
    if s.lower().startswith("the "):
        s = s[4:]
    while s.lower() in REMAPPING:
        if s.lower() == REMAPPING[s.lower()].lower():
            break
        s = REMAPPING[s.lower()]
    s = s.strip().lower()
    return CAPITALIZATION_REMAPPING.get(s, s)


def extract_entities(S, source):
    return [(X.text, X.label_, source)
            for X in nlp(S).ents]


def quit_function(fn_name):
    # print to stderr, unbuffered in Python 2.
    print('{0} took too long'.format(fn_name), file=sys.stderr)
    sys.stderr.flush()  # Python 3 stderr is likely buffered.
    thread.interrupt_main()  # raises KeyboardInterrupt


def exit_after(s):
    '''
    use as decorator to exit process if
    function takes longer than s seconds
    '''
    def outer(fn):
        def inner(*args, **kwargs):
            timer = threading.Timer(s, quit_function, args=[fn.__name__])
            timer.start()
            try:
                result = fn(*args, **kwargs)
            finally:
                timer.cancel()
            return result
        return inner
    return outer


@exit_after(0.05)
def restore_specific_capitalization(cleaned_text, original_text):
    if cleaned_text == original_text:
        return cleaned_text

    slightly_cleaned_text = ' '.join(original_text.split())

    if slightly_cleaned_text.lower().startswith('the '):
        slightly_cleaned_text = slightly_cleaned_text[4:]

    if slightly_cleaned_text.lower().startswith('a '):
        slightly_cleaned_text = slightly_cleaned_text[2:]

    if slightly_cleaned_text.lower().startswith('an '):
        slightly_cleaned_text = slightly_cleaned_text[3:]

    slightly_cleaned_text = slightly_cleaned_text.strip()

    if slightly_cleaned_text.lower() == cleaned_text.lower():
        return slightly_cleaned_text

    merged = merge(slightly_cleaned_text, cleaned_text,
                   slightly_cleaned_text.lower())

    # TODO(woursler): We can do better than this, esp at the start of the string.
    assert '<<<<<<<' not in merged
    assert merged.lower() == cleaned_text.lower(
    ), "%s != %s" % (merged.lower(), cleaned_text)

    return merged


def n_upper_chars(string):
    return sum(map(str.isupper, string))


def restore_capitalization(cleaned_text, original_texts):
    capitalizations = set()
    for original_text in original_texts:
        try:
            capitalizations.add(restore_specific_capitalization(
                cleaned_text, original_text))
        except Exception as e:
            print(e)
            pass
        except KeyboardInterrupt:
            pass

    # Nothing worked, we'll fall back.
    if len(capitalizations) == 0:
        return cleaned_text

    # TODO(woursler): Consider different measures (e.g. right ratio.)
    return max(capitalizations, key=n_upper_chars)


def wiki_link(link_text, link_dest):
    if link_text == link_dest:
        return "[[%s]]" % link_dest
    else:
        return "[[%s|%s]]" % (link_dest, link_text)


def lookup_by_epnum(episode_number):
    df_view = episodes_df[episodes_df.episode_number == episode_number]

    if len(df_view) > 0:
        # Assert only one result?

        return box.Box(df_view.to_dict(orient='records')[0])

    return None


def lookup_wiki_page(page_title):
    df_view = wiki_df[wiki_df.title == page_title]

    if len(df_view) > 0:
        # Assert only one result?

        return box.Box(df_view.to_dict(orient='records')[0])

    return None


REFERENCE_TYPES = {
    'autosub': "AutoSub",
    'otter': "otter.ai",
    'welder': "Welder",
    'manual': "Manually-created",
    'whisper': "OpenAI Whisper",
    'wiki': "Wiki Page",
}


def parse_entity_orgin(value):
    if value.startswith('transcripts'):
        parse_result = parse.parse(r"transcripts\{}.{}.{}", value)
        episode_number, transcript_type, _ = parse_result

        episode_record = lookup_by_epnum(episode_number)

        if episode_record is not None:
            return (episode_number, transcript_type, episode_record.safe_title)
        return (episode_number, transcript_type, None)

    wiki_page = lookup_wiki_page(value)

    if wiki_page is not None and not pd.isna(wiki_page.episodeNumber):
        return (wiki_page.episodeNumber, 'wiki', value)
    return (None, 'wiki', value)


def create_entity_origin_list_mw(entities_origins):
    eo_rows = [parse_entity_orgin(eo) for eo in entities_origins]

    eo_df = pd.DataFrame(
        eo_rows, columns=['episode_number', 'reference_type', 'title'])

    grouped_eo_df = eo_df.groupby('episode_number')

    formatted_entity_strings = []

    for non_episode_entry in eo_df[eo_df['episode_number'].isna()].to_dict(orient='records'):
        formatted_entity_strings.append("%s (via %s)" % (
            non_episode_entry['title'], REFERENCE_TYPES[non_episode_entry['reference_type']]))

    for episode_number, _ in grouped_eo_df:
        group_eo_df = grouped_eo_df.get_group(episode_number)
        reference_types = set(group_eo_df.reference_type.to_list())

        episode_record = lookup_by_epnum(episode_number)

        if episode_record is None:
            slug = episode_number
        else:
            slug = wiki_link(episode_record.title, episode_record.safe_title)

        formatted_entity_strings.append("%s (via %s)" % (
            slug, ', '.join([REFERENCE_TYPES[rt] for rt in sorted(reference_types)])))

    assert len(formatted_entity_strings) > 0

    return natsorted(formatted_entity_strings)


if __name__ == '__main__':
    raw_entities = kfio.load('data/raw_entities.json')

    for raw_entity in raw_entities.to_dict(orient='records'):
        try:
            recap = restore_capitalization(
                raw_entity['entity_name'], raw_entity['entity_sourcetexts'])
            if recap != raw_entity['entity_name']:
                print(raw_entity['entity_name'], '=>', recap)
        except:
            print(repr(raw_entity['entity_name']),
                  raw_entity['entity_sourcetexts'], '=>')
            print('Failure')

    print(raw_entities)
