import _thread as thread
import en_core_web_sm
import kfio
import re
import sys
import threading

from three_merge import merge
from time import sleep

nlp = en_core_web_sm.load()

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
    'alex emmerich jones': 'Alex Jones',
    'Alex E. Jones': 'Alex Jones',
    'Alex Jones\'': 'Alex Jones',
    'Alex': 'Alex Jones',
    'alex jonesy': 'Alex Jones',
    'Alexander Dugan': 'Alexander Dugin',
    'Bobby Barnes': 'Robert Barnes',
    'Chris Maddie': 'Chris Mattei',
    'Dan Bodandi': 'Dan Bidondi',
    'Dan Bodandy': 'Dan Bidondi',
    'Dan Vedandi': 'Dan Bidondi',
    'Dan Vidanti': 'Dan Bidondi',
    'Donald Trump\u200f\u200e': 'Donald Trump',
    'Evan Mcmullen': 'Evan McMullen',
    'Howard Stearn': 'Howard Stern',
    'John Rapoport': "John Rappaport",
    'Leann McAdoo': "Lee Ann McAdoo",
    'Leanne McAdoo': "Lee Ann McAdoo",
    'Marty Derosa': 'Marty DeRosa',
    'Meghan Kelly': 'Megyn Kelly',
    'Neil Hesleyn': 'Neil Heslin',
    'Omar Alfaruk': 'Omar al-Faruq',
    'Owen Troyer': 'Owen Schroyer',
    'Rhonda Santas': 'Ron DeSantis',
    'Ron Desantis': "Ron DeSantis",
    'Steve Quale': "Steve Quayle",
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

    if slightly_cleaned_text.lower() == cleaned_text.lower():
        return slightly_cleaned_text

    merged = merge(slightly_cleaned_text, cleaned_text,
                   slightly_cleaned_text.lower())

    # TODO(woursler): We can do better than this, esp at the start of the string.
    assert '<<<<<<<' not in merged
    assert merged.lower() == cleaned_text.lower(
    ), "%s != %s" % (merged.lower(), cleaned_text)

    return merged


def restore_capitalization(cleaned_text, original_texts):
    for original_text in original_texts:
        try:
            return restore_specific_capitalization(cleaned_text, original_text)
        except:
            pass
    # Nothing worked, we'll fall back.
    return cleaned_text


if __name__ == '__main__':
    raw_entities = kfio.load('data/raw_entities.json')

    for raw_entity in raw_entities.to_dict(orient='records'):
        try:
            restore_capitalization(
                raw_entity['entity_name'], raw_entity['entity_sourcetexts'])
        except:
            print(repr(raw_entity['entity_name']),
                  raw_entity['entity_sourcetexts'], '=>')
            print('Failure')

    print(raw_entities)
