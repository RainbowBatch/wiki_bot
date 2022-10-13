import en_core_web_sm
import kfio
import re

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
]

REMAPPING = {
    "Alex Jones'": 'Alex Jones',
    'Alex E. Jones': 'Alex Jones',
    'Alex': 'Alex Jones',
    'Alexander Dugan': 'Alexander Dugin',
    'Bobby Barnes': 'Robert Barnes',
    'Dan Bodandi': 'Dan Bidondi',
    'Dan Bodandy': 'Dan Bidondi',
    'Dan Vedandi': 'Dan Bidondi',
    'Dan Vidanti': 'Dan Bidondi',
    'Howard Stearn': 'Howard Stern',
    'Meghan Kelly': 'Megyn Kelly',
    'Neil Hesleyn': 'Neil Heslin',
    'Omar Alfaruk': 'Omar al-Faruq',
    'Owen Troyer': 'Owen Schroyer',
    'Rhonda Santas': 'Ron DeSantis',
    'Wolfgang Halbeck': 'Wolfgang Halbig',
    'Zuckerberg': 'Mark Zuckerberg',
    "John Rapoport": "John Rappaport",
    'Donald Trump\u200f\u200e': 'Donald Trump',
    'Marty Derosa': 'Marty DeRosa',
    'Evan Mcmullen': 'Evan McMullen',
    "Leanne McAdoo": "Lee Ann McAdoo",
    "Leann McAdoo": "Lee Ann McAdoo",
    "Ron Desantis": "Ron DeSantis",
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
    for k,v in REMAPPING.items()
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
