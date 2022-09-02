import en_core_web_sm
import kfio

nlp = en_core_web_sm.load()

page_listing = kfio.load('kf_wiki_content/page_listing.json')

scraped_pages = kfio.load('data/scraped_page_data.json')

redirects = scraped_pages[~scraped_pages.redirect.isna()]
# TODO(woursler): Also include external redirects by default?
existing_people = scraped_pages[scraped_pages.redirect.isna() & scraped_pages.wiki_categories.map(set(['People']).issubset)]
external_redirects = scraped_pages[~scraped_pages.redirect.isna() & scraped_pages.is_external_redirect]

missing_pages = kfio.load('data/missing_pages.json')

hardcoded_people = [
 'Omar al-Faruq',
 'Jim Shepherd',
]

LIKELY_PEOPLE = set(
    existing_people.title.to_list() + missing_pages.title.to_list() + external_redirects.title.to_list() + hardcoded_people
)

REMAPPING = {
    'Rhonda Santas': 'Ron DeSantis',
    'Owen Troyer': 'Owen Schroyer',
    'Meghan Kelly': 'Megyn Kelly',
    'Alex E. Jones': 'Alex Jones',
    'Wolfgang Halbeck': 'Wolfgang Halbig',
    'Howard Stearn': 'Howard Stern',
    'Dan Bodandy': 'Dan Bidondi',
    'Dan Vedandi': 'Dan Bidondi',
    'Dan Vidanti': 'Dan Bidondi',
    'Dan Bodandi': 'Dan Bidondi',
    'Neil Hesleyn': 'Neil Heslin',
    'Alexander Dugan': 'Alexander Dugin',
    'Bobby Barnes': 'Robert Barnes',
    'Omar Alfaruk' : 'Omar al-Faruq',
}

OVERUSED = [
    'Alex Jones',
    'Jordan Holmes',
    'Dan Friesen',
]

for redirect in redirects.to_dict(orient='records'):
    REMAPPING[redirect['title']] = redirect['redirect']

# These are names that have non-misspelling redirects.
del REMAPPING['Neil Heslin']
del REMAPPING['Scarlett Lewis']
del REMAPPING['Adam Lanza']


def simplify_entity(s):
    s=s.replace('\u200f\u200e', '').strip()
    s = ' '.join(s.split())
    if s.endswith("'s"):
        s = s[:-2]
    while s in REMAPPING:
        s = REMAPPING[s]
    return s


def extract_entities(S, source):
    return [(X.text, X.label_, source)
            for X in nlp(S).ents]
