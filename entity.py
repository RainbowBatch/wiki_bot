import _thread as thread
import box
import en_core_web_sm
import kfio
import pandas as pd
import parse
import re
import sys
import threading

from collections import Counter
from collections import defaultdict
from natsort import natsorted
from three_merge import merge
from time import sleep

nlp = en_core_web_sm.load()

episodes_df = kfio.load('data/final.json')

wiki_df = kfio.load('data/scraped_page_data.json')

page_listing = kfio.load('kf_wiki_content/page_listing.json')

scraped_pages = kfio.load('data/scraped_page_data.json')

redirects = scraped_pages[~scraped_pages.redirect.isna()]
# TODO: Also include external redirects by default?
existing_people = scraped_pages[scraped_pages.redirect.isna(
) & scraped_pages.wiki_categories.map(set(['People']).issubset)]
external_redirects = scraped_pages[~scraped_pages.redirect.isna(
) & scraped_pages.is_external_redirect]

missing_pages = kfio.load('data/missing_pages.json')

hardcoded_people = [
    'Omar al-Faruq',
    'Jim Shepherd',
    'Elizabeth Williamson',
    'Ezra Levant',
]

hardcoded_capitalization = [
    'HONR Network',
    'Council on Foreign Relations',
    'GoFundMe.com',
    'GCN Radio Network',
]

LIKELY_PEOPLE = set(
    existing_people.title.to_list() + missing_pages.title.to_list() +
    external_redirects.title.to_list() + hardcoded_people
)

OVERUSED_PEOPLE = [
    'Alex Jones',
    'Jordan Holmes',
    'Dan Friesen',
]

NOT_RELEVANT_PEOPLE = [
    "Academy Award",
    "Adrenochrome",
    "Alex Jordan",
    "Arnold Palmer",
    "Beatles Rock Band",
    "Cinco Demayo",
    "Cookie Monster",
    "Dan Jordan",
    "Dark Knight Rises",
    "Frankenstein",
    "Google Analytics",
    "Illuminati",
    "John Birch Society",
    "New World Order",
    "Project Veritas",
    "Rube Goldberg",
    "Studio Ghibli",
    "The Sandy Hook Elementary Massacre",
    "Wild Wild West",
    "Wolfgang Puck",
    "Young Turks",
    "Zero Hedge",
    "Bilderberg Group",
    "Council on Foreign Relations",
    "Judeo Christian",
    "Sandy Hook Family Members",
    "Gateway Pundit",
] + OVERUSED_PEOPLE

REMAPPING = {
    'Aaron Rogers': 'Aaron Rodgers',
    'Adam Corolla': 'Adam Carolla',
    'Adam Salazar': 'Adan Salazar',
    'Adam Weisshop': 'Adam Weishaupt',
    'Alec Jones': 'Alex Jones',
    'Alex E. Jones': 'Alex Jones',
    'alex emmerich jones': 'Alex Jones',
    'Alex Jones\'': 'Alex Jones',
    'alex jonesy': 'Alex Jones',
    'Alex Lee Boyer': 'Alex Lee Moyer',
    'Alex Lee Moyers': 'Alex Lee Moyer',
    'Alex': 'Alex Jones',
    'Alex\'s True Story References': 'Alex\'s True Story',
    'Alex\'s Tue Story': 'Alex\'s True Story',
    'Alexander Dougan': 'Alexander Dugin',
    'Alexander Dugan': 'Alexander Dugin',
    'Alexander Emmerich Jones': 'Alex Jones',
    'Allie Alexander': 'Ali Alexander',
    'Ally Alexander': 'Ali Alexander',
    'Anders Brevik': 'Anders Breivik',
    'Angela Lampsbury': 'Angela Lansbury',
    'Anthony Cumiya': 'Anthony Cumia',
    'Anthony KU Mia': 'Anthony Cumia',
    'Anthony Kumi': 'Anthony Cumia',
    'Anthony Kumiya': 'Anthony Cumia',
    'arthur c clark': 'Arthur C Clarke',
    'Ashley Bab': 'Ashley Babbitt',
    'Ashley David': 'Ashley Babbitt',
    'Astrazenica': 'AstraZeneca',
    'Barbara Lowe Fisher': 'Barbara Loe Fisher',
    'Bill Ayres': 'Bill Ayers',
    'Bill Gateses': 'Bill Gates',
    'Bobby Barnes': 'Robert Barnes',
    'Bobby Burns': 'Robert Barnes',
    'Bobcat Goldthwaite': 'Bobcat Goldthwait',
    'Book of Revelations': 'Book of Revelation',
    'BrainForce': 'Brain Force',
    'Brandon Straca': 'Brandon Straka',
    'Brian Seltzer': 'Brian Stelter',
    'Brian Stalter': 'Brian Stelter',
    'Brian Stilter': 'Brian Stelter',
    'Brock Obama': 'Barack Obama',
    'Buckley Hammond': 'Buckley Hamman',
    'Caitlin Bennett': 'Kaitlin Bennett',
    'Call Schwab': 'Klaus Schwab',
    'Candace Owen': 'Candace Owens',
    'Candice Owen': 'Candace Owens',
    'Candice Owens': 'Candace Owens',
    'Carol Quickly': 'Carroll Quigley',
    'Carol Quigley': 'Carroll Quigley',
    'Carole Quigley': 'Carroll Quigley',
    'Carrie Cassidy': 'Kerry Cassidy',
    'Charles Leiber': 'Charles Lieber',
    'Chiang Kai Shek': 'Chiang Kai-shek',
    'Chris Kristofferson': 'Kris Kristofferson',
    'Chris Madden': 'Chris Mattei',
    'Chris Maddie': 'Chris Mattei',
    'Chris Maddy': 'Chris Mattei',
    'Chris Matic': 'Chris Mattei',
    'Chris Matisse': 'Chris Mattei',
    'Chris Matti': 'Chris Mattei',
    'Chris Mattie': 'Chris Mattei',
    'Chris Matty': 'Chris Mattei',
    'Chris Mehdi': 'Chris Mattei',
    'Chuckie Schumer': 'Chuck Schumer',
    'Chucky Schumer': 'Chuck Schumer',
    'Clash Schwab': 'Klaus Schwab',
    'Claude Schwab': 'Klaus Schwab',
    'Claus Schwab': 'Klaus Schwab',
    'Cleon Skousen': 'W Cleon Skousen',
    'Clinton Hillary': 'Hillary Clinton',
    'Coast Coast AM': 'Coast To Coast AM',
    'Coast to Coast': 'Coast To Coast AM',
    'Coasttocoastam': 'Coast To Coast AM',
    'Council of Foreign Relations': 'Council On Foreign Relations',
    'Council of Nicea': 'Council of Nicaea',
    'Covington and Burling': 'Covington & Burlington',
    'Covington Burlington': 'Covington & Burlington',
    'Curt Nimmo': 'Kurt Nimmo',
    'Dakari Jackson': 'Jakari Jackson',
    'Dan Badadi': 'Dan Bidondi',
    'Dan Badandi': 'Dan Bidondi',
    'Dan Badani': 'Dan Bidondi',
    'Dan Badondi': 'Dan Bidondi',
    'Dan Bidadi': 'Dan Bidondi',
    'Dan Bodandi': 'Dan Bidondi',
    'Dan Bodandy': 'Dan Bidondi',
    'Dan Madani': 'Dan Bidondi',
    'Dan Padandi': 'Dan Bidondi',
    'Dan Vedandi': 'Dan Bidondi',
    'Dan Vidanti': 'Dan Bidondi',
    'Daniel Estelan': 'Daniel Estulin',
    'Daniel Estelen': 'Daniel Estulin',
    'Daniel Estelin': 'Daniel Estulin',
    'Daniel Estolan': 'Daniel Estulin',
    'Darryl Hamamoto': 'Darrell Hamamoto',
    'Darryl Rundus': 'Darrel Rundus',
    'Daryl Rundes': 'Darrel Rundus',
    'Daryl Rundis': 'Darrel Rundus',
    'Daryl Rundus': 'Darrel Rundus',
    'Dave Dobbin Meyer': 'Dave Daubenmire',
    'Dave Mustane': 'Dave Mustaine',
    'David Dukes': 'David Duke',
    'David Iche': 'David Icke',
    'David Ikes': 'David Icke',
    'Devin Nunez': "Devin Nunes",
    'Diana Lorraine': 'DeAnna Lorraine',
    'Diane Feinstein': 'Dianne Feinstein',
    'Disney Land': 'Disneyland',
    'DJ Dan Arkey': 'DJ Danarchy',
    'DJ Dan Arky': 'DJ Danarchy',
    'DJ Danerkey': 'DJ Danarchy',
    'Don De Grand Prix': 'Donn de Grand Pré',
    'Don DeGran Prix': 'Donn de Grand Pré',
    'Don DeGrand Prix': 'Donn de Grand Pré',
    'Don DeGrande Prix': 'Donn de Grand Pré',
    'Don Friesen': 'Dan Friesen',
    'Donald J Trump': 'Donald Trump',
    'Donald Trump\u200f\u200e': 'Donald Trump',
    'Elliot Roger': 'Elliot Rodger',
    'Eric Lafferty': 'Erica Lafferty',
    'Evan Mcmullen': 'Evan McMullen',
    'Fifth Amendment': '5th Amendment',
    'Fifth Avenue': '5th Avenue',
    'First Amendment': '1st Amendment',
    'Fourteenth Amendment': '14th Amendment',
    'Fourth Amendment 2451': '4th Amendment',
    'Fourth Amendment': '4th Amendment',
    'Gary Allan': 'Gary Allen',
    'Gavin McGinnis': 'Gavin McInnes',
    'George Norey': 'George Noory',
    'George Nori': 'George Noory',
    'George Norie': 'George Noory',
    'George Norrie': 'George Noory',
    'George Thurgood': 'George Thorogood',
    'Gerald Cilente': 'Gerald Celente',
    'Gerald Cilentes': 'Gerald Celente',
    'Gerald Cilenti': 'Gerald Celente',
    'Gerald Cilenty': 'Gerald Celente',
    'Gofundmecom': 'GoFundMe.com',
    'Hillary Clint': 'Hillary Clinton',
    'horizon zero': 'Horizon Zero Dawn',
    'Howard Stearn': 'Howard Stern',
    'Infowars storecom': 'InfowarsStore.com',
    'Infowarsetourcom': 'InfowarsStore.com',
    'Infowarstorecom': 'InfowarsStore.com',
    'Ja\'Carri Jackson': 'Jakari Jackson',
    'Jacari Jackson': 'Jakari Jackson',
    'Jack Basobiec': 'Jack Posobiec',
    'Jack Ma Sobiech': 'Jack Posobiec',
    'Jack Pacific': 'Jack Posobiec',
    'Jack Passobat': 'Jack Posobiec',
    'Jack Passobiac': 'Jack Posobiec',
    'Jack Passobian': 'Jack Posobiec',
    'Jack Passobiec': 'Jack Posobiec',
    'Jack Passobio': 'Jack Posobiec',
    'Jack Pessobek': 'Jack Posobiec',
    'Jack Pessobiac': 'Jack Posobiec',
    'Jack Pessobic': 'Jack Posobiec',
    'Jack Pessobiec': 'Jack Posobiec',
    'Jack Posobick': 'Jack Posobiec',
    'Jack Pysobiek': 'Jack Posobiec',
    'Jack Sobiech': 'Jack Posobiec',
    'Jackari Jackson': "Jakari Jackson",
    'James Alifantis': 'James Alefantis',
    'James Elefantis': 'James Alefantis',
    'James Elefantus': 'James Alefantis',
    'James Oliphant': 'James Alefantis',
    'James Oliphantus': 'James Alefantis',
    'Jaquari Jackson': 'Jakari Jackson',
    'Jar Jar Banks': 'Jar Jar Binks',
    'Jason Berman': 'Jason Bermas',
    'Jason Bermis': 'Jason Bermas',
    'Jason Birmus': 'Jason Bermas',
    'jason burma': 'Jason Bermas',
    'Jason Burmas': 'Jason Bermas',
    'Jason Burmese': 'Jason Bermas',
    'Jason Burmus': 'Jason Bermas',
    'Jason Burris': 'Jason Bermas',
    'JBS John Birch Society': 'John Birch Society',
    'JBS': 'John Birch Society',
    'Jeff Charlotte': 'Jeff Sharlet',
    'Jesse Smollett': 'Jussie Smollett',
    'Jessie Smollett': 'Jussie Smollett',
    'Joanne Richard': 'Jo Ann Richards',
    'Joanne Richardson': 'Jo Ann Richards',
    'Jocelyn Elders': 'Joycelyn Elders',
    'John Hopkins': 'Johns Hopkins',
    'John Rapoport': "John Rappaport",
    'John Rappaport': 'Jon Rappoport',
    'John Rapport': 'Jon Rappoport',
    'Jon Rappaport': 'Jon Rappoport',
    'Joseph Watson': 'Paul Joseph Watson',
    'Josh Koskoff': 'Joshua Koskoff',
    'Julian Asange': 'Julian Assange',
    'kid daniel': 'Kit Daniels',
    'Kid Daniels': 'Kit Daniels',
    'Kitt Daniels': 'Kit Daniels',
    'Kleon Skousen': 'W Cleon Skousen',
    'Knowledgebitecom': 'knowledgefight.com',
    'Knowledgebuycom': 'knowledgefight.com',
    'Ku Klux Klan': 'Klu Klux Klan',
    'l ron hubbard': 'L Ron Hubbard',
    'Larry Clayman': 'Larry Klayman',
    'Larry Flint': "Larry Flynt",
    'Larry Klaiman': 'Larry Klayman',
    'Laura Ingram': 'Laura Ingraham',
    'Leann McAdoo': "Lee Ann McAdoo",
    'Leanne McAdoo': "Lee Ann McAdoo",
    'Leeann McAdoo': 'Lee Ann McAdoo',
    'Lenny Posner': 'Leonard Pozner',
    'Lenny Pozner': 'Leonard Pozner',
    'Leo Sagami': 'Leo Zagami',
    'Leo\'s Egami': 'Leo Zagami',
    'Leon McAdoo': "Lee Ann McAdoo",
    'Leticia James': 'Letitia James',
    'Louis Thoreau': 'Louis Theroux',
    'lron hubbard': 'L Ron Hubbard',
    'Lucian Wintrick': 'Lucian Wintrich',
    'Luke Radowski': 'Luke Rudkowski',
    'Luke Redowski': 'Luke Rudkowski',
    'Luke Ridowski': 'Luke Rudkowski',
    'Luke Rudowski': 'Luke Rudkowski',
    'Madeline Albright': 'Madeleine Albright',
    'Marc Randazza': 'Marc Randazza',
    'Marc Randazzo': 'Marc Randazza',
    'Marianne Williams': 'Marianne Williamson',
    'Marie Le Pen': 'Marine Le Pen',
    'Marine LePen': 'Marine Le Pen',
    'Mark Randazzo': 'Marc Randazza',
    'Mark Richard': 'Mark Richards',
    'Mark Richardson': 'Mark Richards',
    'Marty Derosa': 'Marty DeRosa',
    'Marty Schechter': 'Marty Schachter',
    'Maryanne Williamson': 'Marianne Williamson',
    'Megan Kelly': 'Megyn Kelly',
    'Meghan Kelly': 'Megyn Kelly',
    'Michael jfox': 'Michael J Fox',
    'Michelle Bachman': 'Michele Bachmann',
    'Michelle Bachmann': 'Michele Bachmann',
    'Mike Cernovitches': 'Mike Cernovich',
    'Mike Liddell': 'Mike Lindell',
    'Mike Lindahl': 'Mike Lindell',
    'Mike Lyndale': 'Mike Lindell',
    'Mike Sernovich': 'Mike Cernovich',
    'Neil Haslund': 'Neil Heslin',
    'Neil Hesleyn': 'Neil Heslin',
    'Neil Hessel': 'Neil Heslin',
    'Neil Hesselen': 'Neil Heslin',
    'Neil Hezlin': 'Neil Heslin',
    'Ninth Circuit': '9th Circuit',
    'None Dare Call': 'None Dare Call It Conspiracy',
    'Norm Paddis': 'Norm Pattis',
    'Norm Patis': 'Norm Pattis',
    'Norm Patterson': 'Norm Pattis',
    'Norm Pattice': 'Norm Pattis',
    'Norm Pattison': 'Norm Pattis',
    'Norm Pettis': 'Norm Pattis',
    'NRA Wayne LaPierre': 'Wayne LaPierre',
    'Numark Richards': 'Mark Richards',
    'obama deception': 'The Obama Deception',
    'Occidental Descent': "Occidental Dissent",
    'Old Mantis House Phone': 'Old Man House Phone',
    'Ollie Alexander': 'Ali Alexander',
    'Ollie North': 'Oliver North',
    'Omar Alfaruk': 'Omar al-Faruq',
    'Operation Northwood': 'Operation Northwoods',
    'Oscar Schindler': 'Oskar Schindler',
    'Owen Schroyer': 'Owen Shroyer',
    'Owen Schroyers': 'Owen Shroyer',
    'Owen Troyer': 'Owen Schroyer',
    'Pat Robert': 'Pat Robertson',
    'Pat Roberts': 'Pat Robertson',
    'Patrick Berg': 'Patrick Bergy',
    'Patrick Bergey': 'Patrick Bergy',
    'Patrick Bergin': 'Patrick Bergy',
    'Paula White Cane': 'Paula White Cain',
    'Peter Brimlow': 'Peter Brimelow',
    'Peter TEALS': 'Peter Thiel',
    'Pierce Morgan': 'Piers Morgan',
    'PJW': 'Paul Joseph Watson',
    'Posey Wong': 'Policy Wonk',
    'Project Vertias': 'Project Veritas',
    'Queen Elizabeth': 'Queen Elizabeth II',
    'reveal op oliver': 'Revilo P Oliver',
    'Reveal Opie Oliver': 'Revilo P Oliver',
    'Review Loki Oliver': 'Revilo P Oliver',
    'Rhonda Santas': 'Ron DeSantis',
    'Rightwingwatch': 'Right Wing Watch',
    'Robert Barn': 'Robert Barnes',
    'Robert Welsh': 'Robert Welch',
    'Rodger Stone': 'Roger Stone',
    'Rodger Stones': 'Roger Stone',
    'Ron Desantis': "Ron DeSantis",
    'Ron Hubbard': 'L Ron Hubbard',
    'Ruth Bader Ginsberg': 'Ruth Bader Ginsburg',
    'Sama Bin Laden': 'Osama Bin Laden',
    'Second Amendment': '2nd Amendment',
    'Seventh Amendment': '7th Amendment',
    'Shanghai Shek': 'Chiang Kai-shek',
    'Steve Botanik': 'Steve Pieczenik',
    'Steve Botchanik': 'Steve Pieczenik',
    'Steve Crowder': 'Steven Crowder',
    'Steve Minuchin': 'Steve Mnuchin',
    'Steve Pacanich': 'Steve Pieczenik',
    'Steve Pacanik': 'Steve Pieczenik',
    'Steve Pacenik': 'Steve Pieczenik',
    'Steve Pachana': 'Steve Pieczenik',
    'Steve Pachani': 'Steve Pieczenik',
    'Steve Pachanic': 'Steve Pieczenik',
    'Steve Pachanik': 'Steve Pieczenik',
    'Steve Pachannik': 'Steve Pieczenik',
    'Steve Pachannock': 'Steve Pieczenik',
    'Steve Pachenik': 'Steve Pieczenik',
    'Steve Pachenik': 'Steve Pieczenik',
    'Steve Pachinik': 'Steve Pieczenik',
    'Steve Pachinnik': 'Steve Pieczenik',
    'Steve Pacinek': 'Steve Pieczenik',
    'Steve Pacinik': 'Steve Pieczenik',
    'Steve Paczentk': 'Steve Pieczenik',
    'Steve Pagenek': 'Steve Pieczenik',
    'Steve Pagenik': 'Steve Pieczenik',
    'Steve Pajanik': 'Steve Pieczenik',
    'Steve Patentic': 'Steve Pieczenik',
    'Steve Pechenik': 'Steve Pieczenik',
    'Steve Petchenik': 'Steve Pieczenik',
    'Steve Pichanik': 'Steve Pieczenik',
    'Steve Pieczenick': 'Steve Pieczenik',
    'Steve Pieczenik': 'Steve Pieczenik',
    'Steve Pjanic': 'Steve Pieczenik',
    'Steve Porchenek': 'Steve Pieczenik',
    'Steve Prochenik': 'Steve Pieczenik',
    'Steve Puchenik': 'Steve Pieczenik',
    'Steve Puranic': 'Steve Pieczenik',
    'Steve Quale': "Steve Quayle",
    'Stevie Peace': 'Steve Pieczenik',
    'Stevie Pease': 'Steve Pieczenik',
    'Stewart Road': 'Stewart Rhodes',
    'Stewart Roads': 'Stewart Rhodes',
    'Stu Peters': 'Stew Peters',
    'Stéphane Malinu': 'Stefan Molyneux',
    'Super Female Vitality': 'Super Female Vitality',
    'Supermail Vitality': 'Super Male Vitality',
    'Supermale Vitality': 'Super Male Vitality',
    'Superman Vitality': 'Super Male Vitality',
    'Thomas Rens': 'Thomas Renz',
    'Tom Papert': 'Tom Pappert',
    'tom rennes': 'Thomas Renz',
    'TurboForce': 'Turbo Force',
    'Vaccine Adverse Event Reporting': 'Vaccine Adverse Event Reporting System',
    'Verner Von Braun': 'Wernher Von Braun',
    'Victor Orban': 'Viktor Orban',
    'Warren Buffet': 'Warren Buffett',
    'Washington, DC': 'Washington DC',
    'Werner Von Braun': 'Wernher Von Braun',
    'Will Summer': 'Will Sommer',
    'Willis Cardo': 'Willis Carto',
    'Wolfgang Halbeck': 'Wolfgang Halbig',
    'Wolfgang Helbig': 'Wolfgang Halbig',
    'WorldNetDaily': 'World Net Daily',
    'Yair Bolsonaro': 'Jair Bolsonaro',
    'Young Jevity': 'Youngevity',
    'Yuri Geller': 'Uri Geller',
    'Yuval Noah': 'Yuval Noah Harari',
    'Zachariah Sitchin': 'Zecharia Sitchin',
    'Zacharias Sitchin': 'Zecharia Sitchin',
    'Zechariah Sitchin': 'Zecharia Sitchin',
    'Zuckerberg': 'Mark Zuckerberg',
}

# TODO: AstraZeneca, CenterPoint, CounterPoint, Daniel DuPont, DeAnna Lorraine is capitialized wrong.

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
        + hardcoded_capitalization
    )
}

_RE_COMBINE_WHITESPACE = re.compile(r"\s+")


def simplify_entity(s):
    s = _RE_COMBINE_WHITESPACE.sub(' ', s)
    s = s.replace(u'\u200f', '').replace(u'\u200e', '')
    s = s.replace('.', '')
    s = s.replace('-', ' ')
    s = s.replace('[', '')
    s = s.replace(']', '')
    s = s.strip()
    assert u'\u200f\u200e' not in s
    s = ' '.join(s.split())

    if s.endswith("'s"):
        s = s[:-2]
    if s.endswith("s'"):
        s = s[:-1]
    if s.lower().startswith("a "):
        s = s[2:]
    if s.lower().startswith("an "):
        s = s[3:]
    if s.lower().startswith("the "):
        s = s[4:]
    if s.lower().startswith("this "):
        s = s[5:]
    if s.lower().startswith("these "):
        s = s[6:]
    if s.lower().startswith("their "):
        s = s[6:]

    while s.lower() in REMAPPING:
        if s.lower() == REMAPPING[s.lower()].lower():
            break
        s = REMAPPING[s.lower()]
    s = s.strip().lower()
    return CAPITALIZATION_REMAPPING.get(s, s)


def extract_entities(S, origin):
    S = S.strip()
    if len(S) == 0:
        return []
    return [(X.text, X.label_, origin)
            for X in nlp(S).ents]


def aggregate_proto_entities(entities):
    counter = Counter()
    types = defaultdict(Counter)
    origins = defaultdict(Counter)
    sourcetexts = defaultdict(Counter)

    for text, label, origin in entities:
        counter.update([text])
        types[text].update([label])
        origins[text].update([origin])

    header = [
        "entity_name",
        "entity_count",
        "entity_type",
        "entity_origin",
        "entity_sourcetexts",
    ]
    rows = []

    for e_text, e_count in counter.most_common():
        e_types = dict(types[e_text].most_common())
        e_origins = dict(origins[e_text].most_common())
        e_sourcetext = {e_text: e_count}

        rows.append([e_text, e_count, e_types, e_origins, e_sourcetext])

    return pd.DataFrame(rows, columns=header)


def quit_function(fn_name):
    # print to stderr, unbuffered in Python 2.
    # print('{0} took too long'.format(fn_name), file=sys.stderr)
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

    if slightly_cleaned_text.lower().startswith("this "):
        slightly_cleaned_text = slightly_cleaned_text[5:]

    if slightly_cleaned_text.lower().startswith("these "):
        slightly_cleaned_text = slightly_cleaned_text[6:]

    if slightly_cleaned_text.lower().startswith("their "):
        slightly_cleaned_text = slightly_cleaned_text[6:]

    slightly_cleaned_text = slightly_cleaned_text.strip()

    if slightly_cleaned_text.lower() == cleaned_text.lower():
        return slightly_cleaned_text

    merged = merge(slightly_cleaned_text, cleaned_text,
                   slightly_cleaned_text.lower())

    # TODO: We can do better than this, esp at the start of the string.
    assert '<<<<<<<' not in merged
    assert merged.lower() == cleaned_text.lower(
    ), "%s != %s" % (merged.lower(), cleaned_text)

    return merged


def n_upper_chars(string):
    return sum(map(str.isupper, string))


def capitalization_goodness(string):
    return abs(
        (n_upper_chars(string) / (1 + len(string.split(' ')))) - 1
    )


def restore_capitalization(cleaned_text, original_texts):
    capitalizations = set()
    for original_text in original_texts:
        try:
            capitalizations.add(restore_specific_capitalization(
                cleaned_text, original_text))
        except Exception as e:
            # print(e)
            pass
        except KeyboardInterrupt:
            pass

    # Nothing worked, we'll fall back.
    if len(capitalizations) == 0:
        return cleaned_text

    # TODO: Consider different measures (e.g. right ratio.)
    return min(capitalizations, key=capitalization_goodness)


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
    'scribe': "ElevenLabs Scribe",
    'wiki': "Wiki Page",
    'fek': "FoulEdgeKnight-edited",
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
    eo_rows = [[None if s == 'None' else s for s in eo.split(
        '__')] for eo in entities_origins]

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
            # print(repr(raw_entity['entity_name']),
            #      raw_entity['entity_sourcetexts'], '=>')
            # print('Failure')
            pass

    print(raw_entities)
