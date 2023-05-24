import click
import diff_match_patch as dmp_module
import kfio
import mwparserfromhell
import mwparserfromhell.nodes as wiki_node
import openai
import os
import re
import time

from abc import ABC
from abc import abstractmethod
from attr import attr
from attr import attrs
from box import Box
from entity import simplify_entity
from entity_extraction_util import wikipage_extractor
from parsimonious.nodes import VisitationError
from pprint import pprint
from pygit2 import Repository
from wiki_cleaner import simple_format
from tqdm import tqdm
from collections import Counter

WIKILINK_PATTERN = re.compile(
    r"\[\[(?P<link>[^|\]]+)(?:\|(?P<text>[^]]+))?\]\]")

entities_df = kfio.load('data/raw_entities.json')
page_listing = kfio.load('kf_wiki_content/page_listing.json')
scraped_page_listing = kfio.load('data/scraped_page_data.json')


def lookup_entity(e_key):
    entity_entry = entities_df[entities_df.raw_entity_name == e_key]
    if len(entity_entry) == 0:
        return None
    assert len(entity_entry) == 1
    entity_entry = Box(entity_entry.to_dict(orient='records')[0])
    return entity_entry


git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

# TODO(woursler): assert git_branch == 'pre-upload', "Please checkout pre-upload! Currently on %s." % git_branch

ALLOWABLE = {
    '"Mandatory Service Bill"': {"Mandatory Service Bill"},
    "2011 Norway attacks": {"Anders Breivik"},
    "703: 9/11, Part 1": {"9/11"},
    "Alex Jones": {"Alex", "Alex E. Jones"},
    "Alexandria Ocasio-Cortez": {"AOC"},
    "Anthony Fauci": {"Fauci"},
    "Arthur C Clarke": {"Arthur C. Clarke"},
    "Barack Obama": {"Obama", "President Obama"},
    "Bill Ogden": {"Bill"},
    "Boston Marathon Bombing": {
        "Boston Bombing",
        "Dzhokhar Tsarnaev",
        "Tamerlan Tsarnaev",
    },
    "Brett Kavanaugh": {"Kavanaugh"},
    "Buckley Hamman": {"Buckley"},
    "Cameron Atkinson": {"Cam Atkinson"},
    "Charleston church shooting": {"The Charleston church shooter", "Dylann Roof"},
    "Chris Mattei": {"Attorney Mattei"},
    "Dan Friesen": {"Dan"},
    "Daria Karpova": {"Daria"},
    "Dave Daubenmire": {"Coach Dave"},
    "Donald Trump": {"Trump"},
    "Dwight D Eisenhower": {"Eisenhower"},
    "Edward Group": {"Dr. Group", "Dr Group"},
    "Francis Boyle": {"Dr Francis Boyle"},
    "Free Speech Systems Bankruptcy Case": {
        "Free Speech Systems declared " "bankruptcy",
        "Free Speech Systems had filed for " "Bankruptcy",
        "declared bankruptcy",
    },
    "Free Speech Systems LLC": {
        "FSS",
        "Free Speech Systems",
        "Free Speech Systems, LLC",
    },
    "General Michael Flynn": {"Mike Flynn", "Gen. Flynn"},
    "Genesis Communications Network": {"GCN"},
    "George Soros": {"Soros"},
    "George W Bush": {"George W. Bush"},
    "Globalists": {"globalists", "globalist", "the Globalists", "Globalist"},
    "InfoWars": {"Infowars"},
    "January 6th Insurrection": {
        "Capitol riot",
        "Jan 6th",
        "January 6th",
        "storming of the United States Capitol on " "January 6, 2021",
        "the storming of the United States Capitol on " "January 6, 2021",
    },
    "Jeffrey Epstein": {"Epstein"},
    "Jim Fetzer": {"Fetzer"},
    "Joe Rogan": {"Rogan"},
    "John Birch Society": {"the John Birch Society"},
    "Jordan Holmes": {"Jordan"},
    "Judge Andrew Napolitano": {"Andrew Napolitano"},
    "Infowars": {"InfoWars"}, # TODO for non ep pages, prefer...
    "Kanye West": {"Ye", "Kanye West / Ye"},
    "Leo Zagami": {"Leo"},
    "Leonard Pozner": {"Lenny Pozner", "Pozner"},
    "List of Knowledge Fight episodes": {"Episode Listing"},
    "MIAC Report": {"MIAC"},
    "Mao Zedong": {"Mao"},
    "Mark Bankston": {"Mark"},
    "Mark Meechan": {"Count Dankula", "Mark Meechan (aka Count Dankula)"},
    "Mark Zuckerberg": {"Zuckerberg"},
    "Millie Weaver": {"Rainbow Snatch"},
    "Money Bomb": {"solicits direct donations"},
    "New World Order": {"the New World Order"},
    "Norm Pattis": {"Pattis"},
    "Notable Legal Proceedings": {"civil legal trials", "numerous defamation cases"},
    "Osama Bin Laden": {"CIA asset Osama Bin Laden"},
    "Owen Shroyer": {"Owen"},
    "Paul Joseph Watson": {"PJW"},
    "Pewdiepie": {"PewDiePie"},
    "Proud Boys": {"The Proud Boys"},
    "QAnon": {"Q"},
    "Robert Barnes": {"Barnes", "Bobby Barnes"},
    "Sandy Hook Family Members": {
        "Robbie Parker",
        "Robby Parker",
        "Scarlett Lewis",
        "Veronique De La Rosa",
        "family members of Sandy Hook victims",
        "father of Noah Pozner",
        "several families of the victims of Sandy Hook",
    },
    "Satan": {"Devil", "the Devil"},
    "Steve Pieczenik": {"Stevie P"},
    "Texas TUFTA Case": {
        "TX TUFTA lawsuit",
        "a lawsuit was filed under TUFTA",
        "sued to stop and reverse these transfers under the " "TUFTA",
    },
    "The Alex Jones Show": {"the Alex Jones Show", "Alex's show"},
    "The Obama Deception": {"the Obama Deception"},
    "The Sandy Hook Elementary Massacre": {"Sandy Hook"},
    "Vladimir Putin": {"Putin"},
    "Wolfgang Halbig": {"Halbig"},
    "bashar al assad": {"Bashar al-Assad"},
    "chobani v jones": {"Chobani", "Chobani suit", "Chobani v. Jones"},
    "connecticut sandy hook lawsuit (lafferty v jones)": {
        "CT Sandy Hook Lawsuit",
        "Connecticut Lawsuit",
        "Connecticut Sandy Hook " "Case",
        "Connecticut Sandy Hook " "Lawsuit (Lafferty v. " "Jones)",
        "Connecticut Sandy Hook " "case",
        "Connecticut case",
        "Lafferty Case",
        "Lafferty case",
        "Lafferty v. Jones",
        "Lafferty v. Jones in " "Connecticut",
        "defamation case " "against Alex Jones in " "Connecticut",
        "the Connecticut Sandy " "Hook Case",
        "the Connecticut case",
        "the Sandy Hook " "lawsuit",
    },
    "covid 19": {"Covid-19"},
    "infow et al bankruptcy case": {
        "Alex's ill-fated bankruptcy diversion",
        "InfoW et. al. Bankruptcy Case",
        "bankruptcy",
        "declare bankruptcy",
        "ill-fated bankruptcy disruption",
        "the InfoW Bankruptcy",
        "the InfoW Bankruptcy case",
        "three of the shell-company defendants filed " "for bankruptcy",
    },
    "pozner texas sandy hook lawsuit (pozner v jones)": {
        "Posner",
        "Posner v. Jones in " "Texas",
        "Pozner Texas Sandy Hook " "Lawsuit (Pozner v. " "Jones)",
    },
    "protocols of the elders of zion": {
        "PEZ",
        "Protocols of Zion",
        "The Protocols of the Elders of Zion",
        "references to Jews",
        "thinly veiled antisemitic / racist tropes",
        "when it absolutely is",
    },
    "rock (dwayne johnson)": {"The Rock (Dwayne Johnson)"},
    "sandy hook elementary massacre": {
        "Sandy Hook",
        "Sandy Hook Elementary School shooting",
        "The Sandy Hook Elementary Massacre",
    },
    "texas parkland shooting lawsuit (fontaine v jones)": {
        "Fontaine",
        "Fontaine case",
        "Parkland",
        "Texas Parkland " "Shooting Lawsuit " "(Fontaine v. Jones)",
        "court cases",
        "lawsuit regarding his " "misidentifying of the " "Parkland shooter",
        "the Fontaine case",
    },
    "texas sandy hook lawsuit (heslin v jones)": {
        "Heslin",
        "Heslin v. Jones",
        "Heslin v. Jones in Texas",
        "Heslin v. Jones trial",
        "InfoWars legal matters",
        "Sandy Hook",
        "Sandy Hook case",
        "Sandy Hook defamation case in " "Texas",
        "Sandy Hook lawsuit",
        "TX Sandy Hook Case",
        "Texas Sandy Hook Case",
        "Texas Sandy Hook Cases",
        "Texas Sandy Hook Lawsuit " "(Heslin v. Jones)",
        "Texas as well",
        "a similar case, filed by a " "separate group of family " "members, in Texas",
        "multiple",
        "the InfoWars/Sandy Hook " "lawsuit",
        "the Sandy Hook family members " "who sued Alex",
        "the Texas Sandy Hook Case",
        "the Texas Sandy Hook case",
    },
    "year of the seltzer": {"Year of Seltzer", "Year of the Seltzer"},
}

MISSING = Counter()


for page_record in tqdm(page_listing.to_dict(orient='records')):
    page_record = Box(page_record)

    if 'Transcript' in page_record.title:
        continue

    fname = 'kf_wiki_content/%s.wiki' % page_record.slug
    with open(fname, encoding='utf-8') as f:
        page_text = f.read()

    matches = WIKILINK_PATTERN.findall(page_text)

    for target, text in matches:
        if "#" in target:
            target = target.split("#")[0]

        if text is None or len(text.strip()) == 0:
            text = target

        if "Category:" in target:
            continue

        if "File:" in target:
            continue

        if "User:" in target:
            continue

        if "Help:" in target:
            continue

        true_target = simplify_entity(target)
        target_entity = lookup_entity(true_target)
        target_page = scraped_page_listing[scraped_page_listing.title == target]
        if len(target_page) == 0:
            target_page = scraped_page_listing[scraped_page_listing.title == true_target]

        if len(target_page) == 0:
            target_page = None
        elif len(target_page) == 1:
            target_page = Box(target_page.to_dict(orient='records')[0])
        else:
            raise NotImplementedError()

        if target_page is None:
            MISSING.update([target])
            continue  # MISSING?

        if target == true_target and target == text:
            continue

        if target_page is not None and ('Transcripts' in target_page.wiki_categories or 'Episodes' in target_page.wiki_categories):
            continue

        #print("FOO", target_page)
        #print(target, true_target, text, target_entity is None)

        if true_target in ALLOWABLE and text in ALLOWABLE[true_target]:
            continue

        print(page_record.title, "||", text, "=>", true_target)


pprint(MISSING)

