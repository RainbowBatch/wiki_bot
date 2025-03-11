import click
import diff_match_patch as dmp_module
import kfio
import mwparserfromhell
import mwparserfromhell.nodes as wiki_node
import openai
import os
import re
import time
import traceback

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
from retrying import retry
from tqdm import tqdm

WIKILINK_PATTERN = re.compile(
    r"\[\[(?P<link>[^|\]]+)(?:\|(?P<text>[^]]+))?\]\]")

dmp = dmp_module.diff_match_patch()

with open("secrets/openaiorg.txt") as openaiorg_f:
    openai.organization = openaiorg_f.read().strip()
with open("secrets/openaikey.txt") as openaikey_f:
    openai.api_key = openaikey_f.read().strip()

entities_df = kfio.load('data/raw_entities.json')
episodes_df = kfio.load('data/final.json')


class WikitextVisitor(ABC):
    '''Stripped down and modified version of parsimonious.nodes.NodeVisitor'''

    def visit(self, node):
        node_type_name = node.__class__.__name__
        method = getattr(self, 'visit_' + node_type_name, self.generic_visit)

        return method(node, [
            self.visit(n)
            for n in node.__children__()]
            if not isinstance(node, mwparserfromhell.wikicode.Wikicode)
            else [
            self.visit(n)
            for n in node.nodes],
        )

    def process(self, wikitext):
        return [
            self.visit(node) for node in wikitext.nodes
        ]

    @abstractmethod
    def generic_visit(self, node, visited_children):
        pass


@attrs
class ExistingLinkFinderVisitor(WikitextVisitor):
    '''Despite the name, this doesn't actually visit because mwfromhell doesn't support clean copies...'''
    entities = attr(factory=set)

    def generic_visit(self, node, visited_children):
        return node

    def visit_Template(self, node, visited_children):
        if node.name.strip() != "TranscriptBlock":
            return node
        # Only look for links in block templates.
        return self.visit_TranscriptBlock(node, visited_children)

    def visit_TranscriptBlock(self, node, visited_children):
        transcript_text = node.get('text').value
        return self.visit(transcript_text)

    def visit_Wikilink(self, node, _):
        if "Category:" in node.title:
            return node

        if "File:" in node.title:
            return node

        e_key = simplify_entity(str(node.title))
        if node.title != e_key:
            print(node.title, "=>", simplify_entity(str(node.title)))
            # raise NotImplementedError("possible dirty redirect...")
        self.entities.add(e_key)
        return node


def lookup_entity(e_key):
    entity_entry = entities_df[entities_df.raw_entity_name == e_key]
    if len(entity_entry) == 0:
        return None
    assert len(entity_entry) == 1
    entity_entry = Box(entity_entry.to_dict(orient='records')[0])
    return entity_entry


@attrs
class AutoLinkingVisitor(WikitextVisitor):
    '''Despite the name, this doesn't actually visit because mwfromhell doesn't support clean copies...'''
    entities = attr(factory=set)
    present_entities = attr(factory=set)

    def generic_visit(self, node, visited_children):
        # TODO: Integrate children changes
        print("Visiting", type(node), node.__class__.__name__, node)
        return node

    def visit_Template(self, node, visited_children):
        if node.name.strip() != "TranscriptBlock":
            return node
        # Only autolink transcript block templates.
        return self.visit_TranscriptBlock(node, visited_children)

    def visit_TranscriptBlock(self, node, visited_children):
        transcript_text = node.get('text').value
        return self.visit(transcript_text)

    def visit_Wikilink(self, node, _):
        return node # DO NOTHING.

    def visit_ExternalLink(self, node, _):
        return node # DO NOTHING

    def visit_Text(self, node, _):
        # Edit the text.
        if node.value.strip() != '':
            for proto_entity in wikipage_extractor(node.value, origin=None):
                e_key = simplify_entity(proto_entity[0])
                entity_entry = lookup_entity(e_key)

                if entity_entry is None:
                    continue

                if not entity_entry.is_existing:
                    continue
                # if "alex" in e_key.lower():
                #    continue

                # Ensure we haven't already linked to this.
                if e_key not in self.present_entities:
                    self.present_entities.add(e_key)
                    if e_key == "Alex Jones":
                        # For right now, stop trying to link Alex Jones... would be better handled with a special case.
                        continue
                    if (" %s " % e_key) in node.value or (" %s." % e_key) in node.value or (" %s's" % e_key) in node.value or (" %s," % e_key) in node.value or node.value.strip().startswith(e_key) or node.value.strip().endswith(e_key):
                        # Special case, for now make sure we don't incorrectly link up "the alex jones show"
                        if e_key == 'Alex Jones' and 'the alex jones show' in node.value.lower():
                            continue
                        print("Autolinking:", e_key)
                        node.value = node.value.replace(
                            e_key, "[[%s]]" % e_key, 1)
                    elif "[[" in node.value:
                        continue  # Can only handle one at a time.
                    else:
                        print("Attempting GPT-3 based autolink on '%s' (%s)" %
                              (node.value.strip(), e_key))

                        try:
                            replaced_text, clean_link = gpt3_autolink(
                                node.value.strip(),
                                entity_entry,
                            )
                        except:
                            replaced_text = None

                        if replaced_text is not None:
                            print("***", replaced_text, "=>", clean_link)
                            node.value = node.value.replace(
                                replaced_text, clean_link, 1)
                        else:
                            print("FAILED", e_key, node.value)

                    # TODO(woursler): GPT enabled smart editing?
        return node

    def visit_Heading(self, node, _):
        # Don't alter headings... for now.
        return node


def autolink_file(fname, present_entities=None):
    try:
        if present_entities is None:
            present_entities = set()
        with open(fname, encoding='utf-8') as wiki_f:
            raw_mediawiki = wiki_f.read()

        if '#redirect' in raw_mediawiki.lower():
            return

        structured_mediawiki = mwparserfromhell.parse(raw_mediawiki)

        existing_link_visitor = ExistingLinkFinderVisitor()
        existing_link_visitor.process(structured_mediawiki)

        visitor = AutoLinkingVisitor(
            present_entities=existing_link_visitor.entities | present_entities)
        visitor.process(structured_mediawiki)

        # This should now include the modifications.
        edited_mediawiki = simple_format(str(structured_mediawiki))

        if raw_mediawiki.strip() != edited_mediawiki.strip():
            print("Saving non-trivial changes.")
            with open(fname, "w", encoding='utf-8') as wiki_f:
                wiki_f.write(edited_mediawiki)
    except:
        print("Something went wrong:", fname)
        traceback.print_exc()


@click.command()
@click.option('--min-ep-num', default=0, help='Lowest numbered episode to include.')
@click.option('--max-ep-num', default=10**4, help='Highest numbered episode to include.')
def autolink_episodes(min_ep_num, max_ep_num):

    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'pre-upload', "Please checkout pre-upload! Currently on %s." % git_branch

    for EPISODE_NUMBER in episodes_df.episode_number:

        ep_num = int(
            ''.join([s for s in EPISODE_NUMBER if s.isdigit()]))

        if ep_num < min_ep_num:
            continue

        if ep_num > max_ep_num:
            continue

        episode_details = Box(
            episodes_df[episodes_df.episode_number == EPISODE_NUMBER].to_dict(orient='records')[0])

        print("Checking Episode", EPISODE_NUMBER)

        autolink_file(episode_details.ofile)


@click.command()
def autolink_pages():

    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'pre-upload', "Please checkout pre-upload! Currently on %s." % git_branch

    page_listing = kfio.load('kf_wiki_content/page_listing.json')

    for page_record in tqdm(page_listing.to_dict(orient='records')):
        page_record = Box(page_record)

        fname = 'kf_wiki_content/%s.wiki' % page_record.slug

        if 'Transcript' in page_record.title:
            continue

        if 'Dreamy Creamy Summer' in page_record.title:
            continue

        if page_record.title[0] in "01234567890":
            continue

        autolink_file(fname, present_entities=set([page_record.title]))


@click.command()
def autolink_transcripts():

    git_branch = Repository('kf_wiki_content/').head.shorthand.strip()

    assert git_branch == 'pre-upload', "Please checkout pre-upload! Currently on %s." % git_branch

    page_listing = kfio.load('kf_wiki_content/page_listing.json')

    for page_record in tqdm(page_listing.to_dict(orient='records')):
        page_record = Box(page_record)

        fname = 'kf_wiki_content/%s.wiki' % page_record.slug

        if 'Transcript' not in page_record.title:
            continue

        autolink_file(fname)


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=10)
def gpt3_autolink(input_text, entity_entry):
    print("GPT-EDIT on", entity_entry.entity_name)
    result = openai.Edit.create(
        model="text-davinci-edit-001",
        input=input_text,
        n=10,
        instruction="Annotate the text with a mediawiki link to the page named '%s' (e.g. [[Link Text|Page Name]]) on the most closely matching text. Maintain the text of the page. Don't include possessives and other punctuation (e.g. link like [[Bob]]'s not [[Bob|Bob's]])" % entity_entry.entity_name,
    )

    for choice in result['choices']:
        if 'error' in choice:
            continue

        z = dmp.diff_main(input_text, choice["text"].strip())
        dmp.diff_cleanupSemantic(z)

        is_valid = True
        for origin, fragment in z:
            if origin == 1:
                if not (fragment.startswith("[[") or fragment.endswith("]]")):
                    is_valid = False

        if not is_valid:
            # TODO(woursler): Log something?
            continue

        matches = WIKILINK_PATTERN.findall(choice["text"].strip())

        if len(matches) != 1:
            continue

        def validate_link(link):
            link, text = link
            link = link.strip()
            text = text.strip()
            if len(text.strip()) == 0:
                text = None

            linked_text = text if text is not None else link

            if linked_text not in input_text or simplify_entity(linked_text) != entity_entry.entity_name:
                return False, None, None

            if link != entity_entry.entity_name:
                if text is None:
                    text = link
                link = entity_entry.entity_name

            if link == text:
                text = None

            if text is None:
                return True, linked_text,  "[[%s]]" % entity_entry.entity_name
            return True, linked_text, "[[%s|%s]]" % (entity_entry.entity_name, text)

        is_valid, replaced_text, clean_link = validate_link(matches[0])

        if not is_valid:
            continue

        if replaced_text.endswith("s'") or replaced_text.endswith("'s"):
            continue

        # DO NOT SUBMIT: Aggregate?
        return replaced_text, clean_link
    return None, None


if __name__ == '__main__':
    autolink_episodes()
