import os
import openai
import mwparserfromhell
import mwparserfromhell.nodes as wiki_node
from abc import ABC, abstractmethod
from parsimonious.nodes import VisitationError
import kfio
from box import Box


class WikitextVisitor(ABC):
    '''Stripped down and modified version of parsimonious.nodes.NodeVisitor'''

    def visit(self, node):
        node_type_name = node.__class__.__name__
        method = getattr(self, 'visit_' + node_type_name, self.generic_visit)

        return method(node, [
            self.visit(n)
            for n in node.__children__()]
            if not isinstance(node, mwparserfromhell.wikicode.Wikicode)
            # TODO: Might need to handle this differently?
            else [],
        )

    def process(self, wikitext):
        return [
            self.visit(node) for node in wikitext.nodes
        ]

    @abstractmethod
    def generic_visit(self, node, visited_children):
        pass


class AutoLinkingVisitor(WikitextVisitor):
    '''Despite the name, this doesn't actually visit because mwfromhell doesn't support clean copies...'''

    def generic_visit(self, node, visited_children):
        # TODO: Integrate children changes
        # print("Visiting", type(node), node)
        return node

    def visit_Template(self, node, _):
      # Don't alter templates.
      return node

    def visit_Text(self, node, _):
      # Edit the text.
      if node.value.strip() != '':
        node.value = "foo" + node.value
      return node

    def visit_Heading(self, node, _):
        node.title = "bar" + str(node.title)
        return node


entities_df = kfio.load('data/raw_entities.json')
episodes_df = kfio.load('data/final.json')

EPISODE_NUMBER = '40'

episode_details = Box(episodes_df[episodes_df.episode_number == EPISODE_NUMBER].to_dict(orient='records')[0])

print(episode_details)


print(entities_df)
for entity_record in entities_df[entities_df.is_existing & ~entities_df.is_redirect].to_dict(orient='records'):
    entity_record = Box(entity_record)
    origin_episode_numbers = set([o.split('__')[0]
                                  for o in entity_record.entity_origin])
    origin_episode_numbers.discard('None')

    if EPISODE_NUMBER in origin_episode_numbers:
        print("***", entity_record.entity_name)



with open("secrets/openaiorg.txt") as openaiorg_f:
    openai.organization = openaiorg_f.read().strip()
with open("secrets/openaikey.txt") as openaikey_f:
    openai.api_key = openaikey_f.read().strip()


with open(episode_details.ofile) as wiki_f:
    raw_mediawiki = wiki_f.read()
    structured_mediawiki = mwparserfromhell.parse(raw_mediawiki)

    visitor = AutoLinkingVisitor()
    print(
        visitor.process(structured_mediawiki)
    )

    print(structured_mediawiki)

'''
print(openai.Edit.create(
  model="text-davinci-edit-001",
  # input="Alex is a con artist, having founded InfoWars. Harrison and Owen are hangers on.",
  input="Alex references Drudge link to a PJW article",
  n=5,
  instruction="Annotate the text with a mediawiki link to the page named 'Paul Joseph Watson' (e.g. [[Link Text|Page Name]]) on the most closely matching text.",
))
'''
