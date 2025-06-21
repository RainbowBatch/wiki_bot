import wikitextparser
from rainbowbatch.entity.entity import extract_entities
import pypandoc


def wikitext_extractor(wiki_text, origin):
    cleaned_text = pypandoc.convert_text(
        wiki_text,
        to='plain',
        format='mediawiki'
    )

    # TODO: Consider processing line by line?
    yield from extract_entities(cleaned_text, origin)


def wikipage_extractor(wiki_text, origin):
    if '#redirect' in wiki_text:
        # Don't process redirects.
        return

    # Parse the wiki text
    wiki_page = wikitextparser.parse(wiki_text)

    # Handle guest section specifically.
    for template in wiki_page.templates:
            template_name = template.name.strip().lower()
            if template_name != 'episode':
                continue
            people = template.get_arg('appearance')
            if people is None:
                continue
            people = people.value.split(',')
            for person in people:
                yield from wikitext_extractor(person.strip(), origin)

    # Loop through the sections of the page
    for section in wiki_page.sections:
        # If the section is named "FooBar", remove it
        if section.title is not None and section.title.strip() == "Relevant Episodes by Release Date":
            continue
            # Skip these sections.

        if section.title is not None:
            yield from wikitext_extractor(section.title.strip(), origin)
        yield from wikitext_extractor(section.contents.strip(), origin)


def transcript_extractor(transcript, origin):
    for block in transcript.blocks:
        # TODO: Directly handle this?
        if block.speaker_name is not None:
            yield from extract_entities(block.speaker_name.strip(), origin)
        yield from extract_entities(block.text.strip(), origin)
