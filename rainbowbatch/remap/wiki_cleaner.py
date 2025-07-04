import re
import wikitextparser

from enum import Enum
from io import StringIO


class LineClassification(Enum):
    UNKNOWN = 0
    SECTION = 1
    PSEUDOSECTION = 2
    BLANK = 3
    BULLET_STAR = 4
    BULLET_HASH = 5
    IGNORED = 6
    CATEGORY_TAG = 7
    TABLE_START = 8
    TABLE_END = 9

    @staticmethod
    def is_preserved(lc):
        return lc == LineClassification.UNKNOWN or lc == LineClassification.SECTION or lc == LineClassification.PSEUDOSECTION

    @staticmethod
    def is_bullet(lc):
        return lc == LineClassification.BULLET_STAR or lc == LineClassification.BULLET_HASH


GARBLED_CHARS = {
    "A¶": "ö",
    "Ã ": "à",
    "Ã¡": "á",
    "Ã¢": "â",
    "Ã£": "ã",
    "Ã¥": "å",
    "Ã§": "ç",
    "Ã¨": "è",
    "Ã©": "é",
    "Ã¯": "ï",
    "Ã±": "ñ",
    "Ã³": "ó",
    "Ã´": "ô",
    "Ã¶": "ö",
    "Ã¶": "ö",
    "Ã¸": "ø",
    "Ã¼": "ü",
    "ÃƒÂ­": "í",
    "ÃƒÂ¶": "ö",
    "Ä‡": "ć",
    "Ä╪": "ć",
    "Å¡": "š",
    "Å‚": "ł",
    'â€"': " - ",
    "-ââ,¬\"": " - ",
    "â€¦": " - ",
    "â€™": "'",
    "Â§": "§",
    "Â£": "£",
    # Andreas KÃ¶lker / JÃƒÂ¶rg Sprave
    # JarosÅ‚aw KarczyÅ„ski
    # Stanislaw KoÅ¼e
    # "â•¼" from Transcript 378.... is weird.
    # "President [[Barack Obama|Obama]]": "[[Barack Obama|President Obama]]",
    # "President [[Donald Trump|Trump]]": "[[Donald Trump|President Trump]]",
    # "the [[The Alex Jones Show|Alex Jones Show]]": "[[The Alex Jones Show|the Alex Jones Show]]",
}

table_start_pattern = re.compile(r"\{\|")
table_end_pattern = re.compile(r"\|\}")
section_pattern = re.compile(r"==+(?P<section_name>[^=]+)==+")
pseudosection_pattern = re.compile(r"'''''+(?P<section_name>[^']+)'''''+:")
citation_link_pattern = re.compile("\\[\S+ Citations\s*\\]")

dreamy_creamy_link1_pattern = re.compile(
    r"\[https://www.gofundme.com/f/dreamycreamysummer .*\]")
dreamy_creamy_link2_pattern = re.compile(
    r"\[https://www.gofundme.com/f/lets-put-a-button-on-the-dreamy-creamy-summer .*\]")
category_tag_pattern = re.compile(
    r'\[\[\s*Category\s*:\s*(?P<category>[^\]]+)\]\]')

# == '''Buckley's Musical Talents''' ==
bolded_category_regex = re.compile(
    r"(=+)\s*'''(?P<section_name>[^=]+)'''\s*\1")


def rewrite_bullet_line(line, classification):
    if classification == LineClassification.BULLET_STAR:
        bullet_index = line.find('*')
        assert bullet_index != -1
        bullet_text = line[bullet_index+1:].strip()
        return "*%s\n" % bullet_text
    if classification == LineClassification.BULLET_HASH:
        bullet_index = line.find('#')
        assert bullet_index != -1
        bullet_text = line[bullet_index+1:].strip()
        return "**%s\n" % bullet_text
    raise NotImplementedError()


def classify_line(line):
    if section_pattern.search(line.strip()) is not None:
        return LineClassification.SECTION

    if category_tag_pattern.search(line.strip()) is not None:
        return LineClassification.CATEGORY_TAG

    if pseudosection_pattern.search(line.strip()) is not None:
        return LineClassification.PSEUDOSECTION

    if dreamy_creamy_link1_pattern.search(line.strip()) is not None:
        return LineClassification.IGNORED

    if dreamy_creamy_link2_pattern.search(line.strip()) is not None:
        return LineClassification.IGNORED

    if citation_link_pattern.search(line.strip()) is not None:
        return LineClassification.IGNORED

    if table_start_pattern.search(line.strip()) is not None:
        return LineClassification.TABLE_START

    if table_end_pattern.search(line.strip()) is not None:
        return LineClassification.TABLE_END

    if line.strip() == '':
        return LineClassification.BLANK
    # TODO: Regex?
    if line.startswith("*") or line.startswith(" *"):
        return LineClassification.BULLET_STAR

    if line.startswith("#") or line.startswith(" #"):
        return LineClassification.BULLET_HASH

    return LineClassification.UNKNOWN


class LinewiseVisitorState(Enum):
    START = 0
    PRESERVE = 1
    BULLETS = 2
    CATEGORIES = 3
    BLANK = 4
    TABLE = 5


def linewise_simplification(raw_mediawiki):
    def process_classified_lines(classified_lines):
        '''This amounts to a really hacky state_machine.'''
        state = LinewiseVisitorState.START

        categories = []

        for classification, line in classified_lines:
            assert isinstance(state, LinewiseVisitorState)

            if classification == LineClassification.CATEGORY_TAG:
                categories.append(line)
                continue

            if classification == LineClassification.IGNORED:
                continue

            if classification == LineClassification.BLANK and state == LinewiseVisitorState.TABLE:
                # No blank lines in tables.
                continue

            # TODO: Handle pseudosections here too?
            line = bolded_category_regex.sub(r"\1\g<section_name>\1\n", line)

            if state == LinewiseVisitorState.START:
                if classification == LineClassification.BLANK:
                    continue
                if LineClassification.is_bullet(classification):
                    state = LinewiseVisitorState.BULLETS
                if LineClassification.is_preserved(classification):
                    state = LinewiseVisitorState.PRESERVE
                if classification == LineClassification.TABLE_START:
                    state = LinewiseVisitorState.TABLE

            if classification == LineClassification.TABLE_START:
                state = LinewiseVisitorState.TABLE

            if classification == LineClassification.TABLE_END and state == LinewiseVisitorState.TABLE:
                state = LinewiseVisitorState.PRESERVE

            if LineClassification.is_bullet(classification) and state != LinewiseVisitorState.BULLETS:
                yield '\n'
                state = LinewiseVisitorState.BULLETS

            if classification == LineClassification.BLANK:
                if state not in [LinewiseVisitorState.START, LinewiseVisitorState.BULLETS, LinewiseVisitorState.CATEGORIES]:
                    state = LinewiseVisitorState.BLANK
                    continue

            if state == LinewiseVisitorState.BULLETS:
                if classification == LineClassification.BLANK:
                    continue
                if not LineClassification.is_bullet(classification):
                    yield '\n'
                    # TODO: Maybe this does slightly the wrong thing?
                    state = LinewiseVisitorState.PRESERVE

            if state == LinewiseVisitorState.BLANK and classification != LineClassification.BLANK:
                yield '\n'

            if LineClassification.is_bullet(classification):
                yield rewrite_bullet_line(line, classification)
            elif classification == LineClassification.PSEUDOSECTION:
                yield "===%s===\n" % pseudosection_pattern.search(line.strip()).group('section_name')
            elif line.strip().endswith("<br>"):
                yield line.strip()[:-4] + '\n\n'
            elif line.strip().endswith("<br/>"):
                yield line.strip()[:-5] + '\n\n'
            else:
                yield line
        yield '\n\n'

        for category in categories:
            yield category
        yield '\n\n'

    classified_lines = [
        (classify_line(line), line) for line in StringIO(raw_mediawiki)
    ]

    return ''.join(process_classified_lines(classified_lines))


def add_kf_citation_reference_to_first_line(raw_mediawiki, reference_link, reference_title=None):
    classified_lines = [
        (classify_line(line), line) for line in StringIO(raw_mediawiki)
    ]

    classifications = [
        classifications for classifications, _ in classified_lines]
    split_lines = [line for _, line in classified_lines]

    def add_citation_to_line(line):
        line = line.strip()
        if reference_title is None:
            reference = "<ref>%s</ref>" % reference_link
        else:
            reference = "<ref>[%s %s]</ref>" % (
                reference_link, reference_title)

        if line.endswith(':'):
            return line[:-1] + reference + ':\n'
        return line + reference + '\n'

    if classifications[0] != LineClassification.UNKNOWN:
        split_lines.insert(
            0,
            '\n',
        )
        split_lines.insert(
            0,
            "This portion was reproduced from the official Knowledge Fight website.\n",
        )

    split_lines[0] = add_citation_to_line(split_lines[0])

    return ''.join(split_lines)


WINDOWS_LINE_ENDING = '\r\n'
UNIX_LINE_ENDING = '\n'


def simple_pformat_pass(raw_mediawiki):
    text = wikitextparser.parse(raw_mediawiki).pformat().replace("–", "-").replace("‚", ",").replace("* *", "**").replace("‘", "'").replace(
        "’", "'").replace('“', '"').replace('”', '"').replace('&quot;', '"').replace('&amp;', '&').replace(u'\xa0', u' ').replace(u'\u200f', '').replace(u'\u200e', '').replace('<br>', '\n\n').replace('<br/>', '\n\n').replace('<br />', '\n\n')

    # Hotfix for a bug in the autolinker.
    text = text.replace(
        "[[The Sandy Hook Elementary Massacre|Sandy Hook]]",
        "[[@@@SANDY_HOOK_LINK]]",
        1,
    ).replace(
        "[[The Sandy Hook Elementary Massacre|Sandy Hook]]",
        "Sandy Hook",
    ).replace(
        "[[@@@SANDY_HOOK_LINK]]",
        "[[The Sandy Hook Elementary Massacre|Sandy Hook]]",
        1,
    )

    for garbled, clean in GARBLED_CHARS.items():
        text = text.replace(garbled, clean)

    text = re.sub(r'<references\s*/>', '', text)

    # Clean up large blocks of blank lines.
    text = text.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)
    text = re.sub(r'(\n\s*)+\n+', '\n\n', text)

    # Remove trailing spaces.
    text = re.sub('[^\S\r\n]+\n', '\n', text)

    # Make sure we have a single trailing newline and no leading whitespace.
    text = text.strip() + '\n'

    return text


def format_citation_block(raw_citation_block, citation_link, citation_title=None):
    return simple_pformat_pass(
        add_kf_citation_reference_to_first_line(
            linewise_simplification(raw_citation_block),
            citation_link,
            citation_title,
        )
    )


def simple_format(raw_mediawiki):
    return simple_pformat_pass(linewise_simplification(raw_mediawiki))


if __name__ == '__main__':

    SAMPLE = '''
{{Stub}}

Today, Dan and Jordan check in on how the last week ended on The Alex Jones Show. In this installment, Alex stokes religious tensions, interviews someone who probably got tricked by a pyramid scheme, and definitely doesn't talk about Matt Gaetz news.

==Tidbits==


The world on June 21, 2015:



* Donald Trump has been running for president for 5 days.

 *  Test 2

# Sub point

 # Sub point 2


* The Congressional Budget Office announces that repealing the Affordable Care Act could increase the national debt by $137 billion


* 84 people die in Mumbai, India as a result of drinking toxic homemade liquor



What Alex covered on June 21:



* Alex has gone from being hesitant to call the Charleston shooting a false flag to being pretty comfortable with the position. He still hedges his claims by saying that &quot;black preachers say this things stinks to high heaven,&quot; but it is clear from his language that this is his position as well.



* Alex has had about enough of talking about Charleston and how the explicitly racially motivated shooting wasn't about race, so he uses his Sunday show to transition to a new narrative. He keeps the &quot;2015 Will Be The Summer Of Rage&quot; element from the previous narrative, but his new story line is primarily about how he has heard from &quot;billionaires&quot; that the Elite Globalists are starting to evacuate to &quot;armored redoubts.&quot; He is never specific about anything, and this is a claim he's been making every day since, that the elites are *just about* to leave.



* His secondary narrative takes up most of the show. He has read a study about how the Earth is currently in the sixth extinction period of our history, [http://www.nbcnews.com/science/environment/scientists-build-case-sixth-extinction-say-it-could-kill-us-n378586 which is not at all inaccurate]. As the argument goes, us humans are in no way immune from the forces that cause extinction, so we should be careful. Granted, the article is titled very &quot;click-baity,&quot; suggesting that humans are up next, but that's what allows Alex to use it to build his narrative that the Globalists are behind the extinctions, and they're telegraphing that they're going to kill off humanity.



* Jakari Jackon does a live report from Charleston about how the community has really come together and it's an inspiring thing to see. He talks about how beautiful the church service he attended was the night before, and seems to be excited about a march that was going to take place that evening. He brings up how weird it is that there are monuments to white supremacists on the same block as Emanuel African Methodist Episcopal Church. It's a great report. The entire time, Alex keeps trying to shift the conversation back to negative things like a possible race war, and imaginary forces wanting to ban the Confederate flag.


* Alex explains that the key to understanding everything (I kid you not) is a Chik-fil-A billboard he saw recently. He explains that its &quot;psychological warfare&quot; because the cows are drawn realistically, whereas the chicken is drawn as a blob which humanizes the cows but not the chickens. I believe he was trying to make an abortion metaphor, based on the fact that the chicken just looks like a pile of goo. What Alex fails to take into account is that the premise of the billboards is that they are drawn by cows, who don't have fingers. They lack the motor skills to draw accurate chickens with their hooves. There are a hundred other complaints I could lob against his interpretation of this very clever ad campaign, but this isn't the place for it. Just listen to his blow-hard analysis.



* Continuing an anti-abortion theme, Alex explains that Michelle Obama is more racist than David Duke.



* Alex has yet to mention that Donald Trump is running for president.

'''

    s2 = linewise_simplification(SAMPLE)

    # print(s2)

    s3 = simple_pformat_pass(s2)

    # print(s3)

    s4 = add_kf_citation_reference_to_first_line(s3, 'https://google.com')

    print(s4)
