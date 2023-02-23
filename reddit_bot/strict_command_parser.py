import re

from .command import BotCommand
from .command import COMMAND_MAPPING
from .command import CommandParsingException

command_extractor_regexp = re.compile("(?<=!)(?P<command>\w+)")
quoted_strings_extractor_regexp = re.compile('"([^"]*)"')


def strict_normalize_verb(verb):
    if verb in COMMAND_MAPPING:
        return COMMAND_MAPPING[verb]
    else:
        print(verb)
        raise CommandParsingException("Unknown command: '%s'" % verb)


def strict_match_verb(query):
    command_match = command_extractor_regexp.search(query)

    if command_match is None:
        raise CommandParsingException("Unable to find !verb.")
    return command_match.group('command')


def strict_match_argument(query, none_is_error=False):
    quoted_strings = quoted_strings_extractor_regexp.findall(query)

    if len(quoted_strings) == 0:
        if none_is_error:
            raise CommandParsingException("Unable to find a quoted argument.")
        return None

    if len(quoted_strings) > 1:
        raise CommandParsingException("Multiple quoted arguments.")

    return quoted_strings[0]


def strict_parse_bot_command(query: str) -> BotCommand:
    original_verb = strict_match_verb(query)
    verb = strict_normalize_verb(original_verb)
    arg = strict_match_argument(query, none_is_error=False)

    if (verb == "transcript_search" or verb == "episode_details") and arg is None:
        raise CommandParsingException("Verb requires argument.")

    return BotCommand(
        command=verb,
        origin_command=original_verb,
        argument=arg,
    )
