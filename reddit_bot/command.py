from attr import attrs, attr
from typing import Optional


COMMAND_MAPPING = {
    'details': 'episode_details',
    'episode_details': 'episode_details',
    'help': 'help',
    'quitquitquit': 'quitquitquit',
    'search': 'transcript_search',
    'search_transcripts': 'transcript_search',
    'shutdown': 'quitquitquit',
    'transcript_search': 'transcript_search',
}


@attrs(repr=False)
class BotCommand:
    command: str = attr()
    argument: Optional[str] = attr()
    origin_command: str = attr()

    def __repr__(self):
        command_slug = self.command
        if self.origin_command is not None and self.origin_command != self.command:
            command_slug = "%s(%s)" % (self.command, self.origin_command)
        if self.argument is None:
            return '!%s' % (command_slug)
        return '!%s "%s"' % (command_slug, self.argument)


class CommandParsingException(Exception):
    pass
