from pathlib import Path
from .strict_command_parser import strict_parse_bot_command
import json

example_queries_fname = Path(
    __file__
).parent.absolute(
) / '../sensitive/example_queries.json'

with example_queries_fname.open() as example_queries_f:
    EXAMPLE_QUERIES = json.load(example_queries_f)

for eq in EXAMPLE_QUERIES:
    eq['parsed_object'] = strict_parse_bot_command(eq['parsed'])
