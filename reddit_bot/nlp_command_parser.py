import numpy as np
import re

from thefuzz import fuzz
from thefuzz import process as fuzzy_process
from transformers import pipeline

from .command import BotCommand
from .command import COMMAND_MAPPING
from .command import CommandParsingException
from .example_queries import EXAMPLE_QUERIES
from .strict_command_parser import strict_match_argument
from .strict_command_parser import strict_match_verb
from .strict_command_parser import strict_normalize_verb
from .strict_command_parser import strict_parse_bot_command


# 125M, 1.3B, 2.7B are all options
GENERATOR = pipeline('text-generation', model='EleutherAI/gpt-neo-125M')


def medoid(inputs):
    n_inputs = len(inputs)
    dist_mat = np.zeros((n_inputs, n_inputs))

    for j in range(n_inputs):
        for i in range(n_inputs):
            if i != j:
                dist_mat[i, j] = 100 - fuzz.ratio(inputs[i], inputs[j])

    medoid_idx = np.argmin(dist_mat.sum(axis=0))  # sum over y

    return medoid_idx, inputs[medoid_idx]


def get_completion(PROMPT):
    PROMPT_LEN = len(PROMPT.split('\n'))

    response = GENERATOR(PROMPT, do_sample=True, min_length=300, max_length=400, pad_token_id=50256)[
        0]['generated_text']

    raw_search_term = response.split('\n')[PROMPT_LEN-1][12:]
    return re.sub(r'[^\w\s\']', '', raw_search_term.strip().lower())


def fuzzy_extract_search_term(query):
    PROMPT = "Extract the search term from each request\n"

    for eq in EXAMPLE_QUERIES:
        if eq['parsed_object'].command != 'transcript_search':
            continue
        PROMPT += "request: %s\n" % eq['raw'].lower()
        PROMPT += "search_term: %s\n" % eq['parsed_object'].argument.lower()

    PROMPT += ("request: %s\n" % query).lower()
    PROMPT += "search_term:"

    PROMPT = PROMPT.strip()

    results = [get_completion(PROMPT) for _ in range(20)]

    plausible_results = [
        result
        for result in results
        if fuzz.partial_ratio(query.lower(), result) > 60
    ]

    # TODO: Handle N=1, N=2 cases better.
    if len(plausible_results) > 0:
        return medoid(plausible_results)[1]
    return None


def fuzzy_normalize_verb(verb):
    nearest_command, similarity = fuzzy_process.extractOne(
        verb,
        COMMAND_MAPPING.keys()
    )

    if similarity < 70:
        raise CommandParsingException("Unknown command: '%s'" % verb)
    return COMMAND_MAPPING[nearest_command]


def fuzzy_match_verb(query):
    _, similarity, index = fuzzy_process.extractOne(
        query.lower(),
        {
            i: eq['raw'].lower() for i, eq in enumerate(EXAMPLE_QUERIES)
        },
        # TODO: This is not the ideal metric, biases strongly to short strings.
        scorer=fuzz.partial_token_sort_ratio,
    )

    if similarity < 80:
        print(EXAMPLE_QUERIES[index], similarity)
        return None  # TODO: More explicit error?

    return EXAMPLE_QUERIES[index]['parsed_object'].command


def fuzzy_parse_bot_command(query: str) -> BotCommand:
    # Try strict parsing first, so there's an unambigious
    # way to formulate commands.
    try:
        return strict_parse_bot_command(query)
    except:
        pass  # No problem if strict parsing didn't work.

    try:
        origin_verb = strict_match_verb(query)
    except:
        origin_verb = fuzzy_match_verb(query)

    if origin_verb is None:
        print("VERB FAILURE")
        # For now, let's just assume it's meant to search the transcripts.
        origin_verb = "transcript_search"

    verb = fuzzy_normalize_verb(origin_verb)

    try:
        arg = strict_match_argument(query, none_is_error=False)
    except:
        arg = None

    if verb == "transcript_search":
        if arg is None:
            arg = fuzzy_extract_search_term(query)
    elif verb == "episode_details":
        if arg is None:
            # TODO: Custom fuzzy extraction...
            raise NotImplementedError()

    return BotCommand(
        command=verb,
        origin_command=origin_verb,
        argument=arg,
    )
