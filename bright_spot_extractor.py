import base64
import os
from google import genai
from google.genai import types
from box import Box
import json


def attempt_bright_spot_extraction(episode_number):
    with open("secrets/gemini.json") as secrets_f:
        secrets = Box(json.load(secrets_f))

    client = genai.Client(
        api_key=secrets.api_key,
    )

    files = [
        # Make the file available in local system working directory
        # TODO: Need to convert SRT to TXT? Upload bytes?
        # TODO: Only upload the first 15 minutes to save tokens.
        client.files.upload(file="correct.py"),
    ]
    print(files)
    model = "gemini-2.0-flash"
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_uri(
                    file_uri=files[0].uri,
                    mime_type=files[0].mime_type,
                ),
                types.Part.from_text(
                    text="""What is Jordan's bright spot? What is Dan's?"""),
            ],
        ),
    ]
    generate_content_config = types.GenerateContentConfig(
        temperature=1,
        top_p=0.95,
        top_k=40,
        max_output_tokens=8192,  # TODO: This can likely be much shorter.
        response_mime_type="text/plain",
    )

    for chunk in client.models.generate_content_stream(
        model=model,
        contents=contents,
        config=generate_content_config,
    ):
        print(chunk.text, end="")


if __name__ == "__main__":
    attempt_bright_spot_extraction("1019")
