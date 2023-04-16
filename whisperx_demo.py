import whisper
import whisperx

from mutagen.mp3 import MP3

from pprint import pprint
from transcripts import create_best_transcript_listing
from transcripts import parse_transcript

device = "cuda"
audio_file = "audio_files/1.mp3"


audio_length = MP3(audio_file).info.length

transcript_listing = create_best_transcript_listing()
transcript_record = transcript_listing[transcript_listing.episode_number == '1'].to_dict(
    orient='records')[0]

print(transcript_record)
# TODO: Move this into transcripts.
transcript = parse_transcript(transcript_record)
transcript.augment_timestamps()
if transcript.blocks[-1].end_timestamp == None:
	transcript.blocks[-1].end_timestamp  = audio_length

# load alignment model and metadata
model_a, metadata = whisperx.load_align_model(
    language_code='en', device=device)

'''
# transcribe with original whisper
model = whisper.load_model("medium.en", device)
result = model.transcribe(audio_file)


def strip_segment(segment):
    return {
        # 'id': 11,
        # 'seek': 5216,
        'start': segment['start'],
        'end':  segment['end'],
        'text':  segment['text'],
        # 'tokens': [50435, 1406, 339, 338, 16418, 2667, 465, 29222, 543, 318, 644, 7360, 790, 18828, 750, 5543, 588, 7288, 379, 18740, 25158, 50765],
        # 'temperature': 0.0,
        # 'avg_logprob': -0.15704868150793988,
        # 'compression_ratio': 1.6414342629482073,
        # 'no_speech_prob': 0.0005096924141980708,
    }


pprint(result["segments"])  # before alignment

# TODO(woursler): Test if we can strip out logprobs from result without issue.

# align whisper output
result_aligned = whisperx.align(
    [
        strip_segment(segment)
        for segment in result["segments"]
    ],
    model_a,
    metadata,
    audio_file,
    device,
)

pprint(result_aligned["segments"])  # after alignment
pprint(result_aligned["word_segments"])  # after alignment


# Demonstrate that whisper can handle partial alignment
result_aligned2 = whisperx.align(
    [
        strip_segment(segment)
        for segment in result["segments"]
    ][-5:-4],
    model_a,
    metadata,
    audio_file,
    device,
)

pprint(result_aligned2["segments"])  # after alignment
pprint(result_aligned2["word_segments"])  # after alignment
'''

# Demonstrate we can work from transcripts:
result_aligned = whisperx.align(
    [
        {
            'start': block.start_timestamp,
            'end': block.end_timestamp,
            'text': block.text,
            'id': 'FOO',
        }
        for block in transcript.blocks # To demonstrate [-10:-5]
    ],
    model_a,
    metadata,
    audio_file,
    device,
)

pprint(result_aligned["segments"])  # after alignment
#pprint(result_aligned["word_segments"])  # after alignment
#pprint(result_aligned.keys())
