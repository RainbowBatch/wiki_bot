import json
import torch
import torchaudio

from box import Box
from pathlib import Path
from pyannote.audio import Inference
from pyannote.audio import Model
from pyannote.audio import Pipeline
from pyannote.audio.pipelines.speaker_verification import PretrainedSpeakerEmbedding
from pyannote.core import Segment
from rainbowbatch.secrets import secret_file

#embedding_model = Model.from_pretrained("pyannote/embedding",
#                                        use_auth_token=access_token)
#inference = Inference(embedding_model, window="whole")


with open(secret_file("huggingface.json")) as secrets_f:
    secrets = Box(json.load(secrets_f))
    access_token = secrets.access_token

pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization",
                                    use_auth_token=access_token)

fname = "audio_files/1.wav"

waveform, sample_rate = torchaudio.load(Path("audio_files/1.mp3"), format="mp3")
audio_in_memory = {"waveform": waveform, "sample_rate": sample_rate}

print("Computing diarization")
diarization = pipeline(audio_in_memory)  # TODO: Figure out how to load mp3s.

print(diarization)

for turn, _, speaker in diarization.itertracks(yield_label=True):
    print(f"start={turn.start:.1f}s stop={turn.end:.1f}s speaker_{speaker}")

    #embedding = inference.crop(fname, Segment(turn.start, turn.end))
    #print(embedding)
