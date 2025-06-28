import datetime
import os
import rainbowbatch.kfio as kfio
import srt
import torch
import webvtt
import whisper
import whisper.audio

from pathlib import Path

TRANSCRIPT_DIR = kfio.TOP_LEVEL_DIR / 'transcripts'
AUDIO_FILES_DIR = kfio.TOP_LEVEL_DIR / 'audio_files'

model = whisper.load_model("medium.en")
device = "cuda" if torch.cuda.is_available() else "cpu"


def write_srt_file(result, output_path):
    subtitles = []
    for i, segment in enumerate(result['segments']):
        start = datetime.timedelta(seconds=segment['start'])
        end = datetime.timedelta(seconds=segment['end'])
        content = segment['text'].strip()
        subtitle = srt.Subtitle(
            index=i + 1,
            start=start,
            end=end,
            content=content
        )
        subtitles.append(subtitle)

    with output_path.open("w", encoding="utf-8") as f:
        f.write(srt.compose(subtitles))


def write_vtt_file(result, output_path):
    vtt = webvtt.WebVTT()
    for segment in result["segments"]:
        caption = webvtt.Caption(
            start=whisper.utils.format_timestamp(
                segment["start"], always_include_hours=True),
            end=whisper.utils.format_timestamp(
                segment["end"], always_include_hours=True),
            text=segment["text"].strip()
        )
        vtt.captions.append(caption)
    vtt.save(str(output_path))


def transcribe_with_whisper(max_transcriptions=2):
    os.makedirs(TRANSCRIPT_DIR, exist_ok=True)
    transcribed_count = 0

    for audio_path in AUDIO_FILES_DIR.glob("*.mp3"):
        if transcribed_count >= max_transcriptions:
            print(f"Reached max_transcriptions limit: {max_transcriptions}")
            break

        episode_number = audio_path.stem
        txt_path = TRANSCRIPT_DIR / f"{episode_number}.whisper.txt"
        srt_path = TRANSCRIPT_DIR / f"{episode_number}.whisper.srt"
        vtt_path = TRANSCRIPT_DIR / f"{episode_number}.whisper.vtt"

        if txt_path.exists() or srt_path.exists() or vtt_path.exists():
            print("Skipping", audio_path.name)
            continue

        print(f"Processing {audio_path.name}...\n")

        # Transcribe and capture result
        result = model.transcribe(str(audio_path), language="en", verbose=True)

        # Save outputs
        write_srt_file(result, srt_path)
        write_vtt_file(result, vtt_path)

        print(f"\nSaved transcript files for {episode_number}.\n")

        transcribed_count += 1


if __name__ == "__main__":
    transcribe_with_whisper(2000)
