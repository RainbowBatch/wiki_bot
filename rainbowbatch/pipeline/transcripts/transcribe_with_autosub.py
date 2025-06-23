import autosub
import rainbowbatch.kfio as kfio

TRANSCRIPT_DIR = kfio.TOP_LEVEL_DIR / 'transcripts'
AUDIO_FILES_DIR = kfio.TOP_LEVEL_DIR / 'audio_files'


def copy_to_transcript_dir():
    for srt_path in AUDIO_FILES_DIR.glob('*.srt'):
        episode_number = srt_path.stem
        new_path = TRANSCRIPT_DIR / f'{episode_number}.autosub.srt'
        with srt_path.open('r') as old_file, new_path.open('w') as new_file:
            new_file.write(old_file.read())


def transcribe_with_autosub(max_transcriptions=2):
    transcribed_count = 0

    copy_to_transcript_dir()

    for audio_path in AUDIO_FILES_DIR.glob('*.mp3'):
        if transcribed_count >= max_transcriptions:
            print(f"Reached max_transcriptions limit: {max_transcriptions}")
            break

        episode_number = audio_path.stem

        already_exists = (
            (AUDIO_FILES_DIR / f'{episode_number}.srt').exists() or
            (TRANSCRIPT_DIR / f'{episode_number}.autosub.srt').exists()
        )
        if already_exists:
            print("Skipping", audio_path.name)
            continue

        print("Processing", audio_path.name)

        autosub.generate_subtitles(
            source_path=str(audio_path),
            concurrency=2,
            src_language='en',
            dst_language='en',
            api_key=None,
            subtitle_file_format='srt',
            output=None,
        )

        copy_to_transcript_dir()

        transcribed_count += 1


if __name__ == '__main__':
    transcribe_with_autosub(0)
