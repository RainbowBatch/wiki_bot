import whisper

model = whisper.load_model("medium.en", device='cuda')
result = model.transcribe(
    "audio_files/infowars/20130101_Tue_Alex.mp3", verbose='True')

with open("transcripts/infowars/20130101_Tue_Alex.txt", "w") as f:
    f.write(result["text"])
