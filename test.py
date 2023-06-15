import speech_recognition as sr
from pydub import AudioSegment

# Specify the paths
audio_path = "podcast_summary/should-the-fed-raise-its-2-inflation-target.wav"
converted_audio_path = "podcast_summary/converted_audio.wav"

# Convert audio to WAV format
audio = AudioSegment.from_file(audio_path)
audio.export(converted_audio_path, format="wav")

# Initialize the recognizer
r = sr.Recognizer()

# Load the converted audio file
with sr.AudioFile(converted_audio_path) as source:
    audio_data = r.record(source)

    # Perform speech recognition
    text = r.recognize_google(audio_data)

# Print the extracted text
print("Extracted Text: ", text)
