import os
import subprocess
from pydub import AudioSegment
import ray
import time

@ray.remote
def process_file(input_folder: str, output_dir: str, mp3_file: str):
    input_file = os.path.join(input_folder, mp3_file)
    wav_file = os.path.join(output_dir, os.path.splitext(mp3_file)[0] + ".wav")
    audio = AudioSegment.from_mp3(input_file)
    audio.export(wav_file, format="wav")
    enhanced_file = os.path.join(output_dir, os.path.splitext(mp3_file)[0] + "_enhanced.wav")
    try:
        subprocess.run(
            ["resemble-enhance", wav_file, enhanced_file],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return f"Enhanced file saved at: {enhanced_file}"
    except subprocess.CalledProcessError as e:
        return f"Error during enhancement for {mp3_file}: {e.stderr.decode()}"

def enhance_audio():
    input_folder = input("Enter the path to the folder containing MP3 files: ").strip()
    output_dir = input("Enter the path to save enhanced files (or press Enter to use 'example-enhance'): ").strip()
    if not output_dir:
        output_dir = "example-enhance"
    if not os.path.isdir(input_folder):
        raise ValueError("The input path must be a directory.")
    os.makedirs(output_dir, exist_ok=True)
    mp3_files = [f for f in os.listdir(input_folder) if f.lower().endswith(".mp3")]
    if not mp3_files:
        print("No MP3 files found in the directory.")
        return
    ray.init()
    start_time = time.time()
    tasks = [process_file.remote(input_folder, output_dir, mp3_file) for mp3_file in mp3_files]
    results = ray.get(tasks)
    for result in results:
        print(result)
    end_time = time.time()
    ray.shutdown()
    print(f"Total processing time: {end_time - start_time:.2f} seconds")
