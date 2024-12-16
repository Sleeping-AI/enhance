import os
import subprocess
from pydub import AudioSegment
import ray
import time

@ray.remote
def process_file(input_file: str, output_dir: str, subdir_name: str, file_name: str):
    wav_file = os.path.join(output_dir, subdir_name, os.path.splitext(file_name)[0] + ".wav")
    audio = AudioSegment.from_mp3(input_file) if file_name.lower().endswith(".mp3") else AudioSegment.from_wav(input_file)
    audio.export(wav_file, format="wav")
    enhanced_file = os.path.splitext(wav_file)[0] + "_enhanced.wav"
    try:
        subprocess.run(
            ["resemble-enhance", wav_file, enhanced_file],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return f"Enhanced file saved at: {enhanced_file}"
    except subprocess.CalledProcessError as e:
        return f"Error during enhancement for {file_name}: {e.stderr.decode()}"
    except Exception as e:
        return f"Error processing {file_name}: {str(e)}"

def enhance_audio():
    input_folder = "/vocal/vocal-exp/fin_vocal_burst/"
    output_dir = "enhanced-audio"
    os.makedirs(output_dir, exist_ok=True)
    
    subdirs = [d for d in os.listdir(input_folder) if os.path.isdir(os.path.join(input_folder, d))]
    
    if not subdirs:
        print("No subdirectories found in the directory.")
        return
    
    ray.init(num_cpus=96)

    start_time = time.time()

    for subdir_name in subdirs:
        subdir_path = os.path.join(input_folder, subdir_name)
        output_subdir = os.path.join(output_dir, subdir_name)
        os.makedirs(output_subdir, exist_ok=True)
        
        audio_files = [f for f in os.listdir(subdir_path) if f.lower().endswith((".mp3", ".wav"))]
        
        if not audio_files:
            print(f"No audio files found in {subdir_name}.")
            continue
        
        tasks = [process_file.remote(os.path.join(subdir_path, file_name), output_dir, subdir_name, file_name) for file_name in audio_files]
        
        results = ray.get(tasks)

        for result in results:
            print(result)
    
    end_time = time.time()
    ray.shutdown()
    print(f"Total processing time: {end_time - start_time:.2f} seconds")
