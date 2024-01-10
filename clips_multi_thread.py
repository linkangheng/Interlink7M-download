import json
import os
from moviepy.editor import VideoFileClip
import time
from concurrent.futures import ThreadPoolExecutor
import threading

# 视频文件目录前缀、输入TSV文件路径、错误日志文件路径和进度日志文件路径定义
base="Howto-Interlink7M_subset_w_all_clips_val"
video_prefix = "/data/hypertext/kangheng/howto100m/download/videos/"+base
tsv_input = "/data/hypertext/kangheng/howto100m/Interlink7M_tsv/"+base+".tsv"
error_log_path = "log/"+base+"/error_log.txt"
error_list_path = "log/"+base+"/error_list.txt"
progress_log_path = "log/"+base+"/progress_log.txt"
max_workers = 32

# 时间字符串转换为秒的函数和错误日志记录函数
def time_str_to_seconds(time_str):
    h, m, s = map(int, time_str.split(':'))
    return h * 3600 + m * 60 + s

def log_error(message):
    with open(error_log_path, 'a') as error_file:
        error_file.write(message + '\n')

def log_progress(message):
    with open(progress_log_path, 'a') as progress_file:
        progress_file.write(message + '\n')
        
def list_error(message):
    with open(error_list_path, 'a') as error_file:
        error_file.write(message + '\n')

# 用于记录处理进度的全局变量
progress_count = 0
total_videos_count = 0

# 处理单个视频的函数
def process_video(line):
    global progress_count
    video_file, clips_data, _ = line.strip().split("\t")
    video_file = video_file.split("/")[-1]
    if video_file == "video":
        return
    clips_data = clips_data.replace('""', '"')[1:-1]
    clips_json = json.loads(clips_data)
    full_video_path = os.path.join(video_prefix, video_file[:-4], video_file)
    if not os.path.exists(full_video_path):
        error_message = f"Video file not found: {full_video_path}"
        log_error(error_message)
        return

    try:
        video = VideoFileClip(full_video_path)
        video_duration = video.duration
        for index, clip_info in enumerate(clips_json):
            start_time, end_time = clip_info['clip'].split(' - ')
            start_seconds = time_str_to_seconds(start_time)
            end_seconds = time_str_to_seconds(end_time)
            if end_seconds > video_duration:
                end_seconds = video_duration
            clip = video.subclip(start_seconds, end_seconds)
            clip_filename = "clip_" + clip_info['clip_id'] + ".mp4"
            clip_directory = os.path.join(video_prefix, video_file[:-4])
            target_path = os.path.join(clip_directory, clip_filename)
            clip.write_videofile(target_path, codec="libx264", audio_codec="aac")
        video.close()

        # 使用锁确保进度计数和日志记录的原子操作
        with threading.Lock():
            progress_count += 1
            progress_message = f"Processed video {progress_count}/{total_videos_count} ({video_file})"
            log_progress(progress_message)

    except Exception as e:
        error_message = f"Error processing file {video_file}: {e}"
        log_error(error_message)
        list_error(video_file)

# 读取TSV文件并计算总视频数量
with open(tsv_input, 'r') as file:
    lines = file.readlines()
    total_videos_count = sum(1 for line in lines if not line.startswith('video'))

# 使用线程池处理视频
with ThreadPoolExecutor(max_workers) as executor:
    futures = [executor.submit(process_video, line) for line in lines if not line.startswith('video')]
    for future in concurrent.futures.as_completed(futures):
        future.result()  # 使用result()来确保异常被捕获
