# coding: utf-8

import requests, os
import threading
import codecs
import time
import argparse
from moviepy.editor import VideoFileClip
from tqdm import tqdm



class TaskManager(object):
    def __init__(self, video_list, num_threads=10, limit=None):
        self.video_list = video_list
        self.num_threads = num_threads
        self._limit = limit

    def create_task(self):
        tasks = []
        for video_url in codecs.open(self.video_list, "r", "utf-8"):
            video_url = video_url.strip()
            if not video_url:
                continue
            tasks.append(video_url)
        return tasks

    def create_split_tasks(self):
        tasks = self._split_tasks(self.create_task(), self.num_threads)
        if self._limit is not None:
            tasks = tasks[: self._limit]
        return tasks

    def _split_tasks(self, tasks, splits):
        total_length = len(tasks)
        num_splits = min(total_length, splits)
        split_tasks = [[] for _ in range(num_splits)]
        for i in range(total_length):
            bin_num = i % num_splits
            split_tasks[bin_num].append(tasks[i])
        return split_tasks


class Saver(object):
    def __init__(self, res_dir):
        self._res_dir = res_dir
        if not os.path.exists(self._res_dir):
            os.makedirs(self._res_dir)

    def dump(self, ret):
        video_name, video_file = ret
        with open(self.get_save_path(video_name), "wb") as writer:
            writer.write(video_file)
            try:
                video=VideoFileClip(self.get_save_path(video_name))
                video.close()
                print(f"downloaded:{video_name}")
            except:
                os.remove(self.get_save_path(video_name))
                raise Exception(f"download failed:{video_name}")

    def get_save_path(self, video_name):
        return os.path.join(self._res_dir,video_name[:-4], video_name)

    def query_exist(self, video_name):
        save_path = self.get_save_path(video_name)
        if os.path.exists(save_path):
            return True
        return False


class RequestThread(threading.Thread):
    def __init__(
        self, saver, tasklist, user_name, password, interval=0.1, *args, **kwargs
    ):
        self.saver = saver
        self.tasklist = tasklist
        self.interval = interval
        self.user_name = user_name
        self.password = password
        super(RequestThread, self).__init__(*args, **kwargs)

    def run(self):
        for video_url in self.tasklist:
            video_name = video_url.split("/")[-1]
            retries=0
            max_retries = 20
            while retries<max_retries:
                try:
                    ret = self.request(video_url)
                    self.saver.dump((video_name, ret))
                    # 写入文件
                    with open("/data/hypertext/kangheng/howto100m/download/log/Howto-Interlink7M_subset_w_sampled_clips_train/download_success.txt","a") as f:
                        f.write(video_name+"\n")
                    break
                except:
                    retries+=1
                    time.sleep(self.interval)
                    continue
            if retries==max_retries:
                with open("/data/hypertext/kangheng/howto100m/download/log/Howto-Interlink7M_subset_w_sampled_clips_train/download_failed.txt","a") as f:
                    f.write(video_name+"\n")
                continue
            

    def request(self, video_url):
        r = requests.get(video_url, auth=(self.user_name, self.password))
        return r.content\


def parse_args():
    parser = argparse.ArgumentParser()

    # Data path
    parser.add_argument(
        "--video_list_file", default="/data/hypertext/kangheng/howto100m/download/log/Howto-Interlink7M_subset_w_sampled_clips_train/download_list.txt",type=str, help="path to howto100m_videos.txt"
    )
    parser.add_argument(
        "--save_dir",
        default="/data/hypertext/kangheng/howto100m/download/videos/Howto-Interlink7M_subset_w_sampled_clips_train",
        type=str,
        help="The directory to save video files.",
    )
    parser.add_argument(
        "--num_threads", default=200, type=int, help="The number of threads to download"
    )

    # Auth info
    parser.add_argument(
        "--user_name", type=str, default="htlog23", help="User name provided by Howto100M dataset owners."
    )
    parser.add_argument(
        "--password", type=str, default="fb93dc3b1950d18", help="Password provided by Howto100M dataset owners."
    )
    return parser.parse_args()

def main():
    args = parse_args()
    saver = Saver(args.save_dir)
    tasks = TaskManager(
        args.video_list_file, num_threads=args.num_threads
    ).create_split_tasks()
    threads = [
        RequestThread(saver, subtask, args.user_name, args.password)
        for subtask in tasks
    ]
    for t in threads:
        t.start()
        # t.run()
    for t in threads:
        t.join()
    


if __name__ == "__main__":
    main()
