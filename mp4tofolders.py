# 将文件夹中所有的mp4文件都移到同名文件夹中
import os
import shutil
from tqdm import tqdm
path="/data/hypertext/kangheng/howto100m/download/videos/Howto-Interlink7M_subset_w_sampled_clips_val/"
for file in tqdm(os.listdir(path)):
    if file.endswith(".mp4"):
        # 判断是否存在同名文件夹
        if not os.path.exists(os.path.join(path,file.split(".")[0])):
            os.mkdir(os.path.join(path,file.split(".")[0]))
        shutil.move(os.path.join(path,file),os.path.join(path,file.split(".")[0],file))
