#!/bin/bash

echo "监控开始"
dir_path="/data/hypertext/kangheng/howto100m/download/videos/Howto-Interlink7M_subset_w_sampled_clips_train"  # 需要你指明监控的目录
prev_count=$(ls -l $dir_path | wc -l)  # 初始化文件数量

while true; do
  current_count=$(ls -l $dir_path | wc -l)  # 获取当前的文件数量
  new_files=$(($current_count-$prev_count))  # 计算新增文件数量
  clear
  echo "Total files: $current_count. New files per second: $new_files."
  prev_count=$current_count  # 更新文件数量，供下一次循环使用
  sleep 1  # 每秒刷新一次
done

