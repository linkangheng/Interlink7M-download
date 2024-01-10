import os 

for i in os.listdir("/data/hypertext/kangheng/howto100m/Interlink7M_tsv"):
    if i.endswith(".tsv"):
        print(i)
        input_file="/data/hypertext/kangheng/howto100m/Interlink7M_tsv/"+i
        output_file="data/"+i[:-4]+".txt"
        # 将tsv文件的第一列写入到txt文件中
        with open(input_file, 'r') as f:
            with open(output_file, 'w') as f1:
                for line in f.readlines():
                    line = line.strip().split('\t')
                    try:
                        context = line[0].split('/')[-1]
                        if context=="video":
                            continue
                        f1.write("http://howto100m.inria.fr/dataset/"+context + '\n')
                    except:
                        print(line)