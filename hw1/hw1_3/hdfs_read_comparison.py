import sys, os, time

from argparse import ArgumentParser
from hdfs import InsecureClient



HDFS_URL = "http://localhost:50070"
HDFS_USERNAME = "jack"
HDFS_DIR = "non_cancer_subset00"



def read_by_bulk(index_fp):
    with open(index_fp) as f:
        file_index_list = f.readline().split(",")
        filename_list = f.readline().split(",")
        file_index_list[-1] = file_index_list[-1].replace("\n", "")
        file_index_list = [int(item) for item in file_index_list]

    client = InsecureClient(HDFS_URL, user=HDFS_USERNAME)
    with client.read(hdfs_path='./bulk_img.tiff') as reader:
        bulk = reader.read()


    images = []
    for i in range(len(file_index_list) - 1):
        img = bulk[file_index_list[i]:file_index_list[i+1]]
        images.append(images)
    img = bulk[file_index_list[-1]:]
    images.append(img)

    print('Total {} images'.format(len(images)))

    return images


def read_by_small():
    client = InsecureClient(HDFS_URL, user=HDFS_USERNAME)
    files_list = client.list(HDFS_DIR)
    images = []

    for fn in files_list:
        with client.read(hdfs_path=os.path.join(HDFS_DIR, fn)) as reader:
            img = reader.read()
            images.append(img)
    
    print(len(img))





if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-m", action="store", required=True, choices=["bulk", "small"])
    parser.add_argument("-i", action="store", help="index file path")

    mode = parser.parse_args().m
    index_fp = parser.parse_args().i

    if mode == "bulk":
        s = time.time()
        read_by_bulk(index_fp)
        e = time.time()
        print("Reading images in bulk file takes {} s".format(e - s))

    elif mode == "small":
        s = time.time()
        read_by_small()
        e = time.time()
        print("Reading images in small files takes {} s".format(e - s))