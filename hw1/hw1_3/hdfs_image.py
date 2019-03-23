import sys, os, time

from argparse import ArgumentParser
from hdfs import InsecureClient



def save_img(img_dir):
    BULK_IMG_FN = "bulk_img.tiff"
    INDEX_FN = "index.txt"
    filename_list = []
    file_index_list = []
    file_index_count = 0
    bulk_img = b'';
    img_list = sorted(os.listdir(img_dir))

    if BULK_IMG_FN in img_list: img_list.remove(BULK_IMG_FN)
    if INDEX_FN in img_list: img_list.remove(INDEX_FN)

    # Compress images into one bulk image file
    output_file = open(os.path.join(img_dir, 'bulk_img.tiff'), 'wb')

    for fn in img_list:
        filename_list.append(fn)
        with open(os.path.join(img_dir, fn), 'rb') as f:
            img = f.read()
            output_file.write(img)
            file_index_list.append(str(file_index_count))
            file_index_count += len(img)

    output_file.close()

    # Build up index file
    with open(os.path.join(img_dir, 'index.txt'), 'w') as f:
        f.write(','.join(file_index_list))
        f.write('\n')
        f.write(','.join(filename_list))

    # Upload the bulk image up to HDFS
    s_upload = time.time()
    client = InsecureClient('http://localhost:50070', user='jack')
    client.upload('./bulk_img.tiff', os.path.join(img_dir, 'bulk_img.tiff'), overwrite=True)
    e_uplaod = time.time()
    print("Uploading images takes {} s".format(e_uplaod - s_upload))


def read_img(img_dir, img_fn):
    with open(os.path.join(img_dir, 'index.txt')) as f:
        file_index_list = f.readline().split(",")
        filename_list = f.readline().split(",")
        file_index_list[-1] = file_index_list[-1].replace("\n", "")

    try:
        start_index = int(file_index_list[filename_list.index(img_fn)])
        end_start = int(file_index_list[filename_list.index(img_fn) + 1])
        with open(os.path.join(img_dir, 'bulk_img.tiff'), 'rb') as f:
            img = f.read()[start_index: end_start]
        
        return img
        
    except Exception as e:
        print("No such a file name {}".format(img_fn))



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-m", action="store", required=True, choices=["s", "r"])
    parser.add_argument("-d", action="store", type=str, required=True)
    parser.add_argument("-f", action="store", type=str)

    mode = parser.parse_args().m
    img_dir = parser.parse_args().d
    img_fn = parser.parse_args().f

    if mode == "s":
        s = time.time()
        save_img(img_dir)
        e = time.time()
        print("Saving images takes {} s".format(e - s))

    elif mode == "r":
        read_img(img_dir, img_fn)

    e = time.time()
