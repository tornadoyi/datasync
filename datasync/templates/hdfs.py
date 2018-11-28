import os
import time
import hashlib
import pickle
import sys


import pyarrow as pa

USER = '{}'

HOST = '{}'

PORT = {}

LOCAL_FILE_PATH = '{}'

HDFS_FILE_PATH = '{}'

MD5_CHECK = {}

UPLOAD_CHECK_TIMEOUT = {}

UPLOAD_CHECK_SLEEP_TIME = {}

FILE_READ_BUFFER = {}

def md5(f):
    m = hashlib.md5()
    while True:
        data = f.read(FILE_READ_BUFFER)
        if not data: break
        m.update(data)
        del data
    return m.hexdigest()


def upload(fs, local_file_path, hdfs_file_path):
    # check and create dir
    hdfs_file_dir = os.path.dirname(hdfs_file_path)
    if not fs.exists(hdfs_file_dir):
        fs.mkdir(hdfs_file_dir)

    # upload file
    with open(local_file_path, 'rb') as f:
        fs.upload(hdfs_file_path, f, FILE_READ_BUFFER)

    # wait file upload finish
    while not fs.exists(hdfs_file_path):
        time.sleep(UPLOAD_CHECK_SLEEP_TIME)


def check(fs, local_file_path, hdfs_file_path):
    # check
    if not MD5_CHECK: return

    # calculate local file md5
    with open(local_file_path, 'rb') as f:
        local_file_md5 = md5(f)

    # calculate hdfs file md5
    with fs.open(hdfs_file_path, 'rb') as f:
        hdfs_file_md5 = md5(f)

    if local_file_md5 != hdfs_file_md5:
        raise Exception('MD5 check failed, upload to hdfs fail')


def clear_upload_file(fs, hdfs_file_path):
    try:
        if not fs.exists(hdfs_file_path): return
        fs.delete(hdfs_file_path)

    except Exception as e: pass


def main():

    # connect hdfs
    fs = pa.hdfs.connect(HOST, PORT, user=USER)

    # do upload and check
    try:
       upload(fs, LOCAL_FILE_PATH, HDFS_FILE_PATH)
       check(fs, LOCAL_FILE_PATH, HDFS_FILE_PATH)

    except Exception as e:
       clear_upload_file(fs, HDFS_FILE_PATH)
       raise e

    finally:
       fs.close()


if __name__ == '__main__':
    main()
    sys.stdout.buffer.write(pickle.dumps(True))
    exit(0)