import sys
import os
import pickle

BASE_FILE_PATH_LIST = {}

SEARCH_FILE_EXTS = {}

deleted_files = []
for f in BASE_FILE_PATH_LIST:
    for ext in SEARCH_FILE_EXTS:
        file_path = f + ext
        if not os.path.isfile(file_path): continue
        os.remove(file_path)
        deleted_files.append(file_path)


sys.stdout.buffer.write(pickle.dumps(deleted_files))
exit(0)