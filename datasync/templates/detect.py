import os
import pickle
import sys

ROOT_PATH = os.path.realpath('{}')

SEARCH_FILE_EXTS = {}

result = False
if os.path.isdir(ROOT_PATH):
    for root, dirs, files in os.walk(ROOT_PATH):
        for f in files:
            ext = os.path.splitext(f)[1]
            if ext not in SEARCH_FILE_EXTS: continue
            result = True
            break
        if result: break

sys.stdout.buffer.write(pickle.dumps(result))
exit(0)
