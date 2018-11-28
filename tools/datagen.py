import os
import argparse
import random

parser = argparse.ArgumentParser(prog='datagen', description="generate test data")
parser.add_argument('-p', '--path', type=str, required=True)
parser.add_argument('-s', '--size', type=int, required=True)
parser.add_argument('-c', '--count', type=int, required=True)
parser.add_argument('-l', '--level', type=int, default=3)
parser.add_argument('-e', '--ext', type=str, default='.log')



_MESSAGE = [
    '"type": "login", "timestamp": 1538389229, "user": "test", id={id}\n',
    '"type": "logout", "timestamp": 1538389229, "user": "test", id={id}\n',
    '"type": "pay", "timestamp": 1538389229, "user": "test", id={id}\n',
    '"type": "press", "timestamp": 1538389229, "user": "test", id={id}\n',
]


def _create_file(dirpath, index, level, size, extension='.log'):
    global _MESSAGE

    # random path
    file_dir = dirpath
    path_level = random.randint(0, level)
    for l in range(path_level):
        p = chr(ord('a') + random.randint(0, 25))
        file_dir = os.path.join(file_dir, p)

    if not os.path.isdir(file_dir): os.makedirs(file_dir)
    filepath = os.path.join(file_dir, 'test_{}{}'.format(index, extension))


    # create
    with open(filepath, 'w') as f:
        total_size, i = 0, 0
        while True:
            if total_size >= size: break
            i += 1
            msg = _MESSAGE[random.randint(0, len(_MESSAGE))-1]
            ln = msg.format(id=i)
            total_size += len(ln)
            f.writelines(ln)
            del ln

    return filepath



def _main():
    args = parser.parse_args()
    for i in range(args.count):
        filepath = _create_file(args.path, i, args.level, args.size, args.ext)
        print('create file: {}'.format(filepath))



if __name__ == '__main__':
    _main()