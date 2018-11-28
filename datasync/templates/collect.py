import asyncio
import os
import sys
import pickle
from pyplus.async import shell
from pyplus.collection import qdict


ROOT_PATH = os.path.realpath('{}')

SEARCH_FILE_EXTS = {}

COMPRESSOR = '{}'

COMPRESSOR_ARGS = '{}'

FILE_PREFIX = '{}'


def _bzip2(file_path, args): return 'bzip2 ' + args + ' ' + file_path

def _cp(file_path, args): return 'cp ' + args + ' ' +  file_path + ' ' + file_path + '.copy'

def _pbzip2(file_path, args): return 'pbzip2 ' + args + ' ' + file_path

SUPPORTED_COMPRESSOR = qdict(
    cp = qdict(extension='.copy', cmd_func=_cp),
    bzip2 = qdict(extension = '.bz2', cmd_func=_bzip2),
    pbzip2 = qdict(extension = '.bz2', cmd_func=_pbzip2)
)

async def collect(file_path):
    # get compressor
    compressor = SUPPORTED_COMPRESSOR[COMPRESSOR]

    # clean previous compressed file
    dst_file_path = file_path + compressor.extension
    if os.path.isfile(dst_file_path): os.remove(dst_file_path)

    # get command and run
    cmd = compressor.cmd_func(file_path, COMPRESSOR_ARGS)
    r = await shell.run(cmd)
    if isinstance(r, BaseException): return r

    # rename
    if len(FILE_PREFIX) > 0:
        pre_dist_file_path = dst_file_path
        dirname = os.path.dirname(pre_dist_file_path)
        basename = os.path.basename(pre_dist_file_path)
        dst_file_path = os.path.join(dirname, FILE_PREFIX+basename)
        os.rename(pre_dist_file_path, dst_file_path)

    return (file_path, dst_file_path)


async def main():
    # detect target files
    target_files = []
    for root, dirs, files in os.walk(ROOT_PATH):
        for f in files:
            fn, ext = os.path.splitext(f)
            if ext not in SEARCH_FILE_EXTS: continue
            target_files.append(os.path.join(root, f))


    # run command
    if len(target_files) == 0: return
    dones, _ = await asyncio.wait([collect(f) for f in target_files])
    return [d.result() for d in dones if not isinstance(d.result(), BaseException)]


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    collects = loop.run_until_complete(main())
    loop.close()
    sys.stdout.buffer.write(pickle.dumps(collects))
    exit(0)

