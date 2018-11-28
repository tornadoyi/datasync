from __future__ import absolute_import, division, print_function

import os
import sys
import argparse
from functools import partial
from datasync.master import SyncMaster
from datasync.config import Config
from datasync import logger
from datasync.statistics import statistics
from pyplus.core import SingletonApplication


def load_config():

    def _parse_command(cmd, args): return (cmd, args)

    # main parser
    parser = argparse.ArgumentParser(prog='datasync', description="A tool dedicated for syncing batch data to hdfs asynchronously")
    sparser = parser.add_subparsers()

    # config
    config = sparser.add_parser('config', help='Check and export default config')
    config.set_defaults(func=partial(_parse_command, 'config'))
    config.add_argument('-p', '--print', action='store_true', default=False, help='Print default config')
    config.add_argument('-s', '--save', type=str, help='Save default config file to specific path')

    # start
    start = sparser.add_parser('start', help='Start httpstorm')
    start.set_defaults(func=partial(_parse_command, 'start'))
    start.add_argument('-c', '--config', type=str, help='Config module path')
    start.add_argument('-s', '--singleton', action='store_true', default=False, help='Start with singleton')

    args = parser.parse_args()
    if getattr(args, 'func', None) is None:
        parser.print_help()
        sys.exit(0)

    return args.func(args)


def cmd_config(args):
    # load default config file
    import datasync.default_config as dc
    with open(dc.__file__) as f:
        content = f.read()

    # print
    if args.print:
        print(content)

    # save
    if args.save is not None:
        save_path = os.path.join(os.path.realpath(args.save), 'config.py')
        with open(save_path, mode='w') as f:
            f.write(content)


def cmd_start(args):
    # check singleton
    if args.singleton:
        app = SingletonApplication('datasync')
        if app.initialized: raise Exception('datasync must be singleton, previous pid: {}'.format(app.pid))

    # load config
    config = Config(args.config)

    # init logger
    logger.initialize(config.log_file_path)

    # start statistic
    statistics.start(config)

    # run
    master = SyncMaster(config)
    master()

    # end
    statistics.end()

    



def main():
    cmd, args = load_config()

    if cmd == 'config':
        cmd_config(args)

    elif cmd == 'start':
        cmd_start(args)

    else: raise Exception('Invalid command {0}'.format(cmd))


if __name__ == '__main__':
    main()