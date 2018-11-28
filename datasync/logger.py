
import os
import logging
from logging import FileHandler


logger = logging.getLogger('datasync')


_title_str_count = 100

def _title(msg, *args, **kwargs):
    sep_count = int((_title_str_count - len(msg) - 2) / 2)
    log =  '='*sep_count + ' ' +  msg + ' ' + '='*sep_count
    logger.info('')
    logger.info(log, *args, **kwargs)
    logger.info('')

logger.title = _title



def initialize(log_file_path):
    global _logger
    # create log path
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    # base config
    log_fmt = '%(levelname)s %(asctime)s %(filename)s[line:%(lineno)d]: %(message)s'
    date_fmt = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_fmt, date_fmt)
    logger.setLevel(logging.INFO)

    # set log file name
    fh = FileHandler(log_file_path)
    fh.setFormatter(formatter)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)



