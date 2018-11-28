
import time

from pyplus import singleton

from datasync.logger import logger
from datasync.tree import Tree

_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

@singleton
class Statistics(object):
    def __init__(self):
        self.__current_status = None
        self.__summary = Tree(privates=['__'])
        self.__summary['total_time'] = None
        self.__summary['total_errors'] = 0
        self.__summary['status'] = Tree(privates=['__'])


    def start(self, config):
        # print config
        logger.info('Start Config\n' + str(config.config_dict))

        # record start time
        self.__summary['__start_time'] = time.time()


    def end(self):
        d = self.__summary
        d['total_time'] = d['total_time'] = '%.2f' % (time.time() - d['__start_time'])
        logger.info('Summary\n' + str(self.__summary))


    def enter_status(self, status):
        self.__current_status = status
        logger.title('enter [{}]'.format(self.__current_status))

        # summary
        d = self.__summary['status'][self.__current_status] = Tree(privates=['__'])
        d['__start_time'] = time.time()



    def exit_status(self):
        if self.__current_status is None: return
        logger.title('exit [{}]'.format(self.__current_status))

        # summary
        d = self.__summary['status'][self.__current_status]
        d['total_time'] = '%.2f' % (time.time() - d['__start_time'])



    def on_detect_data_files(self, remote_paths, results):
        assert len(remote_paths) == len(results)

        # record exceptions and logs
        logs = []
        for i in range(len(results)):
            user, ip, rp = remote_paths[i]
            r = results[i]
            if isinstance(r, BaseException):
                logger.exception(r)
            else:
                if r: logs.append('detecting available signal from: {}:{}'.format(ip, rp))

        # print logs
        for l in logs: logger.info(l)

        # summary results
        self.__summary_task_results(results)



    def on_collect_data_files(self, remote_paths, results):
        assert len(remote_paths) == len(results)

        # record exceptions and logs
        logs = []
        for i in range(len(results)):
            user, ip, rp = remote_paths[i]
            r = results[i]
            if isinstance(r, BaseException):
                logger.exception(r)
            else:
                for s, d in r: logs.append('process on {}: {} ==> {}'.format(ip, s, d))

        # print logs
        for l in logs: logger.info(l)

        # summary results
        self.__summary_task_results(results)


    def on_download_data_files(self, remote_paths, results):
        assert len(remote_paths) == len(results)

        # record exceptions and logs
        logs = []
        for i in range(len(results)):
            user, ip, rp = remote_paths[i]
            r = results[i]
            tag = 'ok'
            if isinstance(r, BaseException):
                logger.exception(r)
                tag = 'fail'
            logs.append('download datas from {}:{} ...... {}'.format(ip, rp, tag))

        # print logs
        for l in logs: logger.info(l)

        # summary results
        self.__summary_task_results(results)



    def on_sync_datas(self, file_pairs, results):
        # record exceptions and logs
        logs = []
        for i in range(len(results)):
            sp, dp = file_pairs[i]
            r = results[i]
            tag = 'ok'
            if isinstance(r, BaseException):
                logger.exception(r)
                tag = 'fail'
            logs.append('sync {} ==> {} ...... {}'.format(sp, dp, tag))

        # print logs
        for l in logs: logger.info(l)

        # summary results
        self.__summary_task_results(results)



    def on_clean_remote_data_files(self, hosts, results):
        assert len(hosts) == len(results)

        # record exceptions and logs
        logs = []
        for i in range(len(results)):
            user, ip = hosts[i]
            r = results[i]
            if isinstance(r, BaseException):
                logger.exception(r)
            else:
                for f in r: logs.append('delete {}:{}'.format(ip, f))

        # print logs
        for l in logs: logger.info(l)

        # summary results
        self.__summary_task_results(results)



    def __summary_task_results(self, results):

        # statistic errore
        errors = 0
        for r in results:
            if not isinstance(r, BaseException): continue
            errors += 1

        d = self.__summary['status'][self.__current_status]
        if 'tasks' not in d: d['tasks'] = 0
        if 'errors' not in d: d['errors'] = 0
        d['tasks'] += len(results)
        d['errors'] += errors

        #
        self.__summary['total_errors'] += errors






statistics = Statistics()