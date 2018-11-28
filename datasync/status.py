from __future__ import absolute_import, division, print_function

import os
import pickle
from pyplus.collection import qdict
from datasync.statistics import statistics


_STATUS_DEFINES = ['collect', 'pull', 'push', 'clean', 'finish']
Status = qdict({_STATUS_DEFINES[i]: qdict(id = i, name = _STATUS_DEFINES[i])  for i in range(len(_STATUS_DEFINES))})



class StatusManager(object):
    def __init__(self, config):
        self.__config = config
        self.__status = {'current': Status.collect.name}
        self.__status_results = {}

        # reload
        self.__reload()


    @property
    def current(self): return Status[self.__status.get('current')]

    @property
    def finish(self): return self.current == Status.finish

    @property
    def beginning(self): return self.current == Status.collect


    def get_pre_status_result(self, default=None):
        pre_status = Status[_STATUS_DEFINES[self.current.id - 1]]
        return self.__status_results.get(pre_status.name, default)


    def restart(self):
        self.__status['current'] = Status.collect.name
        self.__status_results = {}

        # record enter status
        statistics.enter_status(self.current.name)


    def next_status(self, args, persist):
        if self.current == Status.finish: return

        # save current status result
        self.__status_results[self.current.name] = args
        if persist: self.__save_meta_file('{}.meta'.format(self.current.name), args)

        # record exit status
        statistics.exit_status()

        # next status
        next_status = Status[_STATUS_DEFINES[self.current.id+1]]

        # save
        self.__status['current'] = next_status.name
        self.__save_meta_file('status.meta', self.__status)

        # record enter status
        statistics.enter_status(self.current.name)




    def __reload(self):
        # load status.meta
        self.__status = self.__load_meta_file('status.meta', self.__status)

        # load status results
        for n in _STATUS_DEFINES:
            s = Status[n]
            if s.id >= self.current.id: break
            self.__status_results[n] = self.__load_meta_file('{}.meta'.format(n))




    def __load_meta_file(self, file_name, default=None):
        file_path = self.__config.meta_file_path(file_name)
        if not os.path.isfile(file_path): return default
        with open(file_path, 'rb') as f:
            return pickle.load(f)



    def __save_meta_file(self, file_name, content):
        file_path = self.__config.meta_file_path(file_name)
        file_dir = os.path.dirname(file_path)
        os.makedirs(file_dir, exist_ok=True)
        with open(file_path, 'wb') as f:
            pickle.dump(content, f)