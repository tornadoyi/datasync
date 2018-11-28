from __future__ import absolute_import, division, print_function

import os
import shutil
import multiprocessing
import asyncio

from datasync.status import Status, StatusManager
from datasync import tool


class SyncMaster(object):
    def __init__(self, config):
        # load config and set path
        self.__config = config

        # create status manager
        self.__status = StatusManager(self.__config)

        # init processing pool
        self.__pool = multiprocessing.Pool()

        # processors
        self.__status_processes = {
            Status.collect.name: self.__on_collect,
            Status.pull.name: self.__on_pull,
            Status.push.name: self.__on_push,
            Status.clean.name: self.__on_clean,
            Status.finish.name: self.__on_finish,
        }


    def __call__(self, *args, **kwargs):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__async_loop())
        loop.close()


    async def __async_loop(self):
        # check status for recovery mode
        if not self.__status.finish and not self.__status.beginning:
            while not self.__status.finish:
                await self.__run_on_status()

        # new task
        self.__status.restart()
        while not self.__status.finish:
            await self.__run_on_status()


    async def __run_on_status(self):
        cur_status = self.__status.current
        process = self.__status_processes.get(cur_status.name, None)
        if process is None: return Exception('can not process status {}'.format(cur_status.name))

        # enter next stage
        result, persist = await process()
        self.__status.next_status(result, persist)



    async def __on_collect(self):
        # detect
        signals = await tool.detect_data_files(self.__config.remote_path_list, self.__config.search_file_extensions)
        valid_remote_path_list =  [self.__config.remote_path_list[i] for i in range(len(signals)) if signals[i] == True]

        # collect
        compressor = self.__config.compressor
        results = await tool.collect_data_files(valid_remote_path_list, self.__config.search_file_extensions, compressor.name, compressor.args)

        # return available remote id list
        return [i for i in range(len(results)) if not isinstance(results[i], BaseException)], True



    async def __on_pull(self):
        # clear download path
        if os.path.isdir(self.__config.download_path): shutil.rmtree(self.__config.download_path)

        # get available remote id list
        available_remote_indexes = self.__status.get_pre_status_result([])

        # fetch files
        remotes, locals, extensions = self.__config.generate_download_params(available_remote_indexes)
        await tool.download_data_files(remotes, locals, extensions)

        # no results
        return None, False



    async def __on_push(self):
        # user, host, port, local_file_path, hdfs_file_path
        # scan all download files
        sync_files = []
        for root, dirs, files in os.walk(self.__config.download_path):
            for f in files:
                if os.path.splitext(f)[-1] != self.__config.compress_extension: continue
                local_file_path = os.path.join(root, f)
                sync_file_path = self.__config.convert_download_path_to_sync_path(local_file_path)
                sync_files.append((local_file_path, sync_file_path))


        # sync
        s = self.__config.sync_info
        results = await tool.sync_datas(s.user, s.host, s.port, sync_files,
                                        s.check, s.check_timeout, s.check_sleep_time, s.read_buffer_size)

        # record files which are synced successfully
        return [sync_files[i][0] for i in range(len(results)) if not isinstance(results[i], BaseException) ], True



    async def __on_clean(self):
        # check level
        clean_level = self.__config.clean_level
        if clean_level == 0: return None, False
        org_exts = self.__config.search_file_extensions
        zip_exts = [e + self.__config.compress_extension for e in self.__config.search_file_extensions]
        file_extensions = []
        if (clean_level & 1) == 1: file_extensions += org_exts
        if ((clean_level >> 1) & 1) == 1: file_extensions += zip_exts


        # get push list
        push_list = self.__status.get_pre_status_result([])

        clear_dict = {}
        for p in push_list:
            p = self.__convert_download_path_to_base_path(p)
            user, ip, path = self.__config.convert_download_path_to_remote_path(p)
            l = clear_dict.get((user, ip), None)
            if l is None:
                l = [path]
                clear_dict[(user, ip)] = l
            else:
                l.append(path)

        hosts, base_file_paths = [], []
        for h, f in clear_dict.items():
            hosts.append(h)
            base_file_paths.append(f)

        await tool.clean_remote_data_files(hosts, base_file_paths, file_extensions)

        return None, False



    async def __on_finish(self):
        pass




    def __convert_download_path_to_base_path(self, path):
        # clear compress extension
        path, ext = os.path.splitext(path)
        assert ext == self.__config.compress_extension

        # clear search extension
        path, ext = os.path.splitext(path)
        assert ext in self.__config.search_file_extensions

        return path










