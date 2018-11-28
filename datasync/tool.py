from __future__ import absolute_import, division, print_function

import os
import time
import pickle

from pyplus import importer

from datasync import shell
from datasync import templates
from datasync.statistics import statistics


async def detect_data_files(remote_paths, file_extensions):
    # load template
    with open(templates.path('detect.py'), 'r') as f:
        template = f.read()


    # generate ssh script
    host_code_args = []
    for (user, ip, path) in remote_paths:
        code = template.format(path, str(file_extensions))
        host_code_args.append(('{}@{}'.format(user, ip), code))

    # run commands
    results = await shell.ssh_python_run_all(host_code_args, output='pickle')

    # clear
    del host_code_args, template
    statistics.on_detect_data_files(remote_paths, results)
    return results


async def collect_data_files(remote_paths, file_extensions, compressor, compress_args):
    # load template
    with open(templates.path('collect.py'), 'r') as f:
        template = f.read()

    host_code_args = []
    for (user, ip, path) in remote_paths:
        code = template.format(path, str(file_extensions), compressor, compress_args, '')
        host_code_args.append(('{}@{}'.format(user, ip), code))

    # run commands
    results = await shell.ssh_python_run_all(host_code_args, output='pickle')
    statistics.on_collect_data_files(remote_paths, results)
    return results




async def download_data_files(remote_paths, download_paths, file_extensions):

    assert len(remote_paths) == len(download_paths)

    # set args
    arg = '-rz '
    for ext in file_extensions: arg += '--include="*{}" '.format(ext)
    arg += '--exclude="*.*" '

    # generate rsync commands
    hosts_args_list = []
    for i in range(len(remote_paths)):
        # add source host
        user, ip, remote_path = remote_paths[i]
        local_path = download_paths[i]

        # create local path
        os.makedirs(local_path, exist_ok=True)

        # check loacal dir and add destination host
        hosts_args_list.append(('{}@{}:{}'.format(user, ip, remote_path), local_path, arg))

    results = await shell.rsync_all(hosts_args_list)
    statistics.on_download_data_files(remote_paths, results)
    return results




async def sync_datas(user, host, port, file_pairs,
                     check, check_timeout, check_sleep_time, file_read_buffer):
    # load template
    with open(templates.path('hdfs.py'), 'r') as f:
        template = f.read()

    codes = []
    for fp in file_pairs:
        code = template.format(user, host, port, *fp, check, check_timeout, check_sleep_time, file_read_buffer)
        codes.append(code)

    results =  await shell.run_python_all(codes, output='pickle')
    statistics.on_sync_datas(file_pairs, results)
    return results




async def clean_remote_data_files(hosts, base_file_paths, file_extensions):
    assert len(hosts) == len(base_file_paths)

    # load template
    with open(templates.path('clean.py'), 'r') as f:
        template = f.read()

    host_code_args = []
    for i in range(len(hosts)):
        user, ip = hosts[i]
        base_files = base_file_paths[i]
        code = template.format(str(base_files), str(file_extensions))
        host_code_args.append(('{}@{}'.format(user, ip), code))

    # run commands
    results = await shell.ssh_python_run_all(host_code_args, output='pickle')
    return statistics.on_clean_remote_data_files(hosts, results)










def _run_process(cworker, *args):
    worker_class = cworker
    if isinstance(cworker, str): worker_class = importer.import_module(cworker)
    worker = worker_class(*args)
    return worker()

def run_batch_task(pool, params_list, timeout=None, max_parallel=None):

    parallels = len(params_list) if max_parallel is None else max_parallel
    async_results = []

    def process_callback(*args, **kwargs):
        if len(async_results) >= len(params_list): return
        r = pool.apply_async(_run_process, params_list[i], callback=process_callback)
        async_results.append(r)


    # start task async
    for i in range(parallels): process_callback()


    # get results
    end_time = time.time() + timeout if timeout is not None else None
    results = []
    for i in range(len(async_results)):
        left_time = max(end_time - time.time(), 0) if timeout is not None else None
        r = async_results[i].get(left_time)
        results.append(r)

    return results



