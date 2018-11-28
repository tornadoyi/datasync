from __future__ import absolute_import, division, print_function

from pyplus.async import shell

MAX_PARALLEL_SHELL = None

async def _run_all(cmds, **kwargs):

    max_parallel = len(cmds) if MAX_PARALLEL_SHELL is None else MAX_PARALLEL_SHELL

    cur_index, results = 0, []
    while cur_index < len(cmds):
        dst_index = min(len(cmds), cur_index+max_parallel)
        batch_cmds = cmds[cur_index : dst_index]
        results += await shell.run_all(batch_cmds, **kwargs)
        cur_index = dst_index

    return results



def run_python_all(codes, **kwargs):
    '''
    :param codes: python code list
    :return: coroutine
    '''
    cmds = ['python -c \"{}\"'.format(c) for c in codes]
    return _run_all(cmds, **kwargs)


def ssh_run_all(host_cmd_args_list, **kwargs):
    '''
    :param host_cmd_args_list: A list with item which is a tuple with format(host, cmd, ssh args)
    :param kwargs: same with _run_all
    :return: coroutine
    '''

    def _ssh_run_cmd(host, cmd, args=''):
        nocheck = '-o StrictHostKeyChecking=no'
        ssh_cmd = 'ssh {} {} {} \"{}\"'.format(args, nocheck, host, cmd)
        return ssh_cmd

    cmds = [_ssh_run_cmd(it[0], it[1], '' if len(it) < 3 else it[2]) for it in host_cmd_args_list]
    return _run_all(cmds, **kwargs)


def ssh_python_run_all(host_code_args, **kwargs):
    '''
    :param host_code_args: A list with item which is a tuple with format(host, python code, ssh args)
    :param kwargs: same with _run_all
    :return: coroutine
    '''
    host_cmd_args_list = []
    for it in host_code_args:
        it = list(it)
        it[1] = 'python -c \\"{}\\"'.format(it[1])
        host_cmd_args_list.append(it)
    return ssh_run_all(host_cmd_args_list, **kwargs)



def rsync_all(hosts_args_list, **kwargs):
    '''
    :param hosts_arggs_list: A list with item which is a tuple with format(src_host, dst_host, async args)
    :param kwargs: same with _run_all
    :return: coroutine
    '''

    def _rsync_cmd(src_host, dst_host, args=''):
        rsync_cmd = 'rsync {} {} {}'.format(args, src_host, dst_host)
        return rsync_cmd

    cmds = [_rsync_cmd(it[0], it[1], '' if len(it) < 3 else it[2]) for it in hosts_args_list]
    return _run_all(cmds, **kwargs)






