
import os
from pyplus import importer
from pyplus.collection import qdict

from datasync import shell
from datasync.tree import Tree
from datasync.templates import collect


class Config(object):
    def __init__(self, config_path):
        cfg =  self.__load_config_file(config_path)

        # local path
        self.__config_path = config_path
        self.__config_dict = cfg
        self.__download_path = cfg['DOWNLOAD_DIRECTORY']
        self.__meta_path = cfg['META_DIRECTORY']
        self.__log_file_path = cfg['LOG_FILE_PATH']


        # remote paths
        self.__remote_path_list = []
        self.__remote_infos = qdict()
        for r in cfg['REMOTE_PATH_LIST']:
            host, path = r.split(':')
            user, ip = host.split('@')
            if not os.path.isabs(path): raise Exception('remote path must be a absolute path')

            # save
            index = len(self.__remote_path_list)
            self.__remote_infos[index] = qdict(user=user, ip=ip, path=path)
            self.__remote_path_list.append((user, ip, os.path.realpath(path)))


        # sync server and info
        host, port = cfg['SYNC_SERVER'].split(':')
        user, ip = host.split('@')
        self.__sync_info = qdict(
            user=user, host=host, port=int(port), root_path=cfg['SYNC_ROOT_PATH'],
            check = cfg['SYNC_CHECK'], check_timeout = cfg['SYNC_CHECK_TIMEOUT'],
            check_sleep_time=cfg['SYNC_CHECK_SLEEP_TIME'],
            read_buffer_size = cfg['SYNC_READ_BUFFER_SIZE'],
        )

        # search file
        self.__search_file_extensions = cfg['SEARCH_FILE_EXTENSIONS']


        # compressor
        compressor = cfg['COMPRESSOR']
        if compressor not in collect.SUPPORTED_COMPRESSOR: raise Exception('Unsupported compressor {}'.format(compressor))
        self.__compressor = qdict(name=compressor, args=cfg['COMPRESSOR_ARGS'])

        # clean
        self.__clean_level = cfg['CLEAN_LEVEL']

        # shell
        self.__max_parallel_shell = cfg['MAX_PARALLEL_SHELL']
        shell.MAX_PARALLEL_SHELL = self.__max_parallel_shell



    @property
    def config_path(self): return self.__config_path

    @property
    def config_dict(self): return self.__config_dict

    @property
    def download_path(self): return self.__download_path

    @property
    def meta_path(self): return self.__meta_path

    @property
    def log_file_path(self): return self.__log_file_path

    @property
    def remote_path_list(self): return self.__remote_path_list

    @property
    def sync_info(self): return self.__sync_info

    @property
    def search_file_extensions(self): return self.__search_file_extensions

    @property
    def compressor(self): return self.__compressor

    @property
    def compress_extension(self): return collect.SUPPORTED_COMPRESSOR[self.__compressor.name].extension

    @property
    def clean_level(self): return self.__clean_level

    @property
    def max_parallel_shell(self): return self.__max_parallel_shell

    def meta_file_path(self, file_name): return os.path.join(self.__meta_path, file_name)

    def remote_info(self, ip): return self.__remote_infos.get(ip, None)



    def __load_config_file(self, config_file_path):
        from datasync import default_config
        user_cfg = importer.import_file(config_file_path, Tree())
        config = Tree()

        for attr in dir(default_config):
            if attr.startswith('__'): continue
            dval = getattr(default_config, attr)
            v = user_cfg.get(attr, dval)
            if v is None: raise Exception('Config parse error, Attribute {} must be set'.format(attr))
            config[attr] = v

        return config


    def generate_download_params(self, indexes=None):
        if indexes is None: indexes = range(len(self.__remote_path_list))

        remotes, locals = [], []
        for i in indexes:
            r = self.__remote_infos[i]
            remotes.append((r.user, r.ip, r.path+'/'))
            locals.append(os.path.join(self.__download_path, str(i)))

        return remotes, locals, [self.compress_extension]


    def convert_download_path_to_remote_path(self, path):
        rpath = os.path.relpath(os.path.normpath(path), self.__download_path)
        index = int(rpath.split('/')[0])
        r = self.__remote_infos.get(index, None)
        if r is None: raise Exception('Invalid download path {}'.format(path))
        file_rpath = os.path.relpath(rpath, str(index))
        return r.user, r.ip, os.path.join(r.path, file_rpath)



    def convert_download_path_to_sync_path(self, path):
        rpath = os.path.relpath(os.path.normpath(path), self.__download_path)
        index = int(rpath.split('/')[0])
        r = self.__remote_infos.get(index, None)
        if r is None: raise Exception('Invalid download path {}'.format(path))
        file_rpath = os.path.relpath(rpath, str(index))
        raw_sync_file_path = os.path.join(self.__sync_info.root_path, file_rpath)
        sync_file_dir = os.path.dirname(raw_sync_file_path)
        file_name = os.path.basename(raw_sync_file_path)
        sync_file_path = os.path.join(sync_file_dir, '{}_{}'.format(r.ip, file_name))
        return sync_file_path
