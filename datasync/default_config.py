import sys

# Data server with path, format: user@host:path
REMOTE_PATH_LIST = []


# Sync server  format: user@host:port
SYNC_SERVER = None


# Sync root path for saving file relative to it
SYNC_ROOT_PATH = '/'


# Use md5 to check sync file
SYNC_CHECK = True


# Sync process is asynchronous, therefore it is necessary to wait for check before upload finished
SYNC_CHECK_TIMEOUT = 60 # 1m


# A sleep time will be executed between once sync checking and next
SYNC_CHECK_SLEEP_TIME = 0.001 # 1ms


# A buffer for cache file contents on sync process
SYNC_READ_BUFFER_SIZE = 1024 * 1024 * 100  # 100 MB


# Download path used to save files which are fetch from remote server
DOWNLOAD_DIRECTORY = None


# Meta path used to save core status for recovery when errors occur.
META_DIRECTORY = None


# Log file path
LOG_FILE_PATH = None


# File extensions for search
SEARCH_FILE_EXTENSIONS = []


# Used to compress raw data file on collect status, support copy/bzip2/pbzip2
COMPRESSOR = 'bzip2'


# Arguments of compressor, recommend as bellow:
# bzip2: -k -z --fast
# pbzip2: -k -z
COMPRESSOR_ARGS = '-k -z --fast'


# 0: don't clean  1: clean origin file 2: clean compress file 3: all
CLEAN_LEVEL = 3


# Permition of  max number of shell running parallel
MAX_PARALLEL_SHELL = sys.maxsize