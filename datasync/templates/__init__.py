


_CUR_PATH = None

def path(file_name):
    import os
    global _CUR_PATH
    if _CUR_PATH is None: _CUR_PATH = os.path.dirname(__file__)
    return os.path.join(_CUR_PATH, file_name)