import os
import re


def hdfs_make_qualified(path):
    return path if (re.match(r'[.]*://[.]*', path) or 'CF_HADOOP_DEFAULT_FS' not in os.environ) \
        else os.environ['CF_HADOOP_DEFAULT_FS'] + path
