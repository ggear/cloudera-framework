"""
Python script utilities as included from the cloudera-framework-assembly,
do not edit directly
"""

import os
import re


def qualify(path):
    """
    Qualify an s3a:// or hdfs:// URL with environment defaults
    """
    return path if (re.match(r'[.]*://[.]*', path)
                    or 'CF_HADOOP_DEFAULT_FS' not in os.environ) \
        else os.environ['CF_HADOOP_DEFAULT_FS'] + path
