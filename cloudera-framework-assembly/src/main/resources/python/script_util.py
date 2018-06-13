###############################################################################
#
# Python script utilities as included from the cloudera-framework-assembly,
# do not edit directly
#
###############################################################################

import os
import re


def qualify(path):
    return path if (re.match(r'[.]*://[.]*', path)
                    or 'CF_HADOOP_DEFAULT_FS' not in os.environ) \
        else os.environ['CF_HADOOP_DEFAULT_FS'] + path
