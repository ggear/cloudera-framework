#!/usr/bin/python -u
'''
Test python script
Usage: %s [options]
Options:
-h --help                                Show help
--param=<parameter>                      A test parameter
                                         Required
'''

import getopt
import inspect
import logging
import sys
import textwrap

LOG = logging.getLogger(__name__)


def do_call(param):
    print 'Invoked with parameter [%s]' % (param)


def usage():
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc % (sys.argv[0],))


def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)


def main(argv):
    setup_logging(logging.INFO)
    param = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help', 'param='])
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1
    for option, value in opts:
        if option in ('-h', '--help'):
            usage()
            return -1
        elif option in ('--param'):
            param = value
        else:
            print >> sys.stderr, 'Unknown option or flag: ' + option
            usage()
            return -1
    if param is None:
        print >> sys.stderr, 'Required parameters [param] not passed on command line'
        usage()
        return -1
    do_call(param)
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
