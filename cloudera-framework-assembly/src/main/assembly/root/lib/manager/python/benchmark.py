#!/usr/bin/python -u
'''
Provide a Cloudera Manager benchmark pipeline
Usage: %s [options]
Options:
-h --help                                Show help
--host=<cm-server-host>                  Specify a Cloudera Manager Server host
                                         Defaults to 'localhost'
--port=<cm-server-port>                  Override the default Cloudera Manager Server port
                                         Defaults to '7180'
--version=<cm-server-api-version>        Define the Cloudera Manager Server API version
                                         Defaults to latest as defined in the cm_api python module
--user=<cm-server-user>                  The Cloudera Manager user
                                         Defaults to 'admin'
--password=<cm-server-user-password>     The Cloudera Manager user password
                                         Defaults to 'admin'
'''

import datetime
import getopt
import inspect
import json
import logging
import sys
import textwrap
import time

from time import sleep
from cm_api import api_client
from cm_api.api_client import ApiResource
from cm_api.endpoints.dashboards import create_dashboards

LOG = logging.getLogger(__name__)
                    
def do_call(host, port, version, user, password):

    app_name = 'cloudera-framework-example'
    app_version = '10.10.1003'
    app_time = '733'
    app_start = '1469159163'
    app_end = '1469161148'
    app_dashboard = '../dashboard/release.json';

    dashboard_name = 'Release (' + app_name + ', ' + app_version + ', ' + app_time + ' sec, #startTime=' + app_start + '000&endTime=' + app_end + '000)'
    api = ApiResource(host, port, user, password, False, version)
    with open (app_dashboard, 'r') as dashboard_data_file:
        dashboard_data = dashboard_data_file.read()
    # create_dashboards(api, [ApiDashboard(api, dashboard_name, dashboard_data)])

    for view_plot in json.loads(dashboard_data)['viewPlots']:
        for key, value in view_plot['plot'].items():
            if key == 'tsquery':
                for ts in api.query_timeseries(value, datetime.datetime.fromtimestamp(float(app_start)), datetime.datetime.fromtimestamp(float(app_end)))[0].timeSeries:
                    print '--- %s: %s ---' % (ts.metadata.entityName, ts.metadata.metricName)
                    for point in ts.data:
                        print '%s,%s' % (point.timestamp, point.value)

    # Use a print pretty library to print tabular meta data - benchmark.csv, timeseries/cpu.csv etc in HDFS

def usage():
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc % (sys.argv[0],))

def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)

def main(argv):
    setup_logging(logging.INFO)
    host = 'localhost'
    port = 7180
    version = 13  # Do not use api_client.API_CURRENT_VERSION, it is often +1 current production version
    user = 'admin'
    password = 'admin'
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help', 'host=', 'port=', 'version=', 'user=', 'password='])
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1
    for option, value in opts:
        if option in ('-h', '--help'):
            usage()
            return -1
        elif option in ('--host'):
            host = value
        elif option in ('--port'):
            port = value
        elif option in ('--version'):
            version = value
        elif option in ('--user'):
            user = value
        elif option in ('--password'):
            password = value
        else:
            print >> sys.stderr, 'Unknown option or flag: ' + option
            usage()
            return -1
    do_call(host, port, version, user, password)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
