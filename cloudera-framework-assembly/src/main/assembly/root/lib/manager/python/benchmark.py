#!/usr/local/bin/python -u
'''
Provide a Cloudera Manager benchmark pipeline
Usage: %s [options]
Options:
-h --help                                Show help
--user=<cloudera-user>                   The Cloudera services user
                                         Defaults to 'admin'
--password=<cloudera-password>           The Cloudera services password
                                         Defaults to 'admin'
--man_host=<manager-host>                Specify a Cloudera Manager Server host
                                         Defaults to 'localhost'
--man_port=<manager-port>                Override the default Cloudera Manager Server port
                                         Defaults to '7180'
--nav_host=<navigator-host>              Specify a Cloudera Navigator Server host
                                         Defaults to 'localhost'
--nav_port=<navigator-port>              Override the default Cloudera Navigator Server port
                                         Defaults to '7187'
--app_name=<app-name>                    The application name
                                         Required, defaults to None
--app_version=<app-version>              The application version
                                         Required, defaults to None
--app_time=<app-time-seconds>            The application duration time in seconds
                                         Required, defaults to None
--app_start=<app-start-epoch-seconds>    The application start epoch in seconds
                                         Required, defaults to None
--app_end=<app-end-epoch-seconds>        The application  end epoch in seconds
                                         Required, defaults to None
--app_dashboard=<app-dashboard-file>     The application  dashboard file
                                         Required, defaults to None
'''

import datetime
import getopt
import inspect
import json
import logging
import sys
import textwrap
from time import sleep
import time
import urllib

from cm_api import api_client
from cm_api.api_client import ApiResource, ApiException
from cm_api.endpoints.dashboards import ApiDashboard, create_dashboards

import requests
from tabulate import tabulate


LOG = logging.getLogger(__name__)

NAV_API_VERSION = 9
MAN_API_VERSION = 13  # Do not use api_client.API_CURRENT_VERSION, it is often +1 current production version

def update_metadata(user, password, nav_host, nav_port, app_namespace, property_namespace, properties, app_report_only):
    requests.post(nav_uri(nav_host, nav_port, 'models/namespaces'), \
                  auth=(user, password), data='{"name":"' + property_namespace + '","description":"' + property_namespace + ' Metadata"}')
    for property in reversed(properties):
        requests.post(nav_uri(nav_host, nav_port, 'models/namespaces/' + property_namespace + '/properties'), \
                      auth=(user, password), data='{"name":"' + property['name'] + '","description":"' + property['description'] + '","namespace":"' + property_namespace + '","multiValued":true,"type":"TEXT"}')
        requests.post(nav_uri(nav_host, nav_port, 'models/packages/nav/classes/hv_database/properties'), \
                      auth=(user, password), data='[{"name":"' + property['name'] + '","namespace":"' + property_namespace + '"}]')
    app_database = ''
    try:
        app_database = requests.get(nav_uri(nav_host, nav_port, 'entities/?query=' + app_namespace + '%20%26%26%20type%3Adatabase&limit=1&offset=0'), \
                                    auth=(user, password)).json()[0]['identity']
    except IndexError:
        pass
    if not app_database == '':
        try:
            property_values = requests.get(nav_uri(nav_host, nav_port, 'entities/' + app_database), \
                                           auth=(user, password)).json()['customProperties'][property_namespace]
        except KeyError:
            property_values = {}
        for property in properties:
            for property_name in property['value']:
                if property_name in property_values:
                    property_values[property_name] = property_values[property_name] + property['value'][property_name]
                else:
                    property_values[property_name] = property['value'][property_name]
        if not app_report_only:
            requests.put(nav_uri(nav_host, nav_port, 'entities/' + app_database), \
                         auth=(user, password), data='{"customProperties":{"' + property_namespace + '":' + json.dumps(property_values) + '}}')
    app_properties = {'database': app_database, 'properties':[]}
    app_entities = requests.get(nav_uri(nav_host, nav_port, 'entities/?query=' + app_namespace.split('_')[0] + '*%20%26%26%20type%3Adatabase&limit=9999&offset=0'), \
                                auth=(user, password)).json()
    for app_entities_properties in app_entities:
        try:
            app_properties['properties'].append(app_entities_properties['customProperties'][property_namespace])
        except TypeError:
            pass
        except KeyError:
            pass
    return app_properties

def nav_uri(nav_host, nav_port, path):
    return 'http://' + nav_host + ':' + str(nav_port) + '/api/v' + str(NAV_API_VERSION) + '/' + path

def compress_bins(bins, normalising_factor):
    bins_num = 0
    bins_sum = 0
    for bin in bins:
        bins_num += 1
        bins_sum += bin.value / normalising_factor
    if bins_num == 0:
        return 0
    return int(bins_sum / bins_num)  

def do_call(user, password, man_host, man_port, nav_host, nav_port, app_name, app_version, app_namespace, app_time, app_start, app_end, app_dashboard, app_report_only):
    cpu = 0
    hdfs = 0
    network = 0
    if app_report_only:
        app_time = '0'
        app_start = '0'
        app_end = '0'
    dashboard_name = 'Release (' + app_namespace + ', ' + app_time + 's)'
    if not app_report_only:
        api = ApiResource(man_host, man_port, user, password, False, MAN_API_VERSION)
        with open (app_dashboard, 'r') as dashboard_data_file:
            dashboard_data = dashboard_data_file.read()
        try:
            create_dashboards(api, [ApiDashboard(api, dashboard_name, dashboard_data)])
        except ApiException:
            pass
        for view_plot in json.loads(dashboard_data)['viewPlots']:
            for key, value in view_plot['plot'].items():
                if key == 'tsquery':
                    for time_series in api.query_timeseries(value, datetime.datetime.fromtimestamp(float(app_start)), datetime.datetime.fromtimestamp(float(app_end)))[0].timeSeries:
                        if time_series.metadata.metricName == 'cpu_percent_across_hosts':
                            cpu = compress_bins(time_series.data, 1)
                        if time_series.metadata.metricName == 'total_bytes_read_rate_across_datanodes':
                            hdfs += compress_bins(time_series.data, 100000)
                        if time_series.metadata.metricName == 'total_bytes_written_rate_across_datanodes':
                            hdfs += compress_bins(time_series.data, 100000)
                        if time_series.metadata.metricName == 'total_bytes_receive_rate_across_network_interfaces':
                            network += compress_bins(time_series.data, 100000)
                        if time_series.metadata.metricName == 'total_bytes_transmit_rate_across_network_interfaces':
                            network += compress_bins(time_series.data, 100000)
    properties = [ \
                  {'name':'Name', 'description':'Application name', 'value': {'Name': [app_name]}}, \
                  {'name':'Version', 'description':'Application version', 'value': {'Version': [app_version]}}, \
                  {'name':'Run', 'description':'Run time', 'value': {'Run': [app_time + 's']}}, \
                  {'name':'Start', 'description':'Start time', 'value': {'Start': [datetime.datetime.fromtimestamp(float(app_start)).strftime('%Y-%m-%d %H:%M:%S')]}}, \
                  {'name':'Finish', 'description':'Finish time', 'value': {'Finish': [datetime.datetime.fromtimestamp(float(app_end)).strftime('%Y-%m-%d %H:%M:%S')]}}, \
                  {'name':'CPU', 'description':'Relative CPU usage during benchmark', 'value': {'CPU': [str(cpu)]}}, \
                  {'name':'HDFS', 'description':'Relative HDFS usage during benchmark', 'value': {'HDFS': [str(hdfs)]}}, \
                  {'name':'Network', 'description':'Relative Network usage during benchmark', 'value': {'Network': [str(network)]}} \
    ]
    app_properties = update_metadata(user, password, nav_host, nav_port, app_namespace, 'Benchmark', properties, app_report_only)
    app_table_comparison = '{:<15} |{:>15} |{:>15} |{:>15} |{:>15}|'
    app_table = [['Application', app_name + '-' + app_version]]
    if not app_report_only:
        app_table.append(['Run', app_time + 's'])
        app_table.append(['Start', datetime.datetime.fromtimestamp(float(app_start)).strftime('%Y-%m-%d %H:%M:%S')])
        app_table.append(['Finish', datetime.datetime.fromtimestamp(float(app_end)).strftime('%Y-%m-%d %H:%M:%S')])
    if app_properties['database']:
        app_table.append(['Metadata', 'http://localhost:7187/?view=detailsView&id=' + app_properties['database']])
        app_table.append(['Dashboard', 'http://localhost:7180/cmf/views/view?viewName=' + urllib.quote_plus(dashboard_name)])
    app_table.append(['Comparison', app_table_comparison.format('Version', 'Run', 'CPU', 'HDFS', 'Network')])        
    for properties_value in app_properties['properties']:
        app_table.append([None, app_table_comparison.format(', '.join(properties_value['Version']), ', '.join(properties_value['Run']), ', '.join(properties_value['CPU']), ', '.join(properties_value['HDFS']), ', '.join(properties_value['Network']))])        
    print tabulate(app_table, tablefmt='grid')

def usage():
    doc = inspect.getmodule(usage).__doc__
    print >> sys.stderr, textwrap.dedent(doc % (sys.argv[0],))

def setup_logging(level):
    logging.basicConfig()
    logging.getLogger().setLevel(level)
    logging.getLogger("requests").setLevel(logging.WARNING)

def main(argv):
    setup_logging(logging.INFO)
    user = 'admin'
    password = 'admin'
    man_host = 'localhost'
    man_port = 7180
    nav_host = 'localhost'
    nav_port = 7187
    app_name = None
    app_version = None
    app_namespace = None
    app_time = None
    app_start = None
    app_end = None
    app_dashboard = None
    app_report_only = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help', 'user=', 'password=', 'man_host=', 'man_port=', 'nav_host=', 'nav_port=', 'app_name=', 'app_version=', 'app_namespace=', 'app_time=', 'app_start=', 'app_end=', 'app_dashboard=', 'app_report_only='])
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1
    for option, value in opts:
        if option in ('-h', '--help'):
            usage()
            return -1
        elif option in ('--user'):
            user = value
        elif option in ('--password'):
            password = value
        elif option in ('--man_host'):
            man_host = value
        elif option in ('--man_port'):
            man_port = value
        elif option in ('--nav_host'):
            nav_host = value
        elif option in ('--nav_port'):
            nav_port = value
        elif option in ('--app_name'):
            app_name = value
        elif option in ('--app_version'):
            app_version = value
        elif option in ('--app_namespace'):
            app_namespace = value
        elif option in ('--app_time'):
            app_time = value
        elif option in ('--app_start'):
            app_start = value
        elif option in ('--app_end'):
            app_end = value
        elif option in ('--app_dashboard'):
            app_dashboard = value
        elif option in ('--app_report_only'):
            if value.upper() == 'TRUE':
                app_report_only = True
        else:
            print >> sys.stderr, 'Unknown option or flag: ' + option
            usage()
            return -1
    if app_name is None or app_version is None or app_namespace is None or app_time is None or app_start is None or app_end is None or app_dashboard is None:
        print >> sys.stderr, 'Required parameters [app_name, app_version, app_namespace, app_time, app_start, app_end, app_dashboard] not passed on command line'
        usage()
        return -1
    do_call(user, password, man_host, man_port, nav_host, nav_port, app_name, app_version, app_namespace, app_time, app_start, app_end, app_dashboard, app_report_only)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
