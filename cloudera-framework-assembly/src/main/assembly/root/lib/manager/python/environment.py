#!/usr/bin/env python
"""
Provide a flat list of cluster connectivity parameters 
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
--user=<cm-server-user-password>         The Cloudera Manager user password
                                         Defaults to 'admin'
--cluster_node_user=<cluster-node-user>  The local user able to connect to each cluster node
                                         Defaults to 'root'
--cluster_node_key=<cluster-node-key>    The key path which enables connection to each cluster node
                                         Defaults to '/root/.ssh/launchpad'
--cluster_name=<cluster-name>            Name the cluster to reflect on
                                         Defaults to the zeroth cluster defined                                   
--service_role_name=<service-role-type>  Filter to apply to service and role name
                                         Defaults to all roles defined in the cluster                                   
--random_index=<true|false>              For parameters that are multi-value, define singular value
                                         via a random index rather than the zeroth element
                                         Defaults to 'false'
"""

import getopt
import inspect
import logging
import sys
import textwrap
from random import randint
from cm_api import api_client
from cm_api.api_client import ApiResource

LOG = logging.getLogger(__name__)

def do_print_header():
    print '###############################################################################'
    print '# '
    print '# Cluster connectivity parameters'
    print '#'
    print '###############################################################################'
    print ''    

def do_print_footer():
    print '###############################################################################'

def do_print_line_item_manual(service_name, role_name, keys, values):
  service_role_name = service_name.upper() + '_' + role_name.upper();
  print '# ' + service_role_name
  for key, value in zip(keys, values):
      print service_role_name + '_' + key + '=' + value
  print ''    
  return True

def do_print_line_item(api, service, service_role_name_filter, random_index, service_name, role_name, role_port_name, hosts, ports):
  service_role_name = service_name.upper() + '_' + role_name.upper();
  if service_role_name_filter is None or service_role_name_filter.upper() == service_role_name and service.type == service_name:
    if len(hosts) == 0:
      for role in service.get_roles_by_type(role_name):
        host = api.get_host(role.hostRef.hostId).hostname
        port = role.get_config('full')[role_port_name].value
        if port is None:
          port = role.get_config('full')[role_port_name].default
        hosts.append(host)
        ports.append(port)
    if len(hosts) > 0:
      index = 0
      if str(random_index).title() == 'True':
        index = randint(0, len(hosts) - 1)
      print '# ' + service_role_name
      print service_role_name + '_HOST=' + hosts[index]
      print service_role_name + '_PORT=' + ports[index]
      print service_role_name + '_HOSTS=' + ",".join(hosts)
      print service_role_name + '_PORTS=' + ",".join(ports)
      print service_role_name + '_HOSTS_AND_PORTS=' + ','.join([host + ':' + port for host, port in zip(hosts, ports)])
      print ''
      return True
  return False
                    
def do_call(host, port, version, user, password, cluster_name, cluster_node_user, cluster_node_key, service_role_name, random_index):
    api = ApiResource(host, port, user, password, False, version);
    for cluster in api.get_all_clusters():
      if cluster_name is None:
        break
      elif cluster_name == cluster.name:
        break
    if cluster_name is not None and cluster_name != cluster.name:
        print >> sys.stderr, "Cloud not find cluster: " + cluster_name
        return -2;
    do_print_header()
    do_print_line_item_manual('CLUSTER', 'NODE', ['USER', 'KEY'], [cluster_node_user, cluster_node_key])
    do_print_line_item(api, None, service_role_name, random_index, 'MANAGER', 'SERVER', None, [host], [str(port)])
    do_print_line_item(api, api.get_cloudera_manager().get_service(), service_role_name, random_index, 'MANAGER', 'NAVIGATORMETASERVER', 'navigator_server_port', [], [])
    for service in cluster.get_all_services():
      do_print_line_item(api, service, service_role_name, random_index, 'HDFS', 'NAMENODE', 'namenode_port', [], [])
      do_print_line_item(api, service, service_role_name, random_index, 'HUE', 'HUE_SERVER', 'hue_http_port', [], [])
      do_print_line_item(api, service, service_role_name, random_index, 'HIVE', 'HIVESERVER2', 'hs2_thrift_address_port', [], [])
      do_print_line_item(api, service, service_role_name, random_index, 'IMPALA', 'IMPALAD', 'beeswax_port', [], [])
      do_print_line_item(api, service, service_role_name, random_index, 'FLUME', 'AGENT', 'agent_http_port', [], [])
      do_print_line_item(api, service, service_role_name, random_index, 'KAFKA', 'KAFKA_BROKER', 'port', [], [])
      do_print_line_item(api, service, service_role_name, random_index, 'ZOOKEEPER', 'SERVER', 'clientPort', [], [])
    do_print_footer()

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
    version = 12  # Do not use api_client.API_CURRENT_VERSION, it is often +1 current production version
    user = 'admin'
    password = 'admin'
    cluster_name = None
    cluster_node_user = 'ec2-user'
    cluster_node_key = '/home/graham/.ssh/id_rsa'
    service_role_name = None
    random_index = False
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help", "host=", "port=", "version=", "user=", "password=", "random_index=", "cluster_name=", "cluster_node_user=", "cluster_node_key=", "service_role_name="])
    except getopt.GetoptError, err:
        print >> sys.stderr, err
        usage()
        return -1
    for option, value in opts:
        if option in ("-h", "--help"):
            usage()
            return -1
        elif option in ("--host"):
            host = value;
        elif option in ("--port"):
            port = value;
        elif option in ("--version"):
            version = value;
        elif option in ("--user"):
            user = value;
        elif option in ("--password"):
            password = value;
        elif option in ("--random_index"):
            random_index = value;
        elif option in ("--cluster_name"):
            cluster_name = value;
        elif option in ("--cluster_node_user"):
            cluster_node_user = value;
        elif option in ("--cluster_node_key"):
            cluster_node_key = value;
        elif option in ("--service_role_name"):
            service_role_name = value;
        else:
            print >> sys.stderr, "Unknown option or flag: " + option
            usage()
            return -1
    do_call(host, port, version, user, password, cluster_name, cluster_node_user, cluster_node_key, service_role_name, random_index)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
