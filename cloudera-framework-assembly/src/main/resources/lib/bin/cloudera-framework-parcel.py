#!/usr/bin/python -u
'''
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
--password=<cm-server-user-password>     The Cloudera Manager user password
                                         Defaults to 'admin'
--cluster_name=<cluster-name>            The cluster name, if not defined will reflect on all clusters
                                         Defaults to not defined
--parcel_name=<parcel-name>              The parcel name, required
                                         Throws an error if not defined
--parcel_version=<parcel-version>        The full parcel version, sans architecture, required
                                         Throws an error if not defined
--parcel_repo=<parcel-repo>              The parcel http repository URL sans base version
                                         Throws an error if not defined
'''

import getopt
import inspect
import logging
from random import randint
import re
import sys
import textwrap
from time import sleep
import time

from cm_api import api_client
from cm_api.api_client import ApiResource


LOG = logging.getLogger(__name__)

POLL_SEC = 2
TIMEOUT_SEC = 180
REGEX_VERSION = '[1-9][0-9]\.[1-9][0-9]\.[1-9][0-9][0-9][0-9]'

def do_parcel_op(cluster, parcel_name, parcel_version, parcel_op_label, stage_enter, stage_exit, parcel_op):
    parcel = cluster.get_parcel(parcel_name, parcel_version)
    if parcel.stage == stage_enter:            
        print 'Parcel [%s] starting ... ' % (parcel_op_label)    
        getattr(parcel, parcel_op)()
        while True:
          time.sleep(POLL_SEC)
          parcel = cluster.get_parcel(parcel_name, parcel_version)
          if parcel.stage == stage_exit:
            break
          if parcel.state.errors:
            raise Exception(str(parcel.state.errors))
          print 'Parcel [%s] %s/%s' % (parcel_op_label, parcel.state.progress, parcel.state.totalProgress)
        print 'Parcel [%s] finished' % (parcel_op_label)    
                    
def do_call(host, port, version, user, password, cluster_name, parcel_name, parcel_version, parcel_repo):
    api = ApiResource(host, port, user, password, False, version)
    if not parcel_repo.endswith('/'):
        parcel_repo += '/'
    if re.match(REGEX_VERSION, parcel_version) is None or re.match(REGEX_VERSION, parcel_version).group() != parcel_version:
        raise Exception('Parcel [' + parcel_name + '] is qualified by invalid version [' + parcel_version + '] expected to match regular expression [' + REGEX_VERSION + ']')
    if not parcel_repo.endswith(parcel_version + '/'):
        raise Exception('Parcel [' + parcel_name + '] is qualified by invalid version [' + parcel_version + '] when compared with repository [' + parcel_repo + ']')    
    cm_config = api.get_cloudera_manager().get_config(view='full')
    repo_config = cm_config['REMOTE_PARCEL_REPO_URLS']
    repo_list = repo_config.value or repo_config.default
    if parcel_repo not in repo_list:     
        repo_list += ',' + parcel_repo
        api.get_cloudera_manager().update_config({'REMOTE_PARCEL_REPO_URLS': repo_list})
        time.sleep(POLL_SEC)  # The parcel synchronize end-point is not exposed via the API, so sleep instead
    cluster_names = []
    if cluster_name is None:
        for cluster in api.get_all_clusters():
            cluster_names.append(cluster.name)
    else:
        cluster_names.append(cluster_name)
    for cluster_name_itr in cluster_names:
        print 'Cluster [DEPLOYMENT] starting ... '
        cluster = api.get_cluster(cluster_name_itr)
        parcel = cluster.get_parcel(parcel_name, parcel_version)
        print 'Parcel [DEPLOYMENT] starting ... '
        do_parcel_op(cluster, parcel_name, parcel_version, 'DEACTIVATE', 'ACTIVATED', 'DISTRIBUTED', 'deactivate')
        do_parcel_op(cluster, parcel_name, parcel_version, 'DOWNLOAD', 'AVAILABLE_REMOTELY', 'DOWNLOADED', 'start_download')
        do_parcel_op(cluster, parcel_name, parcel_version, 'DISTRIBUTE', 'DOWNLOADED', 'DISTRIBUTED', 'start_distribution')
        do_parcel_op(cluster, parcel_name, parcel_version, 'ACTIVATE', 'DISTRIBUTED', 'ACTIVATED', 'activate')
        parcel = cluster.get_parcel(parcel_name, parcel_version)
        if parcel.stage != 'ACTIVATED':
            raise Exception('Parcel is currently mid-stage [' + parcel.stage + '], please wait for this to complete')
        print 'Parcel [DEPLOYMENT] finished'
        print 'Cluster [CONFIG_DEPLOYMENT] starting ... '
        cluster.deploy_client_config()
        cmd = cluster.deploy_client_config()
        if not cmd.wait(TIMEOUT_SEC).success:
            raise Exception('Failed to deploy client configs')
        print 'Cluster [CONFIG_DEPLOYMENT] finihsed'
        print 'Cluster [STOP] starting ... '
        cluster.stop().wait()
        print 'Cluster [STOP] finihsed'
        print 'Cluster [START] starting ... '
        cluster.start().wait()
        print 'Cluster [START] finihsed'
        print 'Cluster [DEPLOYMENT] finished'

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
    parcel_name = None
    parcel_version = None
    parcel_repo = None    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['help', 'host=', 'port=', 'version=', 'user=', 'password=', 'cluster_name=', 'parcel_name=', 'parcel_version=', 'parcel_repo='])
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
        elif option in ('--cluster_name'):
            cluster_name = value
        elif option in ('--parcel_name'):
            parcel_name = value
        elif option in ('--parcel_version'):
            parcel_version = value
        elif option in ('--parcel_repo'):
            parcel_repo = value
        else:
            print >> sys.stderr, 'Unknown option or flag: ' + option
            usage()
            return -1
    if parcel_name is None or  parcel_version is None or parcel_repo is None:
        print >> sys.stderr, 'Required parameters [parcel_name, parcel_version, parcel_repo] not passed on command line'        
        usage()
        return -1    
    do_call(host, port, version, user, password, cluster_name, parcel_name, parcel_version, parcel_repo)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
