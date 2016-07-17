#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
DEPLOY_CONFIG=${2:-true}
MANAGER_SERVER_USER=${3:-"admin"}
MANAGER_SERVER_PWORD=${4:-"admin"}
MANAGER_SERVER_HOST=${5:-"$MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${6:-"$MANAGER_SERVER_PORT"}
HDFS_NAMENODE_HOST=${7:-"$HDFS_NAMENODE_HOST"}
KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS=${8:-"$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS"}
ZOOKEEPER_SERVER_HOSTS_AND_PORTS=${9:-"$ZOOKEEPER_SERVER_HOSTS_AND_PORTS"}
FLUME_AGENT_NAME=${10:-"$DATABASE_APP"}
FLUME_PLUGINS_DIR=${11:-"/opt/cloudera/parcels/$PARCEL_NAME/lib/flume"}
FLUME_PROPERTIES=${12:-"$ROOT_DIR/lib/flume/flume-conf.properties"}
ROOT_HDFS=${13:-"$ROOT_HDFS"}
ROOT_DIR_HDFS=${14:-"$ROOT_DIR_HDFS"}

if [ ! -f $FLUME_PROPERTIES ]; then
  exit 1
fi
 
export FLUME_AGENT_CONFIG=$(cat $FLUME_PROPERTIES | \
	sed -e "s|\$FLUME_AGENT_NAME|$FLUME_AGENT_NAME|g" | \
	sed -e "s|\$ROOT_HDFS|$ROOT_HDFS|g" | \
	sed -e "s|\$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$ROOT_DIR_HDFS|"$ROOT_DIR_HDFS"|g" \
)

for SCRIPT in $(ls $(dirname $FLUME_PROPERTIES) 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    source $(dirname $FLUME_PROPERTIES)/$SCRIPT
  fi
done

python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$FLUME_AGENT_NAME" "$FLUME_AGENT_CONFIG" "$FLUME_PLUGINS_DIR" "$DEPLOY_CONFIG" << END
import sys
from cm_api import api_client
from cm_api.api_client import ApiResource
def main(argv):
  config = argv[6]
  if argv[8] == 'false':
    config = '#Empty config'
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'FLUME':
        print 'Stoping flume agents ...'
        service.stop().wait()
        print 'Flume agents stopped'
        for group in service.get_all_role_config_groups():
          if group.roleType == 'AGENT':
            print 'Updating Flume config ...'
            print 'Agent name [%s], plugin directory [%s], config:' % (argv[5], argv[7])
            print config
            group.update_config({'agent_name': argv[5]})
            group.update_config({'agent_plugin_dirs': argv[7]})
            group.update_config({'agent_config_file': config})
        print 'Starting flume agents ...'
        service.start().wait()
        print 'Flume agents started'
  return 0
if __name__ == '__main__':
  sys.exit(main(sys.argv))
END
