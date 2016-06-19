#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
MANAGER_SERVER_USER=${2:-"admin"}
MANAGER_SERVER_PWORD=${3:-"admin"}
MANAGER_SERVER_HOST=${4:-"$MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${5:-"$MANAGER_SERVER_PORT"}
HDFS_NAMENODE_HOST=${6:-"$HDFS_NAMENODE_HOST"}
KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS=${7:-"$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS"}
ZOOKEEPER_SERVER_HOSTS_AND_PORTS=${8:-"$ZOOKEEPER_SERVER_HOSTS_AND_PORTS"}
FLUME_AGENT_NAME=${9:-"$NAME_SPACE_DATABASE"}
FLUME_PLUGINS_DIR=${10:-"/opt/cloudera/parcels/CLOUDERA_FRAMEWORK_EXAMPLE/lib/flume"}
ROOT_HDFS=${11:-"$ROOT_HDFS"}
ROOT_DIR_HDFS_STAGED=${12:-"$ROOT_DIR_HDFS_STAGED"}
ROOT_DIR_HDFS_STAGED_CANONICAL=${13:-"$ROOT_DIR_HDFS_STAGED_CANONICAL"}
RECORD_FORMAT=${14:-"xml"}

$ROOT_DIR/lib/bin/cloudera-framework-hadoop.sh "fs -mkdir -p $ROOT_DIR_HDFS_STAGED"
$ROOT_DIR/lib/bin/cloudera-framework-hadoop.sh "fs -chmod -R 777 $ROOT_DIR_HDFS_STAGED"
 
FLUME_AGENT_CONFIG=$(cat $ROOT_DIR/lib/cfg/flume/flume-conf.properties | \
	sed -e "s|\$ROOT_HDFS|$ROOT_HDFS|g" | \
	sed -e "s|\$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$ROOT_DIR_HDFS_STAGED_CANONICAL|"$ROOT_DIR_HDFS_STAGED_CANONICAL"|g" | \
	sed -e "s|\$RECORD_FORMAT|"$RECORD_FORMAT"|g" \
)

python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$FLUME_AGENT_NAME" "$FLUME_AGENT_CONFIG" "$FLUME_PLUGINS_DIR" << END
import sys
from cm_api import api_client
from cm_api.api_client import ApiResource
def main(argv):
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'FLUME':
        print 'Stoping flume agent ...'
        service.stop().wait()
        for group in service.get_all_role_config_groups():
          if group.roleType == 'AGENT':
            print 'Updating Flume config ...'
            group.update_config({'agent_name': argv[5]})
            group.update_config({'agent_plugin_dirs': argv[7]})
            group.update_config({'agent_config_file': argv[6]})
  return 0
if __name__ == '__main__':
  sys.exit(main(sys.argv))
END
