#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
CLUSTER_NODE_USER=${2:-"$CLUSTER_NODE_USER"}
CLUSTER_NODE_KEY=${3:-"$CLUSTER_NODE_KEY"}
MANAGER_SERVER_USER=${4:-"admin"}
MANAGER_SERVER_PWORD=${5:-"admin"}
MANAGER_SERVER_HOST=${6:-"$MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${7:-"$MANAGER_SERVER_PORT"}
HDFS_NAMENODE_HOST=${8:-"$HDFS_NAMENODE_HOST"}
KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS=${9:-"$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS"}
ZOOKEEPER_SERVER_HOSTS_AND_PORTS=${10:-"$ZOOKEEPER_SERVER_HOSTS_AND_PORTS"}
FLUME_AGENT_HOSTS=${11:-"$FLUME_AGENT_HOSTS"}
FLUME_AGENT_DIR_LIB=${12:-"/usr/lib/flume-ng/plugins.d/$NAME_SPACE_DATABASE"}
FLUME_AGENT_NAME=${13:-"$NAME_SPACE_DATABASE"}
ROOT_HDFS=${14:-"$ROOT_HDFS"}
ROOT_DIR_HDFS_RAW=${15:-"$ROOT_DIR_HDFS_RAW"}
ROOT_DIR_HDFS_STAGED=${16:-"$ROOT_DIR_HDFS_STAGED"}
RECORD_FORMAT=${17:-"xml"}

$ROOT_DIR/bin/*-shell-hadoop.sh "fs -mkdir -p $ROOT_DIR_HDFS_RAW"
$ROOT_DIR/bin/*-shell-hadoop.sh "fs -chmod 777 $ROOT_DIR_HDFS_RAW"
$ROOT_DIR/bin/*-shell-hadoop.sh "fs -mkdir -p $ROOT_DIR_HDFS_STAGED"
$ROOT_DIR/bin/*-shell-hadoop.sh "fs -chmod 777 $ROOT_DIR_HDFS_STAGED"

FLUME_AGENT_HOSTS_ARRAY=(${FLUME_AGENT_HOSTS//,/ })
for FLUME_AGENT_HOST in "${FLUME_AGENT_HOSTS_ARRAY[@]}"; do
  ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$FLUME_AGENT_HOST \
  	"mkdir -p $FLUME_AGENT_DIR_LIB/lib"
  scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY \
  	$ROOT_DIR/lib/jar/*.jar $CLUSTER_NODE_USER@$FLUME_AGENT_HOST:$FLUME_AGENT_DIR_LIB/lib
  ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$FLUME_AGENT_HOST \
  	"mkdir -p $FLUME_AGENT_DIR_LIB/libext"
  scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY \
  	$ROOT_DIR/lib/jar/dep/*.jar $CLUSTER_NODE_USER@$FLUME_AGENT_HOST:$FLUME_AGENT_DIR_LIB/libext
done
 
FLUME_AGENT_CONFIG=$(cat $ROOT_DIR/lib/cfg/flume/flume-conf.properties | \
	sed -e "s|\$ROOT_HDFS|$ROOT_HDFS|g" | \
	sed -e "s|\$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$ROOT_DIR_HDFS_RAW|$ROOT_DIR_HDFS_RAW|g" | \
	sed -e "s|\$ROOT_DIR_HDFS_STAGED|"$ROOT_DIR_HDFS_STAGED"|g" | \
	sed -e "s|\$RECORD_FORMAT|"$RECORD_FORMAT"|g" \
)

python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$FLUME_AGENT_NAME" "$FLUME_AGENT_CONFIG" << END
import sys
from cm_api import api_client
from cm_api.api_client import ApiResource
def main(argv):
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'FLUME':
        for group in service.get_all_role_config_groups():
          if group.roleType == 'AGENT':
            print 'Updating Flume config ...'
            group.update_config({'agent_name': argv[5]})
            group.update_config({'agent_config_file': argv[6]})
        print 'Restarting Flume service ...'
        service.restart().wait()
  return 0
if __name__ == '__main__':
  sys.exit(main(sys.argv))
END
