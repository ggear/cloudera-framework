#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
USER_ADMIN=${2:-"$USER_ADMIN"}
NAME_SPACE_SERVER=${3:-"$NAME_SPACE_SERVER"}
NAME_SPACE_DATABASE=${4:-"$NAME_SPACE_DATABASE"}
CLUSTER_NODE_USER=${5:-"$CLUSTER_NODE_USER"}
CLUSTER_NODE_KEY=${6:-"$CLUSTER_NODE_KEY"}
MANAGER_SERVER_USER=${7:-"admin"}
MANAGER_SERVER_PWORD=${8:-"admin"}
MANAGER_SERVER_HOST=${9:-"$MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${10:-"$MANAGER_SERVER_PORT"}
HIVE_HIVESERVER2_HOSTS=${11:-"$HIVE_HIVESERVER2_HOSTS"}
HIVESERVER2_DIR_LIB=${12:-"/opt/local/hive/lib/"}

if [ $($ROOT_DIR/bin/*-shell-impala.sh -q "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN|wc -l) -eq 0 ]; then
  HIVE_HIVESERVER2_HOSTS_ARRAY=(${HIVE_HIVESERVER2_HOSTS//,/ })
  for HIVE_HIVESERVER2_HOST in "${HIVE_HIVESERVER2_HOSTS_ARRAY[@]}"; do
    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$HIVE_HIVESERVER2_HOST \
  	  "mkdir -p $HIVESERVER2_DIR_LIB"
    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY \
      $ROOT_DIR/lib/jar/*.jar $ROOT_DIR/lib/jar/dep/*.jar $CLUSTER_NODE_USER@$HIVE_HIVESERVER2_HOST:$HIVESERVER2_DIR_LIB
  done  
  python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$HIVESERVER2_DIR_LIB" << END
import sys
from cm_api import api_client
from cm_api.api_client import ApiResource
def main(argv):
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'HIVE':
        print 'Updating Hive config ...'
        service.update_config({'hive_aux_jars_path_dir': argv[5]})
        print 'Restarting Hive service ...'
        service.restart().wait()
  return 0
if __name__ == '__main__':
  sys.exit(main(sys.argv))
END
  $ROOT_DIR/bin/*-shell-hive.sh \
    --hivevar my.user=$USER_ADMIN \
    --hivevar my.server.name=$NAME_SPACE_SERVER \
    --hivevar my.database.name=$NAME_SPACE_DATABASE \
    --hivevar my.database.location=$ROOT_DIR_HDFS \
    -f $ROOT_DIR/lib/ddl/hive/database_create.ddl
  until $ROOT_DIR/bin/*-shell-impala.sh -r -q "SHOW TABLES"; do
  	echo "Sleeping while waiting for database and roles to sync ... "
    sleep 5
  done
fi
