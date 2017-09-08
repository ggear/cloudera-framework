#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

DEPLOY_CONFIG=${1:-true}
MANAGER_SERVER_USER=${2:-"admin"}
MANAGER_SERVER_PWORD=${3:-"admin"}
FLUME_PROPERTIES=${4:-"$ROOT_DIR/lib/flume/flume-conf.properties"}
RECORD_FORMAT=${5:-"xml"}

if [ ! -f $FLUME_PROPERTIES ]; then
  exit 0
fi
 
export FLUME_AGENT_CONFIG=$(cat $FLUME_PROPERTIES | \
	sed -e "s|\$FLUME_AGENT_NAME|$PARCEL_NAMESPACE|g" | \
	sed -e "s|\$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|$KAFKA_KAFKA_BROKER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|$ZOOKEEPER_SERVER_HOSTS_AND_PORTS|g" | \
	sed -e "s|\$TOPIC_APP|$TOPIC_APP|g" | \
	sed -e "s|\$HDFS_URL|$HDFS_URL|g" | \
	sed -e "s|\$HDFS_APP|"$HDFS_APP"|g" \
)

for SCRIPT in $(find $(dirname $FLUME_PROPERTIES) -maxdepth 1 -type f 2> /dev/null); do
  if [ ${SCRIPT: -3} == ".sh" ]; then
    source $SCRIPT
  fi
done

python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$PARCEL_NAMESPACE" "$FLUME_AGENT_CONFIG" "/opt/cloudera/parcels/$PARCEL_NAME/lib/flume" "$DEPLOY_CONFIG" << END
import sys
from cm_api import api_client
from cm_api.api_client import ApiResource
def main(argv):
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'FLUME':
        if service.serviceState == 'STARTED':
          print 'Stopping flume agents ...'
          service.stop().wait()
        print 'Flume agents stopped'
        for group in service.get_all_role_config_groups():
          if group.roleType == 'AGENT':
            print 'Updating Flume config ...'
            group.update_config({'agent_name': argv[5]})
            group.update_config({'agent_plugin_dirs': argv[7]})
            group.update_config({'agent_config_file': argv[6]})
            print 'Flume config updated'
        if argv[8] == 'true':
          print 'Starting flume agents ...'
          service.start().wait()
          print 'Flume agents started'
  return 0
if __name__ == '__main__':
  sys.exit(main(sys.argv))
END
