#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

START_DEPLOYMENT=${1:-true}
MANAGER_SERVER_USER=${2:-"admin"}
MANAGER_SERVER_PWORD=${3:-"admin"}
FLUME_PROPERTIES=${4:-"$ROOT_DIR/lib/flume/flume-conf.properties"}

REMOTE_DEPLOY=${REMOTE_DEPLOY:-true}

if [ -f $FLUME_PROPERTIES ]; then
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
  if $REMOTE_DEPLOY; then
    python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$PARCEL_NAMESPACE" "$FLUME_AGENT_CONFIG" "/opt/cloudera/parcels/$PARCEL_NAME/lib/flume" "$START_DEPLOYMENT" << END
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
  else
    sudo service flume-ng-agent stop
    sudo rm -rf /usr/lib/flume-ng/plugins.d/arouter/lib*
    sudo mkdir -p /usr/lib/flume-ng/plugins.d/arouter/lib
    sudo mkdir -p /usr/lib/flume-ng/plugins.d/arouter/libext
    sudo cp -rvf $ROOT_DIR/lib/jar/*.jar /usr/lib/flume-ng/plugins.d/arouter/lib
    sudo cp -rvf $ROOT_DIR/lib/jar/dep/*.jar /usr/lib/flume-ng/plugins.d/arouter/libext
    sudo rm -rf /usr/lib/flume-ng/plugins.d/arouter/avro
    sudo cp -rvf $ROOT_DIR/lib/avro /lib/flume-ng/plugins.d/arouter
    sudo chown -R flume.flume /lib/flume-ng/plugins.d/arouter/avro
    sudo chmod -R 644 /lib/flume-ng/plugins.d/arouter/avro
    sudo chmod -R +X /lib/flume-ng/plugins.d/arouter/avro
    sudo mkdir -p /usr/lib/flume-ng/plugins.d/arouter/store
    sudo chown -R flume.flume /lib/flume-ng/plugins.d/arouter/store
    sudo chmod 666 /etc/flume-ng/conf/flume.conf
    sudo echo "$FLUME_AGENT_CONFIG" > /etc/flume-ng/conf/flume.conf
    sudo chmod 600 /etc/flume-ng/conf/flume.conf
    sudo chown flume.flume /etc/flume-ng/conf/flume.conf
    $START_DEPLOYMENT && sudo service flume-ng-agent start
  fi
fi

if $REMOTE_DEPLOY; then
  if [ -f $ROOT_DIR/lib/parcel/parcel.env ]; then
    id -u $PARCEL_NAMESPACE &>/dev/null || sudo useradd $PARCEL_NAMESPACE
    $ROOT_DIR/lib/manager/python/deploy.py \
      --user $MANAGER_SERVER_USER \
      --password $MANAGER_SERVER_PWORD \
      --man_host $MANAGER_SERVER_HOST \
      --parcel_name $PARCEL_NAME \
      --parcel_version $PARCEL_VERSION \
      --parcel_repo $PARCEL_REPO/$PARCEL_VERSION_SHORT \
      --init_pre_dir $ROOT_DIR/bin/init/pre \
      --init_post_dir $ROOT_DIR/bin/init/post
  fi
fi
