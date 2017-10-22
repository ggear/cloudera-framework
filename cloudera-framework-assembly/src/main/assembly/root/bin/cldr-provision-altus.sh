#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

DELETE_CLUSTER=${1:-"false"}
PROXY_CONNECT=${2:-"false"}
SERVICE_TYPE=${3:-"SPARK"}
WORKERS_NUMBER=${4:-"3"}
CLUSTER_NAME=${5:-"graham-test"}
INSTANCE_TYPE=${6:-"m4.large"}
CDH_VERSION=${7:-"CDH512"}
SSH_KEY=${8:-"file:///Users/graham/.ssh/director"}
ALTUS_ENV=${9:-"anz_se_southeast2"}
MANAGER_SERVER_USER=${10:-"graham"}
MANAGER_SERVER_PWORD=${11:-"sp@rk2.2"}

if [ "$DELETE_CLUSTER" = "true" ]; then
  altus dataeng delete-cluster --cluster-name=$CLUSTER_NAME
else
  if [ $(altus dataeng list-clusters --cluster-names $CLUSTER_NAME 2>&1 | grep "No cluster found" | wc -l) -ne 0 ]; then
    altus dataeng create-aws-cluster \
      --service-type=$SERVICE_TYPE\
      --workers-group-size=$WORKERS_NUMBER \
      --cluster-name=$CLUSTER_NAME \
      --instance-type=$INSTANCE_TYPE \
      --cdh-version=$CDH_VERSION \
      --ssh-private-key=$SSH_KEY \
      --environment-name=$ALTUS_ENV \
      --cloudera-manager-username=$MANAGER_SERVER_USER \
      --cloudera-manager-password=$MANAGER_SERVER_PWORD
  fi
  while [ $(altus dataeng list-clusters --cluster-names $CLUSTER_NAME | grep status | grep CREATED | wc -l) -eq 0 ]; do
    echo "Waiting for cluster to come up ... " && sleep 5
  done
  if [ "$PROXY_CONNECT" = "true" ]; then
    altus dataeng socks-proxy --cluster-name $CLUSTER_NAME --ssh-private-key=~/.ssh/director --open-cloudera-manager="yes"
  fi
fi
