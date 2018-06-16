#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"true"}
DELETE_CLUSTER=${2:-"false"}
PROXY_CONNECT=${3:-"false"}
CLUSTER_NAME=${4:-"$CLUSTER_NAME"}
SERVICE_TYPE=${5:-"$CLUSTER_SERVICE_TYPE"}
WORKERS_NUMBER=${6:-"$CLUSTER_WORKERS_NUMBER"}
INSTANCE_TYPE=${7:-"$CLUSTER_INSTANCE_TYPE"}
CDH_VERSION=${8:-"$CLUSTER_CDH_VERSION"}
ALTUS_ENV=${9:-"$CLUSTER_ENVIRONMENT"}
SSH_KEY=${10:-"/Users/graham/.ssh/director"}
MANAGER_SERVER_USER=${11:-"cmuser"}
MANAGER_SERVER_PWORD=${12:-"Q_Dr@7bE"}

CREATE_TIMESTAMP="/tmp/.altus_provision_epoch"
BOOTSTRAP_FILE=$ROOT_DIR/bin/cldr-provision-altus-bootstrap.sh

[[ ! -f "$SSH_KEY" ]] && SSH_KEY="$ROOT_DIR/cfg/provision"

if [ "$DELETE_CLUSTER" = "true" ]; then
  TIME=$(date +%s)
  if [ $(altus dataeng list-clusters --cluster-names "$CLUSTER_NAME" 2>&1 | grep status | grep CREATED | wc -l) -eq 1 ]; then
    altus dataeng delete-cluster --cluster-name="$CLUSTER_NAME"
  fi
  if [ "$WAIT_TASK" = "true" ]; then
    while [ $(altus dataeng list-clusters --cluster-names "$CLUSTER_NAME" 2>&1 | grep NOT_FOUND | wc -l) -eq 0 ]; do
      echo "Waiting for cluster delete to finish ... " && sleep 5
    done
    TIME="$(($(date +%s) - $TIME))"
    echo "Cluster delete took ["$(printf '%02d:%02d:%02d\n' $(($TIME/3600)) $(($TIME%3600/60)) $(($TIME%60)))"] time"
    if [ -f "$CREATE_TIMESTAMP" ]; then
      TIME="$(($(date +%s) - $(cat $CREATE_TIMESTAMP)))"
      echo "Cluster spent up ["$(printf '%02d:%02d:%02d\n' $(($TIME/3600)) $(($TIME%3600/60)) $(($TIME%60)))"] time"
      rm -rf "$CREATE_TIMESTAMP"
    fi
  fi
else
  if [ $(altus dataeng list-clusters --cluster-names "$CLUSTER_NAME" 2>&1 | grep "No cluster found" | wc -l) -ne 0 ]; then
    echo "virtualenv /tmp/pyspark-env" > $BOOTSTRAP_FILE
    if [ -f $ROOT_DIR/lib/python/conda.yml ]; then
      LIB_PIPS=($(grep = $ROOT_DIR/lib/python/conda.yml))
      for LIB_PIP in "${LIB_PIPS[@]}"; do
        [[ "$LIB_PIP" != "-" ]] && \
          [[ $(echo $LIB_PIP | grep "pyspark=" | wc -l) -eq 0 ]] && \
          [[ $(echo $LIB_PIP | grep "python=" | wc -l) -eq 0 ]] && \
          echo "/tmp/pyspark-env/bin/pip install "$(echo "$LIB_PIP" | sed 's/=/==/g') >> $BOOTSTRAP_FILE
      done
    fi
    [[ ! -f "$SSH_KEY" ]] && ssh-keygen -N '' -f "$SSH_KEY"
    date +%s > "$CREATE_TIMESTAMP"
    altus dataeng create-aws-cluster \
      --service-type="$SERVICE_TYPE" \
      --workers-group-size="$WORKERS_NUMBER" \
      --cluster-name="$CLUSTER_NAME" \
      --instance-type="$INSTANCE_TYPE" \
      --cdh-version="$CDH_VERSION" \
      --ssh-private-key="file://$SSH_KEY" \
      --environment-name="$ALTUS_ENV" \
      --instance-bootstrap-script=file://"$BOOTSTRAP_FILE" \
      --cloudera-manager-username="$MANAGER_SERVER_USER" \
      --cloudera-manager-password="$MANAGER_SERVER_PWORD"
  fi
  if [ "$WAIT_TASK" = "true" ]; then
    while [ $(altus dataeng list-clusters --cluster-names "$CLUSTER_NAME" | grep status | grep CREATED | wc -l) -eq 0 ]; do
      echo "Waiting for cluster create to finish ... " && sleep 5
    done
    if [ -f "$CREATE_TIMESTAMP" ]; then
      TIME="$(($(date +%s) - $(cat $CREATE_TIMESTAMP)))"
      echo "Cluster create took ["$(printf '%02d:%02d:%02d\n' $(($TIME/3600)) $(($TIME%3600/60)) $(($TIME%60)))"] time"
    fi
  fi
  if [ "$PROXY_CONNECT" = "true" ]; then
    altus dataeng socks-proxy --cluster-name "$CLUSTER_NAME" --ssh-private-key="$SSH_KEY" --open-cloudera-manager="yes"
  fi
fi
