#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"false"}
JOB_NAME=${2:-"my-job"}
JOB_CLASS=${3:-"com.my.Job"}
JOB_ARGS=${4:-""}
SPARK_ARGS=${5:-"--num-executors 5 --executor-cores 1 --executor-memory 1G"}
S3_LIB=${6:-"s3a://my-bucket/jar/"}
CLUSTER_NAME=${7:-"$CLUSTER_NAME"}

if [ "$USER_ADMIN" = true ]; then
  USER_APP=$USER_ADMIN_HDFS
fi

# TODO: Resolve lack of sudo and local user on CDSW nodes
#sudo su $USER_APP -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; spark-submit $@"
#bash -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; spark-submit $@"

if [ "$CLUSTER_PROVISION" = "altus" ]; then
  LIB_JARS=($(ls $ROOT_DIR/lib/jar/dep/*.jar))
  LIB_JARS+=($(ls $ROOT_DIR/lib/jar/*.jar))
  LIB_JARS_CSV=''
  for LIB_JAR in "${LIB_JARS[@]}"; do
    [[ ! -z "$LIB_JARS_CSV" ]] && LIB_JARS_CSV="$LIB_JARS_CSV"','
    LIB_JARS_CSV="$LIB_JARS_CSV"'"'"$S3_LIB"$(basename "$LIB_JAR")'"'
  done
  aws s3 sync "$ROOT_DIR/lib/jar/dep" "${S3_LIB/s3a:\/\//s3://}"
  aws s3 cp $ROOT_DIR/lib/jar/*.jar "${S3_LIB/s3a:\/\//s3://}"
  JOB_ARGS=($JOB_ARGS)
  JOB_ARGS_CSV=""
  for JOB_ARG in "${JOB_ARGS[@]}"; do
    [[ ! -z "$JOB_ARGS_CSV" ]] && JOB_ARGS_CSV="$JOB_ARGS_CSV"','
    JOB_ARGS_CSV="$JOB_ARGS_CSV"'"'$JOB_ARG'"'
  done
  altus dataeng submit-jobs \
    --cluster-name "$CLUSTER_NAME" \
    --jobs '{
              "name": "'"$JOB_NAME"'",
              "sparkJob": {
                "jars": ['"$LIB_JARS_CSV"'],
                "mainClass": "'"$JOB_CLASS"'",
                "applicationArguments": ['"$JOB_ARGS_CSV"'],
                "sparkArguments": "'"$SPARK_ARGS"'"
              }
            }'
  if [ "$WAIT_TASK" = "true" ]; then
    CLUSTER_CRN=$(altus dataeng list-clusters --cluster-name "$CLUSTER_NAME" | grep \"crn\" | awk '{print $2}' | sed 's/"//g' | sed 's/,//g')
    while [ $(altus dataeng list-jobs --cluster-crn $CLUSTER_CRN --job-statuses QUEUED SUBMITTING RUNNING TERMINATING | grep status | wc -l) -ne 0 ]; do
      echo "Waiting for jobs to complete ... " && sleep 5
    done
  fi
else
  LIB_JAR=($(ls $ROOT_DIR/lib/jar/*.jar))
  LIB_JARS=($(ls $ROOT_DIR/lib/jar/dep/*.jar))
  LIB_JARS_CSV=''
  for LIB_JAR in "${LIB_JARS[@]}"; do
    [[ ! -z "$LIB_JARS_CSV" ]] && LIB_JARS_CSV="$LIB_JARS_CSV"','
    LIB_JARS_CSV="$LIB_JARS_CSV""file://""$LIB_JAR"
  done
  spark-submit2 \
    --name "$JOB_NAME"
    --master yarn \
    --deploy-mode cluster \
    --jars "$LIB_JARS_CSV" \
    --class "$JOB_CLASS" \
    $SPARK_ARGS \
    $LIB_JAR \
    $JOB_ARGS
fi
