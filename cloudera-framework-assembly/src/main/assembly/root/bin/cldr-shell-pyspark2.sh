#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "$BASH_SOURCE" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x -e

WAIT_TASK=${1:-"false"}
JOB_NAME=${2:-"my-job"}
JOB_MAINPY=${3:-"my_job.py"}
JOB_ARGS=${4:-""}
SPARK_ARGS=${5:-"--num-executors 5 --executor-cores 1 --executor-memory 1G"}
S3_LIB=${6:-"s3a://my-bucket/py/"}
CLUSTER_NAME=${7:-"$CLUSTER_NAME"}

if [ "$USER_ADMIN" = true ]; then
  USER_APP=$USER_ADMIN_HDFS
fi

# TODO: Resolve lack of sudo and local user on CDSW nodes
#sudo su $USER_APP -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; spark-submit $@"
#bash -c "export HADOOP_CLASSPATH=$HADOOP_CLASSPATH; spark-submit $@"

if [ "$CLUSTER_PROVISION" = "altus" ]; then
  LIB_PYS=($(ls $ROOT_DIR/lib/py/*.py))
  LIB_PYS_CSV=''
  for LIB_PY in "${LIB_PYS[@]}"; do
    aws s3 cp "$LIB_PY" "${S3_LIB/s3a:\/\//s3://}"
    [[ ! -z "$LIB_PYS_CSV" ]] && LIB_PYS_CSV="$LIB_PYS_CSV"','
    LIB_PYS_CSV="$LIB_PYS_CSV"'"'"$S3_LIB"$(basename "$LIB_PY")'"'
  done
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
              "pySparkJob": {
                "mainPy": "'"$S3_LIB""$JOB_MAINPY"'",
                "pyFiles": ['"$LIB_PYS_CSV"'],
                "applicationArguments": ['"$JOB_ARGS_CSV"'],
                "sparkArguments": "'"$SPARK_ARGS"" --conf spark.pyspark.python=/tmp/pyspark-env/bin/python"'"
              }
            }'
  if [ "$WAIT_TASK" = "true" ]; then
    CLUSTER_CRN=$(altus dataeng list-clusters --cluster-name "$CLUSTER_NAME" | grep \"crn\" | awk '{print $2}' | sed 's/"//g' | sed 's/,//g')
    while [ $(altus dataeng list-jobs --cluster-crn $CLUSTER_CRN --job-statuses QUEUED SUBMITTING RUNNING TERMINATING | grep status | wc -l) -ne 0 ]; do
      echo "Waiting for jobs to complete ... " && sleep 5
    done
  fi
else
  LIB_PY=($(ls $ROOT_DIR/lib/jar/*.jar))
  LIB_PYS=($(ls $ROOT_DIR/lib/jar/dep/*.jar))
  LIB_PYS_CSV=''
  for LIB_JAR in "${LIB_PYS[@]}"; do
    [[ ! -z "$LIB_PYS_CSV" ]] && LIB_PYS_CSV="$LIB_PYS_CSV"','
    LIB_PYS_CSV="$LIB_PYS_CSV""file://""$LIB_JAR"
  done
  spark-submit2 \
    --name "$JOB_NAME"
    --master yarn \
    --deploy-mode cluster \
    --jars "$LIB_PYS_CSV" \
    --class "$JOB_CLASS" \
    $SPARK_ARGS \
    $LIB_JAR \
    $JOB_ARGS
fi
