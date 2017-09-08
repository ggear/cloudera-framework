#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

for env in $ROOT_DIR/cfg/*.env; do if [ -f "$env" ]; then . "$env"; fi; done

set -x

REPORT_ONLY=${1:-"false"}
MANAGER_SERVER_USER=${2:-"admin"}
MANAGER_SERVER_PWORD=${3:-"admin"}
SLEEP_PERIOD_S=${4:-"5"}
STREAM_PERIOD_S=${5:-"35"}

function benchmark_failed {
  set +x
  echo "" && echo "-------------------------------------------------------------------------------"
  echo "Benchmark pipeline failed, see above"
  echo "-------------------------------------------------------------------------------" && echo ""
  exit 1
}

if ! $REPORT_ONLY; then
  { $ROOT_DIR/bin/cldr-deploy.sh; } || { benchmark_failed; }
  START_SEC=$(date +%s)
  { $ROOT_DIR/bin/cldr-data-stream.sh true || benchmark_failed; } && { sleep $STREAM_PERIOD_S; }
  { $ROOT_DIR/bin/cldr-data-stream.sh false && sleep $SLEEP_PERIOD_S; } || { benchmark_failed; }
  [ $(find $ROOT_DIR/bin -maxdepth 1 -type f -name "*-process.sh" | wc -l) -gt 1 ] && { echo "Error - multiple process scripts found" && benchmark_failed; }
  [ $(find $ROOT_DIR/bin -maxdepth 1 -type f -name "*-process.sh" | wc -l) -eq 1 ] && { $ROOT_DIR/bin/*-process.sh && sleep $SLEEP_PERIOD_S || benchmark_failed; }
  { $ROOT_DIR/bin/cldr-data-query.sh && sleep $SLEEP_PERIOD_S; } || { benchmark_failed; }
  FINISH_SEC=$(date +%s)
  DURATION_SEC=$((FINISH_SEC-START_SEC))
fi
{ $ROOT_DIR/lib/manager/python/benchmark.py \
  --user "$MANAGER_SERVER_USER" \
  --password "$MANAGER_SERVER_PWORD" \
  --man_host "$MANAGER_SERVER_HOST" \
  --nav_host "$MANAGER_NAVIGATORMETASERVER_HOST" \
  --app_name "$PARCEL_NAME_LONG" \
  --app_version "$PARCEL_VERSION" \
  --app_namespace "$PARCEL_NAMESPACE" \
  --app_time "$DURATION_SEC" \
  --app_start "$START_SEC" \
  --app_end "$FINISH_SEC" \
  --app_dashboard "$ROOT_DIR/lib/manager/dashboard/release.json" \
  --app_report_only "$REPORT_ONLY"; } || { benchmark_failed; }
