#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../../..

source $ROOT_DIR/bin/*.env

set -x

CMD_LINE_ARGUMENTS="$1"
STOP_AGENTS=${2:-false}
MANAGER_SERVER_USER=${3:-"admin"}
MANAGER_SERVER_PWORD=${4:-"admin"}
MANAGER_SERVER_HOST=${5:-"$MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${6:-"$MANAGER_SERVER_PORT"}

python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$STOP_AGENTS" << END
import sys
from cm_api import api_client
from cm_api.api_client import ApiResource
def main(argv):
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'FLUME':
		if argv[5] is 'true':
	        print 'Stopping flume agent ...'
	        service.stop().wait()
		else:
	        print 'Starting flume agent ...'
	        service.start().wait()
  return 0
if __name__ == '__main__':
  sys.exit(main(sys.argv))
END
