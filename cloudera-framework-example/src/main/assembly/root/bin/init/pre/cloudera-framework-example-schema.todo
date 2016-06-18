#!/bin/bash

# TODO: Convert to parcel defines
#
#if [ $($ROOT_DIR/lib/bin/cloudera-framework-impala.sh -q "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN|wc -l) -eq 0 ]; then
#  
#  HIVE_HIVESERVER2_HOSTS_ARRAY=(${HIVE_HIVESERVER2_HOSTS//,/ })
#  for HIVE_HIVESERVER2_HOST in "${HIVE_HIVESERVER2_HOSTS_ARRAY[@]}"; do
#    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$HIVE_HIVESERVER2_HOST \
#  	  "mkdir -p $HIVESERVER2_DIR_LIB"
#    scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY \
#      $ROOT_DIR/lib/jar/*.jar $ROOT_DIR/lib/jar/dep/*.jar $CLUSTER_NODE_USER@$HIVE_HIVESERVER2_HOST:$HIVESERVER2_DIR_LIB
#  done  
#  
#  python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$HIVESERVER2_DIR_LIB" << END
#import sys
#from cm_api import api_client
#from cm_api.api_client import ApiResource
#def main(argv):
#  print ''
#  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
#  for cluster in api.get_all_clusters():
#    for service in cluster.get_all_services():
#      if service.type == 'HIVE':
#        print 'Updating Hive config ...'
#        service.update_config({'hive_aux_jars_path_dir': argv[5]})
#        print 'Restarting Hive service ...'
#        service.restart().wait()
#  return 0
#if __name__ == '__main__':
#  sys.exit(main(sys.argv))
#END