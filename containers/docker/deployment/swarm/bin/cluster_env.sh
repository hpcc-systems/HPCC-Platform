#!/bin/bash

SCRIPT_HOME=$(dirname $0)
. ${SCRIPT_HOME}/common

function usage()
{
 cat <<EOF
    Push environment.xml from admin node /etc/HPCCSystems/source/ to cluster nodes
    Usage: $(basename $0) <options>
      <options>:
      -a: app name. The default is hpcc
      -d: a directory contain environment.xml.xml in admin node
      -D: a directory contain environment.xml.xml in local host

EOF
   exit 2
}

app_name=hpcc
nodeDir=
hostDir=

# Process command-line parameters
while getopts "*a:d:D:h" arg
do
   case $arg in
      a) app_name=${OPTARG}
         ;;
      d) nodeDir=${OPTARG}
         ;;
      D) comp=${OPTARG}
         ;;
      h) usage
         ;;
      ?)
         echo "Unknown option $OPTARG"
         usage
         ;;
   esac
done

cid=$(${SCRIPT_HOME}/cluster_query.sh -a $app_name -q id -g admin)

if [ -n "$hostDir" ]
then
   env_file=${hostDir}/environment.xml
   if [ ! -e "${env_file}" ]
   then
      echo "${env_file} doesn't exist"
      exit 1
   fi
   $DOCKER_SUDO docker cp ${env_file} ${cid}:/etc/HPCCSystems/source/
   if [ $? -ne 0 ]
   then
       echo "Failed to copy environment.xml from local $hostDir to /etc/HPCCSystems/source/"
       exit 1
   fi
elif [ -n "$nodeDir" ]
then
   env_file=${nodeDir}/environment.xml
   $DOCKER_SUDO docker exec $cid cp $env_file /etc/HPCCSystems/source/
   if [ $? -ne 0 ]
   then
       echo "Failed to copy environment.xml from $nodeDir to /etc/HPCCSystems/source/"
       exit 1
   fi
fi

$DOCKER_SUDO docker exec $cid /opt/hpcc-tools/stop_hpcc.sh
$DOCKER_SUDO docker exec $cid /opt/hpcc-tools/push_env.sh
$DOCKER_SUDO docker exec $cid /opt/hpcc-tools/start_hpcc.sh
