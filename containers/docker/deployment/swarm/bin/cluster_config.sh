#!/bin/bash -e

SCRIPT_HOME=$(dirname $0)
. ${SCRIPT_HOME}/common


function usage()
{

 cat <<EOF
    Usage: $(basename $0) <options>
      <options>:
      -a: app name. The default is hpcc
      -e: number of esp nodes
      -r: number of roxie node.
      -s: number of support nodes.
      -t: number of thor nodes.
      -h: print this help

EOF
   exit 2
}


app_name=hpcc
network_name=
numEsp=
numRoxie=
numSupport=
numThor=
notGenEnv=0

# Process command-line parameters
while getopts "*a:e:hr:s:t:X" arg
do
   case $arg in
      a) app_name=${OPTARG}
         ;;
      e) numEsp=${OPTARG}
         ;;
      r) numRoxie=${OPTARG}
         ;;
      s) numSupport=${OPTARG}
         ;;
      t) numThor=${OPTARG}
         ;;
      X) notGenEnv=1
         ;;
      h) usage
         ;;
      ?)
         echo "Unknown option $OPTARG"
         usage
         ;;
   esac
done

network_name=$(getNetWorkName)
echo "Application name: ${app_name},  network: $network_name"

cid=$($DOCKER_SUDO docker container ls | sed 's/[[:space:]][[:space:]]*/ /g' | grep " ${app_name}_admin\." | cut -d' ' -f1)
echo "admin container id: ${cid}"

echo "$DOCKER_SUDO docker network inspect ${network_name} > /tmp/${network_name}.json"
$DOCKER_SUDO docker network inspect ${network_name} > /tmp/${network_name}.json

cat /tmp/${network_name}.json | sed "s/^\[//; s/^\]//" > /tmp/${network_name}.json.tmp
mv /tmp/${network_name}.json.tmp /tmp/${network_name}.json

echo "$DOCKER_SUDO docker container cp /tmp/${network_name}.json $cid:/tmp/"
$DOCKER_SUDO docker container cp /tmp/${network_name}.json $cid:/tmp/

#$DOCKER_SUDO docker container exec $cid ls -l /tmp/${network_name}.json

cmd="${DOCKER_SUDO} docker container exec $cid /opt/hpcc-tools/config_hpcc.sh -a $app_name -n $network_name -x"

if [ $notGenEnv -eq 0 ]
then
   [ -n "$numSupport" ] && cmd="$cmd -s $numSupport"
   [ -n "$numEsp" ] && cmd="$cmd -e $numEsp"
   [ -n "$numRoxie" ] && cmd="$cmd -e $numRoxie"
   [ -n "$numThor" ] && cmd="$cmd -e $numThor"
else
   cmd="$cmd -X"
   $DOCKER_SUDO container exec $cid mkdir -p /etc/HPCCSystems/cluster
   $DOCKER_SUDO container exec $cid mkdir -p /etc/HPCCSystems/source/environment.xml ../cluster/
fi
echo "$cmd"
eval $cmd

echo ""
echo "Status"
${SCRIPT_HOME}/cluster_run.sh -a $app_name status

echo ""
if [ $? -eq 0 ]
then
   echo "Configure Cluster successfully"
else
   echo "Configure Cluster failed"
fi
