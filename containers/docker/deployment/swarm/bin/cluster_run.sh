#!/bin/bash

SCRIPT_HOME=$(dirname $0)
. ${SCRIPT_HOME}/common

function usage()
{
 cat <<EOF
    Usage: $(basename $0) <options>  <action>
      <options>:
      -a: app name. The default is hpcc
      -c: component name, for example, mythor, mydali, configmgr etc
      -i: container id
      -p: ip
      <action>:
         status, stop, start, restart
         For status id or ip must be provided. It can only check one node a time.

EOF
   exit 2
}

function runHPCC()
{
   [ -n "$ip" ] && cid=$(${SCRIPT_HOME}/cluster_query.sh -a $app_name -q id -p $ip)
   [ -z "$cid" ] && return 1
   cmd="$DOCKER_SUDO docker exec $cid /etc/init.d/hpcc-init"
   if [ -n "$comp_name" ]
   then
      cmd="$cmd -c $comp_name"
   fi
   cmd="$cmd $1"
   #echo "$cmd"
   eval "$cmd"
}

function runHPCCCluster()
{
   echo ""
   echo "###############################################"
   echo "#"
   echo "# $1 HPCC Cluster ..."
   echo "#"
   echo "###############################################"
   cid=$(${SCRIPT_HOME}/cluster_query.sh -a $app_name -q id -g admin)
   cmd="$DOCKER_SUDO docker exec $cid /opt/hpcc-tools/$1_hpcc.sh"
   echo "$cmd"
   eval "$cmd"

   echo ""
   echo "###############################################"
   echo "#"
   echo "# Status:"
   echo "#"
   echo "###############################################"
   ${SCRIPT_HOME}/cluster_run.sh status
}

function stxxxHPCC()
{
   if [ "$action" = "restart" ] || [ -z "$cid" ] && [ -z "$ip" ]
   then
       runHPCCCluster $1
       return
   fi
   runHPCC $1
}

app_name=hpcc
network_name=
comp_name=
cid=
ip=
action=

# Process command-line parameters
while getopts "*a:c:d:hi:p:q:" arg
do
   case $arg in
      a) app_name=${OPTARG}
         ;;
      d) ipDir=${OPTARG}
         ;;
      c) comp_name=${OPTARG}
         ;;
      i) cid=${OPTARG}
         ;;
      p) ip=${OPTARG}
         ;;
      q) query=${OPTARG}
         ;;
      h) usage
         ;;
      ?)
         echo "Unknown option $OPTARG"
         usage
         ;;
   esac
done

shift $((OPTIND -1))
action=$1

if [ -z "$action" ]
then
    echo "Missing action"
    usage
fi

case $action in
   status)
      if [ -z "$id" ] && [ -z "$ip" ]
      then
         rc=0
         ${SCRIPT_HOME}/cluster_query.sh -a $app_name -g hpcc | \
         while read line
         do
            node_name=$(echo $line | cut -d' ' -f1)
            ip=$(echo $line | cut -d' ' -f2)
            echo
            echo "Status of $node_name ($ip):"
            runHPCC status
            rc=$(expr $rc \+ $?)
         done
         exit $rc
      else
         runHPCC status
      fi
      ;;
   start)
      if [ "$comp_name" = "configmgr" ]
      then
         cid=$(${SCRIPT_HOME}/cluster_query.sh -a $app_name -q id -g admin)
         $DOCKER_SUDO docker exec -it $cid /opt/HPCCSystems/sbin/configmgr
         exit $?
      else
         stxxxHPCC start
      fi
      ;;
   stop)
      stxxxHPCC stop
      ;;
   restart)
      stxxxHPCC stop
      stxxxHPCC start
      ;;
   ?)
      echo "Unknown action $action"
      usage
esac
