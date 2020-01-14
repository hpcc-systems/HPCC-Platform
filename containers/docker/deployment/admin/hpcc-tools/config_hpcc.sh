#!/bin/bash -x
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
################################################################################

SCRIPT_DIR=$(dirname $0)

if [ -n "${KUBERNETES_PORT}" ]
then
   CLOUD_PLATFORM=kubernetes
   source ${SCRIPT_DIR}/kube/config_hpcc_functions
else
   source ${SCRIPT_DIR}/docker/config_hpcc_functions
fi


function usage()
{
    cat <<EOF
    Usage: $(basename $0) <options>
      <options>:
      -a: app name for docker stack. The default is hpcc
      -D: HPCC home directory. The default is /opt/HPCCSystems
      -d: directory to save collecting ips. The default is /tmp/ips
      -e: number of esp nodes. The default is 1
      -n: network name for docker stack. The default is <appName>_ovnet.
      -N: do not push environment.xml and restart environment.xml
      -r: number of roxie nodes
      -s: number of support nodes
      -t: number of thor nodes
      -u: update mode. It will only re-create dali/thor master environment.xml
          and environment.xml with real ip. Re-generate ansible host file,
          run updtdalienv and restart thor master.
      -x: do not retrieve cluster ips. The ips should be under directory /tmp/
          cluster ips file name <app name>_<network name.json for docker and
          pods_ip.lst kubernetes
      -X  do not generate environmen.xml which may be created with configmgr

EOF
   exit 1
}

function create_ips_string()
{
   IPS=
   [ ! -e "$1" ] &&  return

   while read ip
   do
      ip=$(echo $ip | sed 's/[[:space:]]//g;s/;//g')
      [ -n "$ip" ] && IPS="${IPS}${ip}\\;"
   done < $1
}

function create_simple_envxml()
{
   # if there is node file it should define supportnode, espnode (optioal), roxienode and thornode
   #    use envgen to generate environment.xml

   if [ -n "${numSupport}" ]
   then
       support_nodes=${numSupport}
   elif [ -n "${SUPPORT_NODES}" ]
   then
       support_nodes=${SUPPORT_NODES}
   else
       support_nodes=1
   fi

   if [ -n "${numEsp}" ]
   then
       esp_nodes=${numEsp}
   elif [ -n "${ESP_NODES}" ]
   then
       esp_nodes=${ESP_NODES}
   else
       esp_nodes=1
   fi

   if [ -n "${numThor}" ]
   then
       thor_nodes=${numThor}
   elif [ -n "${THOR_NODES}" ]
   then
       thor_nodes=${THOR_NODES}
   else
       thor_nodes=1
   fi

   if [ -n "${numRoxie}" ]
   then
       roxie_nodes=${numRoxie}
   elif [ -n "${ROXIE_NODES}" ]
   then
       roxie_nodes=${ROXIE_NODES}
   else
       roxie_nodes=1
   fi

   create_ips_string  ${ipDir}/esp
   cmd="$SUDOCMD ${HPCC_HOME}/sbin/envgen -env ${wkDir}/${ENV_XML_FILE}   \
       -override roxie,@copyResources,true \
       -override roxie,@roxieMulticastEnabled,false \
       -override thor,@replicateOutputs,true \
       -override esp,@method,htpasswd \
       -override thor,@replicateAsync,true                 \
       -thornodes ${thor_nodes} -slavesPerNode ${slaves_per_node} \
       -espnodes ${esp_nodes} -roxienodes ${roxie_nodes} \
       -supportnodes ${support_nodes} -roxieondemand 1 \
       -ipfile ${ipDir}/node -assign_ips esp $IPS"

    echo "$cmd"
    eval "$cmd"

}

function create_complex_envxml()
{
   if [ ! -e ${ipDir}/support ]
   then
       echo "Can't find support node ip"
       exit 1
   fi

   support_nodes=$(cat ${ipDir}/support | wc -l)

   if [ -n "${ESP_NODES}" ]
   then
        esp_nodes=${ESP_NODES}
   elif [ -e ${ipDir}/esp-* ]
   then
        esp_nodes=0
   else
        esp_nodes=1
   fi

   if [ -n "${THOR_NODES}" ]
   then
       thor_nodes=${THOR_NODES}
   else
       thor_nodes=0
   fi

   if [ -n "${ROXIE_NODES}" ]
   then
       roxie_nodes=${ROXIE_NODES}
   else
       roxie_nodes=0
   fi

   cmd="$SUDOCMD ${HPCC_HOME}/sbin/envgen2 -env ${wkDir}/${ENV_XML_FILE} \
       -thornodes ${thor_nodes} -slavesPerNode ${slaves_per_node} \
       -espnodes ${esp_nodes} -roxienodes ${roxie_nodes} \
       -supportnodes ${support_nodes} -roxieondemand 1 \
       -ipfile ${ipDir}/support"

   process_comp_settings support ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}

   spark_entry=$(grep sparkthor ${HPCC_HOME}/componentfiles/configxml/buildset.xml)
   [ "$spark_entry" = "sparkthor" ] && cmd="$cmd -rmv spark#mysparkthor:Instance@netAddress=."

   echo "$cmd"
   eval "$cmd"

   #cp   ${wkDir}/env_base.xml ${wkDir}/env_dali.xml

   if [ -e ${ipDir}/dali ]
   then
      ip=$(cat ${ipDir}/dali | sed 's/[[:space:]]//g; s/;//g')
      $SUDOCMD ${HPCC_HOME}/sbin/envgen2 -env-in ${wkDir}/${ENV_XML_FILE} \
          -env-out ${wkDir}/${ENV_XML_FILE} -mod-node dali#mydali@ip=${ip}

      process_comp_settings dali ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   fi

   if [ -e ${ipDir}/esp-* ]
   then
      esp_ip=$(${HPCC_HOME}/sbin/configgen -env ${wkDir}/${ENV_XML_FILE} -listall2 | grep EspProcess | cut -d',' -f3)
      if [ -n "${esp_ip}" ]
      then
        cmd="$SUDOCMD ${HPCC_HOME}/sbin/envgen2 -env-in ${wkDir}/${ENV_XML_FILE} -env-out ${wkDir}/${ENV_XML_FILE} \
            -rmv sw:esp#myesp:Instance@netAddress=${esp_ip}"

        echo "$cmd"
        eval "$cmd"
      fi

   fi

   add_comp_to_envxml esp myesp ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   add_roxie_to_envxml ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   add_thor_to_envxml ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   add_comp_to_envxml eclcc myeclccserver ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   add_comp_to_envxml scheduler myscheduler ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   add_comp_to_envxml spark mysparkthor ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}

   # Create topology
   create_topology ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}

   # Override attributes
   #${ENV_XML_FILE}
   #cmd="$SUDOCMD ${HPCC_HOME}/sbin/envgen2 -env-in ${wkDir}/${ENV_XML_FILE} \
   #    -env-out ${wkDir}/${ENV_XML_FILE}  \
   #    -override roxie,@copyResources,true \
   #    -override roxie,@roxieMulticastEnabled,false \
   #    -override thor,@replicateOutputs,true \
   #   -override esp,@method,htpasswd "

   #echo "$cmd"
   #eval "$cmd"

}


function create_envxml()
{

   if [ -e ${ipDir}/node ]
   then
       create_simple_envxml
   else
       create_complex_envxml
   fi

   process_category_settings ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   process_override_settings ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   process_xpathattrs_settings ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
   add_xml_contents ${wkDir}/${ENV_XML_FILE} ${wkDir}/${ENV_XML_FILE}
}

function adjust_node_type_for_ansible()
{
    [ -d ${ipDir2} ] && rm -rf ${ipDir2}
    mkdir -p ${ipDir2}

    found_dali=false
    cluster_node_types=$(cat ${wkDir}/hpcc.conf | grep -i "cluster_node_types" | cut -d'=' -f2)
    for node_type in $(echo ${cluster_node_types} | sed 's/,/ /g')
    do
         [ "$node_type" = "dali" ] && found_dali=true
         node_type2=$(echo $node_type | cut -d'-' -s -f1);
         [ -z "$node_type2" ] &&  node_type2=$node_type
         cluster_name=$(echo $node_type | cut -d'-' -s -f2);
         [ -n "$cluster_name" ] && node_type2=${node_type2}-${cluster_name}
         cp ${ipDir}/${node_type} ${ipDir2}/${node_type2}
    done

    [ "${found_dali}" = "true" ] && return  || :
    [ -n "$cluster_node_types" ] && cluster_node_types="${cluster_node_types}," || :
    cluster_node_types="${cluster_node_types}dali"
    echo "cluster_node_types=${cluster_node_types}" > ${wkDir}/hpcc.conf

    # Find dali ip
    dali_ip=$($SUDOCMD /opt/HPCCSystems/sbin/configgen -env ${clusterConfigDir}/environment.xml -listall -t dali | cut -d',' -f3)
    echo $dali_ip > ${ipDir2}/dali

    if [ -e ${ipDir2}/node ]
    then
         node_to_process=node
    else
         node_to_process=support
    fi
    cat ${ipDir2}/${node_to_process} | grep -v ${dali_ip} > ${wkDir}/node_tmp || :

    mv ${wkDir}/node_tmp ${ipDir2}/${node_to_process}
    non_dali_nodes=$(cat ${ipDir2}/${node_to_process} | wc -l)
    if [ ${non_dali_nodes} -eq 0 ]
    then
        rm -rf ${ipDir2}/${node_to_process}
    fi
    hpcc_config=$(ls ${ipDir2} | grep -v "admin" | tr '\n' ',')
    echo "cluster_node_types=${hpcc_config%,}" > ${wkDir}/hpcc.conf
}

function setup_ansible_hosts()
{
  $SUDOCMD ${SCRIPT_DIR}/ansible/setup.sh -d ${ipDir2} -c ${wkDir}/hpcc.conf
  export ANSIBLE_HOST_KEY_CHECKING=False
}

#------------------------------------------
# Need root or sudo
#
SUDOCMD=
[ $(id -u) -ne 0 ] && SUDOCMD=sudo


#------------------------------------------
# LOG

#
LOG_DIR=~/tmp/log/hpcc-tools
mkdir -p $LOG_DIR
LONG_DATE=$(date "+%Y-%m-%d_%H-%M-%S")
LOG_FILE=${LOG_DIR}/config_hpcc_${LONG_DATE}.log
touch ${LOG_FILE}
exec 2>$LOG_FILE
set -x

update=0
appName=hpcc
ipDir=~/tmp/ips
ipDir2=~/tmp/ips2
wkDir=~/tmp/work
notPush=0
notGetIps=0
notCreateEnv=0
HPCC_HOME=/opt/HPCCSystems
clusterConfigDir=/etc/HPCCSystems/source
numSupport=
numEsp=
numRoxie=
numThor=
networkName=${appName}_ovnet

[ ! -d ${wkDir} ] && mkdir -p ${wkDir}

while getopts "*a:d:D:e:hn:Nr:s:t:uxX" arg
do
   case $arg in
      a) appName=${OPTARG}
         ;;
      d) ipDir=${OPTARG}
         ;;
      D) HPCC_HOME=${OPTARG}
         ;;
      e) numEsp=${OPTARG}
         ;;
      h) usage
         ;;
      n) networkName=${OPTARG}
        ;;
      N) notPush=1
        ;;
      r) numRoxie=${OPTARG}
         ;;
      s) numSupport=${OPTARG}
         ;;
      t) numThor=${OPTARG}
         ;;
      u) update=1
        ;;
      x) notGetIps=1
        ;;
      X) notCreateEnv=1
        ;;
      ?)
         echo "Unknown option $OPTARG"
         usage
         ;;
   esac
done

echo "update mode: $update"
#----------------------------------------o
# Start sshd
#
ps -efa | grep -v sshd |  grep -q sshd ||  $SUDOCMD mkdir -p /var/run/sshd; $SUDOCMD  /usr/sbin/sshd -D &

#------------------------------------------
# Collect containers' ips
#
if [ "${CLOUD_PLATFORM}" = "kubernetes" ]
then
  # This is Kubernetes
  cluster_ips="pods_ip.lst"
else
  # This is Docker Stack
  cluster_ips=${networkName}.json
fi
[ -d ${ipDir} ] && rm -rf ${ipDir}
collect_ips

#------------------------------------------
# handle update
if [ $update  -eq 1 ] && [ -d ${wkDir}/ips ]
then
    diff ${ipDir} ${wkDir}/ips > /tmp/ips.diff
    ips_diff_size=$(ls -s /tmp/ips_diff.txt | cut -d' ' -f1)
    [ $ips_diff_size -eq 0 ] && exit 0
fi

#------------------------------------------
# Create HPCC components file
#
hpcc_config=$(ls ${ipDir} | grep -v "admin" | tr '\n' ',')
echo "cluster_node_types=${hpcc_config%,}" > ${wkDir}/hpcc.conf

#backup
[ -d ${wkDir}/ips ] && rm -rf ${wkDir}/ips
cp -r ${ipDir} ${wkDir}/
ENV_XML_FILE=environment.xml

mkdir -p ${wkDir}/tmp

#------------------------------------------
# Generate environment.xml
#
#set_vars_for_envgen2
if [ $notCreateEnv -eq 0 ]
then
   slaves_per_node=1
   [ -n "$SLAVES_PER_NODE" ] && slaves_per_node=${SLAVES_PER_NODE}
   create_envxml
   mkdir -p $clusterConfigDir
   cp ${wkDir}/environment.xml  ${clusterConfigDir}/
fi

#------------------------------------------
# Setup Ansible hosts
#
adjust_node_type_for_ansible
setup_ansible_hosts
dali_ip=$(cat /etc/ansible/ips/dali)


#  set_hpcc_data_owner
echo "Stop HPCC Cluster"
${SCRIPT_DIR}/stop_hpcc.sh

if [ $notPush -ne 1 ]
then
  echo "Push environment.xml to HPCC Cluster"
  ${SCRIPT_DIR}/push_env.sh
fi

echo "Start HPCC Cluster"
${SCRIPT_DIR}/start_hpcc.sh

set +x
echo "$SUDOCMD /opt/HPCCSystems/sbin/configgen -env ${clusterConfigDir}/environment.xml -listall2"
$SUDOCMD /opt/HPCCSystems/sbin/configgen -env ${clusterConfigDir}/environment.xml -listall2
echo "HPCC cluster configuration is done."
