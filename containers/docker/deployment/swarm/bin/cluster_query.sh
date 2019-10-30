#!/bin/bash

SCRIPT_HOME=$(dirname $0)

. ${SCRIPT_HOME}/common


function usage()
{

 cat <<EOF
    Usage: $(basename $0) <options>
      <options>:
      -a: app name. The default is hpcc
      -c: component name. DaliServerProcess, Dai, ThorMaster, ThorSlave, .* (all) sourceetc
      -d: directory to save collecting ips.
      -g: ip group by service defined in Docker compose config file.
          For example: support, thor, thor-mythor, roxie, or hpcc, etc
      -p: ip
      -q: query : ${query_usage}

EOF
   exit 2
}

function getIdByIp()
{
    cmd="$DOCKER_SUDO docker ps -q | xargs $DOCKER_SUDO docker inspect --format \
        '{{ .Config.Hostname }} {{ .NetworkSettings.Networks.${network_name}.IPAMConfig.IPv4Address }}' \
         | grep $1 |  cut -d' ' -f1"
    QUERY_RESULT=$(eval $cmd)
}

function getIpById()
{
    cmd="SDOCKER_SUDO docker ps -q | xargs $DOCKER_SUDO docker inspect --format \
        '{{ .Config.Hostname }} {{ .NetworkSettings.Networks.${network_name}.IPAMConfig.IPv4Address }}' \
         | grep $1 |  cut -d' ' -f2"
    QUERY_RESULT=$(eval $cmd)
}

function getIpsByGroup()
{
    cmd="$DOCKER_SUDO docker ps -q | xargs $DOCKER_SUDO docker inspect --format \
        '{{ .Name }} {{ .NetworkSettings.Networks.${network_name}.IPAMConfig.IPv4Address }}' \
         | grep \"^/${app_name}_$1\" |  cut -d' ' -f2"
    QUERY_RESULT=$(eval $cmd)
}

function getHPCCIps()
{
    cmd="$DOCKER_SUDO docker ps -q | xargs $DOCKER_SUDO docker inspect --format \
        '{{ .Name }} {{ .NetworkSettings.Networks.${network_name}.IPAMConfig.IPv4Address }}' \
         | grep  \"dali\|esp\|support\|roxie\|thor\|node\|eclcc\|scheduler\|backup\|sasha\|dropzone\|spark\""
    QUERY_RESULT=$(eval $cmd | sed "s/\/${app_name}_\([^\.]\+\)\..* /\1 /" | sort )
}

function getAdminId()
{
    QUERY_RESULT=$($DOCKER_SUDO docker container ls | sed 's/[[:space:]][[:space:]]*/ /g' | grep " ${app_name}_admin\." | cut -d' ' -f1)
}

function getCompByName()
{
    getAdminId
    QUERY_RESULT=$($DOCKER_SUDO docker exec ${QUERY_RESULT} /opt/HPCCSystems/sbin/configgen -env /etc/HPCCSystems/source/environment.xml -listall2 | grep $comp)

}

query_usage="ip (default), id, network, comp"

query=ip
app_name=hpcc
network_name=
comp=
cid=
ip=
ip_group=
QUERY_RESULT=

# Process command-line parameters
while getopts "*a:c:d:g:hi:p:q:" arg
do
   case $arg in
      a) app_name=${OPTARG}
         ;;
      d) ipDir=${OPTARG}
         ;;
      c) comp=${OPTARG}
         ;;
      g) ip_group=${OPTARG}
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

if [ -z "${query}" ]
then
    echo "Missing query request. Provide query wiht -q <value>"
    echo "Possible query: ${query_usage}"
    exit 1

fi

[ -z "$network_name" ] &&  network_name=$(getNetWorkName)
if [ -z "$network_name" ]
then
    echo "Can't get network name form app name $app_name"
    exit 1
fi

# Perform query
case $query in
   comp)
      if [ -n "$comp" ]
      then
          getCompByName $comp
      fi
      ;;
   ip)
      if [ -n "$cid" ]
      then
          getIpById $cid
      elif [ "$ip_group" = "hpcc" ]
      then
          getHPCCIps
      elif [ -n "$ip_group" ]
      then
          getIpsByGroup  $ip_group
      fi
      ;;
   id)
      if [ -n "$ip" ]
      then
          getIdByIp $ip
      elif [ "$ip_group" = "admin" ]
      then
          getAdminId
      fi
      ;;
   network)
      echo "$network_name"
      ;;
   ?)
      echo "Unknown query type"
      echo "Validated input for \"-q\":  ${query_usage}"
      exit 2
esac

if [ -n "$QUERY_RESULT" ]
then
    echo "$QUERY_RESULT"
fi

exit
