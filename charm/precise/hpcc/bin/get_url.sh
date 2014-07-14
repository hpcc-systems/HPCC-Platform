#!/bin/bash


function get_service_information()
{


   service_info=$(python ${ABS_CWD}/parse_status.py)
   service_list=( $service_info )
   for item in "${service_list[@]}"
   do
       key=$(echo $item | cut -d '=' -f1)
       value=$(echo $item | cut -d '=' -f2)
       if [ "$key" = "service_name" ]
       then
         service_name=$value
         break
       fi
   done
}


##
## Main
##################

CWD=$(dirname $0)

CUR_DIR=$(pwd)
cd $CWD
ABS_CWD=$(pwd)
cd $CUR_DIR

CHARM_NAME=$(basename $(dirname $ABS_CWD))


unit_name=$1
service_name=

eclwatch_url_file=eclwatch_url.txt
LOCAL_ENV_FILE=/tmp/environment.xml
LOCAL_URL_FILE=/tmp/${eclwatch_url_file}

[ -e $LOCAL_ENV_FILE ] && rm -rf  $LOCAL_ENV_FILE
[ -e $LOCAL_URL_FILE ] && rm -rf  $LOCAL_URL_FILE


if [ -z "$unit_name" ]
then
  get_service_information
  unit_name=$(juju status | grep "${service_name}/" | \
    /usr/bin/head -n 1 | cut -d: -f1)
fi

juju scp ${unit_name}:/etc/HPCCSystems/environment.xml $LOCAL_ENV_FILE  > /dev/null 2>&1

juju scp ${unit_name}:/var/lib/HPCCSystems/charm/${eclwatch_url_file} $LOCAL_URL_FILE > /dev/null 2>&1

if [ -e $LOCAL_URL_FILE ]
then
   echo "ECLWatch URL: $(cat $LOCAL_URL_FILE)"
else
   echo "Failed to get $eclwatch_url_file from unit ${unit_name}"
fi

if [ -e $LOCAL_ENV_FILE ]
then
   echo "HPCC environment.xml is available under /tmp/"
else
   echo "Failed to get environment.xml from unit ${unit_name}"
fi
