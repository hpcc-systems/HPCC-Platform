#!/bin/bash

function usage()
{
   cat <<EOF

    Trigger HPCC envgen with charm configure changed hook
    Usage:  $(basename $0) <options>
    where
       -name <service name> :
          Juju charm service name. Then name can be provided at charm
          deploy name. The default is the same as charm name.  This will
          identify HPCC cluster.

       -supportnodes <number of support nodes>:
          Number of nodes to be used for non-Thor and non-Roxie components.
          If not specified or specified as 0, thor and roxie nodes may
          overlap with support nodes. If an invalid value is provided,
          support nodes are treated to be 0

       -roxienodes <number of roxie nodes>:
          Number of nodes to be generated for roxie. If not specified or
          specified as 0, no roxie nodes are generated

       -thornodes <number of thor nodes>: Number of nodes to be generated
          for thor slaves. A node for thor master is automatically added.
          If not specified or specified as 0, no thor nodes are generated

       -slavespernode <number of thor slaves per node>:
          Number of thor nodes per slave.


       -list:
          List envgen optios only.

       -updateonly :
          Update config values only. Will not trigger envgen. This is
          mainly for update values before add new nodes. So new nodes
          relation changed hooks will generate desired environment.xml.

    The value with "+" mean add to original number.
    The value with "-" mean substract from original number.

EOF
}

function get_service_information()
{
   _options=

   [ -n "${service[service_name]}" ] && _options="-s ${service[service_name]}"

   service_info=$(python ${ABS_CWD}/parse_status.py $_options)
   service_list=( $service_info )
   for item in "${service_list[@]}"
   do
       key=$(echo $item | cut -d '=' -f1)
       value=$(echo $item | cut -d '=' -f2)
       [ -n "$key" ] && service["$key"]=$value
   done
}

function get_current_config()
{
   juju get ${service[service_name]} > /tmp/${service[service_name]}.cfg
   config_info=$(python ${CWD}/parse_config.py /tmp/${service[service_name]}.cfg)
   config_list=( $config_info )
   for item in "${config_list[@]}"
   do
       key=$(echo $item | cut -d '=' -f1)
       value=$(echo $item | cut -d '=' -f2)
       [ -n "$key" ] && config["$key"]=$value
   done
   current_signature="${config[supportnodes]}-${config[roxienodes]}-${config[thornodes]}-${config[slavesPerNode]}"
}


function display_current_configuration()
{
   printf "%s%-15s: %8d\n" "$INDENT" "Support nodes" ${config[supportnodes]}
   printf "%s%-15s: %8d\n" "$INDENT" "Roxie nodes" ${config[roxienodes]}
   printf "%s%-15s: %8d\n" "$INDENT" "Thor nodes" ${config[thornodes]}
   printf "%s%-15s: %8d\n" "$INDENT" "Slaves per node" ${config[slavesPerNode]}

   #envgen_status="Current environment.xml probably already has above settings"
   #if [ "$current_signation" != "${config[slavesPerNode]}" ]
   #then
   #   envgen_status="Current environment.xml probably doesnot have above settings"
   #fi
   #printf "%s%-15s: %s\n"  "$INDENT" "Status" "$envgen_status"

   printf "\n"

}

function update_value()
{
   original_value=$1
   [ -z "$2" ] && echo "$original_value" && return

   input_value=$2

   echo $input_value | grep -q -e "^[[:digit:]]"
   [ $? -eq 0 ] && echo "${input_value}" && return

   echo $input_value | grep -q -e "^+"
   [ $? -eq 0 ] &&  input_value=${input_value:1}

   echo "$(expr $original_value \+ ${input_value})"

}

function update_configuration()
{
   printf "%s%-15s  %8s %8s\n" "$INDENT" "ENVGEN OPTIONS" "BEFORE" "NOW"
   printf "%s%s\n" "$INDENT" "----------------------------------"

   supportnodes=$(update_value ${config[supportnodes]}  ${inputs[supportnodes]})
   printf "%s%-15s: %8d %8d\n" "$INDENT" "Support nodes" \
           ${config[supportnodes]} $supportnodes
   config[supportnodes]=$supportnodes

   roxienodes=$(update_value ${config[roxienodes]}  ${inputs[roxienodes]})
   printf "%s%-15s: %8d %8d\n" "$INDENT" "Roxie nodes" \
           ${config[roxienodes]} $roxienodes
   config[roxienodes]=$roxienodes

   thornodes=$(update_value ${config[thornodes]}  ${inputs[thornodes]})
   printf "%s%-15s: %8d %8d\n" "$INDENT" "Thor nodes" \
           ${config[thornodes]} $thornodes
   config[thornodes]=$thornodes

   slavesPerNode=$(update_value ${config[slavesPerNode]}  ${inputs[slavesPerNode]})
   printf "%s%-15s: %8d %8d\n" "$INDENT" "Slaves Per Node" \
           ${config[slavesPerNode]} $slavesPerNode
   config[slavesPerNode]=$slavesPerNode

   printf "\n"

   if [ $update_only -eq 0 ]
   then
      config[envgen_signature]="${config[supportnodes]}-${config[roxienodes]}-${config[thornodes]}-${config[slavesPerNode]}"
   fi

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


declare -A inputs
inputs[thornodes]=
inputs[roxienodes]=
inputs[supportnodes]=
inputs[slavesPerNode]=

list_only=0
update_only=0

declare -A config
declare -A service
service[service_name]=

INDENT="    "


num_args=$#
for ((i=1; i<=num_args; i++))
do
  case $1 in
    -name) shift
            i=$(expr $i \+ 1)
            service[service_name]=$1
            ;;
    -thornodes) shift
            i=$(expr $i \+ 1)
            inputs[thornodes]=$1
            ;;
    -roxienodes) shift
            i=$(expr $i \+ 1)
            inputs[roxienodes]=$1
            ;;
    -supportnodes) shift
            i=$(expr $i \+ 1)
            inputs[supportnodes]=$1
            ;;
    -slavespernode) shift
            i=$(expr $i \+ 1)
            inputs[slavesPerNode]=$1
            ;;
    -list) list_only=1
            ;;
    -updateonly) update_only=1
            ;;
    *) usage
       exit 0
  esac
  shift
done

get_service_information
get_current_config

#for key in "${!inputs[@]}"
#do
#   echo "$key : ${inputs[$key]}"
#done


#echo "list_only=$list_only"
#echo "update_only=$update_only"

printf "\n"
printf "%s%-15s: %-20s\n" "$INDENT" "CHARM NAME" "$CHARM_NAME"
printf "%s%-15s: %-20s\n" "$INDENT" "SERVICE NAME" "${service[service_name]}"
printf "%s%-15s: %-20s\n" "$INDENT" "UNIT NUMBER" "${service[unit_number]}"
printf "\n"

if [ $list_only -eq 1 ]
then
   display_current_configuration
   exit
fi


update_configuration


juju set ${service[service_name]} \
           envgen-signature=${config[envgen_signature]} \
           thornodes=${config[thornodes]} \
           roxienodes=${config[roxienodes]} \
           supportnodes=${config[supportnodes]} \
           slavesPerNode=${config[slavesPerNode]}
