#!/bin/bash
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

SCRIPT_HOME=$(dirname $0)
. ${SCRIPT_HOME}/common

no_prompt=false
[ "$1" = "-f" ] && no_prompt=true

if [ "${no_prompt}" = "false" ]
then
  echo
  read -p "This command will delete all running and stopped containers. Do you want to continue? [Y|n] " answer
  if [ "$answer" != "Y" ] && [ "$answer" != "y" ]
  then
    exit 0
  fi
fi

$DOCKER_SUDO docker ps  | while read line
do
  id=$(echo $line | cut -d ' ' -f1)
  [ "CONTAINER" = "$id" ] && continue
  $DOCKER_SUDO docker stop $id
done

$DOCKER_SUDO docker ps -a | while read line
do
  id=$(echo $line | cut -d ' ' -f1)
  [ "CONTAINER" = "$id" ] && continue
  $DOCKER_SUDO docker rm $id
done
