#!/bin/bash
SCRIPT_HOME=$(dirname $0)
. ${SCRIPT_HOME}/common


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
