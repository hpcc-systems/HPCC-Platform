#!/bin/sh
set -e

if [ "${START_SSHD}" = "true" ]
then
  /usr/bin/ssh-keygen -A
  service ssh start
fi

 [ "${START_HPCC}" = "true" ] && /etc/init.d/hpcc-init start

if [ "${EXEC_IN_LOOP}" = "true" ]
then
  interval=5
  [ -n EXEC_INTERVAL ] && interval=${EXEC_INTERVAL}
  while [ 1 ]
  do
    [ -e "$1" ] && exec "$@"
    sleep $interval
  done
else
  exec "$@"
fi
