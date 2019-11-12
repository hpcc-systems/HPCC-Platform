#!/bin/sh
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
