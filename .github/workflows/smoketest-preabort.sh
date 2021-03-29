#!/bin/bash

dumpstacks()
{
  local processName=$1
  for p in $(pidof ${processName}); do
    echo "${processName}[${p}] stacks:"
    sudo gdb --batch --quiet -ex "set interactive-mode off" -ex "thread apply all bt" -ex "quit" $(which ${processName}) ${p}
    echo '==============='
  done
}

echo 'List of processes:'
ps aux 

dumpstacks daserver
dumpstacks esp
dumpstacks ecl
dumpstacks eclcc

echo 'job queues meta data:'
daliadmin . export /JobQueues jq.xml
cat jq.xml
echo '***************'
