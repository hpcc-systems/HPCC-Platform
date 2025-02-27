#!/bin/bash

# ----------------------------------------------------------------------------
# This script is used by the 'postrun' sidecar container to monitor the
# main containers by tracking their 'running' and 'stopped' files.
# If a 'running' file is not updated within a certain time period, the
# script will trigger a postmortem collection.
# This can happen if the main containers are abruptly halted by k8s,
# e.g. due to k8s OOM evictions.
# NB: The isJob option causes the script to exit if it detects that the
# job has finished (i.e. the main container has stopped). Otherwise, it
# will continue to loop and wait for a new instance of the main container to
# restart.
# ----------------------------------------------------------------------------

config=""
daliServer=""
directory=""
workunit=""
declare -a containers=()
declare -a processes=()

usage()
{
  echo "Usage: $0 --directory=DIRECTORY --daliServer=SERVER:PORT [container process container process ...]"
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

isJob=false
declare -a originalArgs=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --daliServer=*)
      daliServer="${1#*=}"
      originalArgs+=("$1")
      shift
      ;;
    --directory=*)
      directory="${1#*=}"
      originalArgs+=("$1")
      shift
      ;;
    --workunit=*)
      workunit="${1#*=}"
      originalArgs+=("$1")
      shift
      ;;
    --isJob)
      isJob=true
      originalArgs+=("$1")
      shift
      ;;
    --*)
      echo "Error: Unknown option '$1'"
      usage
      ;;
    *)
      # Once a non-option argument is encountered, break out of the options parsing
      break
      ;;
  esac
done

if [[ -z "$directory" ]]; then
  echo "Error: --directory option is required."
  usage
fi

# Ensure that the remaining arguments are in pairs
if (( $# % 2 != 0 )); then
  echo "Error: After options, arguments should be in container process pairs."
  usage
fi

# Collect container-process pairs
while [[ $# -gt 0 ]]; do
  containers+=($1)
  processes+=($2)
  shift 2
done

SIGNALLEDFILENAME="/tmp/postrunSignalled"
monitor_container()
{
  local container="$1"
  local process="$2"
  RUNNINGFILENAME="/tmp/${container}/running"
  STOPPEDFILENAME="/tmp/${container}/stopped"

  while true; do
    #echo "HPCC-LOCAL-LOG: Waiting for ${RUNNINGFILENAME} to be created..."
    until [ -f "${RUNNINGFILENAME}" ]; do
      sleep 5
      if [ -f "${STOPPEDFILENAME}" ]; then
        break # will cause while loop below to exit
      fi
    done

    #echo "HPCC-LOCAL-LOG: ${RUNNINGFILENAME} found. Starting to monitor."

    retCode=0
    # Monitor the file
    while true; do
      if [ -f "${STOPPEDFILENAME}" ]; then
        #echo "HPCC-LOCAL-LOG: ${STOPPEDFILENAME} file detected. Exiting."
        break
      fi
      if [ ! -f "${RUNNINGFILENAME}" ]; then
        #echo "HPCC-LOCAL-LOG: ${RUNNINGFILENAME} has been removed. Exiting."
        break
      fi

      # Check the file's age
      CHKSECS=10
      MAXAGESECS=30
      FILE_MOD_TIME=$(stat -c %Y "${RUNNINGFILENAME}")
      CURRENT_TIME=$(date +%s)
      FILE_AGE=$((CURRENT_TIME - FILE_MOD_TIME))

      if [ "${FILE_AGE}" -ge "${MAXAGESECS}" ]; then
        echo "WARNING: Dead container detected: ${container} - ${RUNNINGFILENAME} is ${MAXAGESECS} seconds or older. Exiting."
        retCode=1 # indicating bad container
        break
      else
        #echo "HPCC-LOCAL-LOG: ${container} file age is ${FILE_AGE}"
        if [ "${FILE_AGE}" -lt "${CHKSECS}" ]; then
          sleep $((CHKSECS - FILE_AGE))
        else
          sleep ${CHKSECS}
        fi
      fi
    done
    #echo "HPCC-LOCAL-LOG: removing ${STOPPEDFILENAME} and ${RUNNINGFILENAME}, retCode=${retCode}"
    rm -f "${STOPPEDFILENAME}"
    rm -f "${RUNNINGFILENAME}"

    if [[ $retCode -eq 1 ]]; then
      collect_postmortem.sh "${originalArgs[@]}" --container=${container} --process=${process} --external
    fi
    if [ -f "${SIGNALLEDFILENAME}" ]; then
      #echo "HPCC-LOCAL-LOG: ${SIGNALLEDFILENAME} file detected. Exiting."
      break
    fi
    if [[ $isJob == true ]]; then
      #echo "HPCC-LOCAL-LOG: ${container} finished in job. No longer monitoring."
      break
    fi
  done
}

function exitTrap
{
  #echo "HPCC-LOCAL-LOG: Postrun signalled"
  > ${SIGNALLEDFILENAME} # cause monitor_container to exit when passed running age check (instead of looping around expecting new container)
  wait
}

trap exitTrap SIGTERM SIGINT SIGABRT SIGQUIT SIGHUP

for i in "${!containers[@]}"; do
  container="${containers[i]}"
  process="${processes[i]}"
  #echo "HPCC-LOCAL-LOG: Container: $container, Process: $process"
  monitor_container "$container" "$process" &
done

wait