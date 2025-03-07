#!/bin/bash

# ----------------------------------------------------------------------------
# This script is the main entry point for all HPCC helm components.
# It will launch the component process and check its exist status.
# If the process exits with a non-zero status, it will collect post-mortem
# information and associate with the workunit if specified.
# It is also responsible for periodically updating the running file that
# the postrun sidecar monitors (see container_watch.sh).
# ----------------------------------------------------------------------------

# The yaml passes in all arguments as a single string, due to the way the args are built up,
# and additional args _HPCC_ARGS_ are substituted in.
# Split the single string argument into individual arguments
eval set -- "$1"

usage() {
  echo "Usage: check_executes.sh [options] -- cmd args"
  echo "    -c <name>          The name of the container"
  echo "    -d <directory>     Mounted directory to store post-mortem info in"
  echo "    -a                 Always collect post-mortem info, even if the process exits cleanly"
  echo "    -v                 Run the process under valgrind"
  echo "    -p                 This is a postrun sidecar container"
}

PMD_DIRECTORYBASE=
PMD_PROGNAME=
PMD_DALISERVER=
PMD_WORKUNIT=
PMD_CONTAINERNAME=
PMD_ALWAYS=false
PMD_VALGRIND=false
PMD_POSTRUN=false


while [ "$#" -gt 0 ]; do
  arg=$1
  if [[ ${arg:0:1} == '-' ]]; then
    case "${arg:1:1}" in
      -) shift
         PMD_PROGNAME=$1
         shift
         break
         ;;
      c) shift
         PMD_CONTAINERNAME=$1
         ;;
      d) shift;
         PMD_DIRECTORYBASE=$1
         ;;
      a) PMD_ALWAYS=true
         ;;
      v) PMD_VALGRIND=true
         ;;
      p) PMD_POSTRUN=true
         ;;
      *) echo "Unknown option: ${arg:1:1}"
         usage
         exit
         ;;
    esac
  else
    echo "Unknown argument: $arg"
    usage
    exit
  fi
  shift
done

if [[ -z ${PMD_PROGNAME} ]] ; then
  usage
  exit
fi

# Scan managed process parameters for additional information
for (( arg=1; arg <= "$#"; arg++ )); do
  optname=${!arg%=*}
  optval=${!arg#*=}
  if [[ ${optname} == '--config' ]]; then
    PMD_CONFIG=(${optval})
  elif [[ ${optname} == '--daliServers' ]]; then
    PMD_DALISERVER=${optval}
  elif [[ ${optname} == '--workunit' ]]; then
    PMD_WORKUNIT=${optval}
  fi
done

ulimit -c unlimited

function cleanup
{
  echo "EXIT via signal for $progPid"

  if [ -n "$progPid" ]; then
    if kill -0 $progPid 2>/dev/null; then
      echo "Sending SIGTERM to process (PID $progPid)"
      kill $progPid
      wait $progPid
      retVal=$?
    fi
  fi
}

# Ensure any signals to the script kill the child process
# NB: do not include SIGEXIT since when handled, it will cause the script to exit prematurely.
trap cleanup SIGTERM SIGINT SIGABRT SIGQUIT SIGHUP

runningFilename=/tmp/running
stoppedFilename=/tmp/stopped
if [ "$PMD_POSTRUN" = "true" ]; then
  if [ -f ${runningFilename} ] || [ -f ${stoppedFilename} ]; then
    echo "${runningFilename} and/or ${stoppedFilename} already exists. It suggests this container restarted quickly (postrun sidecar hasn't spotted and cleared up yet)"
    secs=40
    #echo "HPCC-LOCAL-LOG: Waiting ${secs} seconds for postrun sidecar to spot and collate postmortem."
    sleep ${secs}
    #echo "HPCC-LOCAL-LOG: Continuing..."
  fi
fi

# Execute the main program, defaulting postmortem logging on (can be overriden by program's config file)
if [ "$PMD_VALGRIND" = "true" ]; then
  echo running valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --num-callers=8 --log-fd=1 ${PMD_PROGNAME} --logging.postMortem=1000 "$@"
  valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --num-callers=8 --log-fd=1 ${PMD_PROGNAME} --logging.postMortem=1000 "$@" &
else
  echo running ${PMD_PROGNAME} --logging.postMortem=1000 "$@"
  ${PMD_PROGNAME} --logging.postMortem=1000 "$@" &
fi

progPid=$!

> ${runningFilename}

/bin/bash -c "while true; do touch ${runningFilename}; sleep 5; done" &
heartbeatPid=$!

echo "Waiting for child process $progPid"
# If the signal handler (cleanup) was called, it will wait and capture retVal and cause this 'wait $progPid' to exit on completion.
# NB: If the signal handler itself doesn't wait, then it will still cause this statement to complete before the child process has exited.
wait $progPid
retVal2=$?
if [ ! -v retVal ]; then
  retVal=$retVal2
fi
echo "Child process $progPid exited with exit code $retVal"
kill -1 $heartbeatPid

> ${stoppedFilename}

trap '' SIGTERM SIGINT SIGABRT SIGQUIT SIGHUP

# If it did not exit cleanly, copy some post-mortem info
if [ $PMD_ALWAYS = true ] || [ $retVal -ne 0 ]; then
  extraArgs=()
  if [[ -n "$PMD_WORKUNIT" ]]; then
    extraArgs+=("--workunit=$PMD_WORKUNIT")
  fi
  if [[ -n "$PMD_DIRECTORYBASE" ]]; then
    collect_postmortem.sh "--directory=${PMD_DIRECTORYBASE}" "--daliServer=${PMD_DALISERVER}" "--container=${PMD_CONTAINERNAME}" "--process=${PMD_PROGNAME}" "${extraArgs[@]}"
  else
    : #null statment so the else is valid
    #echo "HPCC-LOCAL-LOG: Post mortem directory not provided, unable to collect post mortem info"
  fi
else
  : #null statment so the else is valid
  #echo "HPCC-LOCAL-LOG: Process exited cleanly (code=0)"
fi
k8s_postjob_clearup.sh
exit $retVal
