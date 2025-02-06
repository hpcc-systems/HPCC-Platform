#!/bin/bash

# ----------------------------------------------------------------------------
# This script collects various post-mortem information and writes it to a unique
# subdirectory with the specified output directory (which should be on persistent
# storage, e.g. the debug plane).
# It will then associate this new directory with the workunit if available.
# It is either launched from the main container's entrypoint script (check_executes.sh),
# or from the 'postrun' sidecar container (container_watch.sh).
# ----------------------------------------------------------------------------

container=""
daliServer=""
directory=""
external=false
process=""
workunit=""

usage()
{
  echo "Usage: $0 --directory=DIRECTORY --daliServer=SERVER:PORT --container=CONTAINER_NAME --process=PROCESS_NAME"
  exit 1
}

if [[ $# -lt 1 ]]; then
  usage
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --container=*)
      container="${1#*=}"
      shift
      ;;
    --daliServer=*)
      daliServer="${1#*=}"
      shift
      ;;
    --directory=*)
      directory="${1#*=}"
      shift
      ;;
    --external|--external=true)
      external=true
      shift
      ;;
    --process=*)
      process="${1#*=}"
      shift
      ;;
    --workunit=*)
      workunit="${1#*=}"
      shift
      ;;
    --isJob)
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

if [[ -z "$process" ]]; then
  echo "Error: --process option is required."
  usage
fi

if [[ "$external" == true ]]; then
  # we are in shared root of containers that mount with subPath, ${container} is the sub path
  cd ${container}
  pwd
fi

containerBaseTmpDir="/tmp"
configFile="/etc/config"
if [[ "$external" == true ]]; then
  containerBaseTmpDir="${containerBaseTmpDir}/${container}"
  configFile="${configFile}/${container}"
fi
configFile="${configFile}/${container}.yaml"
wuidFilename="./wuid"
if [[ -e $wuidFilename ]]; then # takes precedence over command line option
  workunit=$(cat ${wuidFilename})
fi
POST_MORTEM_DIR="$directory"
if [[ -n "${workunit}" ]]; then
  POST_MORTEM_DIR="${directory}/${workunit}"
fi
POST_MORTEM_DIR=${POST_MORTEM_DIR}/$(date -Iseconds)/$(hostname)/${container}/${process}
mkdir -p ${POST_MORTEM_DIR}
echo "Post-mortem info gathered in $POST_MORTEM_DIR"

readarray -t core_files < <(find . -maxdepth 1 -type f -name 'core*' -print)
# we only expect one, but cater for multiple
if [[ ${#core_files[@]} -gt 0 ]]; then
  for file in "${core_files[@]}"; do
    echo "Generating info from core file($file) to $POST_MORTEM_DIR/info.log" | tee -a $POST_MORTEM_DIR/info.log
    gdb -batch -ix /opt/HPCCSystems/bin/.gdbinit -x /opt/HPCCSystems/bin/post-mortem-gdb /opt/HPCCSystems/bin/${process} $file 2>$POST_MORTEM_DIR/info.err >>$POST_MORTEM_DIR/info.log
    echo "Generated info from core file($file)" | tee -a $POST_MORTEM_DIR/info.log
    rm $file
  done
else
  echo "Container instance ${container} stopped abruptly (OOM?)" | tee $POST_MORTEM_DIR/info.log
fi

if [[ -f $configFile ]]; then
  cp $configFile $POST_MORTEM_DIR
  echo "Copied $configFile to $POST_MORTEM_DIR" | tee -a $POST_MORTEM_DIR/info.log
fi
cp `ls -rt ${containerBaseTmpDir}/postmortem.*.log.*` $POST_MORTEM_DIR
rm ${containerBaseTmpDir}/postmortem.*.log.*

dmesg -xT > $POST_MORTEM_DIR/dmesg.log
if [[ -n "${daliServer}" ]] && [[ -n "${workunit}" ]]; then
  wutool postmortem ${workunit} DALISERVER=${daliServer} PMD=${POST_MORTEM_DIR}
  echo Updated workunit ${workunit}
fi

