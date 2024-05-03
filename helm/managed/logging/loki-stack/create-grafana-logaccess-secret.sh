#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-loganalytics

k8scommand="kubectl"
secretname="grafana-logaccess"
secretsdir="${WORK_DIR}/secrets-templates"
namespace="default"

usage()
{
    echo "Creates necessary k8s secret used by HPCC's logAccess to access Loki data source through Grafana"
    echo "> create-grafana-logaccess-secret.sh [Options]"
    echo ""
    echo "Options:"
    echo "-d       Specifies directory containing required secret values in self named files."
    echo "         Defaults to <workingdir>/<${secretssubdir}>"
    echo "-h       Print Usage message"
    echo "-n       Specifies namespace for secret"
    echo ""
    echo "Requires directory containing secret values in dedicated files."
    echo "Defaults to  ${secretssubdir} if not specified via -d option."
    echo ""
    echo "Expected directory structure:"
    echo "${secretsdir}/"
    echo "   password  - Should contain Grafana user name"
    echo "   username  - Should contain Grafana password"
}

while [ "$#" -gt 0 ]; do
  arg=$1
  case "${arg}" in
      -h) 
         usage
         exit
         ;;
      -d) shift
         secretsdir=$1
         ;;
      -n) shift
         namespace=$1
         ;;
    esac
  shift
done

echo "Creating '${namespace}/${secretname}' secret."

command -v ${k8scommand} >/dev/null 2>&1 || { echo >&2 "Aborting - '${k8scommand}' not found!"; exit 1; }

errormessage=$(${k8scommand} get secret ${secretname} -n ${namespace} 2>&1)
if [[ $? -eq 0 ]]
then
  echo "WARNING: Target secret '${namespace}/${secretname}' already exists! Delete it and re-run if secret update desired."
  echo "${errormessage}"
  exit 1
fi

errormessage=$(${k8scommand} create secret generic ${secretname} --from-file=${secretsdir} -n ${namespace} )
if [[ $? -ne 0 ]]
then
  echo "Error creating: Target secret '${namespace}/${secretname}'!"
  echo >&2
  usage
  exit 1
else
  echo "Target secret '${namespace}/${secretname}' successfully created!"
  ${k8scommand} get secret ${secretname} -n ${namespace}
fi
