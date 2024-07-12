#!/bin/bash
WORK_DIR=$(dirname $0)

k8scommand="kubectl"
secretname="grafana-logaccess"
namespace="default"
username="admin"
password=""

usage()
{
    echo "Creates necessary k8s secret used by HPCC's logAccess to access Loki data source through Grafana"
    echo "> create-grafana-logaccess-secret.sh [Options]"
    echo ""
    echo "Example: create-grafana-logaccess-secret.sh -u admin -p mypassword -n mynamespace"
    echo ""
    echo "Options:"
    echo "-u       Grafana user name (default: admin)"
    echo "-p       Grafana password (required)"
    echo "-h       Print Usage message"
    echo "-n       Specifies namespace for secret (default: default)"
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
      -u) shift
         username=$1
         ;;
      -p) shift
          password=$1
          ;;
      -n) shift
         namespace=$1
         ;;
    esac
  shift
done

if [ -z "${password}" ];
then
  echo "Error: Missing required password!"
  echo >&2
  usage
  exit 1
fi
echo "Creating '${namespace}/${secretname}' secret."

command -v ${k8scommand} >/dev/null 2>&1 || { echo >&2 "Aborting - '${k8scommand}' not found!"; exit 1; }

errormessage=$(${k8scommand} get secret ${secretname} -n ${namespace} 2>&1)
if [[ $? -eq 0 ]]
then
  echo "WARNING: Target secret '${namespace}/${secretname}' already exists! Delete it and re-run if secret update desired."
  echo "${errormessage}"
  echo "use this command: '${k8scommand} delete secret ${secretname} -n ${namespace}'"
  exit 1
fi

errormessage=$(${k8scommand} create secret generic ${secretname} --from-literal=username=${username} --from-literal=password=${password} -n ${namespace})
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
