#!/bin/bash
WORK_DIR=$(dirname $0)
source ${WORK_DIR}/env-loganalytics

k8scommand="kubectl"
secretname="azure-logaccess"
secretsdir="${WORK_DIR}/secrets-templates"

usage()
{
    echo "Creates necessary k8s secret used by HPCC's logAccess to access Azure Log Analytics"
    echo "> create-azure-logaccess-secret.sh [Options]"
    echo ""
    echo "Options:"
    echo "-d       Specifies directory containing required secret values in self named files."
    echo "         Defaults to <workingdir>/<${secretssubdir}>"
    echo "-h       Print Usage message"
    echo ""
    echo "Requires directory containing secret values in dedicated files."
    echo "Defaults to  ${secretssubdir} if not specified via -d option."
    echo ""
    echo "Expected directory structure:"
    echo "${secretsdir}/"
    echo "   aad-client-id     - Should contain the ID of the AAD registered Application"
    echo "   aad-tenant-id     - Should contain the subscription tenant of theAAD registered Application"
    echo "   aad-client-secret - Should contain access secret provided by AAD registered Application"
    echo "   ala-workspace-id  - Should contain target Azure Log Analytics workspace ID. (Optional if provided in LogAccess configuration)"
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
    esac
  shift
done

echo "Creating '${secretname}' secret."

command -v ${k8scommand} >/dev/null 2>&1 || { echo >&2 "Aborting - '${k8scommand}' not found!"; exit 1; }

errormessage=$(${k8scommand} get secret ${secretname} 2>&1)
if [[ $? -eq 0 ]]
then
  echo "WARNING: Target secret '${secretname}' already exists! Delete it and re-run if secret update desired."
  echo "${errormessage}"
  exit 1
fi

errormessage=$(${k8scommand} create secret generic ${secretname} --from-file=${secretsdir})
if [[ $? -ne 0 ]]
then
  echo "Error creating: Target secret '${secretname}'!"
  echo >&2
  usage
  exit 1
else
  echo "Target secret '${secretname}' successfully created!"
  ${k8scommand} get secret ${secretname}
fi
