#!/bin/bash
#
# Automatically tag an existing release candidate build as gold
#

set -euo pipefail
IFS=$'\n\t'

function doit()
{
    eval $1
}

function doit2()
{
    eval $1
}

function usage()
{
    echo "Usage: $0 <major.minor.point-sequence>"
    echo "Examples:"
    echo "  $0 10.2.8-1"
    echo "  $0 9.12.78-rc1"
}

if [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    usage
    exit 0
fi

if [[ -z "$1" ]]; then
    usage
    exit 1
fi

if [[ -n "${2-}" ]]; then
    echo "Unexpected argument: ${2}"
    usage
    exit 1
fi

if [[ ! "$1" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)-((rc)?[0-9]+)$ ]]; then
    echo "Invalid version format: $1"
    echo "Expected: <major.minor.point-sequence> (examples: 9.12.78-3, 9.12.78-rc1)"
    exit 1
fi

HPCC_MAJOR=${BASH_REMATCH[1]}
HPCC_MINOR=${BASH_REMATCH[2]}
HPCC_POINT=${BASH_REMATCH[3]}
HPCC_SEQUENCE=${BASH_REMATCH[4]}
HPCC_SHORT_TAG=$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT

scriptDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repoRootDir="$(cd "${scriptDir}/../.." && pwd)"
if [[ "${repoRootDir##*/}" == "hpcc" ]]; then
    HPCC_DIR="${repoRootDir}"
else
    HPCC_DIR="${repoRootDir}/../hpcc"
fi
helmChartDir="${repoRootDir}/../helm-chart"

if [[ ! -d "${HPCC_DIR}" ]]; then
    echo "HPCC_DIR not found: ${HPCC_DIR}"
    exit 1
fi

if [[ ! -d "${HPCC_DIR}/helm" ]]; then
    echo "HPCC helm directory not found: ${HPCC_DIR}/helm"
    exit 1
fi

if [[ ! -d "${helmChartDir}" ]]; then
    echo "helm-chart repo not found: ${helmChartDir}"
    exit 1
fi

doit2 "pushd ${helmChartDir} 2>&1 > /dev/null"
doit "git fetch origin"
doit "git checkout master"
doit "git merge --ff-only origin/master"
doit "git submodule update --init --recursive"
HPCC_PROJECTS=hpcc-helm
HPCC_NAME=HPCC
if [[ "$HPCC_MAJOR" == "8" ]] && [[ "$HPCC_MINOR" == "10" ]] ; then
    doit "rm -rf ./helm"
    doit "cp -rf $HPCC_DIR/helm ./helm" 
    doit "rm -f ./helm/*.bak" 
    doit "git add -A ./helm"
fi
doit2 "cd docs"
while IFS= read -r -d '' f; do
    doit "helm package ${f%/*}/ --dependency-update"
done < <(find "${HPCC_DIR}/helm/examples" "${HPCC_DIR}/helm/managed" -name Chart.yaml -print0)
doit "helm package ${HPCC_DIR}/helm/hpcc/"
doit "helm repo index . --url https://hpcc-systems.github.io/helm-chart"
doit "git add *.tgz"

doit "git commit -a -s -m \"$HPCC_NAME Helm Charts $HPCC_SHORT_TAG-$HPCC_SEQUENCE\""
if [[ "$HPCC_MAJOR" == "8" ]] && [[ "$HPCC_MINOR" == "10" ]] ; then
    doit "git tag $FORCE $HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT && git push origin $HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT $FORCE"
fi
# doit "git push origin master $FORCE"
doit2 "popd 2>&1 > /dev/null"

echo "***Success - you can now manually push to GitHub***"
