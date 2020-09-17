#!/bin/bash
##############################################################################
#
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems® .
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
##############################################################################

# Use this script to build local images for Machine Learning HPCC Systems Docker images
#

usage()
{
  echo "Usage: build.sh [options]"
  echo "    -h     Display help"
  echo "    -l     Tag the images as the latest"
  echo "    -m     ML feature: one of ml, gnn and gnn-gpu"
  echo "    -t     Tag of base image hpccsystems/platform-core"
  exit
}
LABEL=
FEATURE=
while getopts “hlm:t:” opt; do
  case $opt in
    l) TAGLATEST=1 ;;
    m) FEATURE=$OPTARG ;;
    t) LABEL=$OPTARG ;;
    h) usage   ;;
  esac
done
shift $(( $OPTIND-1 ))

[[ -z ${FEATURE} ]] && usage

ml_features=(
  'ml'
  'gnn'
  'gnn-gpu'
)

found="false"
for ml_feature in ${ml_features[@]}
do
  if [[ $ml_feature ==  $FEATURE ]]
  then
    found="true"
    break
  fi
done

if [[ "$found" == "false" ]]
then
	echo "Unknown ML feature $FEATURE"
fi


[[ -z ${LABEL} ]] && LABEL=latest



build_image()
{
  name=$1
  docker image build -t hpccsystems/platform-${name}:${LABEL} \
     --build-arg DOCKER_REPO=hpccsystems \
     --build-arg BUILD_LABEL=${LABEL} \
     ${name}/

  if [ "$TAGLATEST" = "1" ] && [ "${LABEL}" != "latest" ]; then
     docker tag hpccsystems/platform-${name}:${LABEL}  hpccsystems/platform-${name}
  fi

}

echo .
echo "build_image $FEATURE"
build_image $FEATURE
