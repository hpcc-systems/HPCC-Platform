#!/bin/bash

# Generate the expected version tag(s) corresponding to the settings in version.cmake

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_cmake.sh

parse_cmake

for f in ${HPCC_PROJECT} ; do
  set_tag $f
  echo $HPCC_LONG_TAG
done
