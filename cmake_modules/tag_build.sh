#!/bin/bash
#
# Automatically tag a build and push the tags, from the information in version.cmake
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_cmake.sh

do_tag