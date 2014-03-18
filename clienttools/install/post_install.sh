#!/bin/bash
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.
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
################################################################################


SRC_DIR=$(dirname $0)
TARGET_DIR=/usr/bin

# Files in bin/ need to create/remove symbolic links
####################################################
files_to_link=(
   'dfuplus'
   'ecl'
   'eclcc'
   'ecl-packagemap'
   'eclplus'
   'ecl-queries'
   'ecl-roxie'
   'soapplus'
   'wuget'
   'ecl-bundle'
)

# Check if this is post uninstall
#################################
IS_UNINSTALL=0
[ "$1" = "-u" ] && IS_UNINSTALL=1



# Get absolute path of clienttools bin directory
################################################
CUR_DIR=$(pwd)
[ "$SRC_DIR" != "." ] && cd $SRC_DIR 
SRC_DIR=$(pwd)
CT_HOME=$(echo $SRC_DIR | sed -n "s/^\(.*clienttools\)\(.*\)/\1/p")
CT_BIN=${CT_HOME}/bin
ECL_TEST_DIR=${CT_HOME}/testing/regress
cd $CUR_DIR


# Handle symbolic links
# Only proceed if HPCC Platform is not installed
################################################
[ -e /opt/HPCCSystems/etc/init.d/hpcc-init ] && exit 0

if [ $IS_UNINSTALL -eq 0 ]
then
    # Add symbolic link
    for file in ${files_to_link[@]}
    do
       [ -e ${CT_BIN}/${file} ] && \
           ln -sf ${CT_BIN}/${file}  ${TARGET_DIR}/${file}  
    done
    [ -e ${ECL_TEST_DIR}/ecl-test ] && \
           ln -sf ${ECL_TEST_DIR}/ecl-test  ${TARGET_DIR}/ecl-test  
    

else
    # Remove symbolic link
    for file in ${files_to_link[@]}
    do
       [ ! -e ${TARGET_DIR}/${file} ] && continue
       ls -l ${TARGET_DIR}/${file} | egrep -q "${CT_BIN}/${file}$"
       [ $? -eq 0 ] && rm -rf ${TARGET_DIR}/${file}
    done
    if [ -e ${TARGET_DIR}/ecl-test ]
    then
       ls -l ${TARGET_DIR}/ecl-test | egrep -q "${ECL_TEST_DIR}/ecl-test"
       [ $? -eq 0 ] && rm -rf ${TARGET_DIR}/ecl-test
    fi
fi

exit 0
