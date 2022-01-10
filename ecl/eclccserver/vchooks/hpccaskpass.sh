#!/bin/bash
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.
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

################################################################################
# Script to allow username and password to be securly passed to git when
# compiling source from multiple directories
#
# This hook expects environment variables to be set as follows:
# HPCC_GIT_USERNAME   - The name of the service account used to read the repos
# HPCC_GIT_SECRETPATH - The path to the root of the secret directory
#
# The filename providing the password is git/<username>/password within the
# HPCC_GIT_SECRETPATH directory
################################################################################

#!/bin/bash
secretPath="${HPCC_GIT_SECRETPATH:-/opt/HPCCSystems/secrets}"
if [[ $1 =~ ^[Uu]sername ]]; then
  echo $HPCC_GIT_USERNAME
elif [[ $1 =~ ^[Pp]assword ]]; then
  cat $secretPath/git/$HPCC_GIT_USERNAME/password
fi
