#!/bin/bash
################################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.
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
# Hook to use a git repository for eclcc compilation
# Files will be fetched from the remote repo before each compile
#
# This hook expecte environment variables to be set as follows:
# GIT_URL       - The remote repository location
# GIT_BRANCH    - The branch/commit/tag to use  (if ommitted, master is assumed)
# GIT_DIRECTORY - can be set to allow the dir to be maintained separately - it is
#                 assumed (at present) that it will refer to a 'bare' repository
################################################################################

originalDir=$PWD
if [ -z "$GIT_URL" ]; then
    echo "Need to set GIT_URL" 1>&2
    exit 2
fi

if [ -n "$WU_GIT_VERBOSE" ]; then
    GIT_VERBOSE=1
fi

if [ -n "$WU_GIT_BRANCH" ]; then
    if [ -z GIT_BRANCH_LOCKED ]; then
        echo "GIT: Overrideing branch is not allowed" 1>&2
        exit 2
    else
        GIT_BRANCH=$WU_GIT_BRANCH
    fi
fi

if [ -z "$GIT_BRANCH" ]; then
    GIT_BRANCH=master
    if [ -n "$GIT_VERBOSE" ]; then
        echo "GIT: No branch specified - assuming master" 1>&2
    fi
else
    if [ -n "$GIT_VERBOSE" ]; then
        echo "GIT: using branch $GIT_BRANCH" 1>&2
    fi
fi

if [ -z "$GIT_DIRECTORY" ]; then
    # We are executed in the eclccserver's home dir - typically /var/lib/HPCCSystems/myeclccserver
    mkdir -p $PWD/repos/
    if [ $? -ne 0 ]; then
        echo "Unable to create directory $PWD/repos/" 1>&2
        exit 2
    fi
    cd $PWD/repos/

    # URL is likely to be of the form git@github.com:path/to/dir.git
    # MORE - we could check it is of the expected form...
    splitURL=(${GIT_URL//[:\/]/ })
    splitLen=${#splitURL[@]}
    tail=${splitURL[$splitLen-1]}

    # tail is now the directory that git clone would create for this url
    GIT_DIRECTORY=$PWD/$tail

    if [ ! -d $GIT_DIRECTORY ] ; then
        if [ -n "$GIT_VERBOSE" ]; then
            echo "GIT: performing initial clone"
            git clone --bare $GIT_URL 1>&2 || exit $?
        else
            git clone --bare $GIT_URL 2>&1 >/dev/null
            if [ $? -ne 0 ]; then
                echo "Failed to run git clone $GIT_URL" 1>&2
                exit 2
            fi
        fi
    fi
fi

cd $GIT_DIRECTORY
# MORE - may want to not do every time? Add option to check time since last fetched?
if [ -n "$GIT_VERBOSE" ]; then
    echo "GIT: using directory $GIT_DIRECTORY" 1>&2
    git fetch $GIT_URL ${GIT_BRANCH}  1>&2 || exit $?
else
    git fetch $GIT_URL ${GIT_BRANCH} 2>&1 >/dev/null || exit $?
    if [ $? -ne 0 ]; then
        echo "Failed to run git fetch $GIT_URL ${GIT_BRANCH}" 1>&2
        exit 2
    fi
fi

cd $originalDir
if [ -n "$GIT_VERBOSE" ]; then
    echo GIT: calling eclcc -I$GIT_DIRECTORY/{$GIT_BRANCH}/ "$@"  1>&2
fi
eclcc -I$GIT_DIRECTORY/{$GIT_BRANCH}/ "$@"
