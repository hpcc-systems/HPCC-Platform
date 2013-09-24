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
if [ -n "$WU_GIT_VERBOSE" ]; then
    GIT_VERBOSE=1
fi

function fetch_repo {
    repo=$1
    prefix="$(echo $repo | tr '[a-z]' '[A-Z]')"
    eval git_url=\${${prefix}_URL}
    eval wu_git_branch=\${WU_${prefix}_BRANCH}
    eval git_branch=\${${prefix}_BRANCH}
    eval git_branch_locked=\${${prefix}_BRANCH_LOCKED}
    eval git_directory=\${${prefix}_DIRECTORY}

    if [ -z "$git_url" ]; then
        echo "Need to set ${prefix}_URL" 1>&2
        exit 2
    fi

    if [ -n "$wu_git_branch" ]; then
        if [ -z git_branch_locked ]; then
            echo "GIT: Overriding branch is not allowed" 1>&2
            exit 2
        else
            git_branch=$wu_git_branch
        fi
    fi

    if [ -z "$git_branch" ]; then
        git_branch=master
        if [ -n "$GIT_VERBOSE" ]; then
            echo "GIT: No branch specified for $git_url - assuming master" 1>&2
        fi
    else
        if [ -n "$GIT_VERBOSE" ]; then
            echo "GIT: using branch $git_branch for $git_url" 1>&2
        fi
    fi

    if [ -z "$git_directory" ]; then
        # We are executed in the eclccserver's home dir - typically /var/lib/HPCCSystems/myeclccserver
        mkdir -p $PWD/repos/
        if [ $? -ne 0 ]; then
            echo "Unable to create directory $PWD/repos/" 1>&2
            exit 2
        fi
        cd $PWD/repos/

        # URL is likely to be of the form git@github.com:path/to/dir.git
        # MORE - we could check it is of the expected form...
        splitURL=(${git_url//[:\/]/ })
        splitLen=${#splitURL[@]}
        tail=${splitURL[$splitLen-1]}

        # tail is now the directory that git clone would create for this url
        git_directory=$PWD/$tail

        if [ ! -d $git_directory ] ; then
            if [ -n "$GIT_VERBOSE" ]; then
                echo "GIT: performing initial clone"
                git clone --bare $git_url 1>&2 || exit $?
            else
                git clone --bare $git_url 2>&1 >/dev/null
                if [ $? -ne 0 ]; then
                    echo "Failed to run git clone $git_url" 1>&2
                    exit 2
                fi
            fi
        fi
    fi
    cd $git_directory
    # MORE - may want to not do every time? Add option to check time since last fetched?
    if [ -n "$GIT_VERBOSE" ]; then
        echo "GIT: using directory $git_directory" 1>&2
        echo "GIT: Running git fetch $git_url" 1>&2
        git fetch $git_url 2>&1 >/dev/null
    else
        git fetch $git_url >/dev/null
    fi
    if [ $? -ne 0 ]; then
        echo "Failed to run git fetch $git_url" 1>&2
        exit 2
    fi
    # Map the branch to a SHA, to avoid issues with the branch being updated by another eclcc process
    # while this one is compiling (not 100% failsafe, but good enough)
    last_commit=$(git rev-parse --short $git_branch)
    if [ $? -ne 0 ]; then
        echo "Failed to run git rev-parse $git_branch" 1>&2
        exit 2
    fi
    export GIT_INCLUDE_PATH="${GIT_INCLUDE_PATH} -I${git_directory}/{$last_commit}/ -a${repo}_commit=${last_commit}"
}

repositories=(${GIT_REPOSITORIES//:/ })
for repo in ${repositories[@]} ; do
  fetch_repo $repo
done

cd $originalDir
if [ -n "$GIT_VERBOSE" ]; then
    echo GIT: calling eclcc $GIT_INCLUDE_PATH "$@"  1>&2
fi
eclcc $GIT_INCLUDE_PATH "$@"
