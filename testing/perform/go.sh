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

trap "exit" SIGHUP SIGINT SIGTERM

originalDir=$PWD
target=thor
server=.
user=me
password=
userflags=-fallowVariableRoxieFilenames
queries=
eclccoutput=( `eclcc --version` )
version=${eclccoutput[1]}

#process command line options to override the various settings.
if [[ $* != '' ]]; then
    while getopts "f:p:s:q:t:u:v:x" opt; do
        case $opt in
            f)
                userflags="$userflags -f$OPTARG"
                ;;
            p)
                password=$OPTARG
                ;;
            q)
                queries="$queries $OPTARG"
                ;;
            s)
                server=$OPTARG
                ;;
            t)
                target=$OPTARG
                ;;
            u)
                user=$OPTARG
                ;;
            v)
                version=$OPTARG
                ;;
            x)
                userflags="$userflags $OPTARG"
                ;;
            :)
                echo $syntax
                exit -1
                ;;
        esac
    done
fi


if [[ "$queries" != '' ]]; then
   files=$queries
else
   #Process the files in alphabetical order.  Filenames follow the convention <stage><subsequence>_<name>
   files=`ls *.ecl`
fi

now=`date -u +%Y_%m_%d`

output=
if [[ -z "$queries" ]]; then
    prefix="history/$target.$now.$version"
    output="$prefix.perf"
    mkdir history 2> /dev/null
    rm $output 2> /dev/null
    cp perform/system.ecl $prefix.sys
fi

for f in $files
do
    echo -n "$f ... "
    /usr/bin/time -o deploytime.log ecl deploy "$f" --target=$target --username=$user --password=$password --server=$server $userflags --limit=0 -v > deploy.log 2> deploy.err

    # Which query just ran - there should be an easier way to do this!
    wuidline=`grep wuid: deploy.log`
    wuid="${wuidline/wuid:/}"
    wuid="${wuid// /}"

    if [[ -z "$wuid" ]]; then
       echo "failed to compile"
       cat deploy.err
       echo
    else
        /usr/bin/time -o runtime.log -f %e ecl run "$wuid" --username=$user --password=$password --server=$server > results.log
        timetaken=`cat runtime.log`
        echo $timetaken seconds

        if [[ -n "$output" ]]; then
           echo \"$now\",\"$version\",\"$f\",$timetaken >> $output
        fi
    fi

    rm deploy.log deploy.err results.log deploytime.log runtime.log 2> /dev/null
done
