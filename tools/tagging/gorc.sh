#!/bin/bash
# This script is used to tag a new rc for either a candidate branch, or a point release
#
# <path>/gorc.sh <version>

if [[ -z $1 ]] ; then
   echo Expected a version number
   exit 2
fi

if [[ -z $all ]]; then
   echo "List of repos not configured (environment variable 'all')"
   exit 2
fi

if [[ -z $gitroot ]]; then
   echo "Root git directory not specified (environment variable 'gitroot')"
   exit 2
fi

scriptdir=$(dirname -- "$( readlink -f -- ""$0""; )")
hpccdir=$scriptdir/../..

echo Create new RC candidate-$1

for f in $all ; do
   cd $gitroot/$f
   git fetch origin
   git checkout candidate-$1
   if [ $? -ne 0 ]; then
      echo "Target branch candidate-$1 failed to check out"
      exit 1
   fi

   git merge origin/candidate-$1 --ff-only
   if [ $? -ne 0 ]; then
      echo "Target branch candidate-$1 is inconsistent with origin/candidate-$1"
      exit 1
   fi
   git submodule update --recursive --init --force
done

echo Press any key to go rc
read -n 1 -s

for f in $all ; do
   cd $gitroot/$f
   echo "Process $f"
   $hpccdir/cmake_modules/go_rc.sh
done
