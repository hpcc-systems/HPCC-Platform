#!/bin/bash
# This script is used to tag the rc for a new major version
#
# <path>/gomajor.sh

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
gitroot="${gitroot/#\~/$HOME}"

for f in $all ; do
   cd $gitroot/$f
   git fetch origin
   git checkout master
   if [ $? -ne 0 ]; then
      echo "Target branch master failed to check out"
      exit 1
   fi

   git merge origin/master --ff-only
   if [ $? -ne 0 ]; then
      echo "Target branch master is inconsistent with origin/master"
      exit 1
   fi
   git submodule update --recursive --init --force
done

echo Press any key to create a new major version for "$all"
read -n 1 -s

for f in $all ; do
   cd $gitroot/$f
   echo "Process $f"
   $hpccdir/cmake_modules/go_rc.sh --major $*
done
