#!/bin/bash
# This script is used to tag a set of repos as gold.
#
# <path>/gogold.sh <version>

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
gitroot="${gitroot/#\~/$HOME}"

echo Go gold with candidate-$1

for f in $all ; do cd "${gitroot}/$f" ; git fetch origin ; git checkout candidate-$1 ; git su --force ; done
echo Press any key to go gold
read -n 1 -s

for f in $all ; do cd $gitroot/$f ; $hpccdir/cmake_modules/go_gold.sh ; done
