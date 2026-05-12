#!/bin/bash
# This script is used to synchronize the oss repository with the master repository for the candidate branches.
#
# <path>/syncoss.sh [--dry-run] [--originuser <origin_user>] [--ossuser <oss_user>]

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

sync="$all helm-chart"

echo Press any key to sync oss for "$sync"
read -n 1 -s

for f in $sync ; do
   if ! cd "$gitroot/$f"; then
      echo "Failed to change directory to $gitroot/$f" >&2
      exit 1
   fi
   echo "Process $f"
   $hpccdir/cmake_modules/syncoss.sh "$@"
done
