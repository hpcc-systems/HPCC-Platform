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

major_minor=$(echo "$1" | grep -oE '^[0-9]+\.[0-9]+')
if [[ -z $major_minor ]]; then
   echo "Invalid version format. Expected N.N.x"
   exit 2
fi

point=$(echo "$1" | grep -oE '\.([0-9]+|x)$' | sed 's/\.//')
if [[ -z $point ]]; then
   echo "Invalid version format. Expected N.N.x or N.N.N"
   exit 2
fi

scriptdir=$(dirname -- "$( readlink -f -- ""$0""; )")
hpccdir=$scriptdir/../..
gitroot="${gitroot/#\~/$HOME}"
version=candidate-$1

if [[ $point == "x" ]]; then
   pushd "$gitroot/hpcc" > /dev/null
   git fetch origin 'refs/tags/community_'$major_minor'.*:refs/tags/community_'$major_minor'.*'
   if ! git tag --list "community_${major_minor}.0-1" | grep -q .; then
      echo "community_${major_minor}.0-1 does not exist.  Did you mean './gorc.sh $major_minor.0'"
      exit 2
   fi
   popd > /dev/null
fi

shift

echo Create new RC $version

for f in $all ; do
   cd $gitroot/$f
   git fetch origin
   git checkout $version
   if [ $? -ne 0 ]; then
      echo "Target branch $version failed to check out"
      exit 1
   fi

   git merge origin/$version --ff-only
   if [ $? -ne 0 ]; then
      echo "Target branch $version is inconsistent with origin/$version"
      exit 1
   fi
   git submodule update --recursive --init --force
done

echo Press any key to go rc for "$all"
read -n 1 -s

for f in $all ; do
   cd $gitroot/$f
   echo "Process $f"
   $hpccdir/cmake_modules/go_rc.sh $*
done
