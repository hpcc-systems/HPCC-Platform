#!/bin/bash

if [[ -z $1 ]] ; then
   echo Expected a source version number
   exit 2
fi

if [[ -z $2 ]] ; then
   echo Expected a target version number
   exit 2
fi

if [[ -n $3 ]] ; then
   all=$3
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

echo Upmerge changes from candidate-$1 to $2 for \'$all\'

for f in $all ; do
   cd $gitroot/$f
   git fetch origin
   git log -1 --oneline origin/candidate-$1 > /dev/null 2>&1
   if [ $? -ne 0 ]; then
      echo "$f: Source branch origin/candidate-$1 does not exist"
      exit 1
   fi

   git log -1 --oneline origin/$2 > /dev/null 2>&1
   if [ $? -ne 0 ]; then
      echo "$f: Target branch origin/$2 does not exist"
      exit 1
   fi

   git checkout $2
   if [ $? -ne 0 ]; then
      echo "$f: Target branch $2 failed to check out"
      exit 1
   fi
   git merge origin/$2 --ff-only
   if [ $? -ne 0 ]; then
      echo "$f: Target branch $2 is inconsistent with origin/$2"
      exit 1
   fi

   git submodule update --recursive --init --force
done

echo "----------- upmerging ---------"

for f in $all ; do
   echo ------- $f --------

   cd $gitroot/$f
   git merge origin/candidate-$1 --no-commit;
   $scriptdir/git-unupmerge

   if [[ -f "version.cmake" ]]; then
       git checkout HEAD -- version.cmake
   fi
   if [[ -f "pom.xml" ]]; then
       git checkout HEAD -- pom.xml
   fi
   if [[ -f "commons-hpcc/pom.xml" ]]; then
       git checkout HEAD -- commons-hpcc/pom.xml
       git checkout HEAD -- dfsclient/pom.xml
       git checkout HEAD -- wsclient/pom.xml
   fi

   CONFLICTS=$(git ls-files -u | wc -l)
   if [ "$CONFLICTS" -eq 0 ] ; then
       git commit -s --no-edit
       git push origin $2
   else
       echo ************ $f is incomplete **************
   fi
done
