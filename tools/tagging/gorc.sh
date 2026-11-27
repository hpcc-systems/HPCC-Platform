#!/bin/bash
# This script is used to tag a new rc for either a candidate branch, or a point release
#
# <path>/gorc.sh <version> [--gold]
#
# --gold splits a new point release from candidate-A.B.x and tags it gold directly.
# All repositories are prepared locally first, then each is pushed with a single
# atomic push in the order of $all (the platform must be last), followed by helm-chart.

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

if ! command -v helm >/dev/null 2>&1; then
   echo "helm command could not be found 'apt, snap, brew, etc. may be used to install helm'"
   exit 1
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

shift

gold=
dryrun=
nopush=
remote=origin
go_rc_args=()
# Preserve wrapper flags for go_rc.sh while separately controlling wrapper pushes.
while [[ $# -gt 0 ]] ; do
   case "$1" in
      --gold) gold=1; go_rc_args+=("$1"); shift;;
      -d|--dryrun) dryrun=1; go_rc_args+=("$1"); shift;;
      --no-push) nopush=1; go_rc_args+=("$1"); shift;;
      -r|--remote)
         if [[ $# -lt 2 ]]; then
            echo "$1 option requires an argument"
            exit 2
         fi
         remote=$2
         go_rc_args+=("$1" "$2")
         shift 2;;
      *) go_rc_args+=("$1"); shift;;
   esac
done
if [[ -n $gold ]] && [[ $point != "x" ]]; then
   echo "--gold requires a version of the form N.N.x"
   exit 2
fi
if [[ -n $gold ]] && [[ $remote != origin ]]; then
   echo "--gold only supports the origin remote"
   exit 2
fi

if [[ $point == "x" ]]; then
   pushd "$gitroot/hpcc" > /dev/null
   git fetch origin 'refs/tags/community_'$major_minor'.*:refs/tags/community_'$major_minor'.*'
   if ! git tag --list "community_${major_minor}.0-1" | grep -q .; then
      echo "community_${major_minor}.0-1 does not exist.  Did you mean './gorc.sh $major_minor.0'"
      exit 2
   fi
   popd > /dev/null
fi

echo Create new RC $version

declare -A BASE_SHA=() RELEASE_BRANCH=() PUSH_REFS=()
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
   if [[ -n $gold ]]; then
      BASE_SHA[$f]=$(git rev-parse HEAD)
   fi
   git submodule update --recursive --init --force
done

if [[ -n $gold ]] && [[ -d $gitroot/helm-chart ]]; then
   cd $gitroot/helm-chart
   git fetch origin && git checkout master && git merge origin/master --ff-only
   if [ $? -ne 0 ]; then
      echo "helm-chart master is inconsistent with origin/master"
      exit 1
   fi
fi

echo Press any key to go rc for "$all"
read -n 1 -s

if [[ -z $gold ]]; then
   # Process every repository, but return the final go_rc.sh failure to the caller.
   status=0
   for f in $all ; do
      cd $gitroot/$f
      echo "Process $f"
      "$hpccdir/cmake_modules/go_rc.sh" "${go_rc_args[@]}" || status=$?
   done
   exit "$status"
fi

for f in $all ; do
   cd $gitroot/$f
   echo "Prepare $f"
   if ! "$hpccdir/cmake_modules/go_rc.sh" "${go_rc_args[@]}" --no-push ; then
      echo "Preparing $f failed.  Nothing has been pushed; reset the local $version branches before retrying."
      exit 1
   fi
   if [[ -z $dryrun ]]; then
      newbranch=$(git rev-parse --abbrev-ref HEAD)
      if [[ ! "$newbranch" =~ ^candidate-${major_minor//./\\.}\.[0-9]+$ ]]; then
         echo "Unexpected gold branch $newbranch in $f"
         exit 1
      fi
      RELEASE_BRANCH[$f]=$newbranch
      refs=("$version" "$newbranch")
      while IFS= read -r t ; do
         refs+=("refs/tags/$t")
      done < <(git tag --points-at HEAD)
      PUSH_REFS[$f]="${refs[*]}"
   fi
done

# go_rc.sh cannot prevent pushes performed by this wrapper, so gate both push phases here.
if [[ -z "$dryrun$nopush" ]]; then
   release_branch=
   # Compare every prepared repository before attempting any remote update.
   for f in $all ; do
      if [[ -z "$release_branch" ]]; then
         release_branch=${RELEASE_BRANCH[$f]}
      elif [[ "${RELEASE_BRANCH[$f]}" != "$release_branch" ]]; then
         echo "Prepared gold branches disagree: $f has ${RELEASE_BRANCH[$f]}, expected $release_branch. Nothing has been pushed."
         exit 1
      fi
   done

   # Leases require the .x ref to remain unchanged and the point branch to remain absent.
   for f in $all ; do
      cd $gitroot/$f
      read -r -a refs <<< "${PUSH_REFS[$f]}"
      leases=("--force-with-lease=refs/heads/$version:${BASE_SHA[$f]}"
              "--force-with-lease=refs/heads/$release_branch:")
      if ! git push --dry-run --atomic "${leases[@]}" origin "${refs[@]}" ; then
         echo "Preflight push failed for $f; a remote branch or tag changed. Nothing has been pushed."
         exit 1
      fi
   done

   for f in $all ; do
      cd $gitroot/$f
      read -r -a refs <<< "${PUSH_REFS[$f]}"
      leases=("--force-with-lease=refs/heads/$version:${BASE_SHA[$f]}"
              "--force-with-lease=refs/heads/$release_branch:")
      echo "Push $f: ${refs[*]}"
      if ! git push --atomic "${leases[@]}" origin "${refs[@]}" ; then
         echo "Push failed for $f.  Later repositories (and helm-chart) have not been pushed."
         exit 1
      fi
   done

   if [[ -d $gitroot/helm-chart ]]; then
      cd $gitroot/helm-chart
      if [[ $(git rev-list --count origin/master..master) != 0 ]]; then
         git push origin master || { echo "Push failed for helm-chart"; exit 1; }
      fi
   fi
fi
