#!/bin/bash
#
# Automatically tag the first rc for a new point release
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_cmake.sh
if [ -e pom.xml ] ; then
  . $SCRIPT_DIR/parse_hpcc_pom.sh
fi

sync_git
parse_cmake

if [ "$HPCC_MATURITY" = "closedown" ] ; then
  if (( "$HPCC_POINT" % 2 != 1 )) ; then
    if [ "$HPCC_POINT" = "0" ] ; then
      # special case when creating new minor release
      NEW_POINT=0
      if (( "$HPCC_MINOR" % 2 == 1 )) ; then
        NEW_MINOR=$((HPCC_MINOR+1))
      else
        echo "A closedown version should have an odd point or minor version to create a new rc"
        exit 2
      fi
    else
      echo "A closedown version should have an odd point version to create a new rc"
      exit 2
    fi
  else
    NEW_POINT=$((HPCC_POINT+1))
  fi
  if [ "$GIT_BRANCH" != "candidate-$HPCC_MAJOR.$HPCC_MINOR.x" ]; then
    echo "Current branch should be candidate-$HPCC_MAJOR.$HPCC_MINOR.x"
    exit 2
  fi
  doit "git checkout -b candidate-$HPCC_MAJOR.$HPCC_MINOR.$NEW_POINT"
  doit "git checkout $GIT_BRANCH"
  doit "git submodule update --init --recursive"
  update_version_file closedown $((NEW_POINT+1)) 0
  doit "git add $VERSIONFILE"
  doit "git commit -s -m \"Split off $HPCC_MAJOR.$HPCC_MINOR.$NEW_POINT\""
  doit "git push $REMOTE"
  GIT_BRANCH=candidate-$HPCC_MAJOR.$HPCC_MINOR.$NEW_POINT
  doit "git checkout $GIT_BRANCH"
  doit "git submodule update --init --recursive"
  NEW_SEQUENCE=1
else
  if [ "$HPCC_MATURITY" != "rc" ] ; then
    echo "Current branch should have closedown or rc maturity"
    exit 2
  fi  
  if [ "$GIT_BRANCH" != "candidate-$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT" ]; then
    echo "Current branch should be candidate-$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT"
    exit 2
  fi
  NEW_POINT=$HPCC_POINT
  NEW_SEQUENCE=$((HPCC_SEQUENCE+1))
fi

update_version_file rc $NEW_POINT $NEW_SEQUENCE $NEW_MINOR
HPCC_MATURITY=rc
HPCC_SEQUENCE=$NEW_SEQUENCE
HPCC_POINT=$NEW_POINT
set_tag

# Commit the change
doit "git add $VERSIONFILE"
doit "git commit -s -m \"$HPCC_NAME $HPCC_SHORT_TAG Release Candidate $HPCC_SEQUENCE\""
doit "git push $REMOTE $GIT_BRANCH $FORCE"

# tag it
do_tag
