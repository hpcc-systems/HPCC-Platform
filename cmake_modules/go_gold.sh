#!/bin/bash
#
# Automatically tag an existing release candidate build as gold
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_cmake.sh

sync_git
parse_cmake

NEW_SEQUENCE=1

if [ "$HPCC_MATURITY" != "rc" ] ; then
  if [ "$HPCC_MATURITY" = "release" ] ; then
    NEW_SEQUENCE=$((HPCC_SEQUENCE+1))
    if [ -z "$RETAG" ] ; then
      echo "Current version is already at release level. Specify --retag to create $HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT-$NEW_SEQUENCE"
      exit 2
    fi
  else
    echo "Current version should be at rc level to go gold"
    exit 2
  fi
fi
if (( "$HPCC_POINT" % 2 == 1 )) ; then
  echo "Current version should have even point version to go gold"
  exit 2
fi
if [ "$GIT_BRANCH" != "candidate-$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT" ]; then
  echo "Current branch should be candidate-$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT"
  exit 2
fi

for f in ${HPCC_PROJECT}; do
  set_tag $f
  if [ $(git rev-parse HEAD) != $(git rev-parse $HPCC_LONG_TAG) ] ; then 
    if [ -z "$IGNORE" ] ; then
      git diff $HPCC_LONG_TAG
      echo "There are changes on this branch since $HPCC_LONG_TAG. Use --ignore if you still want to tag Gold"
      exit 2
    fi
  fi
done

update_version_file release $HPCC_POINT $NEW_SEQUENCE
HPCC_MATURITY=release
HPCC_SEQUENCE=$NEW_SEQUENCE
set_tag

# Commit the change
doit "git add $VERSIONFILE"
doit "git commit -s -m \"$HPCC_NAME $HPCC_SHORT_TAG Gold\""
doit "git push $REMOTE $GIT_BRANCH $FORCE"

# tag it
do_tag