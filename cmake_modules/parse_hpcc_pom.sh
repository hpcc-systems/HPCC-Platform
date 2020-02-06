#!/bin/bash
#
# Common utilities used by tools for automating tagging and release
#

#We want any failures to be fatal
# Depends: maven

set -e

if ! `which mvn 1>/dev/null 2>&1` ; then
  echo "Maven dependency not located"
  exit 2
fi

# overwrite versionfile to pom.xml
VERSIONFILE=pom.xml
if [ ! -f $VERSIONFILE ]; then
  echo "Expected $VERSIONFILE not found"
  exit 2
fi


# overwrite parse_cmake to target pom files
# expect ex: 7.6.2-0-SNAPSHOT, 7.6.2-1 (gold release), etc
# Major.Minor.Point- Sequence if gold, Maturity if rc
function parse_cmake()
{
  HPCC_PROJECT=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
  HPCC_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
  HPCC_MAJOR=$(echo $HPCC_VERSION | awk 'BEGIN {FS="[.-]"}; {print $1};')
  HPCC_MINOR=$(echo $HPCC_VERSION | awk 'BEGIN {FS="[.-]"}; {print $2};')
  HPCC_POINT=$(echo $HPCC_VERSION | awk 'BEGIN {FS="[.-]"}; {print $3};')
  HPCC_SEQUENCE=$(echo $HPCC_VERSION | awk 'BEGIN {FS="[.-]"}; {print $4};')
  HPCC_MATURITY=$(echo $HPCC_VERSION | awk 'BEGIN {FS="[.-]"}; {print $5};')

  if ! `echo $HPCC_SEQUENCE | grep -Eq ^[0-9]+$`; then
    echo "Error: HPCC_SEQUENCE not an Integer"
    echo "-- possibly on branch using old versioning scheme"
    exit 2
  fi

  # translate pom maturity to what go_rc/go_gold understand
  if [ -z "$HPCC_MATURITY" ] ; then
    # branch on gold release
    HPCC_MATURITY=release
  elif [ "$HPCC_MATURITY" == "SNAPSHOT" ] ; then
    if (( "$HPCC_POINT" % 2 != 1 )) ; then
      HPCC_MATURITY=rc
    else
      HPCC_MATURITY=closedown
    fi
  fi

}

function set_tag()
{
    local _prefix=$1
    local _maturity=$HPCC_MATURITY
    if [ "$HPCC_MATURITY" == "rc" ] ; then
      _maturity=SNAPSHOT
    fi
    # will keep -release or -SNAPSHOT as maturity for tagging
    HPCC_SHORT_TAG=${HPCC_MAJOR}.${HPCC_MINOR}.${HPCC_POINT}-${HPCC_SEQUENCE}-${_maturity}
    HPCC_LONG_TAG=${_prefix}_$HPCC_SHORT_TAG
}

function update_version_file()
{
    # Update the pom.xml file
    local _new_maturity=$1
    local _new_point=$2
    local _new_sequence=$3
    local _new_minor=$4
    if [ -z "$_new_minor" ] ; then
      _new_minor=$HPCC_MINOR
    fi
    if [ "$_new_maturity" == "rc" ]; then
      _new_maturity=-SNAPSHOT
    elif [ "$_new_maturity" == "closedown" ]; then
      _new_maturity=-SNAPSHOT
    else
      # don't set for non-snapshots
      _new_maturity=
    fi
    local _v="${HPCC_MAJOR}.${_new_minor}.${_new_point}-${_new_sequence}${_new_maturity}"
    local mvn_version_update_cmd="mvn versions:set -DnewVersion=$_v"
    if [ -n "$VERBOSE" ] ; then
      echo  "$mvn_version_update_cmd"
    fi
    if [ -z "$DRYRUN" ] ; then 
      eval "$mvn_version_update_cmd"
      find . -name 'pom.xml' | xargs git add
    else
      echo  "Update to $_v"
    fi
}
