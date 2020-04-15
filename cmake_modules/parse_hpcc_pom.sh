#!/bin/bash
#
# Common utilities used by tools for automating tagging and release
#

#We want any failures to be fatal
# Depends: maven

set -e

#default _mvn_return_value should be 0 for found
_mvn_return_value=0

if ! `which mvn 1>/dev/null 2>&1` ; then
  echo "Maven dependency not located"
  echo "-- Using gnu utils instead"
  _mvn_return_value=1
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
  if [ $_mvn_return_value -eq 0 ]; then
    HPCC_PROJECT=$(mvn help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
    HPCC_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
  else
    HPCC_PROJECT=$(grep -m1 '<artifactId>' $VERSIONFILE | sed "s/^[ \t]*//" | awk 'BEGIN {FS="[<>]";} {print $3;}')
    HPCC_VERSION=$(grep -m1 '<version>' $VERSIONFILE | sed "s/^[ \t]*//" | awk 'BEGIN {FS="[<>]";} {print $3;}')
  fi
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
    if [ $_mvn_return_value -eq 0 ]; then
        local version_update_cmd="mvn versions:set -DnewVersion=$_v"
    else
        local version_update_cmd="sed -i .old 's/${HPCC_VERSION}/${_v}/' pom.xml"
    fi
    if [ -n "$VERBOSE" ] ; then
      echo  "$version_update_cmd"
    fi
    if [ -z "$DRYRUN" ] ; then
        if [ $_mvn_return_value -eq 0 ]; then
            eval "$version_update_cmd"
            # find all modified pom.xml in directory tree
            find . -name 'pom.xml' | xargs git add
        else
            #handle submodules since maven isn't being used to do it for us
            local _submodules=($(sed 's/^[ \t]*//' pom.xml | awk 'BEGIN {FS="[<>]";} /<module>/ {print $3;}'))
            if [ ${#_submodules[@]} -gt 0 ]; then
                for _d in "${_submodules[@]}"; do
                    pushd $_d > /dev/null
                    eval "$version_update_cmd"
                    git add pom.xml
                    popd > /dev/null
                done
            fi
            eval "$version_update_cmd"
            git add pom.xml
        fi
    else
      echo  "Update to $_v"
    fi
}
