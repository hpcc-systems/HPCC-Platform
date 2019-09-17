#!/bin/bash
#
# Common utilities used by tools for automating tagging and release
#

#We want any failures to be fatal

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

REMOTE=origin
FORCE=
DRYRUN=
IGNORE=
RETAG=
VERBOSE=
VERSIONFILE=version.cmake
if [ ! -f $VERSIONFILE ]; then
  VERSIONFILE=$SCRIPT_DIR/../version.cmake
fi

POSITIONAL=()
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    -f|--force)
    FORCE=-f
    shift
    ;;
    -i|--ignore)
    IGNORE=$1
    shift
    ;;
    -v|--verbose)
    VERBOSE=$1
    shift
    ;;
    --retag)
    RETAG=$1
    shift
    ;;
    -d|--dryrun)
    DRYRUN=$1
    shift
    ;;
    -r|--remote)
    if [ $# -eq 1 ] ; then
      echo "$1 option requires an argument"
      exit 2
    fi
    REMOTE="$2"
    shift 
    shift
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
  esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [ "$#" -ge 1 ] ; then
  VERSIONFILE=$1
  shift 1
fi
if [ ! -f $VERSIONFILE ]; then
  echo "File $VERSIONFILE not found"
  exit 2
fi

GIT_BRANCH=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)

# brute-force way to read cmake values by "parsing" the line that sets them
# note that this makes some assumptions about how the version.cmake is laid out!

function extract()
{
    local _file=$1
    local _name=$2
    local _search=$2
    local _result=`grep -i "set *( *$_search " $_file | sed -E "s/^.*$_search *\"?//" | sed -E "s/\"? *\)//"`
    eval "$_name='$_result'"
}

function parse_cmake()
{
    extract $VERSIONFILE HPCC_NAME
    extract $VERSIONFILE HPCC_PROJECT
    extract $VERSIONFILE HPCC_MAJOR
    extract $VERSIONFILE HPCC_MINOR
    extract $VERSIONFILE HPCC_POINT
    extract $VERSIONFILE HPCC_MATURITY
    extract $VERSIONFILE HPCC_SEQUENCE

    if [ -z "$HPCC_NAME" ] ; then
      if [ "$HPCC_PROJECT" == "community" ] ; then
        HPCC_NAME="Community Edition"
      else
        HPCC_NAME="Enterprise Edition"
      fi
    fi
}

function doit()
{
    if [ -n "$VERBOSE" ] || [ -n "$DRYRUN" ] ; then echo $1 ; fi
    if [ -z "$DRYRUN" ] ; then eval $1 ; fi
}

function set_tag()
{
    local _prefix=$1
    local _maturity=$HPCC_MATURITY
    if [ "$HPCC_MATURITY" = "release" ]; then
      _maturity=
    fi
    HPCC_SHORT_TAG=$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT-$_maturity$HPCC_SEQUENCE
    HPCC_LONG_TAG=${_prefix}_$HPCC_SHORT_TAG
}

function update_version_file()
{
    # Update the version.cmake file
    local _new_maturity=$1
    local _new_point=$2
    local _new_sequence=$3
    local _new_minor=$4
    if [ -z "$_new_minor" ] ; then
      _new_minor=$HPCC_MINOR
    fi
    
    if [ -n "$VERBOSE" ] ; then
      echo sed -E \
       -e "\"s/HPCC_MINOR +$HPCC_MINOR *\)/HPCC_MINOR $_new_minor )/\"" \
       -e "\"s/HPCC_POINT +$HPCC_POINT *\)/HPCC_POINT $_new_point )/\"" \
       -e "\"s/HPCC_SEQUENCE +$HPCC_SEQUENCE *\)/HPCC_SEQUENCE $_new_sequence )/\"" \
       -e "\"s/HPCC_MATURITY +\"$HPCC_MATURITY\" *\)/HPCC_MATURITY \"$_new_maturity\" )/\"" \
       -i.bak $VERSIONFILE
    fi
    if [ -z "$DRYRUN" ] ; then 
      sed -E \
       -e "s/HPCC_MINOR +$HPCC_MINOR *\)/HPCC_MINOR $_new_minor )/" \
       -e "s/HPCC_POINT +$HPCC_POINT *\)/HPCC_POINT $_new_point )/" \
       -e "s/HPCC_SEQUENCE +$HPCC_SEQUENCE *\)/HPCC_SEQUENCE $_new_sequence )/" \
       -e "s/HPCC_MATURITY +\"$HPCC_MATURITY\" *\)/HPCC_MATURITY \"$_new_maturity\" )/" \
       -i.bak $VERSIONFILE
       cat $VERSIONFILE
    else
      sed -E \
       -e "s/HPCC_MINOR +$HPCC_MINOR *\)/HPCC_MINOR $_new_minor )/" \
       -e "s/HPCC_POINT +$HPCC_POINT *\)/HPCC_POINT $_new_point )/" \
       -e "s/HPCC_SEQUENCE +$HPCC_SEQUENCE *\)/HPCC_SEQUENCE $_new_sequence )/" \
       -e "s/HPCC_MATURITY +\"$HPCC_MATURITY\" *\)/HPCC_MATURITY \"$_new_maturity\" )/" \
       $VERSIONFILE
    fi
}

function do_tag()
{
    for f in ${HPCC_PROJECT}; do
      set_tag $f
      if [ "$FORCE" = "-f" ] ; then
        doit "git tag -d $HPCC_LONG_TAG"
      fi
      doit "git tag $HPCC_LONG_TAG"
      doit "git push $REMOTE $HPCC_LONG_TAG $FORCE"
    done
}

function sync_git()
{
    doit "git fetch $REMOTE"
    doit "git merge --ff-only $REMOTE/$GIT_BRANCH"
    doit "git submodule update --init --recursive"
}
