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
CREATE_NEW_MAJOR=0
GOLD=
NOPUSH=

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
    -m|--major)
    CREATE_NEW_MAJOR=1
    shift
    ;;
    --gold)
    GOLD=$1
    shift
    ;;
    --no-push)
    # Work on local state only: no fetch, submodule update or push.
    # The caller is responsible for pushing the resulting branches and tags.
    NOPUSH=$1
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

GIT_BRANCH=$(git rev-parse --symbolic-full-name --abbrev-ref HEAD)

# brute-force way to read cmake values by "parsing" the line that sets them
# note that this makes some assumptions about how the version.cmake is laid out!

function extract()
{
    local _file=$1
    local _name=$2
    local _search=$2
    local _result
    # Normalize conditional definitions before assigning the value without eval.
    _result=$(grep -i "set *( *$_search " "$_file" |
      sed -E "s/^.*$_search *\"?//" | sed -E "s/\"? *\)//" |
      tr '\n' ' ' | sed -E 's/[[:space:]]+$//')
    printf -v "$_name" '%s' "$_result"
}

function parse_cmake()
{
    if [ ! -f $VERSIONFILE ]; then
      echo "File $VERSIONFILE not found"
      exit 2
    fi
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

function validate_metadata()
{
    # Legacy command construction uses these values, so reject shell metacharacters first.
    [[ "${HPCC_NAME:-}" == "" || "$HPCC_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_.\ -]*$ ]] ||
      { echo "Invalid HPCC_NAME metadata" >&2; return 2; }
    [[ "${HPCC_PROJECT:-}" =~ ^[A-Za-z0-9_.-]+( [A-Za-z0-9_.-]+)*$ ]] ||
      { echo "Invalid HPCC_PROJECT metadata" >&2; return 2; }
    [[ "${HPCC_MAJOR:-}" =~ ^[0-9]+$ && "${HPCC_MINOR:-}" =~ ^[0-9]+$ &&
       "${HPCC_POINT:-}" =~ ^[0-9]+$ && "${HPCC_SEQUENCE:-}" =~ ^[0-9]+$ ]] ||
      { echo "Invalid numeric version metadata" >&2; return 2; }
    [[ "${HPCC_MATURITY:-}" =~ ^(trunk|closedown|rc|release|SNAPSHOT)$ ]] ||
      { echo "Invalid HPCC_MATURITY metadata" >&2; return 2; }
}

function doit()
{
    if [ -n "$VERBOSE" ] || [ -n "$DRYRUN" ] ; then echo $1 ; fi
    if [ -z "$DRYRUN" ] ; then eval $1 ; fi
}

function doit2()
{
    if [ -n "$VERBOSE" ] || [ -n "$DRYRUN" ] ; then echo $1 ; fi
    eval $1
}

# Commands that contact the remote are skipped with --no-push
function doremote()
{
    if [ -n "$NOPUSH" ] ; then
      echo "Skipped (--no-push): $1"
    else
      doit "$1"
    fi
}

function update_submodules()
{
    doremote "git submodule update --init --recursive"
}

function set_tag()
{
    local _prefix=$1
    if [ "$HPCC_MATURITY" = "release" ]; then
      HPCC_SHORT_TAG=$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT
      HPCC_LONG_TAG=${_prefix}_$HPCC_SHORT_TAG-$HPCC_SEQUENCE
    else
      HPCC_SHORT_TAG=$HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT-$HPCC_MATURITY$HPCC_SEQUENCE
      HPCC_LONG_TAG=${_prefix}_$HPCC_SHORT_TAG
    fi
}

function update_version_file()
{
    # Update the version.cmake file
    local _new_maturity=$1
    local _new_point=$2
    local _new_sequence=$3
    local _new_minor=$4
    local _new_major=$5
    local _new_timestamp=$(date -u "+%Y-%m-%dT%H:%M:%SZ")

    if [ -z "$_new_minor" ] ; then
      _new_minor=$HPCC_MINOR
    fi
    if [ -z "$_new_major" ] ; then
      _new_major=$HPCC_MAJOR
    fi
    
    if [ -n "$VERBOSE" ] ; then
      echo sed -E \
       -e "\"s/HPCC_MAJOR +$HPCC_MAJOR *\)/HPCC_MAJOR $_new_major )/\"" \
       -e "\"s/HPCC_MINOR +$HPCC_MINOR *\)/HPCC_MINOR $_new_minor )/\"" \
       -e "\"s/HPCC_POINT +$HPCC_POINT *\)/HPCC_POINT $_new_point )/\"" \
       -e "\"s/HPCC_SEQUENCE +$HPCC_SEQUENCE *\)/HPCC_SEQUENCE $_new_sequence )/\"" \
       -e "\"s/HPCC_MATURITY +\"$HPCC_MATURITY\" *\)/HPCC_MATURITY \"$_new_maturity\" )/\"" \
       -e "\"s/HPCC_TAG_TIMESTAMP.*\)/HPCC_TAG_TIMESTAMP \"$_new_timestamp\" )/\"" \
       -i.bak $VERSIONFILE
    fi
    if [ -z "$DRYRUN" ] ; then 
      sed -E \
       -e "s/HPCC_MAJOR +$HPCC_MAJOR *\)/HPCC_MAJOR $_new_major )/" \
       -e "s/HPCC_MINOR +$HPCC_MINOR *\)/HPCC_MINOR $_new_minor )/" \
       -e "s/HPCC_POINT +$HPCC_POINT *\)/HPCC_POINT $_new_point )/" \
       -e "s/HPCC_SEQUENCE +$HPCC_SEQUENCE *\)/HPCC_SEQUENCE $_new_sequence )/" \
       -e "s/HPCC_MATURITY +\"$HPCC_MATURITY\" *\)/HPCC_MATURITY \"$_new_maturity\" )/" \
       -e "s/HPCC_TAG_TIMESTAMP.*\)/HPCC_TAG_TIMESTAMP \"$_new_timestamp\" )/" \
       -i.bak $VERSIONFILE
       cat $VERSIONFILE
    else
      sed -E \
       -e "s/HPCC_MAJOR +$HPCC_MAJOR *\)/HPCC_MAJOR $_new_major )/" \
       -e "s/HPCC_MINOR +$HPCC_MINOR *\)/HPCC_MINOR $_new_minor )/" \
       -e "s/HPCC_POINT +$HPCC_POINT *\)/HPCC_POINT $_new_point )/" \
       -e "s/HPCC_SEQUENCE +$HPCC_SEQUENCE *\)/HPCC_SEQUENCE $_new_sequence )/" \
       -e "s/HPCC_MATURITY +\"$HPCC_MATURITY\" *\)/HPCC_MATURITY \"$_new_maturity\" )/" \
       -e "s/HPCC_TAG_TIMESTAMP.*\)/HPCC_TAG_TIMESTAMP \"$_new_timestamp\" )/" \
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
      # Provide a tag message to avoid interactive editor prompts when tag.gpgSign is enabled.
      doit "git tag -m \"$HPCC_NAME $HPCC_SHORT_TAG\" $HPCC_LONG_TAG"
      doremote "git push $REMOTE $HPCC_LONG_TAG $FORCE"
    done
}

function sync_git()
{
    doremote "git fetch $REMOTE"
    doremote "git merge --ff-only $REMOTE/$GIT_BRANCH"
    update_submodules
}

# Package the helm charts for the current version into ../helm-chart and commit them.
# Must be called after set_tag.  Gold releases also publish the example and managed charts.
function publish_helm_charts()
{
    local _gold=$1
    local _message
    if [ -n "$_gold" ] ; then
      _message="HPCC Helm Charts $HPCC_SHORT_TAG-$HPCC_SEQUENCE"
    else
      _message="HPCC Helm Charts $HPCC_SHORT_TAG Release Candidate $HPCC_SEQUENCE"
    fi

    # We publish any tagged version of helm chart to the helm-chart repo
    # but only copy helm chart sources across for "latest stable" version
    HPCC_DIR="$( pwd )"
    doit2 "pushd ../helm-chart 2>&1 > /dev/null"
    doremote "git fetch $REMOTE"
    doit "git checkout master"
    doremote "git merge --ff-only $REMOTE/master"
    update_submodules
    if [ -n "$_gold" ] && [[ "$HPCC_MAJOR" == "8" ]] && [[ "$HPCC_MINOR" == "10" ]] ; then
      doit "rm -rf ./helm"
      doit "cp -rf $HPCC_DIR/helm ./helm"
      doit "rm -f ./helm/*.bak"
      doit "git add -A ./helm"
    fi
    doit2 "cd docs"
    if [ -n "$_gold" ] ; then
      for f in `find ${HPCC_DIR}/helm/examples ${HPCC_DIR}/helm/managed -name Chart.yaml` ; do
        doit "helm package ${f%/*}/ --dependency-update"
      done
    fi
    doit "helm package ${HPCC_DIR}/helm/hpcc/"
    doit "helm repo index . --url https://hpcc-systems.github.io/helm-chart"
    doit "git add *.tgz"

    doit "git commit -a -s -m \"$_message\""
    if [ -n "$_gold" ] && [[ "$HPCC_MAJOR" == "8" ]] && [[ "$HPCC_MINOR" == "10" ]] ; then
      # Provide a tag message to avoid interactive editor prompts when tag.gpgSign is enabled.
      doit "git tag $FORCE -m \"$_message\" $HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT"
      doremote "git push $REMOTE $HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT $FORCE"
    fi
    doremote "git push $REMOTE master $FORCE"
    doit2 "popd 2>&1 > /dev/null"
}
