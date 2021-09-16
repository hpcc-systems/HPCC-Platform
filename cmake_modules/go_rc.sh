#!/bin/bash
#
# Automatically tag the first rc for a new point release
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_cmake.sh
. $SCRIPT_DIR/parse_hpcc_chart.sh

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
    NEW_MINOR=$HPCC_MINOR
  fi
  if [ "$GIT_BRANCH" != "candidate-$HPCC_MAJOR.$NEW_MINOR.x" ]; then
    echo "Current branch should be candidate-$HPCC_MAJOR.$NEW_MINOR.x"
    exit 2
  fi
  doit "git checkout -b candidate-$HPCC_MAJOR.$NEW_MINOR.$NEW_POINT"
  doit "git checkout $GIT_BRANCH"
  doit "git submodule update --init --recursive"
  update_version_file closedown $((NEW_POINT+1)) 0 $NEW_MINOR
  if [ -e helm/hpcc/Chart.yaml ] ; then
    update_chart_file helm/hpcc/Chart.yaml closedown $((NEW_POINT+1)) 0 $NEW_MINOR 
    doit "git add helm/hpcc/Chart.yaml"
    for f in helm/hpcc/templates/* ; do
      update_chart_file $f closedown $((NEW_POINT+1)) 0 $NEW_MINOR 
    if [ "$CHART_CHANGED" != "0" ] ; then
        doit "git add $f"
      fi
    done
  fi
  doit "git add $VERSIONFILE"
  doit "git commit -s -m \"Split off $HPCC_MAJOR.$NEW_MINOR.$NEW_POINT\""
  doit "git push $REMOTE"
  GIT_BRANCH=candidate-$HPCC_MAJOR.$NEW_MINOR.$NEW_POINT
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
  NEW_MINOR=$HPCC_MINOR
  NEW_SEQUENCE=$((HPCC_SEQUENCE+1))
fi

update_version_file rc $NEW_POINT $NEW_SEQUENCE $NEW_MINOR
if [ -e helm/hpcc/Chart.yaml ] ; then
  update_chart_file helm/hpcc/Chart.yaml rc $NEW_POINT $NEW_SEQUENCE $NEW_MINOR 
  doit "git add helm/hpcc/Chart.yaml"
  for f in helm/hpcc/templates/* ; do
    update_chart_file $f rc $NEW_POINT $NEW_SEQUENCE $NEW_MINOR 
    if [ "$CHART_CHANGED" != "0" ] ; then
      doit "git add $f"
    fi
  done
fi

HPCC_MATURITY=rc
HPCC_SEQUENCE=$NEW_SEQUENCE
HPCC_MINOR=$NEW_MINOR
HPCC_POINT=$NEW_POINT
set_tag

# Commit the change
doit "git add $VERSIONFILE"
doit "git commit -s -m \"$HPCC_NAME $HPCC_SHORT_TAG Release Candidate $HPCC_SEQUENCE\""
doit "git push $REMOTE $GIT_BRANCH $FORCE"

# tag it
do_tag

if [ -e helm/hpcc/Chart.yaml ] ; then
  # We publish any tagged version of helm chart to the helm-chart repo
  # but only copy helm chart sources across for "latest stable" version
  HPCC_DIR="$( pwd )"
  doit2 "pushd ../helm-chart 2>&1 > /dev/null"
  doit "git fetch $REMOTE"
  doit "git checkout master"
  doit "git merge --ff-only $REMOTE/master"
  doit "git submodule update --init --recursive"
  HPCC_PROJECTS=hpcc-helm
  HPCC_NAME=HPCC
  doit2 "cd docs"
  doit "helm package ${HPCC_DIR}/helm/hpcc/"
  doit "helm repo index . --url https://hpcc-systems.github.io/helm-chart"
  doit "git add *.tgz"
  
  doit "git commit -a -s -m \"$HPCC_NAME Helm Charts $HPCC_SHORT_TAG Release Candidate $HPCC_SEQUENCE\""
  doit "git push $REMOTE master $FORCE"
  doit2 "popd 2>&1 > /dev/null"
fi
