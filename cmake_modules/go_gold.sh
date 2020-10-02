#!/bin/bash
#
# Automatically tag an existing release candidate build as gold
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_cmake.sh
. $SCRIPT_DIR/parse_hpcc_chart.sh

if [ -e pom.xml ] ; then
  . $SCRIPT_DIR/parse_hpcc_pom.sh
fi

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
if [ -e helm/hpcc/Chart.yaml ] ; then
  update_chart_file helm/hpcc/Chart.yaml release $HPCC_POINT $NEW_SEQUENCE
  doit "git add helm/hpcc/Chart.yaml"
fi

HPCC_MATURITY=release
HPCC_SEQUENCE=$NEW_SEQUENCE
set_tag

# Commit the change
doit "git add $VERSIONFILE"
doit "git commit -s -m \"$HPCC_NAME $HPCC_SHORT_TAG Gold\""
doit "git push $REMOTE $GIT_BRANCH $FORCE"

# tag it
do_tag

if [ -e helm/hpcc/Chart.yaml ] ; then
  # We publish any tagged version of helm chart to the helm-chart repo
  # but only copy helm chart sources across for "latest stable" version

  HPCC_DIR="$( pwd )"
  pushd ../helm-chart 2>&1 > /dev/null
  doit "git fetch $REMOTE"
  doit "git checkout master"
  doit "git merge --ff-only $REMOTE/master"
  doit "git submodule update --init --recursive"
  HPCC_PROJECTS=hpcc-helm
  HPCC_NAME=HPCC
  if [[ "$HPCC_MAJOR" == "7" ]] && [[ "$HPCC_MINOR" == "12" ]] ; then
    doit "rm -rf ./helm"
    doit "cp -rf $HPCC_DIR/helm ./helm" 
    doit "rm ./helm/hpcc/*.bak" 
    doit "git add -A ./helm"
  fi
  cd docs
  for f in `find ${HPCC_DIR}/helm/examples -name Chart.yaml` ; do 
    doit "helm package ${f%/*}/"  
  done
  doit "helm package ${HPCC_DIR}/helm/hpcc/"
  doit "helm repo index . --url https://hpcc-systems.github.io/helm-chart"
  doit "git add *.tgz"
  
  doit "git commit -a -s -m \"$HPCC_NAME Helm Charts $HPCC_SHORT_TAG\""
  if [[ "$HPCC_MAJOR" == "7" ]] && [[ "$HPCC_MINOR" == "10" ]] ; then
    doit "git tag $FORCE $HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT && git push $REMOTE $HPCC_MAJOR.$HPCC_MINOR.$HPCC_POINT $FORCE"
  fi
  doit "git push $REMOTE master $FORCE"
  popd
fi
