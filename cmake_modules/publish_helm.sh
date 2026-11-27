#!/bin/bash
#
# Publish the helm charts for the currently checked out version to ../helm-chart.
# Used to recover a release that was tagged but whose charts were not published.
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

. $SCRIPT_DIR/parse_cmake.sh
. $SCRIPT_DIR/parse_hpcc_chart.sh

parse_cmake

if [ ! -e helm/hpcc/Chart.yaml ] ; then
  echo "No helm chart found in $(pwd)"
  exit 2
fi

set_tag
if [ "$HPCC_MATURITY" = "release" ] ; then
  publish_helm_charts gold
else
  publish_helm_charts
fi
