#!/usr/bin/env bash
echoerr() { echo "$@" 1>&2; }

set -e

# Base directory for this entire project
BASEDIR=$(cd $(dirname $0) && pwd)

# Source directory for unbuilt code
SRCDIR="$BASEDIR/src"

# Destination directory for built code
DISTDIR=$1

# Configuration over. Main application start up!

echo "EclWatch:  Building to $DISTDIR."

echo -n "EclWatch:  Cleaning $DISTDIR"
rm -rf "$DISTDIR"
echo "EclWatch:  Cleaning Done"

mkdir -p "$DISTDIR"
mkdir "$DISTDIR/crossfilter"
cp "$SRCDIR/crossfilter/crossfilter.min.js" "$DISTDIR/crossfilter/crossfilter.min.js"
cp "$SRCDIR/crossfilter/LICENSE" "$DISTDIR/LICENSE"

# Copy & minify stub.htm to dist
cat "$SRCDIR/stub.htm" | tr '\n' ' ' | \
perl -pe "
  s/<\!--.*?-->//g;                          # Strip comments
  s/\s+/ /g;                                 # Collapse white-space" > "$DISTDIR/stub.htm"

cd "$SRCDIR"

echo "EclWatch:  Build complete"
