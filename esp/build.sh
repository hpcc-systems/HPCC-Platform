#!/usr/bin/env bash

set -e

# Base directory for this entire project
BASEDIR=$(cd $(dirname $0) && pwd)

# Source directory for unbuilt code
SRCDIR="$BASEDIR/src"

# Directory containing dojo build utilities
TOOLSDIR="$SRCDIR/util/buildscripts"

# Destination directory for built code
DISTDIR=$1

# Module ID of the main application package loader configuration
LOADERMID="eclwatch/run"

# Main application package loader configuration
LOADERCONF="$SRCDIR/$LOADERMID.js"

# Main application package build configuration
PROFILE="$BASEDIR/profiles/eclwatch.profile.js"

# Configuration over. Main application start up!

echo "Building application with $PROFILE to $DISTDIR."

echo -n "Cleaning old files..."
rm -rf "$DISTDIR"
echo " Done"

if [ ! -d "$TOOLSDIR" ]; then
    echo "Can't find Dojo build tools -- did you initialise submodules? (git submodule update --init --recursive)"
    exit 1
fi

mkdir -p "$DISTDIR"
cp -r "$SRCDIR/CodeMirror2" "$DISTDIR/CodeMirror2"
mkdir "$DISTDIR/dc"
cp "$SRCDIR/dc/dc.min.js" "$DISTDIR/dc/dc.js"
mkdir "$DISTDIR/crossfilter"
cp "$SRCDIR/crossfilter/crossfilter.min.js" "$DISTDIR/crossfilter/crossfilter.js"

cd "$TOOLSDIR"

if which node >/dev/null; then
    node ../../dojo/dojo.js load=build --require "$LOADERCONF" --profile "$PROFILE" --releaseDir "$DISTDIR" ${*:2}
elif which java >/dev/null; then
    java -Xms256m -Xmx256m  -cp ../shrinksafe/js.jar:../closureCompiler/compiler.jar:../shrinksafe/shrinksafe.jar org.mozilla.javascript.tools.shell.Main  ../../dojo/dojo.js baseUrl=../../dojo load=build --require "$LOADERCONF" --profile "$PROFILE" --releaseDir "$DISTDIR" ${*:2}
else
    echo "Need node.js or Java to build!"
    exit 1
fi

cd "$BASEDIR"

LOADERMID=${LOADERMID//\//\\\/}

# Copy & minify stub.htm to dist
cat "$SRCDIR/stub.htm" | tr '\n' ' ' | \
perl -pe "
  s/<\!--.*?-->//g;                          # Strip comments
#  s/isDebug: *1/deps:['$LOADERMID']/;        # Remove isDebug, add deps
#  s/<script src=\"$LOADERMID.*?\/script>//;  # Remove script eclwatch/run
  s/\s+/ /g;                                 # Collapse white-space" > "$DISTDIR/stub.htm"

echo "Build complete"
