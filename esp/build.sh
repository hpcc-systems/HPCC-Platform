#!/usr/bin/env bash
echoerr() { echo "$@" 1>&2; }

set -e

# Base directory for this entire project
BASEDIR=$(cd $(dirname $0) && pwd)

# Source directory for unbuilt code
SRCDIR="$BASEDIR/src"

# Directory containing dojo build utilities
TOOLSDIR="$SRCDIR/util/buildscripts"

# Destination directory for built code
DISTDIR=$1

# Main application package build configuration
PROFILE="$BASEDIR/profiles/eclwatch.profile.js"

# Configuration over. Main application start up!

echo "Building application with $PROFILE to $DISTDIR."

echo -n "Cleaning old files..."
rm -rf "$DISTDIR"
echo " Done"

if [ ! -d "$TOOLSDIR" ]; then
    echoerr "ERROR:  Can't find Dojo build tools -- did you initialise submodules? (git submodule update --init --recursive)"
    exit 1
fi

mkdir -p "$DISTDIR"
cp -r "$SRCDIR/CodeMirror2" "$DISTDIR/CodeMirror2"

# Copy & minify stub.htm to dist
cat "$SRCDIR/stub.htm" | tr '\n' ' ' | \
perl -pe "
  s/<\!--.*?-->//g;                          # Strip comments
  s/\s+/ /g;                                 # Collapse white-space" > "$DISTDIR/stub.htm"

echo "Building: $SRCDIR/Visualization"
cd "$SRCDIR/Visualization/"
./build.sh "$DISTDIR/Visualization/widgets"

cd "$TOOLSDIR"

if which node >/dev/null; then
    node ../../dojo/dojo.js baseUrl=../../dojo load=build --profile "$PROFILE" --releaseDir "$DISTDIR" ${*:2}
else
    echoerr "ERROR:  node.js is required to build - see https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager"
    exit 1
fi

echo "Build complete"

cd "$BASEDIR"

for dojodir in dojo dojox dijit
do
  for f in  $(find ${DISTDIR}/${dojo_dir} -type f -perm /a+x ! -name "*.sh" \
              ! -name "*.php" ! -name "*.cgi" -print)
  do
     chmod -x $f
  done
done

echo "Post process complete"
