#!/bin/bash
#Display a list of the last changes for each repo for a given version to help decide whether anything needs upmerging

gitroot="${gitroot/#\~/$HOME}"

for f in $all ; do
  cd $gitroot/$f
  git fetch origin
done
for f in $all ; do
  cd $gitroot/$f
  echo $f: $(git log -1 origin/candidate-$1 --oneline | grep -v "Split off")
done
