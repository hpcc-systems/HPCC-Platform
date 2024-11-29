#!/bin/bash

UI_TEST_PATH=$1
ECLWATCH_URL=$2

cd $UI_TEST_PATH

javac *.java

declare -i RESULT=0

for file in *.class; do
    echo "$(basename "$file")"
    java "${file%.*}" $ECLWATCH_URL
    RESULT+=$?
done

exit $RESULT
