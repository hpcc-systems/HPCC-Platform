#!/bin/bash

cd ./tests

declare -i RESULT=0

for file in *.class; do
    echo "$(basename "$file")"
    java "${file%.*}" $1
    RESULT+=$?
done

exit $RESULT
