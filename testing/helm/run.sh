#!/bin/bash
scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
options="--set global.image.version=latest"
hpccchart=$scriptdir/../../helm/hpcc

echo Running valid tests...
for file in $scriptdir/tests/*.yaml
do
   helm lint $hpccchart ${options} --values $file > results.txt 2> errors.txt
   if [ $? -ne 0 ]
   then
      echo $file failed
      cat errors.txt
      cat results.txt
   fi
done

echo Running invalid tests...
for file in $scriptdir/errtests/*.yaml
do
   helm lint $hpccchart ${options} --values $file > results.txt 2> errors.txt
   if [ $? -eq 0 ]
   then
      echo $file should have failed
   else
      echo $file failed
      cat results.txt
   fi
done
