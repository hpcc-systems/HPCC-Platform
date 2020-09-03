#!/bin/bash
scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
options="--set global.image.version=someversion --set global.image.pullPolicy=Always"
hpccchart=$scriptdir/../../helm/hpcc
failed=0

helm version
echo Testing unmodified values file
helm lint $hpccchart ${options} > results.txt 2> errors.txt
if [ $? -ne 0 ]
then
   echo Unmodified failed
   cat errors.txt
   cat results.txt
   failed=1
fi

echo Running valid tests...
for file in $scriptdir/tests/*.yaml
do
   helm lint $hpccchart ${options} --values $file > results.txt 2> errors.txt
   if [ $? -ne 0 ]
   then
      echo $file failed
      cat errors.txt
      cat results.txt
      failed=1
   fi
done

echo Running invalid tests...
for file in $scriptdir/errtests/*.yaml
do
   helm lint $hpccchart ${options} --values $file > results.txt 2> errors.txt
   if [ $? -eq 0 ]
   then
      echo $file should have failed
      failed=1
   else
      echo "$file failed - correctly"
      cat results.txt
   fi
done

if type kubeval >/dev/null 2> /dev/null; then
   echo Running kubeval...
   helm template $hpccchart ${options} | kubeval --strict - >results.txt 2>errors.txt
   if [ $? -ne 0 ]
   then
      echo $file failed
      cat errors.txt
      cat results.txt
      failed=1
   fi
fi

if type kube-score >/dev/null 2> /dev/null; then
   echo Running kube-score...
   # Note we force all replicas to be > 1 as some checks are not done on replicas=1 cases e.g. antiaffinity
   helm template $hpccchart ${options} | sed "s/replicas: 1/replicas: 2/" | kube-score score --output-format ci - >results.txt 2>errors.txt
   if [ $? -ne 0 ]
   then
      echo $file failed
      cat errors.txt
      cat results.txt
      failed=1
   fi
fi

exit $failed
