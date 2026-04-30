#!/bin/bash
scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
options="--set global.image.version=someversion --set global.image.pullPolicy=Always"
hpccchart=$scriptdir/../../helm/hpcc
failed=0

assert_contains() {
   local file=$1
   local pattern=$2
   local message=$3

   if ! grep -q "$pattern" "$file"
   then
      echo "$message"
      failed=1
   fi
}

assert_not_contains() {
   local file=$1
   local pattern=$2
   local message=$3

   if grep -q "$pattern" "$file"
   then
      echo "$message"
      failed=1
   fi
}

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
   else
      helm template $hpccchart ${options} --values $file > results.txt 2> errors.txt
      if [ $? -ne 0 ]
      then
         echo $file failed
         cat errors.txt
         cat results.txt
         failed=1
      else
             base=$(basename "$file" .yaml)
             if [[ "$base" =~ ^component-config-sasha-([^-]+)-(enabled|disabled)$ ]]; then
                service="${BASH_REMATCH[1]}"
                state="${BASH_REMATCH[2]}"
                configmap_name="sasha-${service}-configmap-volume"
                mount_path="/etc/component-configs/sasha-${service}"

                if [[ "$state" == "enabled" ]]; then
                   assert_contains results.txt "$mount_path" "$file did not mount $mount_path for $service enabled"
                   assert_contains results.txt "name: $configmap_name" "$file did not render $configmap_name for $service enabled"
                else
                   assert_not_contains results.txt "$mount_path" "$file unexpectedly mounted $mount_path for $service disabled"
                   assert_not_contains results.txt "name: $configmap_name" "$file unexpectedly rendered $configmap_name for $service disabled"
                fi
             fi
      fi
   fi
done

echo Running invalid tests...
for file in $scriptdir/errtests/*.yaml
do
   helm lint $hpccchart ${options} --values $file > results.txt 2> errors.txt
   if [ $? -eq 0 ]
   then
      helm template $hpccchart ${options} --values $file > results.txt 2> errors.txt
      if [ $? -eq 0 ]
      then
         echo $file should have failed
         failed=1
      else
         echo "$file failed - correctly"
         cat errors.txt
      fi
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
      echo kubeval --strict failed
      cat errors.txt
      cat results.txt
      failed=1
   fi
fi

if type kube-score >/dev/null 2> /dev/null; then
   echo Running $(kube-score version)
   # Note we force all replicas to be > 1 as some checks are not done on replicas=1 cases e.g. antiaffinity
   helm template $hpccchart ${options} | sed "s/replicas: 1/replicas: 2/" | \
     kube-score score --output-format ci \
        --ignore-container-cpu-limit \
        --ignore-container-memory-limit \
        --ignore-test deployment-has-poddisruptionbudget \
        --ignore-test statefulset-has-poddisruptionbudget \
        - >results.txt 2>errors.txt
   if [ $? -ne 0 ]
   then
      echo Running kube-score failed
      cat errors.txt
      cat results.txt
      failed=1
   fi
fi

exit $failed
