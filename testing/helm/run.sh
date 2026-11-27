#!/bin/bash
scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
options="--set global.image.version=someversion --set global.image.pullPolicy=Always"
hpccchart=$scriptdir/../../helm/hpcc
failed=0

assert_contains() {
   local file=$1
   local pattern=$2
   local message=$3

   if ! grep -q -- "$pattern" "$file"
   then
      echo "$message"
      failed=1
   fi
}

assert_not_contains() {
   local file=$1
   local pattern=$2
   local message=$3

   if grep -q -- "$pattern" "$file"
   then
      echo "$message"
      failed=1
   fi
}

helm version
echo Testing image version rendering
if ! bash "$scriptdir/image-versions.sh"; then
   failed=1
fi
echo Testing unmodified values file
helm lint $hpccchart ${options} > results.txt 2> errors.txt
if [ $? -ne 0 ]
then
   echo Unmodified failed
   cat errors.txt
   cat results.txt
   failed=1
fi

echo Testing pod fsGroup opt-in
helm template $hpccchart ${options} > results.txt 2> errors.txt
if [ $? -ne 0 ]
then
   echo Default fsGroup render failed
   cat errors.txt
   failed=1
else
   assert_not_contains results.txt "fsGroup:" "Default render unexpectedly set pod fsGroup"
fi

helm template $hpccchart ${options} --set global.user.fsGroup=true > results.txt 2> errors.txt
if [ $? -ne 0 ]
then
   echo Enabled fsGroup render failed
   cat errors.txt
   failed=1
else
   assert_contains results.txt "fsGroup: 10001" "Enabled fsGroup did not default to the HPCC GID"
   assert_contains results.txt "fsGroupChangePolicy: OnRootMismatch" "fsGroupChangePolicy was not rendered"
fi

helm template $hpccchart ${options} --set global.user.gid=12345 --set global.user.fsGroup=true > results.txt 2> errors.txt
if [ $? -ne 0 ]
then
   echo Custom GID fsGroup render failed
   cat errors.txt
   failed=1
else
   assert_contains results.txt "fsGroup: 12345" "Enabled fsGroup did not use the configured HPCC GID"
fi

helm template $hpccchart ${options} --set global.user.fsGroup=false > results.txt 2> errors.txt
if [ $? -ne 0 ]
then
   echo Disabled fsGroup render failed
   cat errors.txt
   failed=1
else
   assert_not_contains results.txt "fsGroup:" "Disabled fsGroup was unexpectedly rendered"
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
             if [[ "$base" == "aliasprefixcollision" ]]; then
                # skipped alias must not consume an alias number; mounts must match volumes
                assert_contains results.txt "name: data-volume-alias-1" "$file did not render data-volume-alias-1"
                assert_not_contains results.txt "name: data-volume-alias-2" "$file unexpectedly rendered data-volume-alias-2"
             fi
             if [[ "$base" == "dali-split-owner" ]]; then
                assert_contains results.txt "kind: StatefulSet" "$file did not render a Dali StatefulSet"
                assert_contains results.txt "name: splitdali" "$file did not render the split Dali"
                assert_contains results.txt "name: hpcc-dali" "$file did not render the Dali service account"
                assert_contains results.txt "--daliServers=splitdali:7070" "$file did not use the local Dali endpoint"
                assert_not_contains results.txt "kind: Deployment" "$file unexpectedly rendered an application Deployment"
             elif [[ "$base" == "dali-split-app" ]]; then
                assert_contains results.txt "kind: Deployment" "$file did not render an application Deployment"
                assert_contains results.txt "name: eclwatch" "$file did not render the application"
                assert_contains results.txt "--daliServers=splitdali:7070" "$file did not use the external Dali endpoint"
                assert_not_contains results.txt "--daliServers=localdali:7070" "$file unexpectedly used the disabled local Dali endpoint"
             fi
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
             elif [[ "$base" == "roxie-storage-planes" ]]; then
                assert_contains results.txt "directAccessPlanes:" "$file did not render directAccessPlanes"
                assert_contains results.txt "- direct-plane-alpha" "$file did not render the first direct-access plane"
                assert_contains results.txt "- direct-plane-beta" "$file did not render the second direct-access plane"
                assert_contains results.txt "preferredPlanes:" "$file did not render preferredPlanes"
                assert_contains results.txt "- name: preferred-plane-alpha" "$file did not render the first preferred plane as an object"
                assert_contains results.txt "- name: preferred-plane-beta" "$file did not render the second preferred plane as an object"
                assert_contains results.txt "priority: 25" "$file did not render the preferred-plane priority"
             fi
             if [[ "$base" == "ephemeralplanes" ]]; then
                assert_contains results.txt "ephemeral: true" "$file did not pass ephemeral through to the plane config"
                assert_contains results.txt "storageSize: 10Gi" "$file did not pass storageSize through to the plane config"
                assert_contains results.txt "mountPath: \"/var/lib/HPCCSystems/spill\"" "$file did not mount the ephemeral spill plane"
                assert_contains results.txt "The configuration contains ephemeral planes: \\[spill temp\\]" "$file did not warn about the explicit ephemeral planes"
                assert_contains results.txt "The configuration contains dynamically provisioned planes:" "$file did not describe dynamic storageClass planes"
                assert_not_contains results.txt "The configuration contains dynamically provisioned planes: \[dali" "$file incorrectly described the static dali plane as dynamically provisioned"
                assert_not_contains results.txt "sizeLimit" "$file unexpectedly generated an emptyDir sizeLimit"
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
        --ignore-test container-ephemeral-storage-request-and-limit \
        --ignore-test statefulset-has-servicename \
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
