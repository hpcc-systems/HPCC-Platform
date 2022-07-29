#!/bin/bash

# If pod has used a temp directory, it should have stored it in 'tmpdir'
# If still present, ensure it is deleted
if [[ -s tmpdir ]] && [[ -d $(cat tmpdir) ]]; then
  rm -rf $(cat tmpdir)
fi

# *.k8s files represent child k8s resources orphaned by the pod, that now need clearing up
# NB: k8s resources created by HPCC follow the naming convention: <component-name>-<resource-type>-<job-name>
# In the loop below, each k8s filename is parsed to extract these fields, the resourceName is constructed,
# and it and resourceType are used to delete the resource.
for filename in *.k8s; do
  IFS=, read componentName resourceType jobName <<< $(basename ${filename} .k8s)
  resourceName="${componentName}-${resourceType}-${jobName}"
  echo Performing: kubectl delete $resourceType/$resourceName
  kubectl delete $resourceType/$resourceName
  rm -f $filename
done

exit 0

