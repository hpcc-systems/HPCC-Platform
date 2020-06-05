# HPCC Elastic File System (EFS) Storage

## Directory contents

### hpcc-efs/
The hpcc-efs helm chart will provision a EFS Server Pod. It will also creates a Persistent Volume Claim for each of the required HPCC storage types.

EFS Serve should be created on AWS EFS service first.  hpcc-efs helm chart need EFS region and id to configure the deployment. The default region is us-east-1.

Example use:

helm install efsstorage hpcc-efs/ --set efs.region=<efs region>  --set efs.id=<efs id>
helm install myhpcc hpcc/  --set global.image.version=latest -f examples/efs/values-efs.yaml


### values-efs.yaml
An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-efs" helm chart, or ensure the names in your values files for the storage types matched the PVC names created.


### Notes
helm uninstall will not delete EFS persistant volumes. You can either run "kubectl delete pv <pv name> or --all" or use provided script delete_pv.sh
