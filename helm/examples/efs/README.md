# HPCC Elastic File System (EFS) Storage
Make sure an EFS is available or create one before using this chart.
The chart requires <EFS region> and <EFS ID>
The <EFS ID> can be found in AWS Console EFS service or from AWS Cli:
```console
  aws efs describe-file-systems --region <EFS region> | grep "^FILESYSTEMS" |  awk -F $'\t' '{print $8, $5}'
```
The output will display EFS name and ID.


## Directory contents

### hpcc-efs/
The hpcc-efs helm chart will provision an EFS Server Pod. It will also create a Persistent Volume Claim for each of the required HPCC storage types.

EFS Server should be created on AWS EFS service first.  The hpcc-efs helm chart needs an EFS region and id to configure the deployment. The default region is us-east-1.

Example use:

helm install efsstorage examples/efs/hpcc-efs --set efs.region=<EFS region>  --set efs.id=<EFS ID>
helm install myhpcc hpcc/ --set global.image.version=latest -f examples/efs/values-efs.yaml


### values-efs.yaml
An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-efs" helm chart, or ensure the names in your values files for the storage types match the PVC names created.


### Notes
helm uninstall will not delete EFS persistant volumes. You can either run "kubectl delete pv <pv name> or --all".
