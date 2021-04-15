# HPCC Elastic File System (EFS) Storage

## EFS Server Settings
Make sure an EFS server is available or create one before using this chart.<br/>
EFS Server settings should be set in the file efs-env<br/>
The chart requires <EFS region> and <EFS ID>
The <EFS ID> can be found in AWS Console EFS service or from AWS Cli:
```console
  aws efs describe-file-systems --region <EFS region> | grep "^FILESYSTEMS" |  awk -F $'\t' '{print $8, $5}'
```
The output will display EFS name and ID.

It is recommended to provide the option "--managed" and a security group id during EKS cluster creation. EFS Server can use the same security group or if you know your security group of node pools they are OK to use for EFS security group. Otherwise you need to provide the following:
- EKS cluster name
```console
kubectl config get-clusters | cut -d'.' -f1
```
Set EKS cluster name to variable "EKS_NAME"

- EFS security groups
```console
  aws efs describe-mount-targets --file-system-id <EFS ID> --region <EFS region> |  awk -F $'\t' '{print $7}'
  # For each file-system mount target:
  aws efs describe-mount-targets-security-groups --mount-target-id <mount target id>  --region <EFS region> |  awk -F $'\t' '{print $2}'
```
Add each unique security group id to the variable "EFS_SECURITY_GROUPS"

There is a setting "EFS_CSI_DRIVER", the default is "true" and it is recommended to leave that.

## Deploy efs-provisioner
  efs-provisioner pod should be started first:
```console
  ./install-efs-provision.sh
  # To check the pod:
  kubectl get pod
```
There may be some warnings which can be ignored:
```code
An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule "peer: sg-0a5a005489115aac6, TCP, from port: 2049, to port: 2049, ALLOW" already exists
```

```code
Warning: storage.k8s.io/v1beta1 CSIDriver is deprecated in v1.19+, unavailable in v1.22+; use storage.k8s.io/v1 CSIDriver
csidriver.storage.k8s.io/efs.csi.aws.com configured
```
"v1beta" will be replaced with "v1" when it is available.


## Deploy HPCC cluster with values-auto-efs.yaml
It will automically create Persistent Volume Claims (PVC) and delete them when the HPCC cluster is deleted:<br/>
Under the helm directory:
```console
  helm install myhpcc ./hpcc --set global.image.version=<HPCC Platform version> -f examples/efs/values-auto-efs.yaml
```

## Deploy HPCC cluster with values-retained-efs.yaml
This will require deploying HPCC PVCs first. PVCs will persist after the HPCC cluster is deleted.<br/>
Under the helm directory:
```console
  helm install awsstorage examples/efs/hpcc-efs
  # To start HPCC cluster:
  helm install myhpcc hpcc/ --set global.image.version=latest -f examples/efs/values-retained-efs.yaml
```
An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-efs" helm chart, or ensure the names in your values files for the storage types match the PVC names created. "values-retained-efs.yaml" expects that helm chart installation name is "awsstorage". Change the PVC name accordingly if another name is used.

helm uninstall will not delete EFS persistant volumes claims (PVC). You can either run "kubectl delete pv <pv name> or --all".

## EFS Server
Reference:
- https://docs.aws.amazon.com/efs/latest/ug/gs-step-two-create-efs-resources.html
- https://hpccsystems.com/blog/Cloud-AWS

We will go through some steps of creating EFS server with AWS CLI. If you don't have AWS CLI reference
- https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html

For simple setup we recommend you use the same VPC/Subnets and security group for both EFS and EKS
### Create EFS Server
```console
  aws efs create-file-system --throughput-mode bursting --tags "Key=Name,Value=<EFS Name>" --region <REGION>
```
To get the EFS ID
```Console
  aws efs describe-file-systems --region <REGION>
```
The output shows "NAME" and FileSystemID

The EFS server FQDN will be
```code
  <EFS ID>.efs.<REGION>.amazonaws.com
```

### Creat the Mount Targets
Pick a VPC in the the same region of EFS server
```console
  aws ec2 describe-vpcs --region <REGION>
```
The output shows the VPC IDs.

Get all the subnets of the VPC
```console
  aws ec2 describe-subnets --region <REGION> --fileters "Name=vpc-id,Values=<VPC ID>"
```
The output shows "AvailabilityZone" and "Subnets ID"<br/>
We recommend you use these all or subets of these "AvailabalityZone" and "Subnet IDs" to create EKS cluster with the option "--managed". It will be easier to configure EFS and EKS.

To create the mount target:
```console
  aws efs create-mount-target --region <REGION> --file-system-id <EFS ID> --subnet-id <Subnet id>
```
Repeat this for all subnets. Usually an AWS EKS cluster needs at least two available zones (subnets). If you are not sure, create mount targets for all zones.

To display the mount targets:
```console
  aws efs describe-mount-targets --region <REGION> --file-system-id <EFS ID>
```
