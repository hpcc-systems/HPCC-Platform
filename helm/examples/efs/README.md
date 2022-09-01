# HPCC Elastic File System (EFS) Storage



## EFS CSI Driver
"examples/efs" requires Elastic FileSystem (EFS) Container Storage Interface (CSI) driver.
We provide some scripts to simplify the process. For detail please reference https://docs.aws.amazon.com/eks/latest/userguide/efs-csi.html
Current implementation only tested successfully with a IAM user. It may need update for Active Directory Federation Service (ADFS)

### Prepare environment
Make sure an EFS server is available or create an [EFS server](#efs-server) before using this chart.<br/>
Fill the following information in efs-env
```code
ACCOUNT_ID       # AWS Account ID
EKS_NAME         # Elastic Kubernetes Service (EKS) cluster name: kubectl configure get-cluster  (The first part of the name before ".")
EFS_ID           # EFS ID can be found from AWS Console or command-line:
                   aws efs describe-file-systems --query "FileSystems[*].FileSystemId" --output text
EFS_REGION       # EFS region. We strongly suggest you have the same region for EFS server and EKS cluster
EFS_SECURITY_GROUPS  # EFS security group which is the subnet security group which should be same as EKS
EFS_BASE_PATH    # Base directory to mount in the EFS. If you use EFS for multiple HPCC clusters you should set this differently to avoid clashing with the other clusters
```
#### Get the EFS ID
```Console
  aws efs describe-file-systems --region <REGION>
```
The output shows "NAME" and FileSystemID

#### How to get EFS security groups
```console
  aws efs describe-mount-targets --file-system-id <EFS ID> --region <EFS region> |  awk -F $'\t' '{print $7}'
  # For each file-system mount target:
  aws efs describe-mount-targets-security-groups --mount-target-id <mount target id>  --region <EFS region> |  awk -F $'\t' '{print $2}'
```
Add each unique security group id to the variable "EFS_SECURITY_GROUPS"


### Install EFS CSI Driver
Run the following command:
```code
./install-csi-driver.sh
```
To verify
```console
helm list -n kube-system
NAME                    NAMESPACE       REVISION        UPDATED                                 STATUS          CHART                           APP VERSION
aws-efs-csi-driver      kube-system     1               2022-08-19 17:25:53.6078326 -0400 EDT   deployed        aws-efs-csi-driver-2.2.7        1.4.0

kubectl get pod -n kube-system | grep efs-csi
efs-csi-controller-594c7f67c7-8zk72   3/3     Running   0          32h
efs-csi-controller-594c7f67c7-mncgd   3/3     Running   0          32h
efs-csi-node-8g8fk                    3/3     Running   0          32h
efs-csi-node-fp8vt                    3/3     Running   0          32h
```

### Install "aws-efs" Stroage Class
```console
kubectl apply -f storageclass.yaml
```
To verify
```code
kubectl get sc
NAME            PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
aws-efs         efs.csi.aws.com         Delete          Immediate              false                  32h
```

## Deploy HPCC Storage
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
