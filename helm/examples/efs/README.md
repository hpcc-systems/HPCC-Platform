# HPCC Elastic File System (EFS) Storage



# EFS CSI Driver
"examples/efs" requires Elastic FileSystem (EFS) Container Storage Interface (CSI) driver.
We provide some scripts to simplify the process. For details please reference https://docs.aws.amazon.com/eks/latest/userguide/efs-csi.html
Current implementation only tested successfully with a IAM user. It may need updates for Active Directory Federation Service (ADFS)

## Prepare environment
Make sure an EFS file system is available or create an [EFS File System](#efs-file-system) before using this chart.<br/>
Fill in the following information in the efs-env file
```code
ACCOUNT_ID       # AWS Account ID
EKS_NAME         # Elastic Kubernetes Service (EKS) cluster name: kubectl configure get-cluster  (The first part of the name before ".")
EFS_ID           # EFS ID can be found from AWS Console or command-line:
                   aws efs describe-file-systems --query "FileSystems[*].FileSystemId" --output text
EFS_REGION       # EFS region. We strongly suggest you have the same region for EFS server and EKS cluster
EFS_SECURITY_GROUPS  # EFS security group which should be the same as the EKS cluster security group. Also, make sure the security group for the mount targets is the same as this one.
EFS_BASE_PATH    # Base directory to mount in the EFS. If you use EFS for multiple HPCC clusters you should set this differently to avoid clashing with the other clusters
VPC_ID           # The same VPC ID as the VPC of the EKS cluster
```
### Get the EFS ID
```Console
  aws efs describe-file-systems --region <REGION>
```
The output shows "NAME" and FileSystemID

### How to get EFS security groups
```console
  aws efs describe-mount-targets --file-system-id <EFS ID> --region <EFS region> |  awk -F $'\t' '{print $7}'
  # For each file-system mount target:
  aws efs describe-mount-targets-security-groups --mount-target-id <mount target id>  --region <EFS region> |  awk -F $'\t' '{print $2}'
```
Add each unique security group id to the variable "EFS_SECURITY_GROUPS"


## Install EFS CSI Driver
Run the following command under the efs directory:
```code
./install-efs-csi-driver.sh
```
The script configures the IAM roles and policies, installs the aws-efs-csi-driver, and creates the file for the "aws-efs-auto" storage class. To verify
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
# Storage for HPCC cluster
There are three available storage life cycles/methods for the HPCC cluster.
## 1. Dynamic storage with values-auto-efs.yaml
This method uses the storage class "aws-efs-auto" to dynamically create Persistent Volume Claims (PVC). The PVCs are deleted when the HPCC cluster is deleted, meaning that the storage is deleted when the HPCC cluster is deleted. The storage can't be reused between different HPCC clusters.<br/>

Under the efs directory, run the following command to create the "aws-efs-auto" storage class
```console
kubectl apply -f storageclass.yaml
```
To verify
```code
kubectl get sc
NAME            PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
aws-efs-auto    efs.csi.aws.com         Delete          Immediate              false                  9s
```
To deploy the HPCC cluster, navigate to the helm directory and run:
```console
  helm install myhpcc ./hpcc --set global.image.version=latest -f examples/efs/values-auto-efs.yaml
```
To cleanup, run
```console
  helm uninstall myhpcc
```
## 2. Static storage within Kubernetes with values-retained-efs.yaml
In this method, storage lives on the Kubernetes cluster level. It uses the helm chart "hpcc-efs-dynamic-pv" to manually create PVCs. The PVs are dynamically generated. The PVCs will persist after the HPCC cluster is deleted, meaning that the storage can be reused across different HPCC clusters.<br/>
This method also uses the "aws-efs-auto" storage class. Under the efs directory, run the following command:
```console
kubectl apply -f storageclass.yaml
```
The storage needs to be created before starting the HPCC cluster. Under the helm directory, run the following command:
```console
  helm install awsstorage examples/efs/hpcc-efs-dynamic-pv
```
To verify
```code
kubectl get pvc
NAME                                 STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
dali-awsstorage-hpcc-efs-pvc         Bound    pvc-400ddbb7-1b89-4468-b416-ffece25aab0d   1Gi        RWO            aws-efs        5m52s
data-awsstorage-hpcc-efs-pvc         Bound    pvc-0b99092e-56ba-4c2e-8572-9a5ab4d4da4b   3Gi        RWX            aws-efs        5m52s
dll-awsstorage-hpcc-efs-pvc          Bound    pvc-70a470ec-b754-4376-9afb-f1bd499c51ec   1Gi        RWX            aws-efs        5m52s
mydropzone-awsstorage-hpcc-efs-pvc   Bound    pvc-9ecd2abc-7713-4639-8da2-91392e636e19   1Gi        RWX            aws-efs        5m52s
sasha-awsstorage-hpcc-efs-pvc        Bound    pvc-30c3357c-d982-47f9-ac2c-c1465639b10e   1Gi        RWX            aws-efs        5m52s
```
To start the HPCC cluster, run
```
  helm install myhpcc ./hpcc --set global.image.version=latest -f examples/efs/values-retained-efs.yaml
```
An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-efs" helm chart, or ensure the names in your values files for the storage types match the PVC names created. The "values-retained-efs.yaml" file expects that helm chart installation name is "awsstorage". Change the PVC name accordingly if another name is used.<br/>
To cleanup, run
```console
  helm uninstall myhpcc
  helm uninstall awsstorage
```
## 3. Static storage beyond Kubernetes with values-retained-efs.yaml
In this method, the storage lives beyond the Kubernetes cluster. It uses the helm chart "hpcc-efs-static-pv" to manually create PVs and PVCs and to configure access points in EFS with the Kubernetes cluster. This means that even if the Kubernetes cluster is deleted, the storage will remain and can be reused across different Kubernetes clusters.<br/>
The create-ap.sh script creates five access points in EFS for each of dali, dll, sasha, data, and mydropzone. It also displays a description for each access point. You may need to add additional tags to the access points depending on your organization. For example, for RISK users, the "owner" and "owner_email" tags are required; add these to the script as needed.<br/>
Once you are ready, under the efs directory run the following command:
```code
./create-ap.sh
```
Note that running the script again will create five more access points with the same exact specifications. Use the delete-ap.sh to delete all access points.<br/>
Navigate to hpcc-efs-static-pv -> values.yaml. Paste the EFS ID where it says "efsID" and the access point IDs where it says "apID". You can get the access point IDs either through the console or from the descriptions after running the create-ap.sh script. Make sure the correct access point IDs correspond to the correct names in the values.yaml.<br/>

The storage needs to be created before starting the HPCC cluster. Under the helm directory, run the following command:
```console
  helm install awsstorage examples/efs/hpcc-efs-static-pv
```
To verify
```code
kubectl get sc
NAME             PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
aws-efs-static   efs.csi.aws.com         Delete          Immediate              false                  110s
```
```code
kubectl get pv
NAME                                CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                                        STORAGECLASS     REASON   AGE
dali-awsstorage-hpcc-efs-pv         1Gi        RWO            Retain           Bound    default/dali-awsstorage-hpcc-efs-pvc         aws-efs-static            23s
data-awsstorage-hpcc-efs-pv         3Gi        RWX            Retain           Bound    default/data-awsstorage-hpcc-efs-pvc         aws-efs-static            23s
dll-awsstorage-hpcc-efs-pv          1Gi        RWX            Retain           Bound    default/dll-awsstorage-hpcc-efs-pvc          aws-efs-static            23s
mydropzone-awsstorage-hpcc-efs-pv   1Gi        RWX            Retain           Bound    default/mydropzone-awsstorage-hpcc-efs-pvc   aws-efs-static            23s
sasha-awsstorage-hpcc-efs-pv        1Gi        RWX            Retain           Bound    default/sasha-awsstorage-hpcc-efs-pvc        aws-efs-static            23s
```
```code
kubectl get pvc
NAME                                 STATUS   VOLUME                              CAPACITY   ACCESS MODES   STORAGECLASS     AGE
dali-awsstorage-hpcc-efs-pvc         Bound    dali-awsstorage-hpcc-efs-pv         1Gi        RWO            aws-efs-static   29s
data-awsstorage-hpcc-efs-pvc         Bound    data-awsstorage-hpcc-efs-pv         3Gi        RWX            aws-efs-static   29s
dll-awsstorage-hpcc-efs-pvc          Bound    dll-awsstorage-hpcc-efs-pv          1Gi        RWX            aws-efs-static   29s
mydropzone-awsstorage-hpcc-efs-pvc   Bound    mydropzone-awsstorage-hpcc-efs-pv   1Gi        RWX            aws-efs-static   29s
sasha-awsstorage-hpcc-efs-pvc        Bound    sasha-awsstorage-hpcc-efs-pv        1Gi        RWX            aws-efs-static   29s
```
To start the HPCC cluster, run
```
  helm install myhpcc ./hpcc --set global.image.version=latest -f examples/efs/values-retained-efs.yaml
```
To cleanup, run
```console
  helm uninstall myhpcc
  helm uninstall awsstorage
```

# EFS File System
Reference:
- https://docs.aws.amazon.com/efs/latest/ug/gs-step-two-create-efs-resources.html
- https://hpccsystems.com/blog/Cloud-AWS
You can use either AWS CLI or the console to create the EFS.
## AWS CLI
If you don't have AWS CLI reference
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

### Creating the Mount Targets
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
## Console
### Create EFS Server
Navigate to the EFS service and click "Create file system". Select the same VPC as the one your EKS cluster is running in.
### Configuring the Mount Targets
Click on the file system you just created and navigate to "Network". Your mount targets should be displayed.<br/>
Click on "Manage", and you should see that the security groups for the mount targets are the default security group. Replace these with the EKS cluster security group. To find the EKS cluster security group navigate to EKS. Click on your cluster. Click on "Networking", and find the security group displayed under "Cluster security group". After replacing the default security group for all mount targets, save your changes.