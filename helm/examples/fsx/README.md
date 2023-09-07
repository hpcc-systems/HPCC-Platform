# HPCC FSx for Lustre Storage
AWS FSx for Lustre is a cloud storage service that offers high-performance storage that can scale up to millions of IOPS. It is used for HPC (high performance computing) workloads where speed matters, such as machine learning, video rendering, or genome analysis.

# Prerequisites
## AWS CLI Version
Verify that you are using version 2.12.3 or later of the AWS CLI.<br/>
Run the following command to check the version:
```console
aws --version | cut -d / -f2 | cut -d ' ' -f1
```
If your version is less than 2.12.3 or if you have not installed AWS CLI, go to https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html for AWS's documentation on installing or updating AWS CLI.

## eksctl Version
Verify that you are using version 0.153.0 or later of the eksctl command line tool.<br/>
Run the following command to check the version:
```console
eksctl version
```
If your version is less than 0.153.0 or if you have not installed eksctl, go to https://github.com/eksctl-io/eksctl/blob/main/README.md#installation on Github for installing and updating eksctl.

## kubectl Version
Your kubectl version must be within one minor version difference with your EKS cluster version. For example, a 1.26.X kubectl version works with 1.25, 1.26, and 1.27 EKS cluster versions.<br/>
Run the following command to check the version:
```console
kubectl version --client
```
If it is not installed or if you want to update it to a later version, go to https://docs.aws.amazon.com/eks/latest/userguide/install-kubectl.html for AWS's documentation on installing or updating kubectl.<br/>
Another option is to keep the kubectl version the same while changing the version of your EKS cluster to ensure that they are one minor version difference within each other.

# Prepare Environment
Fill in the following information in the fsx-env file
```code
ACCOUNT_ID        # AWS Account ID
EKS_NAME          # Name of your EKS cluster: kubectl config get-clusters  (The first part of the name before ".")

BRANCH_VERSION    # Branch version of the fsx csi driver, such as 0.10 or 1.0. To view versions: https://github.com/kubernetes-sigs/aws-fsx-csi-driver/branches/active. It is recommended to use the latest released version.
SUBNET_ID         # Subnet ID for your FSx file system
SG_ID             # Comma separated list of security group IDs to be attached. You may use the EKS cluster security group

EKS_REGION        # Region of your EKS cluster, such as us-east-1
```

# Install the FSx CSI Driver
"examples/fsx" requires the FSx CSI driver to be installed. We provide a script to simplify the process. For details please reference https://docs.aws.amazon.com/eks/latest/userguide/fsx-csi.html.<br/>
Run the following command under the fsx directory:
```console
./install-fsx-csi-driver.sh
```
The script configures the IAM roles and policies with the service account, installs the aws-fsx-csi-driver, and creates the file for the "aws-fsx-auto" storage class.<br/>
To verify
```console
helm ls -n kube-system
NAME              	NAMESPACE  	REVISION	UPDATED                             	STATUS  	CHART                   	APP VERSION
aws-fsx-csi-driver	kube-system	1       	2023-08-22 10:07:40.503648 -0400 EDT	deployed	aws-fsx-csi-driver-1.7.0	1.0.0
```

# Apply aws-fsx-auto Storage Class
The storageclass.yaml file is generated after running the script. It creates the "aws-fsx-auto" storage class, which is used to dynamically provision the FSx file systems.
```code
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: aws-fsx-auto
provisioner: fsx.csi.aws.com
parameters:
  subnetId: <SUBNET_ID>
  securityGroupIds: <SG_ID>
  #deploymentType: PERSISTENT_1 # optional
  #kmsKeyId: # optional
  #storageType: HDD # optional
  #perUnitStorageThroughput: "200" # optional
  #driveCacheType: "NONE" # optional
  #automaticBackupRetentionDays: "1" # optional
  #dailyAutomaticBackupStartTime: "00:00" # optional
  #copyTagsToBackups: "true" # optional
  #dataCompressionType: "NONE" # optional
  #weeklyMaintenanceStartTime: "7:09:00" # optional
  #fileSystemTypeVersion: "2.12" # optional
  #extraTags: "Tag1=Value1,Tag2=Value2" # optional
#mountOptions: # optional
  #- flock # optional
```
There are many optional parameters for the FSx file system. We simply provide the minimum necessary: subnetId and securityGroupIds. Modify the other parameters based on your needs. For more details, reference the following:
- https://github.com/kubernetes-sigs/aws-fsx-csi-driver/tree/master/examples/kubernetes/dynamic_provisioning#edit-storageclass
- https://docs.aws.amazon.com/fsx/latest/APIReference/API_LustreFileSystemConfiguration.html

When the storageclass.yaml is ready, run the following command under the fsx directory:
```console
kubectl apply -f storageclass.yaml
```
To verify
```console
kubectl get sc
NAME            PROVISIONER             RECLAIMPOLICY   VOLUMEBINDINGMODE      ALLOWVOLUMEEXPANSION   AGE
aws-fsx-auto    fsx.csi.aws.com         Delete          Immediate              false                  23h
```

# Static storage within Kubernetes with values-retained-fsx.yaml
We will use the hpcc-fsx-dynamic-pv helm chart to create storage that lives on the Kubernetes cluster level. The PVs are dynamically generated, but the PVCs are manually created by the helm chart. The creation of the PVCs will also create the FSx file systems by using the "aws-fsx-auto" storage class. Since the PVCs persist after the HPCC cluster is deleted, the storage can be reused across different HPCC clusters. However, it cannot be reused across different Kubernetes clusters.<br/>
<br/>
The storage needs to be created before starting the HPCC cluster. Under the helm directory, run the following command:
```console
helm install awsstorage examples/fsx/hpcc-fsx-dynamic-pv
```
To verify
```console
kubectl get pvc
NAME                                 STATUS    VOLUME   CAPACITY   ACCESS MODES   STORAGECLASS   AGE
dali-awsstorage-hpcc-fsx-pvc         Pending                                      aws-fsx-auto   24s
data-awsstorage-hpcc-fsx-pvc         Pending                                      aws-fsx-auto   24s
dll-awsstorage-hpcc-fsx-pvc          Pending                                      aws-fsx-auto   24s
mydropzone-awsstorage-hpcc-fsx-pvc   Pending                                      aws-fsx-auto   24s
sasha-awsstorage-hpcc-fsx-pvc        Pending                                      aws-fsx-auto   24s
```
The PVCs will become bound after the FSx file systems are finished creating. You may also check this in the AWS console. It will take around 5-10 minutes.<br/>
To verify
```console
kubectl get pvc
NAME                                 STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
dali-awsstorage-hpcc-fsx-pvc         Bound    pvc-be5c0ca2-478d-498a-88d5-a194a852b0bf   1200Gi     RWO            aws-fsx-auto   7m9s
data-awsstorage-hpcc-fsx-pvc         Bound    pvc-faff5ad2-5be8-4a8b-965a-2f2e9aceb5e8   1200Gi     RWX            aws-fsx-auto   7m9s
dll-awsstorage-hpcc-fsx-pvc          Bound    pvc-7f955252-603f-4e39-a173-708512a413a4   1200Gi     RWX            aws-fsx-auto   7m9s
mydropzone-awsstorage-hpcc-fsx-pvc   Bound    pvc-f48fc70a-f30d-4c78-bfd7-0c2f53934759   1200Gi     RWX            aws-fsx-auto   7m9s
sasha-awsstorage-hpcc-fsx-pvc        Bound    pvc-7826f8b5-c7c8-4975-b08b-2b9d79140c71   1200Gi     RWX            aws-fsx-auto   7m9s
```
Once the FSx file systems are available, navigate to examples/fsx and run the following command:
```console
kubectl apply -f permission-pods.yaml
```
This creates five pods: one for each PVC. They will mount to each FSx file system in order to change its permissions.<br/>
To verify
```console
kubectl get pods
NAME                     READY   STATUS    RESTARTS   AGE
dali-permissions         1/1     Running   0          48s
data-permissions         1/1     Running   0          47s
dll-permissions          1/1     Running   0          48s
mydropzone-permissions   1/1     Running   0          47s
sasha-permissions        1/1     Running   0          47s
```
You may now delete the pods. Run the following command:
```console
kubectl delete -f permission-pods.yaml
```
Navigate to the helm directory. To start the HPCC cluster, run
```
helm install myhpcc ./hpcc --set global.image.version=latest -f examples/fsx/values-retained-fsx.yaml
```
To cleanup, run
```console
helm uninstall myhpcc
helm uninstall awsstorage
```