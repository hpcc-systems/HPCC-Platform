# HPCC Azure storage with a Storage Account

## Directory contents

### Introduction
Persistent storage can be created manually or through Kubernetes Clusters. If created through Kubernetes the storage will be gone when the Persistent Volume (PV) or Kubernetes clusters are deleted. For Azure a manually created shared storage with a storage account will not depend on any Kubernetes cluster. The data generated will be persistent and re-used as long the PV/PVC names and mount points are the same.

### Create a share with a storage account
To create a shared storage including storage account you can reference https://docs.microsoft.com/en-us/azure/aks/azure-files-volume.
The scripts in this directory (sa/) will help you create an Azure storage account if you don't already have one.
Once you have created a storage account or already have one and a secret, the storage account name, share name, and Kubernetes secret can be set when installing the hpcc-azurefile helm chart.

Before you run any script make sure to login to Azure first:
```console
az login
```
Also you need create a Kubernetes cluster in Azure. The Resource Group name, region (SA_LOCATION) storage account name, and subscription should be set in env-sa file.

### Environment variables
env-sa defines some variables for creating a storage account and shared storage.
Some variables have default values but some of them don't and you must provide the values:
- SUBSCRIPTION: Azure subscription
- STORAGE_ACCOUNT_NAME: Storage Account name which will be generated
- SA_RESOURCE_GROUP: a resource group name which must be unique in your subscription
- SA_LOCATION: the region the storage account will be created in. It is recommended to use the same region as the Kubernetes cluster.

hpcc-azurefile/values.yaml defines three variables needed for storage account and shared storage creation:
- secretName:  a Kuberentes secret
- secretNamespace: the namespace of the Kubernetes secret
- shareName: shared storage name which will be used to create the storage. This is defined for each desired plane. If shareName is not specified the plane will use a dynamic azurefile. It is OK to use the same shareName for different planes. For example "dropzone" plane can use the same shareName as "data" plane.
Above values can also be provided using the "--set" parameter when running "helm install" but then these values must be set in env-sa for storage account related processes.

### Create
create-sa.sh will source env-sa, create a storage account and shared storage. It will also save the storage key to $SA_KEY_DIR/
```console
./create-sa.sh
```
### List
list-sa.sh will list the shared storage
```console
./list-sa.sh
```

### Create a Kubernetes secret
A kubernetes secret is required to use the storage. The secret name, secretName, is defined in hpcc-azurefile/values.yaml.<br/>
If this Kuberenetes cluster has never generated the secret before, run following to generate the secret
```console
./create-secret.sh
```
../hpcc-azurefile helm chart should be started before installing the HPCC helm chart. Reference [parent README.md](../README.md) for the details.

### Make storage account and AKS compliant
By default there probably three medium alerts when configure a storage account with an AKS:
```code
Storage account should use a private link connection
Storage accounts should restrict network access using virtual network rules
Storage account public access should be disallowed
```
Two of them and some other compliant issues can be resolved by running following. Make sure you provide either AKS resource group or AKS name and parent resource group
```code
./sa-compliant.sh
```
Currently "Storage account should use a private link connection" alert cannot be fixed wtih AZ Cli but user can do it through Azure Portal:
```code
1) Find the storage account
2) Go to the storage account and click "0" in Networking/"Number of private endpoint connections"
3) Click "Private endpoint connection" to add a private endpoint. Choose "file" for "Target sub-resource"
4) Make sure to Networking/"Number of private endpoint connections" shows 1 or nonzero value.
```
There is a JIRA against this: https://track.hpccsystems.com/browse/HPCC-26566


### Delete the storage
delete-sa.sh will delete the resoruce group which will clean all resources underneath
```console
./delete-sa.sh
```
You must make certain the contents of the shared storage are no longer needed since when it is deleted you can't get it back.
On the other hand, be aware the storage will persist even when the kubernetes cluster is destroyed and there are fees/charges for the Azure storage. Be very cautious before making this decision.

If there is a private endpoint associated with the subnet user cannot delete AKS. The private endpoint must be deleted first:
```code
./delete-ep.sh
```
