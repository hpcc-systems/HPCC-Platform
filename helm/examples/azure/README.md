# HPCC Azure storage

For Kubernetes server version 1.21+ the Azure Files {standard} Container Storage Interface (CSI) driver is used by Azure Kubernetes Service (AKS) to manage the lifecycle of Azure File shares. Currently most AKS regions are using Kubernetes version 1.21 and above. To "Dynamically generate a storage account and shared volume with Storage Class" choose either "file.csi.azure.com" (CSI based) or "kubernetes.io/azure-file" as a legacy provisioner without CSI, for the older Kubernetes versions.

## Directory contents

### values-auto-azurefile.yaml

This is a values file that can be supplied to helm when starting HPCC.
It will override HPCC's storage settings so that the "azurefile"
storage class will be used.

This will mean that Persistent Volumes will be provisioned automatically, with a reclaim policy of "Delete".
If the associated Persistent Volume Claims are deleted, so will the Persist Volumes and any data associated with them.

This may be okay for experimentation, where you do not care that your files are ephemeral. 

### hpcc-azurefile/

The hpcc-azurefile helm chart will provision a new Storage Class and a Persistent Volume Claim for each of the required HPCC storage types.
Once installed the generated PVC names should be used when installing the HPCC helm chart.
The values-retained-azurefile.yaml is an example of the HPCC storage settings that should be applied, after the "hpcc-azurefile" helm chart is installed. "azurestorage-hpcc-azurefile" name is expected when launching "helm install" to match the PVC name in the values-retained-azurefile.yaml.

The storage class used in this example supports mountOptions, which are used to ensure that the mounts are owned by user and group 'hpcc' with suitable file permissions.

There are two cases:
- Dynamically generate a storage account and shared volume with Storage Class
- Manually create storage account and shared volume and provide the following information in values.yaml: secretName, secretNamespace and sharedName. Reference sa/README.md for details.

NB: The output of installing this chart, will contain a generated example with the correct PVC names.

Example use:
```console
  helm install azstorage examples/azure/hpcc-azurefile
```

### values-retained-azurefile.yaml

An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-azurefile" helm chart, or ensure the names in your values files for the storage types matched the PVC names created.

