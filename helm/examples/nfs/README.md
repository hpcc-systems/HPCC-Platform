# HPCC NFS storage

## Directory contents

### hpcc-nfs/

The hpcc-nfs helm chart will provision a NFS Server and optionally a default shared PVC using the default Storage Class.It will also creates a Persistent Volume Claim for each of the required HPCC storage types.

The use case for using this helm chart is where either no ReadWriteMany storage classes are availale on the K8s implementation being targeted, or that the ReadWriteMany storage class available is expensive or inflexible.
For example, under GKE(Google Kubernetes Engine), ReadWriteMany volumes can be provisioned with their 'Filestore' storage class, however, the minimum capacity is 1TB.


Example use:

helm install nfsstorage hpcc-nfs/
helm install myhpcc hpcc/ --set global.image.version=latest -f examples/nfs/values-nfs.yaml

### values-nfs.yaml

An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-nfs" helm chart, or ensure the names in your values files for the storage types matched the PVC names created.

### Notes

The main use case for this example is with GKE (Google Kubernetes Engine).
