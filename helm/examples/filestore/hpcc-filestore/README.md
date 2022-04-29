# HPCC FileStore storage

## Directory contents

### FileStore Instance
Filestore instances are fully managed NFS file servers on Google Cloud for use with applications running on Compute Engine virtual machines (VMs) instances or Google Kubernetes Engine clusters. You can find more information here: https://cloud.google.com/filestore/docs

To create a Filestore instance: https://cloud.google.com/filestore/docs/creating-instances
If possible create a Filestore instance in the same zone

Example of creating a Filestore instance
```console
gcloud filestore instances create -q filestore1 \
    --project=<GCP Project ID> \
    --zone=us-east1-b \
    --tier=STANDARD \
    --network=name=default \
    --file-share=name="hpccdata",capacity=1TB
```

You will need instance ip and file-share name (hpccdata) for further configuration

### hpcc-filestore/
A NFS client provisioner is required to create a storage class and bypass NFS permission related issue.
We pick nfs-subdir-external-provisioner: https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/

"nfs-subdir-external-provisioner" chart will be installed first.
It can either be manually installed with following steps
```console
helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
cd hpcc-filestore
helm dependency update
```
or installed via the provided setup.sh

The hpcc-filestore helm chart creates a Storage Class and Persistent Volume Claim for each of the required HPCC storage types.

Example use:
```console
helm install gcpstorage hpcc-filestore/
helm install myhpcc hpcc/ --set global.image.version=latest -f examples/file/values-filestore.yaml
```

### values-filestore.yaml

An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-filestore" helm chart, or ensure the names in your values files for the storage types match the PVC names created.
