# HPCC Azure storage

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
The values-retained-azurefile.yaml is an example of the HPCC storage settings that should be applied, after the "hpcc-azurefile" helm chart is installed.
NB: The output of installing this chart, will contain a generated example with the correct PVC names.

Example use:

*helm install azstorage hpcc-azurefile/*

### values-retained-azurefile.yaml

An example values file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-azurefile" helm chart, or ensure the names in your values files for the storage types matched the PVC names created.

