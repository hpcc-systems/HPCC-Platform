# HPCC Local (host) storage

## Directory contents

### hpcc-localfile/

The hpcc systems hpcc-localfile helm chart will provision a new Persistent Volume and a Persistent Volume Claim for each of the required HPCC storage types.
Once installed the generated PVC names should be used when installing the HPCC helm chart.
The values-localfile.yaml is an example of the HPCC storage settings that should be applied, after the "localfile" helm chart is installed.
Note: The output of installing this chart, will contain a generated example with the correct PVC names.

The chart assumes that the dalistorage, queries, sasha, and hpcc-data subdirectories already exist.

Examples of use:

#### Docker desktop (using wsl2 to access windows data):

  With a host directory of c:\hpccdata

    helm install localfile examples/local/hpcc-localfile --set common.hostpath=/run/desktop/mnt/host/c/hpccdata
    helm install mycluster hpcc/ --set global.image.version=latest -f examples/local/values-localfile.yaml

Note for the above example 'localfile' is the arbitrary instance name, it could be named anything, but "localfile" will match the delivered examples values-localfile.yaml file.

#### Docker desktop (using osx):

  With a host directory of /Users/myuser/hpccdata

    helm install localfile examples/local/hpcc-localfile --set common.hostpath=/Users/myuser/hpccdata
    helm install mycluster hpcc/ --set global.image.version=latest -f examples/local/values-localfile.yaml

#### Docker desktop (using hyper-v):

  With a host directory of c:\hpccdata<br/>
  In addition, you must add the folder in Docker Desktop's settings for Resources>File Sharing.

    helm install localfile examples/local/hpcc-localfile --set common.hostpath=/c/hpccdata
    helm install mycluster hpcc/ --set global.image.version=latest -f examples/local/values-localfile.yaml

#### Minikube:

  With a host directory of /home/myuser/hpccdata
  When running under minikube the directory will need to be mounted with a command

    minikube mount <host-directory>:<minikube-host-directory> --gid=1000 --uid=999

  which needs to remain running while the cluster is active.  The --gid and --uid options are required to ensure that
  the hpcc user has access to the data from within the container.

  The following examples mounts it to /mnt/hpccdata within minikube to help distinguish the different directories.

    minikube mount /home/myuser/hpccdata:/mnt/hpccdata --gid=1000 --uid=999

    helm install localfile examples/local/hpcc-localfile --set common.hostpath=/mnt/hpccdata
    helm install mycluster hpcc/ --set global.image.version=latest -f examples/local/values-localfile.yaml

### values-localfile.yaml

An example value file to be supplied when installing the HPCC chart.
NB: Either use the output auto-generated when installing the "hpcc-localfile" helm chart, or ensure the names in your values files for the storage types matched the PVC names created.
