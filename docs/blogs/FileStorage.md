# File storage in HPCC

The first blog post about data storage and HPCC was almost two years ago.   Since then the capabilities have been extended to cover new use cases, so it seemed like time for a new blog to summarise the current state of play.

For those who are used to the bare-metal version of the platform, one of the main differences of the Kubernetes (K8s) version is that Kubernetes clearly separates storage from the compute resources.  This is the opposite of a bare-metal installation of HPCC where the storage and processing are tightly coupled. 

The aim of this blog is to describe the options in the HPCC helm charts that allow you to configure the storage and when those options are useful.

# Defining Storage

Whenever the HPCC system needs to access storage, it uses a 'storage plane' to provide all the information needed to use that storage.  These storage planes are defined within the storage section of the values.yaml file.  Many of the storage plane properties are optional and will be covered later, but first what are the basics?  There are two main options for storage:

## Ephemeral Storage
Ephemeral storage is storage that has the same lifetime as the instance of the HPCC cluster.  When the cluster is installed the storage is created, when the cluster is uninstalled the storage is destroyed.  This is very useful for testing, or experimenting with a system, but not so helpful for storing data on a production system!  (Ephemeral storage is also used for temporary spill files.)  This type of storage is used in the default values.yaml file to make it easy to deploy the chart and experiment with the system.

A storage plane for ephemeral storage has the following format:

```
storage:
  planes:
  - name: data
    prefix: "/var/lib/HPCCSystems/hpcc-data"
    storageSize: 1Gi
    storageClass: ""
```

What do the different options mean.
* name\
  a unique name used to identify the storage plane.
* prefix\
  The path that the components will use to access that storage.  The default values.yaml files tend to use paths that are the same as the defaults for bare-metal installations, but that isn't a requirement.  Each storage plane prefix can be anything as long as they are unique for each plane.
* storageSize\
  The main option you are likely to configure for ephemeral storage - how much storage needs to be reserved.
* storageClass\
  The presence of this property indicates that ephemeral storage is being used.  It specifies the type of storage used by this plane - where "" means use the default.  When running in a cloud environment you can use this to select between the different storage options the vendor provides.

When the HPCC system starts up, storage will be created for each storage plane that uses ephemeral storage.

## Persistent data

Kubernetes uses the concept of persistent volumes (pvs) and persistent volume claims (pvcs) to manage storage.  They are used behind the scenes to implement ephemeral storage planes, but for data to be persistent they need to be explicitly managed separately from the cluster because they have different lifetimes.

The pvcs for persistent data must be created before the cluster is installed.  The cluster using those pvcs can then be repeatedly installed and uninstalled, and the data will remain until the pvcs are deleted.  (To confuse things even further the underlying storage may have a different lifetime from the persistent volume.  For example storage that is mounted from the host, or blob storage in azure that is accessed via an NFS persistent volume will continue to exist after the persistent volume is destroyed.)

For details on creating persistent volumes see below.  For now, what information needs to be included in the values.yaml so the system can use them?

```
storage:
  planes:
  - name: data
    prefix: "/var/lib/HPCCSystems/hpcc-data"
    pvc: mydata-pvc
```

The only new property for the storage plane is the name of the pvc.  All the information about how big the volume is, where it is stored, how that storage is configured etc. is defined by the pvc.  The HPCC helm chart only requires  the name of the pvc that will be associated with that storage plane, and the local path that the components will use to access it.

## Persistent volumes

How do you define a persistent volume?  The Kubernetes and Cloud vendor documentation provide the full details.  However, HPCC Systems has also provided some example helm charts to simplify some common cases.  The easiest way to get access to them is to add the helm repo to your configuration:

```
helm repo add hpcc https://hpcc-systems.github.io/helm-chart/
```

In addition to the main hpcc chart this repo contains example charts for creating different types of storage, and other useful components e.g. elastic search or prometheus.

The following command will generate a list of all the charts it contains:

```
helm search repo hpcc
```

Information about each helm chart can be obtained with the command:

```
helm show readme <chart-name>
```

Each helm chart has a values file which can be used to define the characteristics of the storage planes you want to create.  When the storage charts are installed they output a text file which provides (with some minor editing) the storage plane definitions required by the main HPCC chart.  The following are a couple of examples:

### Local persisted data

The hpcc-localdata chart enables you to create a set of persistent volume claims and persistent volumes for the standard storage planes, with the locations relative to the host path provided.

```
helm install localfile hpcc/hpcc-localfile --set common.hostpath=<path> | grep -A1000 storage: > localstorage.yaml
```

This command line creates the pvcs needed by that platform.  The grep command extracts the section of the output to supply when installing the hpcc cluster.  E.g.
```
helm install mycluster hpcc -f localstorage.yaml
```

How does this local storage fit in with a couple of local kubernetes options?

#### Docker desktop:

This works particularly well with OSX and Windows (especially with the new wsl 2 support in Windows).

If you have a windows machine and you want data persisted to the **c:\\hpccdata** directory, the first step is to make sure the relevant directories exist:

    mkdir c:\hpccdata
    mkdir c:\hpccdata\dalistorage
    mkdir c:\hpccdata\queries
    mkdir c:\hpccdata\sasha
    mkdir c:\hpccdata\debug
    mkdir c:\hpccdata\hpcc-data
    mkdir c:\hpccdata\dropzone

Then pass the directory /run/desktop/mnt/host/c/hpccdata to the helm chart for creating the local pvcs.  E.g.

```
helm install localfile hpcc/hpcc-localfile --set common.hostpath=/run/desktop/mnt/host/c/hpccdata | grep -A1000 storage: > localstorage.yaml
```

#### Minikube:

If you are running on a linux machine then you are likely to be using minikube.  (Installation instructions can be found here: <https://kubernetes.io/docs/tasks/tools/install-minikube/>). The process is very similar to docker desktop, but the difference is that the host paths used in the pvc are paths in the virtual machine running minikube.  You have two sets of mappings, one from the host operating system to the minikube vm, and the second from the minikube vm to the k8s processes.  This time we will assume we are storing the data in **/home/\<username\>/hpccdata** (**\~/hpccdata**).

First make sure the directories exist:

    mkdir ~/hpccdata/
    mkdir ~/hpccdata/dalistorage
    mkdir ~/hpccdata/queries
    mkdir ~/hpccdata/sasha
    mkdir ~/hpccdata/debug
    mkdir ~/hpccdata/hpcc-data
    mkdir ~/hpccdata/dropzone

The next step is to mount those directories into the minikube vm.  This is done using the minikube mount command: (10001,10000 are the gid/uid of the 'hpcc' user inside the containers)

    minikube mount /home/<username>/hpccdata:/mnt/hpccdata --gid=10001 --uid=10000 &

The **\~/hpccdata** directory is now mounted as **/mnt/hpccdata** within the minikube VM.  The miniikube mount command must stay running for the duration of the later stages.

Next, install the chart that creates persistent volume claims (pvcs), using the mount path in the minikube VM:

    helm install localfile examples/local/hpcc-localfile --set common.hostpath=/mnt/hpccdata

And then install the hpcc helm chart as before.

### Azure storage

The hpcc-azurefile chart simplifies creating azure persistent storage.  It covers two different cases:
- Dynamically generate a storage account and shared volume
- Create pvcs using a manually created storage account and shared volume

For more details see the readme associated with the example chart.  i.e.

    helm show readme hpcc/hpcc-azurefile

# Different Storage Uses

The storage plane definitions allow you to define how the underlying storage is accessed, but HPCC uses storage for many different purposes.  How do you configure which plane it uses?  The storage planes have an additional "category" property which indicates the kind of data it contains.  A storage plane is only used for a single category - which ensures the different types of data are isolated from each other.  The other reason that different categories have different planes is that they often require different performance characteristics.

The following categories are currently supported _(with some notes about performance characteristics to take into account)_:

* data\
  Where are data files generated by HPCC stored?\
  _Storage costs are likely to be significant.  Sequential access speed is important, but random access is much less so.  For roxie (and keyed operations in thor) random access is important._
* dali\
  The location of the dali metadata store.\
  _Needs to support fast random access (for medium/large binary data that is stored on disk rather than in memory)._
* dll\
  Where are the compiled ECL queries stored?\
  _The storage needs to allow shared objects to be directly loaded from it efficiently._
* spill (optional)\
  Where are spill files from thor written?  By default spills will be written to a local directory, but this option can be used to ensure they are placed on local NVMe disks.\
  _Sequential access speed is very important._
* lz (optional)\
  A landing zone where external users can read and write files.  The HPCC system can import from or export files to a landing zone.\
  _Typically performance is less of an issue, could be blob/s3 bucket storage - accessed either directly or via an NFS mount._
* sasha\
  Location to stored archived workunits etc.\
  _Typically less speed critical and often used for archiving workunits where storage costs are important._
* debug (optional)\
  The HPCC system supports post-mortem debugging - which allows detailed logs, stack traces and other information to be preserved in the event of a crash.  The debug category defines the storage that those files are written to.\
  _Less speed critical and storage costs may be significant._
* git (optional)\
  Recent versions of HPCC have enhanced support for compiling queries directly from git repositories.  However the eclccserver instances are ephemeral.  In order to avoid repeatedly downloading the same repositories, a storage plane with a git category can be defined to cache and share the repositories between instances.\
  _The storage needs to support fast random access._
* remote\
  If you are reading data from another HPCC instance, it may be storing the data in different locations.  This category allows the system to be configured to directly access the files created by the remote cluster.
* temp (optional)\
  Where are temporary files written?  This is used to determine the location to write non-spill temporary files.  It will default to a local temporary directory.

If more than one storage plane is defined with the same category, the first is used as the default.  You can override the default data or spill plane for a Thor (or roxie, eclagent) instance by setting its "dataPlane" or "spillPlane" properties.  Similarly the default location for git repositories used by eclccserver can be configured with the "gitPlane" property.  See the values.schema.json file for more details and other examples.

# Advanced options
A storage plane supports several options that provide greater control over how the storage planes are used.  First some general options:

## General options
* disabled\
  Setting this to true provides a simple way to temporarily remove a storage plane from the installation without haveing to delete the definition from the values.yaml.
* defaultSprayParts\
  When a file is sprayed from a landing zone, it is common to split it into multiple parts to allow it to be read efficiently from Thor.  In bare-metal systems the number of parts created when spraying is determined by the size of the Thor (or Roxie) cluster targetted by the spray operation.  In Kubernetes the storage is separate so there is no associated size, so this property provides the default number of parts to partition the file into.
* subPath\
  This property has two uses.  The first use allows two storage planes to share the same underlying storage.  Two planes can share the same pvc, if the prefix and all the storage properties are identical.  The subPath property specifies which sub directory within this shared storage to use for this category.\
  The second use is detailed below in bare-metal and Kubernetes co-existing.
* cost\
  This allows you to configure the costs associated with the storage.  When the information is provided the HPCC system will calculate the costs of storing and accessing data, and eclwatch will present them as part of the workunit and file information.  The property has several subproperties:
  * storageAtRest: The cost of storing 1GiB for 1 month.
  * storageReads: The costs of 10,000 read operatons.
  * storageWrites: The cost of 10,000 write operations.
* umask\
  The file creation mask (used by despray).  Normally configured on a landing zone, it allows the umask to be changed after the file is desprayed so it is accessible to other users.
* forcePermissions\
  For some types of provisioned storage, the mounted filing system has insufficient permissions to be read by the hpcc pods. Examples include using hostpath storage, or using NFS mounted storage.  This option performs a recursive chown on the storage before starting up to ensure it can be accessed.

## Performance options

* subDirPerFilePart\
  Some cloud storage implementations suffer significant contention if large numbers of nodes concurrently write files to the same directory.  This option places the files in a subdirectory that corresponds to the part number, avoiding the contention.  It defaults on.
* numDevices\
  Sometimes a single storage account cannot provide enough bandwidth to support a thor with a large number of workers.  This option allows files to be striped over multiple pvcs, each of which can correspond to storage on a different storage account.  Each pvc is mounted as a subdirectory within the specified mount prefix.\
  Note: The device used by the first part for a file depends on the filename - which means that single part files are evenly distributed over the devices.

## Bare-metal options

Bare-metal storage planes differ from standard storage planes because they do not have an associated pvc.  Instead the engines communicate with a dafilesrv service running on a set of machines.

There are two aspects to using bare-metal storage in the Kubernetes system.  The first is the 'hostGroups' entry in the storage section which provides named lists of hosts.  The hostGroups entries can take one of two forms:
```
storage:
  hostGroups:
  - name: "The name of the host group process"
    hosts: [ "a list of host names" ]
```
This is the most common form, and directly associates a list of host names with a name.  The second form is likely to be less common:

```
storage:
  hostGroups:
  - name: "The name of the host group process"
    hostGroup: "Name of the hostgroup to create a subset of"
    count: "Number of hosts in the subset"
    offset: "the first host to include in the subset"
    delta:  "Cycle offset to apply to the hosts"
```
It allows one host group to be derived from another.  Some typical examples with bare-metal clusters are smaller subsets of the host, or the same hosts, but storing different parts on different nodes.  E.g.
```
storage:
  hostGroups:
  - name: mainThor                # Explicit list of hosts
    hosts: [A, B, C, D, E, F, G, H]
  - name subThor                  # Subset of the group last 4 hosts
    hostGroup: mainThor
    count: 4
    offset: 4
  - name replicaSubThor           # Same set of hosts, but different part->host mapping
    hostGroup: subThor
    delta: 1
```

The second aspect of the bare-metal support is to add a property to the storage plane definition to indicate which hosts are associated with it.  There are two options:
* hostGroup: \<name>\
  The name of the host group for bare metal.  For historical reasons the name of the hostgroup must match the name of the storage plane.
* hosts: \<list-of-namesname>\
  An inline list of hosts.  Primarily useful for defining one-off external landing zones.

Example:
```
storage:
  planes:
  - name: demoOne
    category: data
    prefix: "/home/gavin/mainThorData"
    hostGroup: mainThor             # The name of the hostGroup
  - name: myDropZone
    category: lz
    prefix: "/home/gavin/mydropzone"
    hosts: [ 'mylandingzone.com' ]  # Inline reference to an external host.
```

## Url-based access

This section is here for completeness...  The prefix is allowed to be a url which allows access to storage via an api rather than a locally mounted storage.  There are provisional implementations for azure and s3 blobs, but it is recommended to use pvcs backed by NFS mounts instead.  The NFS method is generally better supported, and HPCC also benefits from the operating system page cache.

# Advanced applications

## Remote reading

It is possible for one HPCC cluster to read files from another cluster using the "REMOTE::" syntax.  The meta data is retrieved by calling a service on the remote cluster, but how is data accessed?  One possibility would be to stream it via a remote service, but that would require an extra hop and require resources to run the service on the external cluster.  A much better option is for the cluster to read the files directly.

This is achieved through a 'remote' category on a storage plane and an extra "remote" section within the storage section of the values.yaml file.  This contains a list of remote environments.  Each environment entry specifies a name, a service endpoint and a list of external planes and the local plane that they are mapped to.  This is best illustrated with an example:

```
storage:
  planes:
...
  - name: hpcc2-stddata
    pvc: hpcc2-stddata-pv
    prefix: "/var/lib/HPCCSystems/hpcc2/hpcc-stddata"
    category: remote
  - name: hpcc2-fastdata
    pvc: hpcc2-fastdata-pv
    prefix: "/var/lib/HPCCSystems/hpcc2/hpcc-fastdata"
    category: remote
  remote:
  - name: hpcc2
    service: http://20.102.22.31:8010
    planes:
    - remote: data
      local: hpcc2-stddata
    - remote: fastdata
      local: hpcc2-fastdata
```
Files using the logical filename remote::hpcc2::... (which are stored on the storage plane "fastdata" on the remote system) will be accessed on the local system using the definition of hpcc2-fastdata.

If there is no mapping then? [***** Double check with Jake ****]

## Cached access to files.

Sometimes there is a need to cache files accessed from a particular storage plane.  (One example is using HPC-cache for azure to cache Roxie access to keys.)  Naively you might expect to define a new storage plane to provide the access via the cache through a different pvc.  Unfortunately that doesn't work - the cache isn't a separate storage plane.  A file is either present on both or neither, the cache is instead a different way of accessing the same storage plane.

To allow the platform to represent this concept, a storage plane can define one or more aliases.  E.g.

```
- name: data
    pvc: data-blob-nfs-pvc
    prefix: "/var/lib/HPCCSystems/hpcc-data"
    category: data
    numDevices: 5
    aliases:
    - name: cache
      mode: [ random ]
      pvc: data-cache-pvc
      prefix: "/var/lib/HPCCSystems/hpcc-data-cache"
```
The alias defines an alternative pvc to access the data through, and an alternative path to acccess the contents of pvc.  It also contains a list of modes which describe the situations that access method should be used in.   In the example above if the file access requires random access (e.g. roxie index operations or thor keyed-join), then the cache should be used.  Otherwise for sequential reading and writing the default definition will be used.

Currently "random" is the only mode that the system treats differently, but it is likely others will be supported in the future.

## Bare metal and Kubernetes co-existing

There have been some situations where a bare-metal roxie cluster has been used to synchronize data files, with the intent of a K8s cluster reusing them.  (The K8s system would avoid copying them because the files are already copied to the correct place).

The problem with this approach is that by default the bare-metal system puts the files in a roxie subdirectory within the storage location (i.e. hpcc-data/roxie), and the k8s system does not (i.e. hpcc-data).  This means that when the k8s cluster checks to see if the file exists the expected path does not match, the file does not exist so it copies the file again.  This can be worked-around by setting the subPath property of the associated storage plane to "roxie".  Then both systems expect the files in the same place, allowing the k8s cluster to reuse them.

This pattern is not recommended, it is much cleaner to use the K8s system to copy the data files.

# Future extensions

And finally some possibilities for extensions which are likely to be added in the future

* Using cloud api to copy files\
  Any 1:1 file copy operations are likely to be cheaper and more efficient if they use api functions.  The plane definitions will need to be extended (probably using aliases) to provide the information needed to copy the files.
* indexPlane in Thor\
  An equivalent to dataPlane to specify the default location indexes are written to.
* filtering planes in components to reduce the number of pvcs\
  If the number of pvcs on a standard system starts proving to be large this may be necessary.  It would also allow sensitive data to be excluded from some components.
* Locking remote files\
  The long term goal is to support file locking when reading files from remote directories.  This would also introduce the possibility of distributed file catalogs.
* Alias syntax for remote file access.\
  Allowing aliases for filename prefixes would allow PRODUCTION::ABC::DEF to be used as an alias for REMORE::PRODTHOR::ABC::DEF.  It might be useful for aliases within the local directory (but might also be confusing).

