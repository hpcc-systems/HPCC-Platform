
Documentation about the new file work.

YAML files.  The following are the YAML definitions which are used to serialize file information from dali/external store to the engines and if necessary to the worker nodes.

Storage planes
==============
This is already covered in the deployed helm charts.  It has been extended and rationalized slightly.

storage:
  hostGroups:
  - name: <required>
    hosts: [ .... ]

  planes:
    name: <required>
    prefix: <path>              # Root directory for accessing the plane (if pvc defined), or url to access plane.
    numDevices: 1               # number of devices that are part of the plane
    hosts: <name>               # Name of the host group for bare metal
    secret: <secret-id>         # what secret is required to access the files.
    options:                    # not sure if it is needed

The following options are only used for replication on bare metal systems:

    start: <unsigned:0>         # first offset within hosts that is unsigned
    size: <unsigned:#hosts>     # how many hosts within the host group are used ?(default is number of hosts). numDevices = size of hostGroup
    offset: <unsigned:0>        # number added to the part number before mapping to a host/device

Changes:
* The replication information has been removed from the storage plane.  It will now be specified on the thor instance indicating where (if anywhere) files are replicated.
* The hash character (#) in a prefix or a secret name will be substituted with the device number.  This replaces the old includeDeviceInPath property.  This allows more flexible device substition for both local mounts and storage accounts.  The number of hashes provides the default padding for the device number.  (Existing Helm charts will need to be updated to follow these new rules.)
* Neither thor or roxie replication is special cased.  They are represented as multiple locations that the file lives (see examples below).  Existing baremetal environments would be mapped to this new representation with implicit replication planes.  (It is worth checking the mapping to roxie is fine.)

Files
=====

file:
- name: <logical-file-name>
  format: <type>                # e.g. flat, csv, xml, key, parquet
  meta: <binary>                # (opt) format of the file, (serialized hpcc field information).
  metaCrc: <unsigned>           # hash of the meta
  numParts                      # How many file parts.
  singlePartNoSuffix: <boolean> # Does a single part file include .part_1_of_1?
  numRows:                      # total number of rows in the file (if known)
  rawSize:                      # total uncompressed size
  diskSize                      # is this useful?  when binary copying?
  planes: []                    # list of storage planes that the file is stored on.
  tlk:                          # ???Should the tlk be stored in the meta and returned?
  splitType: <split-format>     # Are there associated split points, and if so what format? (And if so what variant?)

#options relating to the format of the input file:
  grouped: <boolean>            # is the file grouped?
  compressed: <boolean>
  blockCompressed: <boolean>
  formatOptions:                # Any options that relate to the file format e.g. csvTerminator.  These are nested because they can be completely free format
  recordSize:                   # if a fixed size record.  Not really sure it is useful

  part:                         # optional information about each of the file parts  (Cannot implement virtual file position without this)
  - numRows: <count>              # number of rows in the file part
    rawSize: <size>               # uncompressed size of the file part
    diskSize: <size>              # size of the part on disk

#  extra fields that are used to return information from the file lookup service

  missing: <boolean>            # true if the file could not be found
  external: <boolean>           # filename of the form external:: or plane::


If the information needs to be signed to be passed to dafilesrv for example, the entire structure of (storage, files) is serialized, and compressed, and that then signed.

Functions
=========
Logically executed on the engine, and retrived from dali or in future versions from an esp service (even if for remote reads).

GetFileInfomation(<logical-filename>, <options>)

The logical-filename can be any logical name - including a super file, or an implicit superfile.

options include:
* Are compressed sizes needed?
* Are signatures required?
* Is virtual fileposition (non-local) required?
* name of the user

This returns a structure that provides information about a list of files

meta:
  hostGroups:
  storage:
  files:
  secrets:
    #The secret names are known, how do we know which keys are required for those secrets?


Some key questions:
* Should the TLK be in the dali meta information?  [Possibly, but not in first phase. ]
* Should the split points be in the dali meta information?  [Probably not, but the meta should indicate whether they exist, and if so what format they are. ]
* Super files (implicit or explicit) can contain the same file information more than once.  Should it be duplicated, or have a flag to indicate a repeat. [I suspect this is fairly uncommon, so duplication would be fine for the first version.]
* What storage plane information is serialized back?  [ all is simplest.  Can optimize later. ]

NOTE: This doesn't address the question of writing to a disk file...

----------------------------------------------------

Local class for interpreting the results.  Logically executed on the manager, and may gather extra information that will be serialized to all workers.  The aim is that the same class implementations are used by all the engines (and fileview in esp).

MasterFileCollection : RemoteFileCollection : FileCollection(eclReadOptions, eclFormatOptions, wuid, user, expectedMeta, projectedMeta);
MasterFileCollection //Master has access to dali
RemoteFileCollection : has access to remote esp // think some more

FileCollection::GatherFileInformation(<logical-filename>, gatherOptions);
- potentially called once per query.
- class is responsible for optimizing case where it matches the previous call (e.g. in a child query).
- possibly responsible for retrieving the split points ()

Following options are used to control whether split points are retrieved when file information is gathered
* number of channels reading the data?
* number of strands reading each channel?
* preserve order?

gatherOptions:
* is it a temporary file?

This class serializes all information to every worker, where it is used to recereate a copy of the master filecollection.  This will contain information derived from dali, and locally e.g. options specified in the activity helper.  Each worker has a complete copy of the file information.  (This is similar to dafilesrv with security tokens.)

The files that are actually required by a worker are calculated by calling the following function.  (Note the derived information is not serialized.)

FilePartition FileCollection::calculatePartition(numChannels, partitionOptions)

partitionOptions:
* number of channels reading the data?
* number of strands reading each channel?
* which channel?
* preserve order?
* myIP

A file partition contains a list of file slices:

class FileSlice (not serialized)
{
  IMagicRowStream * createRowStream(filter, ...);   // MORE!
  File * logicalFile;
  offset_t startOffset;
  offset_t endOffset;
};

Things to bear in mind:
- Optimize same file reused in a child query (filter likely to change)
- Optimize same format reused in a child query (filename may be dynamic)
- Intergrating third party file formats and distributed file systems may require extra information.
- optimize reusing the format options.
- ideally fail over to a backup copy midstream..  and retry in failed read e.g. if network fault

Examples
========
Example definition for a thor400, and two thor200s on the same nodes:

hostGroup:
- name: thor400Group
  host: [node400_01,node400_02,node400_03,...node400_400]

storage:
  planes:
  #Simple 400 way thor
  - name: thor400
    prefix: /var/lib/HPCCSystems/thor400
    hosts: thor400Group
  #The storage plane used for replicating files on thor.
  - name: thor400_R1
    prefix: /var/lib/HPCCSystems/thor400
    hosts: thor400Group
    offset: 1
  # A 200 way thor using the first 200 nodes as the thor 400
  - name: thor200A
    prefix: /var/lib/HPCCSystems/thor400
    hosts: thor400Group
    size: 200
  # A 200 way thor using the second 200 nodes as the thor 400
  - name: thor200B
    prefix: /var/lib/HPCCSystems/thor400
    hosts: thor400Group
    size: 200
    start: 200
  # The replication plane for a 200 way thor using the second 200 nodes as the thor 400
  - name: thor200B_R1
    prefix: /var/lib/HPCCSystems/thor400
    hosts: thor400Group
    size: 200
    start: 200
    offset: 1
  # A roxie storage where 50way files are stored on a 100 way roxie
  - name: roxie100
    prefix: /var/lib/HPCCSystems/roxie100
    hosts: thor400Group
    size: 50
  # The replica of the roxie storage where 50way files are stored on a 100 way roxie
  - name: roxie100_R1
    prefix: /var/lib/HPCCSystems/thor400
    hosts: thor400Group
    start: 50
    size: 50

device = (start + (part + offset) % size;

size <= numDevices
offset < numDevices
device <= numDevices;

There is no special casing of roxie replication, and each file exists on multiple storage planes.  All of these should
be considered when determining which is the best copy to read from a particular engine node.

Creating storage planes from an existing systems [implemented]


Milestones:
-----------

a) Create baremetal storage planes [done]
b) [a] Start simplifying information in dali meta (e.g. partmask, remove full path name)
*c) [a] Switch reading code to use storageplane away from using dali path and environment paths - in ALL disk reading and writing code
- change numDevices so it matches the container
d) [c] Convert dali information from using copies to multiple groups/planes
*e) [a] Reimplement the current code to create an IPropertyTree from dali file information (in a form that can be reused in dali)
*f) [e] Refactor existing PR to use data in an IPropertyTree and cleanly separate the interfaces.
g) Switch hthor over to using the new classes by default and work through all issues
h) Refactor stream reading code.
   Look at the spark interfaces for inspiration/compatibility
i) Refactor disk writing code into common class?
j) [e] create esp service for accessing meta information
k) [h] Refactor and review azure blob code
l) [k] Re-implement S3 reading and writing code.

m) Switch fileview over to using the new classes.  (Great test they can be used in another context + fixes a longstanding bug.)

) Implications for index reading?  Will they end up being treated as a normal file?  Don't implement for 8.0, although interface may support it.

*) My primary focus for initial work.

File reading refactoring
========================

Buffer sizes:
- storage plane specifies an optimal reading minimum
- compression may have a requirement
- the use for the data may impose a requirement e.g. a subset of the data, or only fetching a single record
- parallel disk reading may want to read a big chunk, but then process in sections.  groan.

Look at lambda functions to create split points for a file.  Can we use the java classes to implement it on binary files (and csv/xml)?


******************** Reading classes and meta information ********************
meta comes from a combination of the information in dfs and the helper

The main meta information uses the same structure that is return by the function that returns file infromation from dali.
The format specific options are contained in a nested attribute so they can be completely arbitrary

The helper class also generates a meta structure.  Some options fill in root elements - e.g. compressed.  Some fill in a new section (hints: @x=y).  The format options are generated from the paramaters to the dataset format.

note normally there is only a single (or very few) files, so merging isn't too painful.
queryMeta()
queryOptions()
rename meta to format?
???

DFU server
==========
Where does DFUserver fit in in a container system?

DFU has the following main functionality in a bare metal system:
a) Spray a file from a 1 way landing zone to an N-way thor
b) Convert file format when spraying.  I suspect utf-16->utf8 is the only option actually used.
c) Spray multiple files from a landing zone to a single logical file on an N-way thor
d) Copy a logical file from a remote environment
e) Despray a logical file to an external landing zone.
f) Replicate an existing logical file on a given group.
g) Copy logical files between groups
h) File monitoring
i) logical file operations
j) superfile operations

ECL has the ability to read a logical file directly from a landingzone using 'FILE::<ip>' file syntax, but I don't think it is used very frequently.

How does this map to a containerized system?  I think the same basic operations are likely to be useful.
a) In most scenarios Landing zones are likely to be replaced with (blob) storage accounts.  But for security reasons these are likely to remain distinct from the main location used by HPCC to store datasets.  (The customer will have only access keys to copy files to and from those storage accounts.)  The containerized system has a way for ECL to directly read from a blob storage account ('PLANE::<plane'), but I imagine users will still want to copy the files in many situations to control the lifetime of the copies etc.
b) We still need a way to convert from utf16 to utf8, or extend the platform to allow utf16 to be read directly.
c) This is still equally useful, allowing a set of files to be stored as a single file in a form that is easy for ECL to process.
d) Important for copying data from an existing bare metal system to the cloud, and from a cloud system back to a bare metal system.
e) Useful for exporting results to customers
f+g) Essentially the same thing in the cloud world.  It might still be useful to have
h) I suspect we will need to map this to cloud-specific apis.
i+j) Just as applicable in the container world.

Broadly, landing zones in bare metal map to special storage planes in containerized, and groups also map to more general storage planes.

There are a couple of complications connected with the implementation:
1) Copying is currently done by starting an ftslave process on either the source or the target nodes.  In the container world there is no local node, and I think we would prefer not to start a process in order to copy each file.
2) Copying between storage groups should be done using the cloud provider api, rather than transferring data via a k8s job.

Suggestions:

* Have a load balanced dafilesrv which supports multiple replicas.  It would have a secure external service, and an internal service for trusted components.
* Move the ftslave logic into dafilesrv.  Move the current code for ftslave actions into dafilesrv with new operations.
* When copying from/to a bare metal system the requests are sent to the dafilesrv for the node that currently runs ftslave.  For a container system the requests are sent to the loadbalanced service.
* It might be possible to migrate to lamda style functions for some of the work...
* A later optimization would use a cloud service where it was possible.
* When local split points are supported it may be better to spray a file 1:1 along with partition information.  Even without local split points it may still be better to spray a file 1:1 (cheaper).
* What are the spray targets?  It may need to be storage plane + number of parts, rather than a target cluster.  The default number of parts is the #devices on the storage plane.

=> Milestones
a) Move ftslave code to dafilesrv  (partition, pull, push)  [Should be included in 7.12.x stream to allow remote read compatibility?]
b) Create a dafilesrv component to the helm charts, with internal and external services.
c) use storage planes to determine how files are sprayed etc. (bare-metal, #devices)
   Adapt dfu/fileservices calls to take (storageplane,number) instead of cluster.  There should already be a 1:1 mapping from existing cluster to storage planes in a bare-metal system, so this may not involve much work.  [May also need a flag to indicate if ._1_of_1 is appended?]
d) Select correct dafilesrv for bare-metal storage planes, or load balanced service for other.
   (May need to think through how remote files are represented.)

=> Can import from a bare metal system or a containerized system using command line??
   NOTE: Bare-metal to containerized will likely need push operations on the bare-metal system.  (And therefore serialized security information)
   This may still cause issues since it is unlikely containerized will be able to pull from bare-metal.
   Pushing, but not creating a logical file entry on the containerized system should be easier since it can use a local storage plane definition.

e) Switch over to using the esp based meta information, so that it can include details of storage planes and secrets.
   [Note this would also need to be in 7.12.x to allow remote export to containerized, that may well be a step too far]

f) Add option to configure the number of file parts for spray/copy/despray
g) Ensure that eclwatch picks up the list of storage planes (and the default number of file parts), and has ability to specify #parts.

Later:
h) plan how cloud-services can be used for some of the copies
i) investigate using serverless functions to calculate split points.
j) Use refactored disk read/write interfaces to clean up read and copy code.
k) we may not want to expose access keys to allow remote reads/writes - in which they would need to be pushed from a bare-metal dafilesrv to a containerized dafilesrv.

Other dependencies:
* Refactored file meta information.  If this is switching to being plane based, then the meta information should also be plane based.  Main difference is not including the path in the meta information (can just be ignored)
* esp service for getting file information.  When reading remotely it needs to go via this now...
