Exporting data from a bare metal to a cloud system
==================================================

The most recent versions of the HPCC platform allow you to run a system on Kubernetes in the cloud, and version 8.0 will be suitable for production testing.  But to test a system is suitable for production you often need some real life data.  How do you get data from on existing bare metal system onto the cloud so you can begin some real representative testing?

This document describes the steps to take to export files to a Kubernetes system using azure blob storage.  (Details for other cloud providers will follow later).  It does not need any access from the cloud system back to the bare metal system.

The process makes use of a few changes that have been recently added to the system to make this possible.  You will need the most recent master/7.12.x builds of the platform on both the bare metal and cloud systems.

In brief the steps are:

* export the raw data files from the bare metal system to blob storage
* export information about the files from the bare metal system
* import the meta data into a cloud system.

The rest of this document fills in the details.

Steps for exporting data
------------------------

Steps 1-4 configure the bare metal and cloud systems so that the files can be transferred.  Steps 5 to 9 walk through the process of actually transferring the data.

1. Add a drop zone for the azure blob storage

A dropzone should be added in the environment.xml for the bare metal system.

.. code-block:: xml

  <DropZone build="_"
            directory="azure://mystorageaccount@data"
            name="myazure"
            ECLWatchVisible="true"
            umask="022">
            <ServerList name="ServerList" server="127.0.0.2"/>
  </DropZone>

This uses a fake localhost ip address (127.0.0.2) to be able to identify the dropzone when exporting the data.

2. Add a secret for the blob storage

Save the azure storage account access key into the file ``"/opt/HPCCSystems/secrets/storage/azure-mystorageaccount/key"``.  This will then be used when the azure storage account "mystorageaccount" is accessed.

3. Define a storage plane to the cloud system

(The next couple of steps have been also been described in a previous blog.)

The following section should be added to the helm values file:

.. code-block:: helm

  storage:
    planes:
    ...
    - name: azureblobs
      prefix: azure://mystorageaccount@data
      secret: azure-mystorageaccount

4. Publish a secret

Register a file containing the key used to access the storage account as key with Kubernetes:

.. code-block:: bash

  kubectl apply -f secrets/myazurestorage.yaml

With the following definition of ``secrets/myazurestorage.yaml``:

.. code-block:: helm

  apiVersion: v1
  kind: Secret
  metadata:
    name: myazurestorage
  type: Opaque
  stringData:
    key: <base64-encoded-access-key>

And add the following definition to the helm values file to ensure the Kubernetes secret is associated with the appropriate logical secret name within the hpcc system:

.. code-block:: helm

  secrets:
    storage:
      azure-mystorageaccount: myazurestorage

5. Get a list of files to export

One possibility, especially for a small system, is to use a dfuplus command to get a list of files and superfiles (with an optional name pattern).  E.g.

.. code-block:: bash

  dfuplus action=list server=<src-esp> [name=<filename-mask>]

6. Export the meta definition

For each of the files and superfiles we need to export the metadata to a local file.

.. code-block:: bash

   dfuplus action=savexml server=<src-esp> srcname=<logical-filename> dstxml=<metafile-name>

The same command works for both files and super files - although you will want to import them differently.

7. Export the data from the bare metal system to the azure blob storage:

The despray command can be used to copy a logical file to an external location.

.. code-block:: bash

   dfuplus action=despray server=<src-esp> srcname=<logical-filename> dstip=127.0.0.2 wrap=1 transferBufferSize=4194304

This command line makes use of a some recent changes:

* The wrap=1 options ensures that the file parts are preserved as they are copied.
* The destination filename is now optional and if it is omitted it is derived from the source filename.  Exporting ``"a::b::c.xml"`` will write to the file ``"a/b/c.xml._<n>_of_<N>"``.
* The transferBufferSize is specified because it defaults to 64K in old environment files, which significantly reduces the throughput for large files.

8. Import each of the file definitions

Register the metdata for each of the files with the cloud system (which will now need to be running).

.. code-block:: bash

  dfuplus action=add server=<cloud-esp> dstname=<logical-filename> srcxml=<metafile-name> dstcluster=azureblobs

There is using a new dfuplus option which allows you to specify where the physical files are found.  This should be set to the name of the blob storage plane - in this case azureblobs.  If the physical files do not exist in the correct places then this will fail.

9. Import each of the super file definitions.

Finally once all the files have been imported, superfiles can be added.

.. code-block:: bash

  dfuplus action=add server=<cloud-esp> dstname=<logical-filename> srcxml=<metafile-name>

The syntax is the same as importing the defintion for a locgical file, but there is no need to override the cluster.


Example batch file
------------------

The following is a sample batch file for processing a list of files and superfiles, and performing all of the dfu commands for steps 6-9:

.. code-block:: bash

    #!/bin/bash

    FilesToSpray=(
    regress::local::dg_fetchindex1
    ...
    )

    srcserver=localhost
    tgtserver=192.168.49.2:31056
    newplane=azureBlobs

    # The following is useful for checking the ips hav been configured correctly
    echo "Source contains `dfuplus server=${srcserver} action=list "*" | wc -w` files (${srcserver})"
    echo "Target contains `dfuplus server=${tgtserver} action=list "*" | wc -w` files (${tgtserver})"
    echo "Copying `echo ${FilesToSpray[@]} | wc -w` files from ${srcserver}"
    echo "Press <newline> to continue"
    read

    # Iterate through the files
    for file in "${FilesToSpray[@]}"; do
        #Export the meta data to a file
        dfuplus action=savexml server=$srcserver srcname=$file dstxml=export.$file.xml
        if ! grep -q SuperFile export.$file.xml; then
            #A logical file => export it
            echo dfuplus action=despray server=$srcserver srcname=$file dstip=127.0.0.2 wrap=1 transferBufferSize=4194304
            dfuplus action=despray server=$srcserver srcname=$file dstip=127.0.0.2 wrap=1 transferBufferSize=4194304
        fi
    done

    #Add the remote information for the raw files
    for file in "${FilesToSpray[@]}"; do
        if ! grep -q SuperFile export.$file.xml; then
            echo dfuplus action=add server=$tgtserver dstname=$file srcxml=export.$file.xml dstcluster=$newplane
            dfuplus action=add server=$tgtserver dstname=$file srcxml=export.$file.xml dstcluster=$newplane
        fi
    done

    #Now add the superfile information
    for file in "${FilesToSpray[@]}"; do
        if grep -q SuperFile export.$file.xml; then
        echo super: dfuplus action=add server=$tgtserver dstname=$file srcxml=export.$file.xml
        dfuplus action=add server=$tgtserver dstname=$file srcxml=export.$file.xml
        fi
    done
