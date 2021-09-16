# Changes in 8.2.0

There are a few changes in the way the values.yaml needs to be specified for HPCC clusters
starting with 8.2.0. These were necessary to implement required functionality. If you have a
customized values file that you have been using with 8.0.x builds, you will need to make a
few simple changes to be able to use it with 8.2.x. The required changes are all enforced
by the schema, so you will see schema errors if you try to use an uncorrected values file.

## Service changes

In order to support annotations on services, and thus control whether public services are
connected to internal subnets or published to the internet, we have changed the way that
services are specified. We have also adjusted some names of fields within service definitions
so that Roxie, Sasha and ESP services are more consistent in their syntax.

The following changes should be noted:
1. For ESP and Sasha services, the settings related to the service have been moved into a new 
   service: section within the component settings.
2. For Roxie services, the name of the setting used to define the external port has been
   changed to servicePort
3. The “public” setting on ESP services and the “external” setting on Roxie services have
   been replaced by a new “visibility” setting specifying a user-settable preset. We provide
   pre-populated presets “cluster”, “local” and “global” but others can be added if needed.
4. Annotations and labels can be specified on any service, or for any visibility preset, or
   for any component pod.

If you try to launch with an unmodified 8.0.x values.yaml, you may see errors like this:

```code
- esp.1: service is required
- esp.2: service is required
- esp.3: service is required
- esp.4: service is required
- roxie.0.services.0: servicePort is required
- roxie.0.services.0: Additional property external is not allowed
- sasha: Must validate one and only one schema (oneOf)
- sasha.dfuwu-archiver: Must validate one and only one schema (oneOf)
- sasha.dfuwu-archiver: Additional property servicePort is not allowed
- sasha.dfuwu-archiver: Must validate all the schemas (allOf)
- sasha.wu-archiver: Must validate one and only one schema (oneOf)
- sasha.wu-archiver: Additional property servicePort is not allowed
- sasha.wu-archiver: Must validate all the schemas (allOf)
```

For the ESP “service is required” errors, you will need to change a spec that looks like (for example)

```code
esp:
- name: eclwatch
  application: eclwatch
  auth: none
  replicas: 1
  port: 8888
  servicePort: 8010
  public: true
```

to

```code
- name: eclwatch
  application: eclwatch
  auth: none
  replicas: 1
  service:
    port: 8888
    servicePort: 8010
    visibility: local  # or global if you want to be open to the internet
```

For the roxie errors, change something that looks like (for example)

```code
roxie:
- name: roxie
  disabled: false
  prefix: roxie
  services:
  - name: roxie
    port: 9876
    listenQueue: 200
    numThreads: 30
    external: true
  ...
```

to

```code
roxie:
- name: roxie
  disabled: false
  prefix: roxie
  services:
  - name: roxie
    servicePort: 9876
    listenQueue: 200
    numThreads: 30
    visibility: local
```

The sasha errors are not quite so descriptive but are addressed in the same way as the ESP ones, i.e. change something that looks like:

```code
sasha:
  wu-archiver:
    servicePort: 8877
```

to

```code
sasha:
  wu-archiver:
    service:
       servicePort: 8877
    storage:
```

## Startup probe changes

Some components may take a while to start up, but should not be added to the relevant k8s load balancer until they have done so. We us
standard k8s startup probes and readiness probes to manage this process. In 8.2.0 it is possible to override the default settings for the
startup probe by setting minStartupTime and maxStartupTime on any component (Roxie and Dali are the two that are likely to need it). 

## Storage changes

In 8.2.x all storage for data, dlls, landing zones etc. is defined in the list of storage.planes.

Previously daliStorage, dataStorage and dllStorage could either refer to an existing plane, or could implicitly
define a storage plane.  That implicit plane could be based on a pre-existing persistent volume claim (by setting
existingClaim) or ephemeral storage (by setting storageClass and storageSize).  In version 8.2. these sections are no longer
supported.

Implicit planes that were defined within those sections must now use an explicit plane definition.  The plane definitions
have been extended to allow ephemeral storage to be defined (see dali example below).

Previously the default storage plane for a particular storage category could be specified by naming it in the appropriate storage
section.  E.g. storage.dataStorage.plane could name the default data storage plane. The default is now the first plane with the
corresponding category.

Another change is that previously a storage plane had a labels: attribute to indicate what kind of data was stored on
the plane.  It was a list, and if blank defaulted to \[ data \].  This has now become a single valued "category" attribute.
In the unlikely event of wanting to have multiple categories stored on the same mounted drive, it is possible to define
a plane with a duplicate prefix and storage definition with a different category.  Planes also support the subPath
attribute to allow a subdirectory to be used within a mount point.

For instance if you have the following definition in 8.0.x:

```code
storage:
  daliStorage:
     existingClaim: my-pvc
```

would be defined as follows in 8.2.x:

```code
storage:
  - name: dali
    pvc: my-pvc
    prefix: "/var/lib/HPCCSystems/dalistorage"
    category: dali
```

And similarly a definition that uses ephemeral storage:
`
```code
storage:
  daliStorage:
    storageClass: ""
    storageSize: 1Gi
```

would be become:

```code
storage:
  - name: dali
    storageClass: ""
    storageSize: 1Gi
    prefix: "/var/lib/HPCCSystems/dalistorage"
    category: dali
```

The default values.yaml and example files have been updated to reflect these changes.

## Default data plane changes

Previously the storagePlane property could be set on an engine to change the default data plane.  This has now been
renamed to dataPlane for consistency with the category name.  In future other types of planes will be configurable
in the same way (spill, dll, temp).

Sasha services used to define their storage plane within a nested storage attribute:

```sasha:
  wu-archiver:
    storage:
      plane: sasha
```

This is now simplified to:

```sasha:
  wu-archiver:
    plane: sasha
```

A value of "" can be used mean use the 1st plane with the sasha category.
