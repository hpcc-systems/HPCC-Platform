Docker images
=============

Docker images related to HPCC are structured as follows

**hpccsystems/platform-build-base**

This image contains all the development packages required to build the hpcc platform,
but no HPCC code or sources. It changes rarely. The current version is tagged 7.10 and
is based on Ubuntu 20.04 base image

**hpccsystems/platform-build**

Building this image builds and installs the HPCC codebase for a specified git tag
of the HPCC platform sources. The Dockerfile takes arguments naming the version of
the platform-build-base image to use and the git tag to use. Sources are fetched from
github. An image will be pushed to Dockerhub for every public tag on the HPCC-Platform
repository in GitHub, which developers can use as a base for their own development.

There is a second Dockerfile in platform-build-incremental that can be used by developers
working on a branch that is not yet tagged or merged into upstream, that uses 
hpccsystems/platform-build as a base in order to avoid the need for full rebuilds each time
the image is built.

**hpccsystems/plaform-core**

This uses the build artefacts file from a hpccsystems/plaform-build image to install a copy of the
full platform code, which can be used to run any HPCC component.

If you need additional components installed on your cluster, such as Python libraries, you can create
a Docker image based on platform-core with additional components installed. An example can be found
in the examples/numpy directory. You will then override the image name when deploying a helm chart in
order to enabled your additional components.


---
Bash scripts
============
buildall.sh - Used to create and publish a docker container corresponding to a github tag
clean.sh    - Clean up old docker images (if disk gets full)
incr.sh     - Build local images for testing (delta from a published image)
startall.sh - Start a local k8s cluster
stopall.sh  - Stop a local k8s cluster

---

Helm chart
==========

The Helm chart in helm/hpcc/ can be used to deploy an entire HPCC environment to a K8s cluster.

values.yaml sections
--------------------

global:
    # The global section applies to all components within the HPCC system.

dali:
esp:
roxie:
eclccserver:
etc
    # Each section will specify a list of one or more components of the specified type
    # Within each section, there's a map specifying settings specific to that instance of the component,
    # including (at least) name, plus any other required settings (which vary according to component type).

Template structure
------------------

There are some helper templates in _util.tpl to assist in generation of the k8s yaml for each component.
Many of these are used for standard boilerplate that ends up in every component:

hpcc.utils.addImageAttrs
 - adds information about the container image source, version and pull mode
hpcc.utils.addConfigVolumeMount
hpcc.utils.addConfigVolume
 - add information that mount the global and local configuration information into /etc/config using k8s ConfigMap
hpcc.utils.generateConfigMap
 - generates local and global config files for the above
hpcc.utils.configArg
 - generates the parameter to pass to the container naming the config file 
hpcc.utils.daliArg
 - generates the parameter to pass to the container naming the dali to connect to 

Configuration files
-------------------

Each component can specify local configuration via config: or configFile: settings - configFile names a file
that is copied verbatim into the relevant ConfigMap, while config: allows the config file's contents to be
specified inline.

In addition, global config info (same for every component) is generated into a global.json file and made
available via ConfigMap mechanism. So far, this only contains 

  "version": {{ .root.Values.global.image.version | quote }}

but we can add more.

Roxie modes under K8s
---------------------

When running under K8s, Roxie has 3 fundamental modes of operation:

  1. Scalable array of one-way roxie servers

     Set localSlave=true, replicas=initial number of pods

  2. Per-channel-scalable array of combined servers/slaves

     localSlave=false, numChannels=nn, replicas=initial number of pods per channel (default 2)

     There will be numChannels*replicas pods in total

  3. Scalable array of servers with per-channel-scalable array of slaves

     localSlave=false, numChannels=nn, replicas=pods/channel, serverReplicas=initial number of server pods

     There will be numChannels*replicas slave pods and serverReplicas server pods in total
  
     This mode is somewhat experimental at present!
  
