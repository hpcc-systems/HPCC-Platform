Docker images related to HPCC are structured as follows

hpccsystems/platform-build-base

This image contains all the development packages required to build the hpcc platform,
but no HPCC code or sources. It changes rarely. The current version is tagged 7.8 and
is based on Ubuntu 18.04 base image

hpccsystems/platform-build

Building this image builds an installation package (.deb file) for a specified git tag
of the HPCC platform sources. The Dockerfile takes two arguments, naming the version of
the platform-build-base image to use, and the git tag to use. Sources are fetched from
github. An image will be pushed to Dockerhub for every public tag on the HPCC-Platform
repository in GitHub, which developers can use as a base for their own development.

There is a second Dockerfile inplatform-build-incremental that can be used by developers
working on a branch that is not yet tagged or merged into upstream, that uses 
hpccsystems/platform-build as a base in order to avoid the need for full rebuilds each time
the image is built.

hpccsystems/plaform-core

This uses the .deb file from a hpccsystems/plaform-build image to install a copy of the
full platform code, without specialization to a specific component.

hpccsystems/dali
hpccsystems/roxie
hpccsystems/esp
hpccsystems/eclagent
hpccsystems/eclcc
etc

These are specializations of the platform-core image to run a specific component.
Portions of the platform-core that are not needed by this component may be removed.
These images are the ones that are referred to in helm scripts etc when launching
a cloud cluster.

If launched without further parameters or configuration, a system with default
settings can be started, but it will be more normal to apply some configuration at
container launch time.
