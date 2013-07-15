===============================
Ecl-bundle source documentation
===============================

************
Introduction
************

Purpose
=======
The ecl-bundle executable (normally executed from the ecl executable by specifying 'ecl bundle XXX'
is designed to manipulate ecl bundle files - these are self-contained parcels of ECL code, packaged
into a compressed file like a zip or .tar.gz file, that can be downloaded, installed, removed etc
from an ECL installation.

Design
======
The metadata for ECL bundles is described using an exported module called Bundle within the bundle's
source tree - typically, this means that a file called Bundle.ecl will be added to the highest level
of the bundle's directory tree. In order to extract the information from the Bundle module, eclcc is
run in 'evaluate' mode (using the -Me option) which will parse the bundle module and output the required
fields to stdout.

ecl-bundle also executes eclcc (using the --showpaths option) to determine where bundle files are to
be located.

Directory structure
===================
In order to make versioning easier, bundle files are not copied directly into the bundles directory.
A bundle called "MyBundle" that announces itself as version "x.y.z" will be installed to the directory

$BUNDLEDIR/_versions/MyBundle/x.y.z

A "redirect" file called MyBundle.ecl is then created in $BUNDLEDIR, which redirects any IMPORT MyBundle
statement to actually import the currently active version of the bundle in _versions/MyBundle/x.y.z

By rewriting this redirect file, it is possible to switch to using a different version of a bundle without
having to uninstall and reinstall.

In a future release, we hope to make it possible to specify that bundle A requires version X of bundle B,
while bundle C requires version Y of bundle B. That will require the redirect files to be 'local' to a
bundle (and will require that bundle B uses a redirect file to ensure it picks up the local copy of B
when making internal calls).

Key classes
===========
An IBundleInfo represents a specific copy of a bundle, and is created by explicitly parsing a snippet of
ECL that imports it, with the ECL include path set to include only the specified bundle.

An IBundleInfoSet represents all the installed versions of a particular named bundle.

An IBundleCollection represents all the bundles on the system.

Every individual subcommand is represented by a class derived (directly or indirectly) from EclCmdCommon.
These classes are responsible for command-line parsing, usage text output, and (most importantly) execution
of the desired outcomes.
