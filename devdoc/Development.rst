..  ################################################################################
    #    HPCC SYSTEMS software Copyright (C) 2012-2018 HPCC Systems®.
    #
    #    Licensed under the Apache License, Version 2.0 (the "License");
    #    you may not use this file except in compliance with the License.
    #    You may obtain a copy of the License at
    #
    #       http://www.apache.org/licenses/LICENSE-2.0
    #
    #    Unless required by applicable law or agreed to in writing, software
    #    distributed under the License is distributed on an "AS IS" BASIS,
    #    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    #    See the License for the specific language governing permissions and
    #    limitations under the License.
    ################################################################################

===========
HPCC Source
===========

The most upto date details of building the system are found on the HPCC Wiki at
https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC.

*******************
Getting the sources
*******************

The HPCC Platform sources are hosted on GitHub at https://github.com/hpcc-systems/HPCC-Platform. You can download a
snapshot of any branch using the download button there, or you can set up a git clone of the repository. If you are
planning to contribute changes to the system, see the file CONTRIBUTORS at
https://github.com/hpcc-systems/HPCC-Platform/blob/master/CONTRIBUTORS for information about how to set up a GitHub
fork of the project through which pull-requests can be made.

********************************
Building the system from sources
********************************

Requirements
============
The HPCC platform requires a number of third party tools and libraries in order to build.  The `HPCC Wiki`_ contains the
details of the dependencies that are required for different distributions.

For building any documentation, the following are also required::

    sudo apt-get install docbook
    sudo apt-get install xsltproc
    sudo apt-get install fop

**NOTE:** Installing the above via alternative methods (i.e. from source) may place installations outside of searched
paths.

Building the system
===================

The HPCC system is built using the cross-platform build tool cmake, which is available for Windows, virtually all
flavors of Linux, FreeBSD, and other platforms. You should install cmake version 2.8.3 or later before building the
sources.

On some distros you will need to build cmake from sources if the version of cmake in the standard repositories for
that distro is not modern enough.  It is good practice in cmake to separate the build directory where objects and
executables are made from the source directory, and the HPCC cmake scripts will enforce this.

To build the sources, create a directory where the built files should
be located, and from that directory, run::

    cmake <source directory>

Depending on your operating system and the compilers installed on it,
this will create a makefile, Visual Studio .sln file, or other build
script for building the system. If cmake was configured to create a
makefile, then you can build simply by typing::

    make

If a Visual Studio solution file was created, you can load it simply by typing the name::

    hpccsystems-platform.sln

This will load the solution in Visual Studio where you can build in the usual way.

*********
Packaging
*********

To make an installation package on a supported linux system, use the command::

    make package

This will first do a make to ensure everything is up to date, then will
create the appropriate package for your operating system, Currently supported
package formats are rpm (for RedHat/Centos) and  .deb (for Debian and
Ubuntu). If the operating system is not one of the above, or is not recognized,
make package will create a tarball.

The package installation does not start the service on the machine, so if you
want to give it a go or test it (see below), make sure to start the service manually
and wait until all services are up (mainly wait for EclWatch to come up on port 8010).


******************
Testing the system
******************


After compiling, installing the package and starting the services, you can test
the HPCC platform on a single-node setup.


Unit Tests
==========
Some components have their own unit-tests. Once you have compiled (no need to
start the services), you can already run them. Supposing you build a Debug
version, from the build directory you can run::

    ./Debug/bin/roxie -selftest

and::

    ./Debug/bin/eclagent -selftest

You can also run the Dali regression self-tests::

    ./Debug/bin/daregress localhost

Regression Tests
================

**MORE** Completely out of date - needs rewriting.

Compiler Tests
==============

The ECLCC compiler tests rely on two distinct runs: a known good one and your
test build. For normal development, you can safely assume that the OSS/master
branch in github is good. For overnight testing, golden directories need to
be maintained according to the test infrastructure. There are Bash (Linux)
and Batch (Windows) scripts to run the regressions:

The basic idea behind this tests is to compare the output files (logs and
XML files) between runs. The log files should change slightly (the comparison
should be good enough to filter most irrelevant differences), but the XML
files should be identical if nothing has changed. You should only see
differences in the XML where you have changed in the code, or new tests
were added as part of your development.

On Linux, there are two steps:

Step 1: Check-out OSS/master, compile and run the regressions to populate
the 'golden' directory::

    ./regress.sh -t golden -e buildDir/Debug/bin/eclcc

This will run the regressions in parallel, using as many CPUs as you have,
and using your just-compiled ECLCC, assuming you compiled for Debug version.

Step 2: Make your changes (or check-out your branch), compile and run again,
this time output to a new directory and compare to the 'golden' repo.::

    ./regress.sh -t my_branch -c golden -e buildDir/Debug/bin/eclcc

This will run the regressions in the same way, output to 'my_branch' dir
and compare it to the golden version, highlighting the differences.

NOTE: If you changed the headers that the compiled binaries will use, you
must re-install the package (or provide -i option to the script to the new
headers).

Step 3: Step 2 only listed the differences, now you need to see what they are.
For that, re-run the regressing script omitting the compiler, since the only
thing we'll do is to compare verbosely.::

    ./regress.sh -t my_branch -c golden

This will show you all differences, using the same ignore filters as before,
between your two branches. Once you're happy with the differences, commit and
issue a pull-request.

TODO: Describe compiler tests on Windows.

********************
Debugging the system
********************

On linux systems, the makefile generated by cmake will build a specific
version (debug or release) of the system depending on the options selected
when cmake is first run in that directory. The default is to build a release
system. In order to build a debug system instead, use
command::

    cmake -DCMAKE_BUILD_TYPE=Debug <source directory>

You can then run make or make package in the usual way to build the system.

On a Windows system, cmake always generates s solution file with both debug and
release target platforms in it, so you can select which one to build within
Visual Studio.

.. _HPCC Wiki: https://github.com/hpcc-systems/HPCC-Platform/wiki/Building-HPCC