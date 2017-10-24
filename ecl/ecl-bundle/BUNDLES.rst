=========================
ECL Bundle writer's guide
=========================

************
Introduction
************

Purpose
=======
An ECL Bundle is a self-contained (other than dependencies on other bundles - see below) set of
ECL files, designed to be zipped up and distributed, downloaded, retrieved from online repositories,
and so on.

Structure
=========

An ECL bundle consists of a directory of ECL files, including the file Bundle.ecl which
contains the meta-data about the bundle. For really simple bundles, it is possible to store the
entire bundle in a single .ecl file, with Bundle exported from it as a nested module. Care is needed
in such cases that the Bundle module can be exported without any external dependencies (for example,
by using ,FORWARD on the main module's definition).

You must add the bundle folder as the source folder to have the same import paths for the attribute when 
installed as a bundle.

Metadata
========
The Bundle.ECL should inherit from the standard library module Std.BundleBase, as in the following
example::

  EXPORT Bundle := MODULE(Std.BundleBase)
    EXPORT Name := 'Bloom';
    EXPORT Description := 'Bloom filter implementation, and example of ECL bundle layout';
    EXPORT Authors := ['Richard Chapman','Charles Kaminski'];
    EXPORT License := 'http://www.apache.org/licenses/LICENSE-2.0';
    EXPORT Copyright := 'Copyright (C) 2013 HPCC Systems®';
    EXPORT DependsOn := [];
    EXPORT Version := '1.0.0';
    EXPORT PlatformVersion := '4.0.0';
  END;

The meanings of the various fields in the metadata are described in Std.BundleBase.

Dependencies
============

A bundle can specify dependencies on zero or more other modules, using the DependsOn string set.
After the name of the bundle, a version or version range can be specified to indicate that a particular
version of the bundle is required.

Enabling a Self-Test in a Bundle
=================================
To enable support for automatic testing by the regression suite and the smoketest process, follow these steps:

1. Create a top-level "ecl" folder in the bundle.
2. Add one or more ECL attributes to execute
3. Create a top level "key" folder.
4. Add ECL file(s) to the ecl folder and corresponding XML files to the key folder. Each XML file should contain the expected result of the corresponding ECL file's output.

For example, if the "ecl" folder has MyTest1.ecl and MyTest2.ecl attribute definitions, the "key" folder should have MyTest1.xml and MyTest2.xml files.
The XML files contain the result dataset(s) from running the same named attribute.
The test is declared successful if the contents of the XML file exactly match the contents of the workunit output(s).

**To generate a key file, use this command:**

   ``ecl run --target <target> <path_to_your_ecl_file>``


then remove the **<Result>** and **</Result>** tags and save as XML.

Note: Adding a self-test does not automatically ensure testing by the regression suite or smoketest. 

Running a Self-Test
===================
1. Install and start an HPCC platofrm or VM image installed then hpcc started
2. Install the bundle installed with the ecl command line tool. 

3. Run the Regression Test Engine from the bundle main directory like this:

  ``<TEST_ENGINE_HOME>/ecl-test run -t <TARGET_PLATFORM>``

where

TEST_ENGINE_HOME: /testing/regress

TARGET_PLATFORM: hthor | thor | roxie | all

For example:

from the ML bundle directory: 

``/mnt/disk1/home/hpccdemo/.HPCCSystems/bundles/_versions/PBblas/V3_0_2/PBblas``

use
  ``/mnt/disk1/home/hpccdemo/build/CE/platform/HPCCPlatform/testing/regress/ecl-test run -t thor``

Installing a bundle
===================
To install a bundle to your development machine, use the ecl command line tool: 

   ``ecl bundle install <bundlefile>.ecl`` 

For complete details, see the Client Tools Manual, available in the download section of hpccsystems.com .
