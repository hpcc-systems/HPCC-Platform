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

Metadata
========
The Bundle.ECL should inherit from the standard library module Std.BundleBase, as in the following
example:

  EXPORT Bundle := MODULE(Std.BundleBase)
    EXPORT Name := 'Bloom';
    EXPORT Description := 'Bloom filter implementation, and example of ECL bundle layout';
    EXPORT Authors := ['Richard Chapman','Charles Kaminsky'];
    EXPORT License := 'http://www.apache.org/licenses/LICENSE-2.0';
    EXPORT Copyright := 'Copyright (C) 2013 HPCC Systems';
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