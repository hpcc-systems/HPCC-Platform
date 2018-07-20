/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.  All rights reserved.
############################################################################## */

EXPORT BundleBase := MODULE,VIRTUAL
  /*
   * Record format for Properties dictionary.
   * @return Record format for Properties dictionary.
   */
  EXPORT PropertyRecord := { UTF8 key => UTF8 value };

  /*
   * Name of this bundle.
   * @return Name
   */
  EXPORT STRING Name := '';

  /*
   * Description of this bundle.
   * @return Description
   */
  EXPORT UTF8 Description := 'ECL Bundle';

  /*
   * List of strings containing author name(s).
   * @return Authors list
   */
  /*
   * List of strings containing author name(s).
   * @return Authors list
   */
  EXPORT SET OF UTF8 Authors := [];

  /*
   * URL or text of licence for this bundle. If not overridden by a bundle, the Apache
   * license is assumed.
   * @return License
   */
  EXPORT UTF8 License := 'http://www.apache.org/licenses/LICENSE-2.0';

  /*
   * Copyright message for this bundle.
   * @return Copyright message
   */
  EXPORT UTF8 Copyright := '';

  /*
   * Dependencies. A set of strings containing names of any bundles that this bundle depends
   * on. One or more versions or version ranges may be specified in after the name, separated
   * by spaces.
   * @return Dependency list
   */
  EXPORT SET OF STRING DependsOn := [];

  /*
   * Version of this bundle. This should be of the form X.Y.Z, where X, Y and Z are integers.
   * @return Version string
   */
  EXPORT STRING Version := '1.0.0';

  /*
   * Additional properties, represented as key-value pairs. Not presently used by the bundle system,
   * @return Properties dictionary
   */
  EXPORT Properties := DICTIONARY([], PropertyRecord);

  /*
   * Required version of platform (optional). Can specify a version or version range. If a single version
   * is specified, it is treated as a minimum version.
   * @return Required platform version
   */
  EXPORT STRING PlatformVersion := '';

END;
