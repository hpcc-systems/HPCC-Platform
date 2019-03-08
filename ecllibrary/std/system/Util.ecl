/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

import lib_fileservices;

/**
 * Various utility functions for accessing system function.
 */
 
RETURN MODULE

/**
 * Execute an external program.
 *
 * @param prog          The name of the program to excute.  Can include command line options.
 * @param src           The text that is piped into the program as stdin.
 * @returns             The text output from the pipe program.
 */
 
EXPORT string CmdProcess(varstring prog, string src) :=
    lib_fileservices.FileServices.CmdProcess2(prog, src);

/**
 * Returns the host name associated with a particular ip.
 *
 * @param ipaddress     The ip address to resolve.
 * @returns             The host name.
 */

EXPORT varstring GetHostName(varstring ipaddress ) :=
    lib_fileservices.FileServices.GetHostName(ipaddress );

/**
 * Returns the ip address for a host name.
 *
 * @param hostname      The name of the host to resolve.
 * @returns             The associated ip address.
 */

EXPORT varstring ResolveHostName(varstring hostname ) :=
    lib_fileservices.FileServices.ResolveHostName(hostname );

/**
 * Returns a number that is unique for a particular dali.
 *
 * @param foreigndali   The ip address of the dali to provide the unique number.  Defaults to current.
 * @returns             A 64bit integer which is unique (e.g., across all slaves) to the dali that provided it.
 */

EXPORT unsigned8 getUniqueInteger(varstring foreigndali='') :=
    lib_fileservices.FileServices.getUniqueInteger(foreigndali);

/**
 * Simple function that tests a full version string against the individual
 * platform version constants to determine if the platform's version is at
 * least as high as the argument.
 *
 * Note that this function will be evaluated at compile-time if the argument
 * is a constant.  This makes it useful for embedding in #IF() declarations:
 *
 *      #IF(PlatformVersionCheck('6.2.0-1'))
 *          OUTPUT('Platform check TRUE');
 *      #ELSE
 *          OUTPUT('Platform check FALSE');
 *      #END
 *
 * @param   v       The minimum platform version in either xx.xx.xx, xx.xx,
 *                  or xx format (where xx is an integer and does not need
 *                  to be zero-padded); extra trailing characters (such as
 *                  the '-1' in the example above) are ignored; REQUIRED
 *
 * @return  If TRUE, the platform's current version is equal to or higher than
 *          the argument.
 */
EXPORT PlatformVersionCheck(STRING v) := FUNCTION
    major := (INTEGER)REGEXFIND('^(\\d+)', v, 1);
    minor := (INTEGER)REGEXFIND('^\\d+\\.(\\d+)', v, 1);
    subminor := (INTEGER)REGEXFIND('^\\d+\\.\\d+\\.(\\d+)', v, 1);

    RETURN MAP
        (
            __ecl_version_major__ > major                                                                               =>  TRUE,
            __ecl_version_major__ = major AND __ecl_version_minor__ > minor                                             =>  TRUE,
            __ecl_version_major__ = major AND __ecl_version_minor__ = minor AND __ecl_version_subminor__ >= subminor    =>  TRUE,
            FALSE
        );
END;

END;
