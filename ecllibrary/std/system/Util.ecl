/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

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
 * @param ipaddress     The ip address to resolce.
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

END;
