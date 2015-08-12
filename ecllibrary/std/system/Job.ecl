/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

RETURN MODULE

/*
 * Internal functions for accessing system information relating to the current job.
 *
 * This module is provisional and subject to change without notice.
 */
 
shared externals := 
    SERVICE
varstring daliServer() : once, ctxmethod, entrypoint='getDaliServers';
varstring jobname() : once, ctxmethod, entrypoint='getJobName';
varstring jobowner() : once, ctxmethod, entrypoint='getJobOwner';
varstring cluster() : once, ctxmethod, entrypoint='getClusterName';
varstring platform() : once, ctxmethod, entrypoint='getPlatform';
varstring os() : once, ctxmethod, entrypoint='getOS';
unsigned integer4 logString(const varstring text) : ctxmethod, entrypoint='logString';
    END;

/*
 * How many nodes in the cluster that this code will be executed on.
 */
 
export nodes() := CLUSTERSIZE;

/*
 * Returns the name of the current workunit.
 */
 
export wuid() := WORKUNIT;

/*
 * Returns the dali server this thor is connected to.
 */
 
export daliServer() := externals.daliServer();

/*
 * Returns the name of the current job. 
 */
 
export name() := externals.jobname();

/*
 * Returns the name of the user associated with the current job.
 */
 
export user() := externals.jobowner();

/*
 * Returns the name of the cluster the current job is targetted at.
 */
 
export target() := externals.cluster();

/*
 * Returns the platform type the job is running on.
 */
 
export platform() := externals.platform();

/*
 * Returns a string representing the target operating system.
 */
 
export os() := externals.os();

/*
 * Adds the string argument to the current logging context.
 * 
 * @param text          The string to add to the logging.
 */
 
export logString(const varstring text) := externals.logString(text);

END;
