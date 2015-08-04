/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

/*
 * Internal functions for accessing system information relating to execution on the thor engine.
 *
 * This module is currently treated as internal, and subject to change without notice.
 */
 
externals := 
    SERVICE
unsigned integer4 node() : ctxmethod, entrypoint='getNodeNum';
unsigned integer4 nodes() : ctxmethod, entrypoint='getNodes';
varstring l2p(const varstring name, boolean create=false) : ctxmethod, entrypoint='getFilePart';
unsigned integer getFileOffset(const varstring lfname) : ctxmethod, entrypoint='getFileOffset';
varstring daliServer() : once, ctxmethod, entrypoint='getDaliServers';
varstring cluster() : once, ctxmethod, entrypoint='getClusterName';
varstring getExpandLogicalName(const varstring name) : pure, ctxmethod, entrypoint='getExpandLogicalName';
varstring group() : once, ctxmethod, entrypoint='getGroupName';
varstring platform() : pure ,ctxmethod, entrypoint='getPlatform';
    END;

RETURN MODULE

/*
 * Returns the index of the slave node this piece of code is executing on.  Zero based.
 */
 
export node() := externals.node();

/*
 * Converts a logical filename to a physical filename.
 * 
 * @param name          The logical filename to be converted.
 * @param create        True if creating a new file, false if reading an existing file.
 */
 
export logicalToPhysical(const varstring name, boolean create=false) := externals.l2p(name, create);

/*
 * How many nodes in the cluster that this code will be executed on.
 */
 
export nodes() := CLUSTERSIZE;

/*
 * Returns the dali server this thor is connected to.
 */
 
export daliServer() := externals.daliServer();

/*
 * Returns which thor group the job is currently executing on.
 */
 
export group() := externals.group();

/*
 * Converts a logical filename to a physical filename.
 */

export getExpandLogicalName(const varstring name) := externals.getExpandLogicalName(name);

/*
 * Returns the name of the cluster the query is currently executing on.
 */

export cluster() := externals.cluster();

/*
 * Returns the platform the query is currently executing on.
 */

export platform() := externals.platform();

/*
 * The following are either unused, or should be replaced with a different syntax.
 
export getenv(const varstring name, const varstring defaultValue) := externals.getenv(name, defaultValue);
- use getenv() built in command instead.
export getFileOffset(const varstring lfname) := externals.getFileOffset(lfname);

*/

END;
