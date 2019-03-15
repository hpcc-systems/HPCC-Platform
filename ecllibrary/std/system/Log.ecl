/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */

import lib_logging;

RETURN MODULE

/*
 * Outputs a line of debug logging to the program log file.
 * 
 * @param text          The text to be added to the log file.
 *
 * There is no return value - this is an action
 */

EXPORT dbglog(string text) := lib_logging.Logging.dbglog(text);

/*
 * Outputs an information entry to the current workunit.
 * 
 * @param text          The text to be added to the entry.
 * @param code          An optional code to be associated with the entry.
 *
 * There is no return value - this is an action
 */

EXPORT addWorkunitInformation(varstring text, unsigned code=0) := lib_logging.Logging.addWorkunitInformation(text, code);

/*
 * Outputs a warning to the current workunit.
 * 
 * @param text          The text of the warning message.
 * @param code          An optional warning code.
 *
 * There is no return value - this is an action
 */

EXPORT addWorkunitWarning(varstring text, unsigned code=0) := lib_logging.Logging.addWorkunitWarning(text, code);

/*
 * Outputs an error to the current workunit.
 * 
 * @param text          The text of the error message.
 * @param code          An optional error code.
 *
 * There is no return value - this is an action
 */

EXPORT addWorkunitError(varstring text, unsigned code=0) := lib_logging.Logging.addWorkunitError(text, code, 2);

/*
 * Gets the Global Id associated with the current query or workunit.
 *
 * Returns the Global Id
 */

EXPORT getGlobalId() := lib_logging.Logging.getGlobalId();

/*
 * Gets the Caller Id associated with the current query or workunit.
 *
 * Returns the Caller Id
 */

EXPORT getCallerId() := lib_logging.Logging.getCallerId();

/*
 * Gets the Local Id associated with the current query or workunit.
 *
 * Returns the Local Id
 */

EXPORT getLocalId() := lib_logging.Logging.getLocalId();


END;