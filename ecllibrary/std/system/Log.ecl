/*##############################################################################
## Copyright (c) 2011 HPCC Systems.  All rights reserved.
############################################################################## */

import lib_logging;

RETURN MODULE

/*
 * Outputs a line of debug logging to the program log file.
 * 
 * @param text          The text to be added to the log file.
 */

EXPORT dbglog(string text) := lib_logging.Logging.dbglog(text);

/*
 * Outputs an information entry to the current workunit.
 * 
 * @param text          The text to be added to the entry.
 * @param code          An optional code to be associated with the entry.
 */

EXPORT addWorkunitInformation(varstring text, unsigned code=0) := lib_logging.Logging.addWorkunitInformation(text, code);

/*
 * Outputs a warning to the current workunit.
 * 
 * @param text          The text of the warning message.
 * @param code          An optional warning code.
 */

EXPORT addWorkunitWarning(varstring text, unsigned code=0) := lib_logging.Logging.addWorkunitWarning(text, code);

/*
 * Outputs an error to the current workunit.
 * 
 * @param text          The text of the error message.
 * @param code          An optional error code.
 */

EXPORT addWorkunitError(varstring text, unsigned code=0) := lib_logging.Logging.addWorkunitError(text, code, 2);

END;