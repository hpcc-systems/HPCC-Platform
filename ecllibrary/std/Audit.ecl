/*##############################################################################
## HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.  All rights reserved.
############################################################################## */


IMPORT lib_auditlib;

RETURN MODULE

/*
 * Generates an audit entry, and returns true if successful.  The function writes the message into the Windows event
 * log or Linux system log.
 * 
 * @param audit_type    The type of the audit message.
 *                      DEBUG|INFO|ERROR|ACCESS_FAILURE|ACCESS_SUCCESS|AUDIT_LOG_FAILURE
 * @param msg           The string containing the audit entry text.
 * @param data_block    The binary data to associate with the message.
 */
 
EXPORT BOOLEAN Audit(string audit_type, string msg, data data_block = D'') := 
  lib_auditlib.AuditLib.AuditData(audit_type, msg, data_block);

END;
