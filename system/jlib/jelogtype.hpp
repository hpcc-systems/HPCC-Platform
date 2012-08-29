/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */


/* This header file should never be #included directly.

   It is included three times:
      once in jlog.hpp (and so almost everywhere) with macros defined so that it declares the values for AuditType,
      once in jlog.cpp with macros defined so that it initiates the array of mappings for translation to win32 event log fields,
      and once in the ECL plugin with macros defined so that it initiates the array of mappings from ECL strings to AuditTypes.

   MAKE_AUDIT_TYPE(name, type, categoryid, eventid, level)

   where:

   AUDIT_TYPE_name is the value that will be declared for use in C++, and "name" is the value that will be used in ECL
      (n.b. names should be declared in upper case; ECL strings will be upcased before comparison and so are insensitive)
   type is the win32 event type, one of the values defined in WINNT.H, namely
      EVENTLOG_ERROR_TYPE, EVENTLOG_WARNING_TYPE, EVENTLOG_INFORMATION_TYPE, EVENTLOG_AUDIT_SUCCESS, EVENTLOG_AUDIT_FAILURE
   categoryid is the custom win32 category ID, see SCMSG_CATEGORY_* in jelog.h
   eventid is the custom win32 event ID, see MSG_JLOG_* in jelog.h
   level is the linux log level, see syslog man page, namely one of
      LOG_EMERG, LOG_ALERT, LOG_CRIT, LOG_ERR, LOG_WARNING, LOG_NOTICE, LOG_INFO, LOG_DEBUG

*/

AUDIT_TYPES_BEGIN
MAKE_AUDIT_TYPE(DEBUG,                  EVENTLOG_INFORMATION_TYPE, SCMSG_CATEGORY_DEBUG, MSG_JLOG_INFORMATIONAL, LOG_DEBUG)
MAKE_AUDIT_TYPE(INFO,                   EVENTLOG_INFORMATION_TYPE, SCMSG_CATEGORY_AUDIT, MSG_JLOG_INFORMATIONAL, LOG_INFO)
MAKE_AUDIT_TYPE(ERROR,                  EVENTLOG_INFORMATION_TYPE, SCMSG_CATEGORY_AUDIT, MSG_JLOG_ERROR, LOG_ERR)
MAKE_AUDIT_TYPE(ACCESS_FAILURE,     EVENTLOG_AUDIT_FAILURE,    SCMSG_CATEGORY_AUDIT, MSG_JLOG_INFORMATIONAL, LOG_NOTICE)
MAKE_AUDIT_TYPE(ACCESS_SUCCESS,     EVENTLOG_AUDIT_SUCCESS,    SCMSG_CATEGORY_AUDIT, MSG_JLOG_INFORMATIONAL, LOG_INFO)
MAKE_AUDIT_TYPE(AUDIT_LOG_FAILURE,  EVENTLOG_AUDIT_FAILURE,    SCMSG_CATEGORY_AUDIT, MSG_JLOG_INFORMATIONAL, LOG_NOTICE)
AUDIT_TYPES_END

/* This function is only required in jlog.cpp, where it is used by AuditLogMsgHandler to translate from a LogMsgCategory to an AuditType. It should be kept updated to reflect the available audit types. */

#ifdef CATEGORY_AUDIT_FUNCTION_REQUIRED

AuditType categoryToAuditType(LogMsgCategory const & category)
{
    switch (category.queryClass())
    {
        case MSGCLS_disaster:
        case MSGCLS_error:
            return AUDIT_TYPE_ERROR;
        default:
            return AUDIT_TYPE_DEBUG;
    }
    return AUDIT_TYPE_DEBUG;
}

#endif //CATEGORY_AUDIT_FUNCTION_REQUIRED
