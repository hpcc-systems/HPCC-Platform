;/*##############################################################################
;
;    Copyright (C) 2011 HPCC Systems.
;
;    This program is free software: you can redistribute it and/or modify
;    it under the terms of the GNU Affero General Public License as
;    published by the Free Software Foundation, either version 3 of the
;    License, or (at your option) any later version.
;
;    This program is distributed in the hope that it will be useful,
;    but WITHOUT ANY WARRANTY; without even the implied warranty of
;    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;    GNU Affero General Public License for more details.
;
;    You should have received a copy of the GNU Affero General Public License
;    along with this program.  If not, see <http://www.gnu.org/licenses/>.
;############################################################################## */


MessageIdTypedef=DWORD
SeverityNames=(Success=0x0:STATUS_SEVERITY_SUCCESS
               Informational=0x1:STATUS_SEVERITY_INFORMATIONAL
               Warning=0x2:STATUS_SEVERITY_WARNING
               Error=0x3:STATUS_SEVERITY_ERROR
              )

; // Categories 


MessageId=0x1
SymbolicName=SCMSG_CATEGORY_AUDIT
Language=English
Audit
.
MessageId=0x2
SymbolicName= SCMSG_CATEGORY_TRAP
Language=English
Trap
.
MessageId=0x3
SymbolicName= SCMSG_CATEGORY_DEBUG
Language=English
Debug
.
MessageId=0x4
SymbolicName= SCMSG_CATEGORY_INTERNAL
Language=English
Internal
.
MessageId=0x5
SymbolicName= SCMSG_CATEGORY_OPERATOR
Language=English
Operator
.
MessageId=0x6
SymbolicName= SCMSG_CATEGORY_USER
Language=English
User
.
MessageId=0x7
SymbolicName= SCMSG_CATEGORY_7
Language=English
Category #7
.
MessageId=0x8
SymbolicName= SCMSG_CATEGORY_8
Language=English
Category #8
.
MessageId=0x9
SymbolicName= SCMSG_CATEGORY_9
Language=English
Category #9
.
MessageId=0xa
SymbolicName= SCMSG_CATEGORY_10
Language=English
Category #10
.
MessageId=0xb
SymbolicName= SCMSG_CATEGORY_11
Language=English
Category #11
.
MessageId=0xc
SymbolicName= SCMSG_CATEGORY_12
Language=English
Category #12
.
MessageId=0xd
SymbolicName= SCMSG_CATEGORY_13
Language=English
Category #13
.
MessageId=0xe
SymbolicName= SCMSG_CATEGORY_14
Language=English
Category #14
.
MessageId=0xf
SymbolicName= SCMSG_CATEGORY_15
Language=English
Category #15
.

; // Messages

MessageId=0x10
Severity=Success
SymbolicName=MSG_JLOG_SUCCESS
Language=English
%1.
.

MessageId=0x11
Severity=Informational
SymbolicName=MSG_JLOG_INFORMATIONAL
Language=English
%1.
.

MessageId=0x12
Severity=Warning
SymbolicName=MSG_JLOG_WARNING
Language=English
%1.
.

MessageId=0x13
Severity=Error
SymbolicName=MSG_JLOG_ERROR
Language=English
%1.
.

MessageId=0x14
Severity=Informational
SymbolicName=MSG_JLOG_USER
Language=English
%1.
.

MessageId=0x15
Severity=Informational
SymbolicName=MSG_JLOG_MSG_21
Language=English
%1.
.


MessageId=0x16
Severity=Informational
SymbolicName=MSG_JLOG_MSG_22
Language=English
%1.
.


MessageId=0x17
Severity=Informational
SymbolicName=MSG_JLOG_MSG_23
Language=English
%1.
.


MessageId=0x18
Severity=Informational
SymbolicName=MSG_JLOG_MSG_24
Language=English
%1.
.


MessageId=0x19
Severity=Informational
SymbolicName=MSG_JLOG_MSG_25
Language=English
%1.
.


MessageId=0x1a
Severity=Informational
SymbolicName=MSG_JLOG_MSG_26
Language=English
%1.
.


MessageId=0x1b
Severity=Informational
SymbolicName=MSG_JLOG_MSG_27
Language=English
%1.
.


MessageId=0x1c
Severity=Informational
SymbolicName=MSG_JLOG_MSG_28
Language=English
%1.
.


MessageId=0x1d
Severity=Informational
SymbolicName=MSG_JLOG_MSG_29
Language=English
%1.
.


MessageId=0x1e
Severity=Informational
SymbolicName=MSG_JLOG_MSG_30
Language=English
%1.
.


MessageId=0x1f
Severity=Informational
SymbolicName=MSG_JLOG_MSG_31
Language=English
%1.
.




