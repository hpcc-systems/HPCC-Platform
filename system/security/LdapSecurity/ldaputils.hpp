/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifndef __LDAPUTILS_HPP
#define __LDAPUTILS_HPP

#include "ldapconnection.hpp"

#ifdef _WIN32
#include <windows.h>
#include <winldap.h>
#include <winber.h>
#include <rpc.h>
#include <rpcdce.h>
#include "dsgetdc.h"
#include <lm.h>
#else
#define LDAP_DEPRECATED 1
#include <ldap_cdefs.h>
#include <ldap.h>
#endif

class LdapUtils
{
public:
    static LDAP* LdapInit(const char* protocol, const char* host, int port, int secure_port, bool throwOnError = true);
    static int LdapSimpleBind(LDAP* ld, int ldapTimeout, char* userdn, char* password);
    // userdn is required for ldap_simple_bind_s, not really necessary for ldap_bind_s.
    static int LdapBind(LDAP* ld, int ldapTimeout, const char* domain, const char* username, const char* password, const char* userdn, LdapServerType server_type, const char* method="");
    static void bin2str(MemoryBuffer& from, StringBuffer& to);
    static LDAP* ldapInitAndSimpleBind(const char* ldapserver, const char* userDN, const char* pwd, const char* ldapprotocol, int ldapport, int timeout, int * err);
    static int getServerInfo(const char* ldapserver, const char * user, const char *pwd, const char* ldapprotocol, int ldapport, StringBuffer& domainDN, LdapServerType& stype, const char* domainname, int timeout);
    static void normalizeDn(const char* dn, const char* basedn, StringBuffer& dnbuf);
    static bool containsBasedn(const char* str);
    static void cleanupDn(const char* dn, StringBuffer& dnbuf);
    static bool getDcName(const char* domain, StringBuffer& dc);
    static void getName(const char* dn, StringBuffer& name);
};

#endif
