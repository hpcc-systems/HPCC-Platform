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
    static LDAP* LdapInit(const char* protocol, const char* host, int port, int secure_port, const char * cipherSuite, const char* tlsValidation, const char* caCertFile, bool throwOnError = true);
    static int LdapSimpleBind(LDAP* ld, int ldapTimeout, char* userdn, char* password);
    // userdn is required for ldap_simple_bind_s, not really necessary for ldap_bind_s.
    static int LdapBind(LDAP* ld, int ldapTimeout, const char* domain, const char* username, const char* password, const char* userdn, LdapServerType server_type, const char* method="");
#ifndef _WIN32
    // Simple bind that also surfaces 389ds's password-expiration bind response controls, which the
    // synchronous ldap_bind_s()/ldap_simple_bind_s() calls cannot see (they silently drop all response
    // controls). This uses the async ldap_sasl_bind()+ldap_result()+ldap_parse_result() sequence instead,
    // blocking on ldap_result() for the response - behaviorally equivalent to a normal blocking bind call.
    //
    // Note: 389ds does NOT implement the standard RFC draft-behera-ldap-password-policy control
    // (OID 1.3.6.1.4.1.42.2.27.8.5.1). It instead sends its own legacy Netscape-heritage controls
    // unconditionally (no request control needed):
    //  - 2.16.840.1.113730.3.4.4 "PasswordExpired" - present on bind failure when the password has expired
    //  - 2.16.840.1.113730.3.4.5 "PasswordExpiring" - present on bind success within the warning window;
    //    value is the ASCII decimal string of seconds until expiration (not BER-encoded)
    // (Verified against a live 389ds/dirsrv:latest container - confirmed via ldapwhoami -e ppolicy.)
    //
    // Returns the LDAP bind result code, exactly as LdapSimpleBind() would. On return:
    //  - passwordExpired is true if the "PasswordExpired" control was present (bind will have failed)
    //  - secondsToExpiry is seconds until expiration from the "PasswordExpiring" control, or -1 if absent
    // If the initial bind fails and userdn contains a comma, retries once with the DC components
    // stripped from userdn (389ds/OpenLDAP is happier without the domain component specified),
    // mirroring the equivalent fallback in LdapBind().
    static int LdapSimpleBindWithExpirationStatus(LDAP* ld, int ldapTimeout, const char* userdn, const char* password,
        bool& passwordExpired, int& secondsToExpiry);
    // Binds using LdapSimpleBindWithExpirationStatus() for LDAP_389DS (to detect password expiration
    // via response controls), or falls back to the ordinary LdapBind() for all other server types.
    // Consolidates the server-type dispatch otherwise repeated at every 389ds-expiry-aware bind call site.
    // passwordExpired/secondsToExpiry are always reset by this call; they remain false/-1 for non-389DS binds.
    static int LdapBindDetectExpiry(LDAP* ld, int ldapTimeout, const char* domain, const char* username,
        const char* password, const char* userdn, LdapServerType server_type, const char* method,
        bool& passwordExpired, int& secondsToExpiry);
#endif
    static void bin2str(MemoryBuffer& from, StringBuffer& to);
    static LDAP* ldapInitAndSimpleBind(const char* ldapserver, const char* userDN, const char* pwd, const char* ldapprotocol, int ldapport, const char * cipherSuite, int timeout, int * err, const char* tlsValidation, const char* caCertFile);
    static int getServerInfo(const char* ldapserver, const char * user, const char *pwd, const char* ldapprotocol, int ldapport, const char * cipherSuite, StringBuffer& domainDN, LdapServerType& stype, const char* domainname, int timeout, const char* tlsValidation, const char* caCertFile);
    static void normalizeDn(const char* dn, const char* basedn, StringBuffer& dnbuf);
    static bool containsBasedn(const char* str);
    static void cleanupDn(const char* dn, StringBuffer& dnbuf);
    static bool getDcName(const char* domain, StringBuffer& dc);
    static void getName(const char* dn, StringBuffer& name);
};

// Returns true if any TLS certificate validation is active (strict or permissive).
// Null or empty tlsValidation is treated as "disabled".
inline bool isTLSValidationActive(const char* tlsValidation)
{
    return !isEmptyString(tlsValidation) && (strieq(tlsValidation, "strict") || strieq(tlsValidation, "permissive"));
}

inline bool isTLSValidationStrict(const char* tlsValidation)
{
    return !isEmptyString(tlsValidation) && strieq(tlsValidation, "strict");
}

#endif
