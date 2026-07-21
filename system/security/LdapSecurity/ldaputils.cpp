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

// LDAP prototypes use char* where they should be using const char *, resulting in lots of spurious warnings
#include "jstring.hpp"
#pragma warning( disable : 4786 )
#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wwrite-strings"
#endif

#include "ldaputils.hpp"

#ifndef _WIN32
# include <signal.h>
# ifdef _USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/x509.h>
# endif
#endif

//------------------------------------
// LdapUtils implementation
//------------------------------------
LDAP* LdapUtils::LdapInit(const char* protocol, const char* host, int port, int secure_port, const char * cipherSuite, const char* tlsValidation, const char* caCertFile, bool throwOnError)
{
    LDAP* ld = NULL;
    if(stricmp(protocol, "ldaps") == 0)
    {
#ifdef _WIN32
        ld = ldap_sslinit((char*)host, secure_port, 1);
        if (ld == NULL )
            throw MakeStringException(-1, "ldap_sslinit error" );

        int rc = 0;
        unsigned long version = LDAP_VERSION3;
        long lv = 0;

        rc = ldap_set_option(ld,
            LDAP_OPT_PROTOCOL_VERSION,
            (void*)&version);
        if (rc != LDAP_SUCCESS)
            throw MakeStringException(-1, "ldap_set_option error - %s", ldap_err2string(rc));

        rc = ldap_get_option(ld,LDAP_OPT_SSL,(void*)&lv);
        if (rc != LDAP_SUCCESS)
            throw MakeStringException(-1, "ldap_get_option error - %s", ldap_err2string(rc));

        // If SSL is not enabled, enable it.
        if ((void*)lv != LDAP_OPT_ON)
        {
            rc = ldap_set_option(ld, LDAP_OPT_SSL, LDAP_OPT_ON);
            if (rc != LDAP_SUCCESS)
                throw MakeStringException(-1, "ldap_set_option error - %s", ldap_err2string(rc));
        }

        ldap_set_option(ld, LDAP_OPT_SERVER_CERTIFICATE, verifyServerCert);

        // TLS certificate validation is not supported on Windows. The parameters are accepted
        // in config for cross-platform compatibility but have no effect. Warn if they were set
        // so the administrator knows the configuration is not being honoured.
        if (isTLSValidationActive(tlsValidation))
            WARNLOG("LdapUtils::LdapInit : ldapTLSValidation='%s' is not supported on Windows and will be ignored - certificate validation is disabled", tlsValidation);
        if (!isEmptyString(caCertFile))
            WARNLOG("LdapUtils::LdapInit : ldapCACertFile='%s' is not supported on Windows and will be ignored", caCertFile);
        if (!isEmptyString(cipherSuite))
            WARNLOG("LdapUtils::LdapInit : ldapCipherSuite='%s' is not supported on Windows and will be ignored", cipherSuite);
#else
        // Initialize an LDAP session for TLS/SSL
#ifndef HAVE_TLS
        //throw MakeStringException(-1, "openldap client library libldap not compiled with TLS support");
#endif
        int rc;
        rc = ldap_set_option(nullptr, LDAP_OPT_X_TLS_CIPHER_SUITE, isEmptyString(cipherSuite) ? nullptr : cipherSuite);
        if(rc != LDAP_SUCCESS)
            ERRLOG("LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_TLS_CIPHER_SUITE) error - %s", ldap_err2string(rc));

        bool isStrict     = isTLSValidationStrict(tlsValidation);
        bool isPermissive = !isStrict && isTLSValidationActive(tlsValidation);
        if (!isStrict && !isPermissive && !isEmptyString(tlsValidation) && !strieq(tlsValidation, "disabled"))
            WARNLOG("LdapUtils::LdapInit : unrecognised ldapTLSValidation value '%s', defaulting to disabled", tlsValidation);

        if (isStrict || isPermissive)
        {
            // Set cert verification requirement globally so it applies as a default
            // for any connections not going through the per-handle NEWCTX path below.
            int reqcert = LDAP_OPT_X_TLS_DEMAND;
            rc = ldap_set_option(nullptr, LDAP_OPT_X_TLS_REQUIRE_CERT, &reqcert);
            if (rc != LDAP_SUCCESS)
            {
                if (isStrict && throwOnError)
                    throw MakeStringException(-1, "LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_TLS_REQUIRE_CERT) error - %s", ldap_err2string(rc));
                else
                    WARNLOG("LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_TLS_REQUIRE_CERT) error - %s", ldap_err2string(rc));
            }
        }
        else
        {
            int reqcert = LDAP_OPT_X_TLS_NEVER;
            rc = ldap_set_option(nullptr, LDAP_OPT_X_TLS_REQUIRE_CERT, &reqcert);
            if (rc != LDAP_SUCCESS)
                ERRLOG("LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_TLS_REQUIRE_CERT) error - %s", ldap_err2string(rc));
        }
        StringBuffer uri("ldaps://");
        uri.appendf("%s:%d", host, secure_port);

        StringBuffer tlsInfo;
        tlsInfo.appendf("TLS validation: '%s'", isEmptyString(tlsValidation) ? "disabled" : tlsValidation);
        if (!isEmptyString(caCertFile))
            tlsInfo.appendf(", CA cert: '%s'", caCertFile);
        if (!isEmptyString(cipherSuite))
            tlsInfo.appendf(", cipher suite: '%s'", cipherSuite);
        PROGLOG("Connecting to LDAPS Host '%s' (%s)", uri.str(), tlsInfo.str());

        rc = LDAP_INIT(&ld, uri.str());
        if(rc != LDAP_SUCCESS)
        {
            if (throwOnError)
                throw MakeStringException(-1, "ldap_initialize error %s", ldap_err2string(rc));
            DBGLOG("ldap_initialize error %s", ldap_err2string(rc));
            return nullptr;
        }
        if (isStrict || isPermissive)
        {
            // Build a per-handle TLS context so each pooled connection has its own isolated
            // SSL_CTX (thread-safe). REQUIRE_CERT is inherited from the global setting above;
            // do NOT set it per-handle before NEWCTX as that alters OpenLDAP's hostname
            // verification code path. The CA cert is loaded into the SSL_CTX below rather than
            // via LDAP_OPT_X_TLS_CACERTFILE because per-handle NEWCTX does not automatically
            // inherit the global CACERTFILE setting.
            int newctx = 0;
            rc = ldap_set_option(ld, LDAP_OPT_X_TLS_NEWCTX, &newctx);
            if (rc != LDAP_SUCCESS)
            {
                if (isStrict && throwOnError)
                {
                    LDAP_UNBIND(ld);
                    throw MakeStringException(-1, "LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_TLS_NEWCTX) error - %s", ldap_err2string(rc));
                }
                else
                    WARNLOG("LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_TLS_NEWCTX) error - %s", ldap_err2string(rc));
            }
            if (!isEmptyString(caCertFile))
            {
#ifdef _USE_OPENSSL
                // Per-handle NEWCTX does not inherit global LDAP_OPT_X_TLS_CACERTFILE, so
                // load the CA cert directly into the per-handle SSL_CTX via OpenSSL.
                SSL_CTX * sslCtx = nullptr;
                ldap_get_option(ld, LDAP_OPT_X_TLS_CTX, &sslCtx);
                if (sslCtx)
                {
                    if (SSL_CTX_load_verify_locations(sslCtx, caCertFile, nullptr) != 1)
                    {
                        if (isStrict && throwOnError)
                        {
                            LDAP_UNBIND(ld);
                            throw MakeStringException(-1, "LdapUtils::LdapInit : SSL_CTX_load_verify_locations failed for '%s' - check the file is readable and is a valid PEM CA certificate", caCertFile);
                        }
                        else
                            WARNLOG("LdapUtils::LdapInit : SSL_CTX_load_verify_locations failed for '%s' - check the file is readable and is a valid PEM CA certificate", caCertFile);
                    }
                    else
                    {
                        PROGLOG("LdapUtils::LdapInit : CA cert '%s' loaded successfully for %s", caCertFile, uri.str());
                    }
                }
                else
                {
                    if (isStrict && throwOnError)
                    {
                        LDAP_UNBIND(ld);
                        throw MakeStringException(-1, "LdapUtils::LdapInit : failed to retrieve SSL_CTX from LDAP handle for %s", uri.str());
                    }
                    else
                        WARNLOG("LdapUtils::LdapInit : failed to retrieve SSL_CTX from LDAP handle for %s", uri.str());
                }
#else
                if (isStrict && throwOnError)
                {
                    LDAP_UNBIND(ld);
                    throw MakeStringException(-1, "LdapUtils::LdapInit : CA cert '%s' specified but OpenSSL support is not compiled in - cannot perform cert validation", caCertFile);
                }
                else
                    WARNLOG("LdapUtils::LdapInit : CA cert '%s' specified but OpenSSL support is not compiled in - cert validation skipped", caCertFile);
#endif
            }
        }
#endif
    }
    else
    {
        // Initialize an LDAP session
#ifdef _WIN32
        ld = LDAP_INIT(host, port);
        if(NULL == ld)
        {
            throw MakeStringException(-1, "ldap_init(%s,%d) error %s", host, port, ldap_err2string(LdapGetLastError()));
        }
#else
        StringBuffer uri("ldap://");
        uri.appendf("%s:%d", host, port);
        int rc = LDAP_INIT(&ld, uri.str());
        if(rc != LDAP_SUCCESS)
        {
            if (throwOnError)
                throw MakeStringException(-1, "ldap_initialize(%s,%d) error %s", host, port, ldap_err2string(rc));
            DBGLOG("ldap_initialize error %s", ldap_err2string(rc));
            return nullptr;
        }
#endif

    }

    //Set TLS KeepAlive options
    int kaTime;
    int kaInterval;
    int kaProbes;
    if (queryKeepAlive(kaTime, kaInterval, kaProbes))//query OpenLDAP per connection tcp-keepalive settings
    {
        StringBuffer kaLog;
        int rc;
        if (kaTime != -1)
        {
            kaLog.appendf(" Time: %d", kaTime);
            rc = ldap_set_option(nullptr, LDAP_OPT_X_KEEPALIVE_IDLE, &kaTime);
            if(rc != LDAP_SUCCESS)
                ERRLOG("LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_KEEPALIVE_IDLE, %d) error - %s", kaTime, ldap_err2string(rc));
        }

        if (kaInterval != -1)
        {
            kaLog.appendf(" Interval: %d", kaInterval);
            rc = ldap_set_option(nullptr, LDAP_OPT_X_KEEPALIVE_INTERVAL, &kaInterval);
            if(rc != LDAP_SUCCESS)
                ERRLOG("LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_KEEPALIVE_INTERVAL, %d) error - %s", kaInterval, ldap_err2string(rc));
        }

        if (kaProbes != -1)
        {
            kaLog.appendf(" Probes: %d", kaProbes);
            rc = ldap_set_option(nullptr, LDAP_OPT_X_KEEPALIVE_PROBES, &kaProbes);
            if(rc != LDAP_SUCCESS)
                ERRLOG("LdapUtils::LdapInit : ldap_set_option(LDAP_OPT_X_KEEPALIVE_PROBES, %d) error - %s", kaProbes, ldap_err2string(rc));
        }
    }

    return ld;
}

int LdapUtils::LdapSimpleBind(LDAP* ld, int ldapTimeout, char* userdn, char* password)
{
#ifndef _WIN32
    TIMEVAL timeout = {ldapTimeout, 0};
    ldap_set_option(ld, LDAP_OPT_TIMEOUT, &timeout);
    ldap_set_option(ld, LDAP_OPT_NETWORK_TIMEOUT, &timeout);
#endif
    int srtn = ldap_bind_s(ld, userdn, password, LDAP_AUTH_SIMPLE);
#ifndef _WIN32
    // secure ldap tls might overwrite SIGPIPE handler
    signal(SIGPIPE, SIG_IGN);
#endif
    return srtn;
}

// userdn is required for ldap_simple_bind_s, not really necessary for ldap_bind_s.
int LdapUtils::LdapBind(LDAP* ld, int ldapTimeout, const char* domain, const char* username, const char* password, const char* userdn, LdapServerType server_type, const char* method)
{
    bool binddone = false;
    int rc = LDAP_SUCCESS;
    if(isEmptyString(method))
    {
#ifdef _WIN32
        if(server_type == ACTIVE_DIRECTORY)
        {
            if(username != NULL)
            {
                SEC_WINNT_AUTH_IDENTITY secIdent;
                secIdent.User = (unsigned char*)username;
                secIdent.UserLength = strlen(username);
                secIdent.Password = (unsigned char*)password;
                secIdent.PasswordLength = strlen(password);
                // Somehow, setting the domain makes it slower
                secIdent.Domain = (unsigned char*)domain;
                secIdent.DomainLength = strlen(domain);
                secIdent.Flags = SEC_WINNT_AUTH_IDENTITY_ANSI;
                int rc = ldap_bind_s(ld, (char*)userdn, (char*)&secIdent, LDAP_AUTH_NEGOTIATE);
                if(rc != LDAP_SUCCESS)
                {
                    DBGLOG("ldap_bind_s for user %s failed with %d - %s.", username, rc, ldap_err2string(rc));
                    return rc;
                }
            }
            else
            {
                int rc = ldap_bind_s(ld, NULL, NULL, LDAP_AUTH_NEGOTIATE);
                if(rc != LDAP_SUCCESS)
                {
                    DBGLOG("User Authentication Failed - ldap_bind_s for current user failed with %d - %s.", rc, ldap_err2string(rc));
                    return rc;
                }
            }
            binddone = true;
        }
#endif
    }

    if(!binddone)
    {
        if(userdn == NULL)
            return LDAP_INVALID_CREDENTIALS;

        int rc = LdapSimpleBind(ld, ldapTimeout, (char*)userdn, (char*)password);
        if (rc != LDAP_SUCCESS && (server_type == OPEN_LDAP || server_type == LDAP_389DS) && strchr(userdn,','))
        {   //OpenLDAP and 389ds/Fedora389 are happier without the domain component specified
            StringBuffer cn(userdn);
            cn.toLowerCase();
            const char * pDC = strstr(cn.str(), ",dc=");
            if (pDC)
            {
                cn.setLength(pDC - cn.str());//chop off DC components
                if (cn.length())//disallow call if no cn
                    rc = LdapSimpleBind(ld, ldapTimeout, (char*)cn.str(), (char*)password);
            }
        }
        if (rc != LDAP_SUCCESS )
        {
            // For Active Directory, try binding with NT format username
            if(server_type == ACTIVE_DIRECTORY)
            {
                StringBuffer logonname;
                logonname.append(domain).append("\\").append(username);
                rc = LdapSimpleBind(ld, ldapTimeout, (char*)logonname.str(), (char*)password);
                if(rc != LDAP_SUCCESS)
                {
#ifdef LDAP_OPT_DIAGNOSTIC_MESSAGE
                    char *msg=NULL;
                    ldap_get_option(ld, LDAP_OPT_DIAGNOSTIC_MESSAGE, (void*)&msg);
                    DBGLOG("LDAP bind error for user %s with %d - %s. %s", logonname.str(), rc, ldap_err2string(rc), msg&&*msg?msg:"");
                    ldap_memfree(msg);
#else
                    DBGLOG("LDAP bind error for user %s with 0x%" I64F "x - %s", username, (unsigned __int64) rc, ldap_err2string(rc));
#endif
                    return rc;
                }
            }
            else
            {
                DBGLOG("LDAP bind error for user %s with 0x%" I64F "x - %s", username, (unsigned __int64) rc, ldap_err2string(rc));
                return rc;
            }
        }
    }

    return rc;
}

LDAP* LdapUtils::ldapInitAndSimpleBind(const char* ldapserver, const char* userDN, const char* pwd, const char* ldapprotocol, int ldapport, const char * cipherSuite, int timeout, int * err, const char* tlsValidation, const char* caCertFile)
{
    LDAP* ld = LdapInit(ldapprotocol, ldapserver, ldapport, ldapport, cipherSuite, tlsValidation, caCertFile, isTLSValidationStrict(tlsValidation));
    if (ld == nullptr)
    {
        VStringBuffer uri("%s://%s:%d", ldapprotocol, ldapserver, ldapport);
        bool isPermissive = !isTLSValidationStrict(tlsValidation) && isTLSValidationActive(tlsValidation);
        // In strict mode LdapInit throws rather than returning nullptr, so nullptr here
        // with permissive validation is most likely a TLS soft-failure (e.g. cert error
        // that was tolerated). Downgrade to WARNLOG in that case.
        if (isPermissive)
            WARNLOG("LDAP init error for %s", uri.str());
        else
            ERRLOG("LDAP init error for %s", uri.str());
        *err = -1;
        return nullptr;
    }
    *err = LdapSimpleBind(ld, timeout, (char*)userDN, (char*)pwd);
    if (*err != LDAP_SUCCESS)
    {
#ifndef _WIN32
        char* diagMsg = nullptr;
        ldap_get_option(ld, LDAP_OPT_DIAGNOSTIC_MESSAGE, (void*)&diagMsg);
        if (!isEmptyString(diagMsg))
            WARNLOG("LDAP bind diagnostic for %s://%s:%d : %s", ldapprotocol, ldapserver, ldapport, diagMsg);
        ldap_memfree(diagMsg);
#endif
        LDAP_UNBIND(ld);
        return nullptr;
    }
    return ld;
}

int LdapUtils::getServerInfo(const char* ldapserver, const char* userDN, const char* pwd, const char* ldapprotocol, int ldapport, const char * cipherSuite, StringBuffer& domainDN, LdapServerType& stype, const char* domainname, int timeout, const char* tlsValidation, const char* caCertFile)
{
    LdapServerType deducedSType = LDAPSERVER_UNKNOWN;

    //First try anonymous bind using selected protocol/port
    int err = -1;
    LDAP* ld = ldapInitAndSimpleBind(ldapserver, nullptr, nullptr, ldapprotocol, ldapport, cipherSuite, timeout, &err, tlsValidation, caCertFile);

    //if that failed, try bind with credentials
    if (nullptr == ld)
    {
        ld = ldapInitAndSimpleBind(ldapserver, userDN, pwd, ldapprotocol, ldapport, cipherSuite, timeout, &err, tlsValidation, caCertFile);

        if (ld == nullptr)
        {
            if (err == LDAP_PROTOCOL_ERROR && stype != ACTIVE_DIRECTORY)
            {
                WARNLOG("Unable to connect to %s://%s:%d (%s). If you're trying to connect to an OpenLdap server, make sure you have \"allow bind_v2\" enabled in slapd.conf",
                    ldapprotocol, ldapserver, ldapport, ldap_err2string(err));
            }
            else
            {
                StringBuffer tlsMsg;
                bool isTls = strieq(ldapprotocol, "ldaps");
                bool hasTlsValidation = !isEmptyString(tlsValidation) && !strieq(tlsValidation, "disabled");
                if (isTls)
                {
                    if (err == LDAP_CONNECT_ERROR && hasTlsValidation)
                    {
                        // LDAP_CONNECT_ERROR on an ldaps connection with validation enabled
                        // typically means the TLS handshake failed - likely a certificate problem.
                        tlsMsg.appendf(" TLS certificate validation (ldapTLSValidation='%s') is enabled.", tlsValidation);
                        if (!isEmptyString(caCertFile))
                            tlsMsg.appendf(" Check that ldapCACertFile='%s' is readable and contains the correct CA certificate.", caCertFile);
                        else
                            tlsMsg.append(" No ldapCACertFile is configured - the server certificate must chain to a trusted CA in the system trust store. If the system trust store is not accessible to the hpcc user, set ldapCACertFile to the path of the CA certificate.");
                    }
                    else if (err == LDAP_SERVER_DOWN)
                    {
                        tlsMsg.append(" The server may have rejected the TLS handshake.");
                        if (hasTlsValidation)
                            tlsMsg.appendf(" TLS validation mode is '%s'.", tlsValidation);
                        if (!isEmptyString(cipherSuite))
                            tlsMsg.appendf(" Cipher suite configured: '%s'.", cipherSuite);
                        else
                            tlsMsg.append(" You may need to specify a cipher suite using the 'ldapCipherSuite' attribute in the LDAP configuration.");
                    }
                    else if (!isEmptyString(cipherSuite))
                    {
                        tlsMsg.appendf(" You may need to specify a different cipher suite (current: '%s').", cipherSuite);
                    }
                    else
                    {
                        tlsMsg.append(" You may need to specify a cipher suite using the 'ldapCipherSuite' attribute in the LDAP configuration.");
                    }
                }
                WARNLOG("Unable to connect to %s://%s:%d - %s.%s",
                    ldapprotocol, ldapserver, ldapport, ldap_err2string(err), tlsMsg.str());
            }
            return err;//unable to connect, give up
        }
    }

    LDAPMessage* msg = NULL;
    char* attrs[] = {"namingContexts", NULL};
    TIMEVAL timeOut = {LDAPTIMEOUT,0};
    err = ldap_search_ext_s(ld, NULL, LDAP_SCOPE_BASE, "objectClass=*", attrs, false, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &msg);
    if(err != LDAP_SUCCESS)
    {
        WARNLOG("ldap_search_ext_s error: %s", ldap_err2string( err ));
        if (msg)
            ldap_msgfree(msg);
        return err;
    }
    LDAPMessage* entry = ldap_first_entry(ld, msg);
    if(entry != NULL)
    {
        CLDAPGetValuesLenWrapper vals(ld, entry, "namingContexts");
        if(vals.hasValues())
        {
            int i = 0;
            const char* curdn;
            StringBuffer onedn;
            while((curdn = vals.queryCharValue(i)) != NULL)
            {
                if(*curdn != '\0' && (strncmp(curdn, "dc=", 3) == 0 || strncmp(curdn, "DC=", 3) == 0) && strstr(curdn,"DC=ForestDnsZones")==0 && strstr(curdn,"DC=DomainDnsZones")==0 )
                {
                    if(domainDN.length() == 0)
                    {
                        StringBuffer curdomain;
                        getName(curdn, curdomain);
                        if(onedn.length() == 0)
                        {
                            DBGLOG("Queried '%s', selected basedn '%s'",curdn, curdomain.str());
                            onedn.append(curdomain.str());
                        }
                        else
                            DBGLOG("Ignoring %s", curdn);
                        if(!domainname || !*domainname || stricmp(curdomain.str(), domainname) == 0)
                            domainDN.append(curdn);
                    }
                }
                else if(*curdn != '\0' && strcmp(curdn, "o=NetscapeRoot") == 0)
                    deducedSType = IPLANET;
                i++;
            }

            if(domainDN.length() == 0)
                domainDN.append(onedn.str());

            if (deducedSType == LDAPSERVER_UNKNOWN)
            {
                if(i <= 1)
                    deducedSType = OPEN_LDAP;
                else
                    deducedSType = ACTIVE_DIRECTORY;
            }
        }
    }
    ldap_msgfree(msg);
    LDAP_UNBIND(ld);

    if (stype == LDAPSERVER_UNKNOWN || deducedSType != stype)
    {
        if (deducedSType == ACTIVE_DIRECTORY)
            PROGLOG("Deduced LDAP Server Type 'Active Directory'");
        else if (deducedSType == OPEN_LDAP)
            PROGLOG("Deduced LDAP Server Type 'OpenLDAP'");
        else if (deducedSType == IPLANET)
            PROGLOG("Deduced LDAP Server Type 'iPlanet'");

        if (stype == LDAPSERVER_UNKNOWN)
            stype = deducedSType;
        else
            WARNLOG("Ignoring deduced LDAP Server Type, does not match config");
    }
    return err;
}

void LdapUtils::bin2str(MemoryBuffer& from, StringBuffer& to)
{
    const char* frombuf = from.toByteArray();
    for(unsigned i = 0; i < from.length(); i++)
    {
        unsigned char c = frombuf[i];
        to.append("\\").appendhex(c, false);
    }
}

void LdapUtils::normalizeDn(const char* dn, const char* basedn, StringBuffer& dnbuf)
{
    dnbuf.clear();
    cleanupDn(dn, dnbuf);
    if(!containsBasedn(dnbuf.str()))
        dnbuf.append(",").append(basedn);
}

bool LdapUtils::containsBasedn(const char* str)
{
    if(str == NULL || str[0] == '\0')
        return false;
    else
        return (strstr(str, "dc=") != NULL);
}

void LdapUtils::cleanupDn(const char* dn, StringBuffer& dnbuf)
{
    if(dn == NULL || dn[0] == '\0')
        return;
    dnbuf.append(dn);
    dnbuf.toLowerCase();
}

bool LdapUtils::getDcName(const char* domain, StringBuffer& dc)
{
    bool ret = false;
#ifdef _WIN32
    PDOMAIN_CONTROLLER_INFO psInfo = NULL;
    DWORD dwErr = DsGetDcName(NULL, domain, NULL, NULL, DS_FORCE_REDISCOVERY | DS_DIRECTORY_SERVICE_REQUIRED, &psInfo);
    if( dwErr == NO_ERROR)
    {
        const char* dcname = psInfo->DomainControllerName;
        if(dcname != NULL)
        {
            while(*dcname == '\\')
                dcname++;

            dc.append(dcname);
            ret = true;
        }
        NetApiBufferFree(psInfo);
    }
    else
    {
        DBGLOG("Error getting domain controller, error = %d", dwErr);
        ret = false;
    }
#endif
    return ret;
}

void LdapUtils::getName(const char* dn, StringBuffer& name)
{
    const char* bptr = dn;
    while(*bptr != '\0' && *bptr != '=')
        bptr++;

    if(*bptr == '\0')
    {
        name.append(dn);
        return;
    }
    else
        bptr++;

    const char* colon = strstr(bptr, ",");
    if(colon == NULL)
        name.append(bptr);
    else
        name.append(colon - bptr, bptr);
}
