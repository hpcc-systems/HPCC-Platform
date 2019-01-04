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
#pragma warning( disable : 4786 )
#ifdef __GNUC__
#pragma GCC diagnostic ignored "-Wwrite-strings"
#endif

#include "ldaputils.hpp"

#ifndef _WIN32
# include <signal.h>
#endif

//------------------------------------
// LdapUtils implementation
//------------------------------------
LDAP* LdapUtils::LdapInit(const char* protocol, const char* host, int port, int secure_port, bool throwOnError)
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
#else
        // Initialize an LDAP session for TLS/SSL
#ifndef HAVE_TLS
        //throw MakeStringException(-1, "openldap client library libldap not compiled with TLS support");
#endif
        StringBuffer uri("ldaps://");
        uri.appendf("%s:%d", host, secure_port);
        DBGLOG("connecting to %s", uri.str());
        int rc = LDAP_INIT(&ld, uri.str());
        if(rc != LDAP_SUCCESS)
        {
            if (throwOnError)
                throw MakeStringException(-1, "ldap_initialize error %s", ldap_err2string(rc));
            OERRLOG("ldap_initialize error %s", ldap_err2string(rc));
            return nullptr;
        }
        int reqcert = LDAP_OPT_X_TLS_NEVER;
        ldap_set_option(NULL, LDAP_OPT_X_TLS_REQUIRE_CERT, &reqcert);
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
        DBGLOG("connecting to %s", uri.str());
        int rc = LDAP_INIT(&ld, uri.str());
        if(rc != LDAP_SUCCESS)
        {
            if (throwOnError)
                throw MakeStringException(-1, "ldap_initialize(%s,%d) error %s", host, port, ldap_err2string(rc));
            OERRLOG("ldap_initialize error %s", ldap_err2string(rc));
            return nullptr;
        }
#endif
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
    // By default, use kerberos authentication
    if((method == NULL) || (strlen(method) == 0) || (stricmp(method, "kerberos") == 0))
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
        {
            OERRLOG("userdn can't be NULL in order to bind to ldap server.");
            return LDAP_INVALID_CREDENTIALS;
        }
        int rc = LdapSimpleBind(ld, ldapTimeout, (char*)userdn, (char*)password);
        if (rc != LDAP_SUCCESS && server_type == OPEN_LDAP && strchr(userdn,','))
        {   //Fedora389 is happier without the domain component specified
            StringBuffer cn(userdn);
            cn.replace(',',(char)NULL);
            if (cn.length())//disallow call if no cn
                rc = LdapSimpleBind(ld, ldapTimeout, (char*)cn.str(), (char*)password);
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
                    OERRLOG("LDAP bind error for user %s with %d - %s. %s", logonname.str(), rc, ldap_err2string(rc), msg&&*msg?msg:"");
                    ldap_memfree(msg);
#else
                    OERRLOG("LDAP bind error for user %s with 0x%" I64F "x - %s", username, (unsigned __int64) rc, ldap_err2string(rc));
#endif
                    return rc;
                }
            }
            else
            {
                OERRLOG("LDAP bind error for user %s with 0x%" I64F "x - %s", username, (unsigned __int64) rc, ldap_err2string(rc));
                return rc;
            }
        }
    }

    return rc;
}

LDAP* LdapUtils::ldapInitAndSimpleBind(const char* ldapserver, const char* userDN, const char* pwd, const char* ldapprotocol, int ldapport, int timeout, int * err)
{
    LDAP* ld = LdapInit(ldapprotocol, ldapserver, ldapport, ldapport, false);
    if (ld == nullptr)
    {
        VStringBuffer uri("%s://%s:%d", ldapprotocol, ldapserver, ldapport);
        OERRLOG("ldap init error(%s)",uri.str());
        *err = -1;
        return nullptr;
    }
    *err = LdapSimpleBind(ld, timeout, (char*)userDN, (char*)pwd);
    if (*err != LDAP_SUCCESS)
    {
        OERRLOG("LdapSimpleBind error (%d) - %s for admin user %s", *err, ldap_err2string(*err), isEmptyString(userDN) ? "NULL" : userDN);
        if (!isEmptyString(userDN))
            OERRLOG("Please make sure your LDAP configuration 'systemBasedn' contains the complete path, including the complete 'dc=domainComponent'");
        return nullptr;
    }
    return ld;
}

int LdapUtils::getServerInfo(const char* ldapserver, const char* userDN, const char* pwd, const char* ldapprotocol, int ldapport, StringBuffer& domainDN, LdapServerType& stype, const char* domainname, int timeout)
{
    LdapServerType deducedSType = LDAPSERVER_UNKNOWN;

    //First try anonymous bind using selected protocol/port
    int err = -1;
    LDAP* ld = ldapInitAndSimpleBind(ldapserver, nullptr, nullptr, ldapprotocol, ldapport, timeout, &err);

    //if that failed, try bind with credentials
    if (nullptr == ld)
    {
        ld = ldapInitAndSimpleBind(ldapserver, userDN, pwd, ldapprotocol, ldapport, timeout, &err);

        //if that failed, and was for ldaps, see if we can do anonymous bind using ldap/389
        if (nullptr == ld  && strieq(ldapprotocol,"ldaps"))
            ld = ldapInitAndSimpleBind(ldapserver, nullptr, nullptr, "ldap", 389, timeout, &err);
    }

    if(nullptr == ld)
    {
        OERRLOG("ldap bind error (%d) - %s", err, ldap_err2string(err));

        // for new versions of openldap, version 2.2.*
        if(err == LDAP_PROTOCOL_ERROR)
            OERRLOG("If you're trying to connect to an OpenLdap server, make sure you have \"allow bind_v2\" enabled in slapd.conf");

        return err;
    }

    LDAPMessage* msg = NULL;
    char* attrs[] = {"namingContexts", NULL};
    TIMEVAL timeOut = {LDAPTIMEOUT,0};
    err = ldap_search_ext_s(ld, NULL, LDAP_SCOPE_BASE, "objectClass=*", attrs, false, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &msg);
    if(err != LDAP_SUCCESS)
    {
        OERRLOG("ldap_search_ext_s error: %s", ldap_err2string( err ));
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
                {
                    PROGLOG("Deduced LDAP Server Type 'iPlanet'");
                    deducedSType = IPLANET;
                }
                i++;
            }

            if(domainDN.length() == 0)
                domainDN.append(onedn.str());

            if (deducedSType == LDAPSERVER_UNKNOWN)
            {
                if(i <= 1)
                {
                    PROGLOG("Deduced LDAP Server Type 'OpenLDAP'");
                    deducedSType = OPEN_LDAP;
                }
                else
                {
                    PROGLOG("Deduced LDAP Server Type 'Active Directory'");
                    deducedSType = ACTIVE_DIRECTORY;
                }
            }
        }
    }
    ldap_msgfree(msg);
    LDAP_UNBIND(ld);

    if (stype == LDAPSERVER_UNKNOWN)
        stype = deducedSType;
    else if (deducedSType != stype)
        WARNLOG("Ignoring deduced LDAP Server Type, does not match config LDAPServerType");

    return err;
}

void LdapUtils::bin2str(MemoryBuffer& from, StringBuffer& to)
{
    const char* frombuf = from.toByteArray();
    char tmp[3];
    for(unsigned i = 0; i < from.length(); i++)
    {
        unsigned char c = frombuf[i];
        sprintf(tmp, "%02X", c);
        tmp[2] = 0;
        to.append("\\").append(tmp);
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
