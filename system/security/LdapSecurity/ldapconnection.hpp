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

#ifndef __LDAPCONNECTION_HPP
#define __LDAPCONNECTION_HPP
#include <stdlib.h>
#include "thirdparty.h"
#include "jiface.hpp"
#include "jliball.hpp"
#include "seclib.hpp"

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
#include <stdio.h>
#include <string.h>
#include <ldap_cdefs.h>
#include <ldap.h>
#endif

#ifdef _WIN32
    typedef struct l_timeval TIMEVAL;
#else
    typedef struct timeval TIMEVAL;
#endif

#define LDAPTIMEOUT 60 //20 second connection/search timeout
#define DEFAULT_LDAP_POOL_SIZE 10

// 1 for ActiveDirectory, 2 for iPlanet, 3 for openLdap
enum LdapServerType
{
    LDAPSERVER_UNKNOWN = 0,
    ACTIVE_DIRECTORY = 1,
    IPLANET = 2,
    OPEN_LDAP = 3
};

enum ACT_TYPE
{
    USER_ACT = 0,
    GROUP_ACT = 1
};
    
interface IPermissionProcessor;

interface ILdapConnection : extends IInterface
{
    virtual LDAP* getLd() = 0;
};

interface ILdapConnectionPool : extends IInterface
{
    virtual ILdapConnection* getConnection() = 0;
    virtual ILdapConnection* getSSLConnection() = 0;
};

interface ILdapConfig : extends IInterface
{
    virtual LdapServerType getServerType() = 0;
    virtual StringBuffer& getLdapHost(StringBuffer& hostbuf) = 0;
    virtual void markDown(const char* ldaphost) = 0;
    virtual int getLdapPort() = 0;
    virtual int getLdapSecurePort() = 0;
    virtual const char* getProtocol() = 0;
    virtual const char* getBasedn() = 0;
    virtual const char* getDomain() = 0;
    virtual const char* getAuthMethod() = 0;
    virtual const char* getUserBasedn() = 0;
    virtual const char* getGroupBasedn() = 0;
    virtual const char* getResourceBasedn(SecResourceType rtype) = 0;
    virtual const char* getTemplateName() = 0;
    virtual const char* getSysUser() = 0;
    virtual const char* getSysUserDn() = 0;
    virtual const char* getSysUserCommonName() = 0;
    virtual const char* getSysUserPassword() = 0;
    virtual const char* getSysUserBasedn() = 0;
    virtual bool sysuserSpecified() = 0;
    virtual int getMaxConnections() = 0;
    virtual void setResourceBasedn(const char* rbasedn, SecResourceType rtype = RT_DEFAULT) = 0;
};


class CPermission : public CInterface, implements IInterface
{
    StringBuffer m_account_name;
    ACT_TYPE     m_account_type;
    int          m_allows;
    int          m_denies;

public:
    IMPLEMENT_IINTERFACE
    
    CPermission(const char* account_name, ACT_TYPE account_type, int allows, int denies)
    {
        m_account_name.append(account_name);
        m_account_type = account_type;
        m_allows = allows;
        m_denies = denies;
    }

    const char* getAccount_name() {return m_account_name.str();}
    ACT_TYPE getAccount_type() {return m_account_type;}
    int getAllows() {return m_allows;}
    int getDenies() {return m_denies;}
    void setAllows(int allows) { m_allows = allows;}
    void setDenies(int denies) { m_denies = denies;}
};

class CPermissionAction : public CInterface, implements IInterface
{
public:
    StringBuffer m_action;

    StringBuffer m_basedn;
    SecResourceType m_rtype;
    StringBuffer m_rname;
    
    StringBuffer m_account_name;
    ACT_TYPE     m_account_type;
    int          m_allows;
    int          m_denies;

    IMPLEMENT_IINTERFACE
};

interface ILdapClient : extends IInterface
{
    virtual void init(IPermissionProcessor* pp) = 0;
    virtual LdapServerType getServerType() = 0;
    virtual bool authenticate(ISecUser& user) = 0;
    virtual bool authorize(SecResourceType rtype, ISecUser&, IArrayOf<ISecResource>& resources) = 0;
    virtual bool addResources(SecResourceType rtype, ISecUser& user, IArrayOf<ISecResource>& resources, SecPermissionType ptype, const char* basedn) = 0;
    virtual bool addUser(ISecUser& user) = 0;
    virtual void getGroups(const char *user, StringArray& groups) = 0;
    virtual bool getUserInfo(ISecUser& user, const char* infotype = NULL) = 0;
    virtual ISecUser* lookupUser(unsigned uid) = 0;
    virtual bool lookupAccount(MemoryBuffer& sidbuf, StringBuffer& account_name, ACT_TYPE& act_type) = 0;
    virtual void lookupSid(const char* act_name, MemoryBuffer& act_sid, ACT_TYPE act_type) = 0;
    virtual void setPermissionProcessor(IPermissionProcessor* pp) = 0;
    virtual bool retrieveUsers(IUserArray& users) = 0;
    virtual bool retrieveUsers(const char* searchstr, IUserArray& users) = 0;
    virtual void getAllGroups(StringArray & groups) = 0;
    virtual void setResourceBasedn(const char* rbasedn, SecResourceType rtype = RT_DEFAULT) = 0;
    virtual ILdapConfig* getLdapConfig() = 0;
    virtual bool userInGroup(const char* userdn, const char* groupdn) = 0;
    virtual bool updateUserPassword(ISecUser& user, const char* newPassword, const char* currPassword = 0) = 0;
    virtual bool updateUser(const char* type, ISecUser& user) = 0;
    virtual bool updateUserPassword(const char* username, const char* newPassword) = 0;
    virtual bool getResources(SecResourceType rtype, const char * basedn, const char* prefix, IArrayOf<ISecResource>& resources) = 0;
    virtual bool getResourcesEx(SecResourceType rtype, const char * basedn, const char* prefix, const char* searchstr, IArrayOf<ISecResource>& resources) = 0;
    virtual bool getPermissionsArray(const char* basedn, SecResourceType rtype, const char* name, IArrayOf<CPermission>& permissions) = 0;
    virtual bool changePermission(CPermissionAction& action) = 0;
    virtual void changeUserGroup(const char* action, const char* username, const char* groupname) = 0;
    virtual bool deleteUser(ISecUser* user) = 0;
    virtual void addGroup(const char* groupname) = 0;
    virtual void deleteGroup(const char* groupname) = 0;
    virtual void getGroupMembers(const char* groupname, StringArray & users) = 0;
    virtual void deleteResource(SecResourceType rtype, const char* name, const char* basedn) = 0;
    virtual void renameResource(SecResourceType rtype, const char* oldname, const char* newname, const char* basedn) = 0;
    virtual void copyResource(SecResourceType rtype, const char* oldname, const char* newname, const char* basedn) = 0;
    virtual void normalizeDn(const char* dn, StringBuffer& ndn) = 0;
    virtual bool isSuperUser(ISecUser* user) = 0;
    virtual int countEntries(const char* basedn, const char* objectClass, int limit) = 0;
    virtual int countUsers(const char* searchstr, int limit) = 0;
    virtual int countResources(const char* basedn, const char* searchstr, int limit) = 0;
    virtual ILdapConfig* queryConfig() = 0;
    virtual const char* getPasswordStorageScheme() = 0;
    virtual bool createUserScope(ISecUser& user) = 0;
    virtual aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes) = 0;
    virtual int queryDefaultPermission(ISecUser& user) = 0;
};

ILdapClient* createLdapClient(IPropertyTree* cfg);

#ifdef _WIN32
bool verifyServerCert(LDAP* ld, PCCERT_CONTEXT pServerCert);
#endif

class LdapUtils
{
public:
    static LDAP* LdapInit(const char* protocol, const char* host, int port, int secure_port)
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
            int rc = ldap_initialize(&ld, uri.str());
            if(rc != LDAP_SUCCESS)
            {
                DBGLOG("ldap_initialize error %s", ldap_err2string(rc));
                throw MakeStringException(-1, "ldap_initialize error %s", ldap_err2string(rc));
            }
            int reqcert = LDAP_OPT_X_TLS_NEVER;
            ldap_set_option(NULL, LDAP_OPT_X_TLS_REQUIRE_CERT, &reqcert);
#endif
        }
        else
        {
            // Initialize an LDAP session
            if ((ld = ldap_init( (char*)host, port )) == NULL)
            {
                throw MakeStringException(-1, "ldap_init error");
            }
        }
        return ld;
    }

    static int LdapSimpleBind(LDAP* ld, char* userdn, char* password)
    {
#ifndef _WIN32
        TIMEVAL timeout = {LDAPTIMEOUT, 0};
        ldap_set_option(ld, LDAP_OPT_TIMEOUT, &timeout);
        ldap_set_option(ld, LDAP_OPT_NETWORK_TIMEOUT, &timeout);
#endif
        return ldap_simple_bind_s(ld, userdn, password);
        /*
        //TODO: bugs need to be fixed: (1) in ldap_result, is "1" actually meant for LDAP_MESSAGE_ONE? (2) should call ldap_msgfree on result
        int final_rc  = LDAP_SUCCESS;
        int msgid = ldap_simple_bind(ld, userdn, password); 
        if(msgid < 0)
        {
#ifndef _WIN32
            final_rc = ldap_get_lderrno(ld, NULL, NULL);
#else
            final_rc = LDAP_OTHER;
#endif
        }
        else
        {
            LDAPMessage* result = NULL;
            TIMEVAL timeOut = {LDAPTIMEOUT,0};   
            int rc = ldap_result(ld, msgid, 1, &timeOut, &result); 
            if(rc < 0)
            {
#ifndef _WIN32
                final_rc = ldap_get_lderrno(ld, NULL, NULL);
#else
                final_rc = LDAP_OTHER;
#endif
            }
            else if(rc == 0)
            {
                final_rc = LDAP_TIMEOUT;
            }
            else
            {
                final_rc = ldap_result2error(ld, result, 1);
            }
        }
        return final_rc;
        */
    }

    // userdn is required for ldap_simple_bind_s, not really necessary for ldap_bind_s.
    static int LdapBind(LDAP* ld, const char* domain, const char* username, const char* password, const char* userdn, LdapServerType server_type, const char* method="kerboros")
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
                DBGLOG("userdn can't be NULL in order to bind to ldap server.");
                return LDAP_INVALID_CREDENTIALS;
            }
            int rc = LdapSimpleBind(ld, (char*)userdn, (char*)password);
            if (rc != LDAP_SUCCESS && server_type == OPEN_LDAP && strchr(userdn,','))
            {   //Fedora389 is happier without the domain component specified
                StringBuffer cn(userdn);
                cn.replace(',',(char)NULL);
                if (cn.length())//disallow call if no cn
                    rc = LdapSimpleBind(ld, (char*)cn.str(), (char*)password);
            }
            if (rc != LDAP_SUCCESS )
            {
                // For Active Directory, try binding with NT format username
                if(server_type == ACTIVE_DIRECTORY)
                {
                    StringBuffer logonname;
                    logonname.append(domain).append("\\").append(username);
                    rc = LdapSimpleBind(ld, (char*)logonname.str(), (char*)password);
                    if(rc != LDAP_SUCCESS)
                    {
#ifdef LDAP_OPT_DIAGNOSTIC_MESSAGE
                        char *msg=NULL;
                        ldap_get_option(ld, LDAP_OPT_DIAGNOSTIC_MESSAGE, (void*)&msg);
                        DBGLOG("LDAP bind error for user %s with %d - %s. %s", logonname.str(), rc, ldap_err2string(rc), msg&&*msg?msg:"");
                        ldap_memfree(msg);
#else
                        DBGLOG("LDAP bind error for user %s with 0x%"I64F"x - %s", username, (unsigned __int64) rc, ldap_err2string(rc));
#endif
                        return rc;
                    }
                }
                else
                {
                    DBGLOG("LDAP bind error for user %s with 0x%"I64F"x - %s", username, (unsigned __int64) rc, ldap_err2string(rc));
                    return rc;
                }
            }
        }
        
        return rc;
    }

    static void bin2str(MemoryBuffer& from, StringBuffer& to)
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

    static int getServerInfo(const char* ldapserver, int ldapport, StringBuffer& domainDN, LdapServerType& stype, const char* domainname);

    static void normalizeDn(const char* dn, const char* basedn, StringBuffer& dnbuf)
    {
        dnbuf.clear();
        cleanupDn(dn, dnbuf);
        if(!containsBasedn(dnbuf.str()))
            dnbuf.append(",").append(basedn);
    }

    static bool containsBasedn(const char* str)
    {
        if(str == NULL || str[0] == '\0')
            return false;
        else
            return (strstr(str, "dc=") != NULL);
    }

    static void cleanupDn(const char* dn, StringBuffer& dnbuf)
    {
        if(dn == NULL || dn[0] == '\0')
            return;

        const char* ptr = dn;
        while(ptr && *ptr != '\0')
        {
            char c = *ptr;
            if(!isspace(c))
            {
                c = tolower(c);
                dnbuf.append(c);
            }
            ptr++;
        }
    }
    
    static bool getDcName(const char* domain, StringBuffer& dc)
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

    static void getName(const char* dn, StringBuffer& name)
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
};


#endif

