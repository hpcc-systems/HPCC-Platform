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

#include "permissions.ipp"
#include "aci.ipp"
#include "ldapsecurity.ipp"
#include "jsmartsock.hpp"
#include "jrespool.tpp"
#include "mpbase.hpp"
#include "dautils.hpp"

#undef new
#include <map>
#include <string>
#include <set>
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif
#ifdef _WIN32
#include <lm.h>
#define LdapRename ldap_rename_ext_s
    LDAPMessage* (*LdapFirstEntry)(LDAP*, LDAPMessage*) = &ldap_first_entry;
    LDAPMessage* (*LdapNextEntry)(LDAP* ld, LDAPMessage* entry) = &ldap_next_entry;
#else
#define LdapRename ldap_rename_s
    LDAPMessage* (__stdcall *LdapFirstEntry)(LDAP*, LDAPMessage*) = &ldap_first_message;
    LDAPMessage* (__stdcall *LdapNextEntry)(LDAP* ld, LDAPMessage* entry) = &ldap_next_message;
#endif

#define LDAPSEC_MAX_RETRIES 2
#define LDAPSEC_RETRY_WAIT  3

#ifdef _WIN32 
#define LDAP_NO_ATTRS "1.1"
#endif

#define PWD_NEVER_EXPIRES (__int64)0x8000000000000000

class CLoadBalancer : public CInterface, implements IInterface
{
private:
    StringArray hostArray;
    unsigned    nextIndex;
    Mutex       m_mutex;

public:
    IMPLEMENT_IINTERFACE

    CLoadBalancer(const char* addrlist)
    {
        char *copyFullText = strdup(addrlist);
        char *saveptr;
        char *ip = strtok_r(copyFullText, "|", &saveptr);
        while (ip != NULL)
        {
            if (isdigit(*ip))
            {
                char *dash = strrchr(ip, '-');
                if (dash)
                {
                    *dash = 0;
                    int last = atoi(dash+1);
                    char *dot = strrchr(ip, '.');
                    *dot = 0;
                    int first = atoi(dot+1);
                    for (int i = first; i <= last; i++)
                    {
                        StringBuffer t;
                        t.append(ip).append('.').append(i);
                        hostArray.append(t.str());
                    }
                }
                else
                {
                    hostArray.append(ip);
                }
            }
            else
            {
                hostArray.append(ip);
            }
            ip = strtok_r(NULL, "|", &saveptr);
        }
        free(copyFullText);

        if(hostArray.length() == 0)
        {
            throw MakeStringException(-1, "No valid ldap server address specified");
        }

        Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
        random->seed((unsigned)get_cycles_now());

        unsigned i = hostArray.ordinality();
        while (i > 1)
        {
            unsigned j = random->next() % i;
            i--;
            hostArray.swap(i, j);
        }

        nextIndex = 0;

        for(unsigned ind = 0; ind < hostArray.length(); ind++)
        {
            DBGLOG("Added ldap server %s", hostArray.item(ind));
        }
    }

    virtual ~CLoadBalancer()
    {
    }

    const char* next()
    {
        unsigned curindex = 0;
        
        {
            synchronized block(m_mutex);
            curindex = nextIndex;
            ++nextIndex %= hostArray.ordinality();
        }

        return (hostArray.item(curindex));
    }
};

inline bool LdapServerDown(int rc)
{
    return rc==LDAP_SERVER_DOWN||rc==LDAP_UNAVAILABLE||rc==LDAP_TIMEOUT;
}

class CLdapConfig : public CInterface, implements ILdapConfig
{
private:
    LdapServerType       m_serverType; 
    StringAttr           m_cfgServerType;//LDAP Server type name (ActiveDirectory, Fedora389, etc)

    Owned<IPropertyTree> m_cfg;

    Owned<CLoadBalancer> m_ldaphosts;
    int                  m_ldapport;
    int                  m_ldap_secure_port;
    StringBuffer         m_protocol;
    StringBuffer         m_basedn;
    StringBuffer         m_domain;
    StringBuffer         m_authmethod;

    StringBuffer         m_user_basedn;
    StringBuffer         m_group_basedn;
    StringBuffer         m_resource_basedn;
    StringBuffer         m_filescope_basedn;
    StringBuffer         m_workunitscope_basedn;
    StringBuffer         m_sudoers_basedn;
    StringBuffer         m_template_name;

    StringBuffer         m_sysuser;
    StringBuffer         m_sysuser_commonname;
    StringBuffer         m_sysuser_password;
    StringBuffer         m_sysuser_basedn;

    bool                 m_sysuser_specified;
    StringBuffer         m_sysuser_dn;

    int                  m_maxConnections;
    
    StringBuffer         m_sdfieldname;
public:
    IMPLEMENT_IINTERFACE

    CLdapConfig(IPropertyTree* cfg)
    {
        int version = LDAP_VERSION3;
        ldap_set_option( NULL, LDAP_OPT_PROTOCOL_VERSION, &version);

        m_cfg.set(cfg);

        //Check for LDAP Server type in config
        m_serverType = LDAPSERVER_UNKNOWN;
        m_cfgServerType.set(cfg->queryProp(".//@serverType"));
        if (m_cfgServerType.length())
        {
            if (0 == stricmp(m_cfgServerType, "ActiveDirectory"))
                m_serverType = ACTIVE_DIRECTORY;
            else if (0 == stricmp(m_cfgServerType, "389DirectoryServer"))//uses iPlanet style ACI
                m_serverType = OPEN_LDAP;
            else if (0 == stricmp(m_cfgServerType, "OpenLDAP"))
                m_serverType = OPEN_LDAP;
            else if (0 == stricmp(m_cfgServerType, "Fedora389"))
                m_serverType = OPEN_LDAP;
            else if (0 == stricmp(m_cfgServerType, "iPlanet"))
                m_serverType = IPLANET;
            else
                throw MakeStringException(-1, "Unknown LDAP serverType '%s' specified",m_cfgServerType.get());
        }
        else
        {
            DBGLOG("LDAP serverType not specified, will try to deduce");
        }

        StringBuffer hostsbuf;
        cfg->getProp(".//@ldapAddress", hostsbuf);
        if(hostsbuf.length() == 0)
        {
            throw MakeStringException(-1, "ldapAddress not found in config");
        }
        m_ldaphosts.setown(new CLoadBalancer(hostsbuf.str()));

        cfg->getProp(".//@ldapProtocol", m_protocol);
        if(m_protocol.length() == 0)
        {
            m_protocol.append("ldap");
        }
        
        StringBuffer portbuf;
        cfg->getProp(".//@ldapPort", portbuf);
        if(portbuf.length() == 0)
            m_ldapport = 389;
        else
            m_ldapport = atoi(portbuf.str());

        portbuf.clear();
        cfg->getProp(".//@ldapSecurePort", portbuf);
        if(portbuf.length() == 0)
            m_ldap_secure_port = 636;
        else
            m_ldap_secure_port = atoi(portbuf.str());

        StringBuffer hostbuf, dcbuf;
        int rc = 0;
        getLdapHost(hostbuf);
        for(int retries = 0; retries <= LDAPSEC_MAX_RETRIES; retries++)
        {
            rc = LdapUtils::getServerInfo(hostbuf.str(), m_ldapport, dcbuf, m_serverType, cfg->queryProp(".//@ldapDomain"));
            if(!LdapServerDown(rc) || retries > LDAPSEC_MAX_RETRIES)
                break;
            sleep(LDAPSEC_RETRY_WAIT);
            if(retries < LDAPSEC_MAX_RETRIES)
            {
                DBGLOG("Server %s temporarily unreachable.", hostbuf.str());
                // Retrying next ldap sever, might be the same server
                hostbuf.clear();
                getLdapHost(hostbuf);
                DBGLOG("Retrying with %s...", hostbuf.str());
            }
        }
        if(rc != LDAP_SUCCESS)
        {
            throw MakeStringException(-1, "getServerInfo error - %s", ldap_err2string(rc));
        }
        const char* basedn = cfg->queryProp(".//@commonBasedn");
        if(basedn == NULL || *basedn == '\0')
        {
            basedn = dcbuf.str();
        }
        LdapUtils::cleanupDn(basedn, m_basedn);

        StringBuffer user_basedn;
        cfg->getProp(".//@usersBasedn", user_basedn);
        if(user_basedn.length() == 0)
        {
            throw MakeStringException(-1, "users basedn not found in config");
        }
        LdapUtils::normalizeDn(user_basedn.str(), m_basedn.str(), m_user_basedn);
        
        StringBuffer group_basedn;
        cfg->getProp(".//@groupsBasedn", group_basedn);
        if(group_basedn.length() == 0)
        {
            throw MakeStringException(-1, "groups basedn not found in config");
        }
        LdapUtils::normalizeDn(group_basedn.str(), m_basedn.str(), m_group_basedn);

        StringBuffer dnbuf;
        cfg->getProp(".//@modulesBasedn", dnbuf);
        if(dnbuf.length() == 0)
            cfg->getProp(".//@resourcesBasedn", dnbuf);
        if(dnbuf.length() > 0)
            LdapUtils::normalizeDn(dnbuf.str(), m_basedn.str(), m_resource_basedn);

        dnbuf.clear();
        cfg->getProp(".//@filesBasedn", dnbuf);
        if(dnbuf.length() > 0)
            LdapUtils::normalizeDn(dnbuf.str(), m_basedn.str(), m_filescope_basedn);

        dnbuf.clear();
        cfg->getProp(".//@workunitsBasedn", dnbuf);
        if(dnbuf.length() > 0)
            LdapUtils::normalizeDn(dnbuf.str(), m_basedn.str(), m_workunitscope_basedn);
        
        if(m_resource_basedn.length() + m_filescope_basedn.length() + m_workunitscope_basedn.length() == 0)
        {
            throw MakeStringException(-1, "One of the following basedns need to be defined: modulesBasedn, resourcesBasedn, filesBasedn or workunitScopesBasedn.");
        }

        dnbuf.clear();
        cfg->getProp(".//@sudoersBasedn", dnbuf);
        if(dnbuf.length() == 0)
            dnbuf.append("ou=SUDOers");
        LdapUtils::normalizeDn(dnbuf.str(), m_basedn.str(), m_sudoers_basedn);

        cfg->getProp(".//@templateName", m_template_name);
        cfg->getProp(".//@authMethod", m_authmethod);
        cfg->getProp(".//@ldapDomain", m_domain);
        if(m_domain.length() == 0)
        {
            const char* dptr = strchr(m_basedn.str(), '=');
            if(dptr != NULL)
            {
                dptr++;
                while(*dptr != 0 && *dptr != ',')
                {
                    char c = *dptr++;
                    m_domain.append(c);
                }
            }
        }

        m_sysuser_specified = true;
        cfg->getProp(".//@systemUser", m_sysuser);
        if(m_sysuser.length() == 0)
        {
            m_sysuser_specified = false;
        }

        cfg->getProp(".//@systemCommonName", m_sysuser_commonname);
        if(m_sysuser_specified && (m_sysuser_commonname.length() == 0))
        {
            throw MakeStringException(-1, "SystemUser commonname is empty");
        }

        StringBuffer passbuf;
        cfg->getProp(".//@systemPassword", passbuf);
        decrypt(m_sysuser_password, passbuf.str());

        StringBuffer sysuser_basedn;
        cfg->getProp(".//@systemBasedn", sysuser_basedn);

        if(sysuser_basedn.length() == 0)
        {
            if(m_serverType == ACTIVE_DIRECTORY)
                LdapUtils::normalizeDn( "cn=Users", m_basedn.str(), m_sysuser_basedn);
            else if(m_serverType == IPLANET)
                m_sysuser_basedn.append("ou=administrators,ou=topologymanagement,o=netscaperoot");
            else if(m_serverType == OPEN_LDAP)
                m_sysuser_basedn.append(m_basedn.str());
        }
        else
        {
            if(m_serverType == ACTIVE_DIRECTORY)
                LdapUtils::normalizeDn(sysuser_basedn.str(), m_basedn.str(), m_sysuser_basedn);
            else
                m_sysuser_basedn.append(sysuser_basedn.str());
        }

        if(m_sysuser_specified)
        {
            if(m_serverType == IPLANET)
                m_sysuser_dn.append("uid=").append(m_sysuser.str()).append(",").append(m_sysuser_basedn.str());
            else if(m_serverType == ACTIVE_DIRECTORY)
                m_sysuser_dn.append("cn=").append(m_sysuser_commonname.str()).append(",").append(m_sysuser_basedn.str());
            else if(m_serverType == OPEN_LDAP)
            {
                if (0==strcmp("Directory Manager",m_sysuser_commonname.str()))
                    m_sysuser_dn.append("cn=").append(m_sysuser_commonname.str()).append(",").append(m_sysuser_basedn.str());
                else
                    m_sysuser_dn.append("uid=").append(m_sysuser_commonname.str()).append(",").append(m_sysuser_basedn.str()).append(",").append(m_basedn.str());
            }
        }

        m_maxConnections = cfg->getPropInt(".//@maxConnections", DEFAULT_LDAP_POOL_SIZE);
        if(m_maxConnections <= 0)
            m_maxConnections = DEFAULT_LDAP_POOL_SIZE;

        if(m_serverType == ACTIVE_DIRECTORY)
            m_sdfieldname.append("ntSecurityDescriptor");
        else if(m_serverType == IPLANET)
            m_sdfieldname.append("aci");
        else if(m_serverType == OPEN_LDAP)
            m_sdfieldname.append("aci");
    }

    virtual LdapServerType getServerType()
    {
        return m_serverType;
    }

    virtual const char * getCfgServerType() const
    {
        return m_cfgServerType.get();
    }

    virtual const char* getSdFieldName()
    {
        return m_sdfieldname.str();
    }

    virtual StringBuffer& getLdapHost(StringBuffer& hostbuf)
    {
        hostbuf.clear();
        try
        {
            hostbuf.append(m_ldaphosts->next());
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            DBGLOG("getLdapHost exception - %s", emsg.str());
            e->Release();
        }
        catch(...)
        {
            DBGLOG("getLdapHost unknown exception");
        }

        return hostbuf;
    }

    virtual void markDown(const char* ldaphost)
    {
        //SocketEndpoint ep(ldaphost, 0);
        //m_ldaphosts->setStatus(ep, false);
    }

    virtual int getLdapPort()
    {
        return m_ldapport;
    }

    virtual int getLdapSecurePort()
    {
        return m_ldap_secure_port;
    }

    virtual const char* getProtocol()
    {
        return m_protocol.str();
    }


    virtual const char* getBasedn()
    {
        return m_basedn.str();
    }

    virtual const char* getDomain()
    {
        return m_domain.str();
    }

    virtual const char* getAuthMethod()
    {
        return m_authmethod.str();
    }

    virtual const char* getUserBasedn()
    {
        return m_user_basedn.str();
    }

    virtual const char* getGroupBasedn()
    {
        return m_group_basedn.str();
    }

    virtual const char* getResourceBasedn(SecResourceType rtype)
    {
        if(rtype == RT_DEFAULT || rtype == RT_MODULE || rtype == RT_SERVICE)
            return m_resource_basedn.str();
        else if(rtype == RT_FILE_SCOPE)
            return m_filescope_basedn.str();
        else if(rtype == RT_WORKUNIT_SCOPE)
            return m_workunitscope_basedn.str();
        else if(rtype == RT_SUDOERS)
            return m_sudoers_basedn.str();
        else
            return m_resource_basedn.str();
    }

    virtual const char* getTemplateName()
    {
        return m_template_name.str();
    }

    virtual const char* getSysUser()
    {
        return m_sysuser.str();
    }

    virtual const char* getSysUserDn()
    {
        return m_sysuser_dn.str();
    }

    virtual const char* getSysUserCommonName()
    {
        return m_sysuser_commonname.str();
    }

    virtual const char* getSysUserPassword()
    {
        return m_sysuser_password.str();
    }

    virtual const char* getSysUserBasedn()
    {
        return m_sysuser_basedn.str();
    }

    virtual bool sysuserSpecified()
    {
        return m_sysuser_specified;
    }

    virtual int getMaxConnections()
    {
        return m_maxConnections;
    }

    // For now, only sets default resourcebasedn, since it's only used by ESP services
    virtual void setResourceBasedn(const char* rbasedn, SecResourceType rtype)
    {
        if(rbasedn == NULL || rbasedn[0] == '\0')
            return;

        if(rtype == RT_DEFAULT || rtype == RT_MODULE || rtype == RT_SERVICE)
        {
            LdapUtils::normalizeDn(rbasedn, m_basedn.str(), m_resource_basedn);
        }
        else if(rtype == RT_FILE_SCOPE)
        {
            LdapUtils::normalizeDn(rbasedn, m_basedn.str(), m_filescope_basedn);
        }
        else if(rtype == RT_WORKUNIT_SCOPE)
        {
            LdapUtils::normalizeDn(rbasedn, m_basedn.str(), m_workunitscope_basedn);
        }
        else
        {
            LdapUtils::normalizeDn(rbasedn, m_basedn.str(), m_resource_basedn);
        }
    }

    virtual void getDefaultSysUserBasedn(StringBuffer& sysuser_basedn)
    {
        if(m_serverType == ACTIVE_DIRECTORY)
            LdapUtils::normalizeDn( "cn=Users", m_basedn.str(), sysuser_basedn);
        else if(m_serverType == IPLANET)
            sysuser_basedn.append("ou=administrators,ou=topologymanagement,o=netscaperoot");
    }
};


class CLdapConnection : public CInterface, implements ILdapConnection
{
private:
    LDAP                *m_ld;
    Owned<CLdapConfig>   m_ldapconfig;

    time_t               m_lastaccesstime;
    bool                 m_connected;

public:
    IMPLEMENT_IINTERFACE

    CLdapConnection(CLdapConfig* ldapconfig)
    {
        m_ldapconfig.setown(LINK(ldapconfig));
        m_ld = NULL;
        m_connected = false;
        m_lastaccesstime = 0;
    }

    ~CLdapConnection()
    {
        if(m_ld != NULL)
        {
            ldap_unbind(m_ld);
        }
    }

    virtual int connect(const char* ldapserver, const char* protocol)
    {
        if(!ldapserver || *ldapserver == '\0')
            return -1;

        m_ld = LdapUtils::LdapInit(protocol, ldapserver, m_ldapconfig->getLdapPort(), m_ldapconfig->getLdapSecurePort());
        int rc = LDAP_SUCCESS;
        if(m_ldapconfig->sysuserSpecified())
            rc =  LdapUtils::LdapBind(m_ld, m_ldapconfig->getDomain(), m_ldapconfig->getSysUser(), m_ldapconfig->getSysUserPassword(), m_ldapconfig->getSysUserDn(), m_ldapconfig->getServerType(), m_ldapconfig->getAuthMethod());
        else
            rc =  LdapUtils::LdapBind(m_ld, m_ldapconfig->getDomain(), NULL, NULL, NULL, m_ldapconfig->getServerType(), m_ldapconfig->getAuthMethod());

        if(rc == LDAP_SUCCESS)
        {
            time(&m_lastaccesstime);
            m_connected = true;
            const char * ldap = NULL;
            switch (m_ldapconfig->getServerType())
            {
            case ACTIVE_DIRECTORY:
                ldap = "Active Directory";
                break;
            case OPEN_LDAP:
                ldap = "OpenLDAP";
                break;
            case IPLANET:
                ldap = "iplanet";
                break;
            default:
                ldap = "unknown";
                break;
            }
            DBGLOG("Connected to '%s' LdapServer %s using protocol %s", ldap, ldapserver, protocol);
        }
        else
        {
            DBGLOG("LDAP: sysuser bind failed - %s", ldap_err2string(rc));
            ldap_unbind(m_ld);
            m_ld = NULL;
        }

        return rc;
    }

    virtual bool connect(bool force_ssl = false)
    {
        StringBuffer hostbuf;
        m_ldapconfig->getLdapHost(hostbuf);
        int rc = LDAP_SERVER_DOWN;
        const char* proto;
        if(force_ssl)
            proto = "ldaps";
        else
            proto = m_ldapconfig->getProtocol();
        
        for(int retries = 0; retries <= LDAPSEC_MAX_RETRIES; retries++)
        {
            rc = connect(hostbuf.str(), proto);
            if(!LdapServerDown(rc) || retries > LDAPSEC_MAX_RETRIES)
                break;
            sleep(LDAPSEC_RETRY_WAIT);
            if(retries < LDAPSEC_MAX_RETRIES)
                DBGLOG("Server temporarily unreachable, retrying ...");
            // Retrying next ldap sever, might be the same server
            hostbuf.clear();
            m_ldapconfig->getLdapHost(hostbuf);
        }

        if(rc == LDAP_SERVER_DOWN)
        {
            StringBuffer dc;
            LdapUtils::getDcName(m_ldapconfig->getDomain(), dc);
            if(dc.length() > 0)
            {
                WARNLOG("Using automatically obtained LDAP Server %s", dc.str());
                rc = connect(dc.str(), proto);
            }
        }
        if(rc == LDAP_SUCCESS)
            return true;
        else
            return false;
    }

    virtual LDAP* getLd()
    {
        return m_ld;
    }

    virtual bool validate()
    {
        time_t now;
        time(&now);

        if(!m_connected)
            return connect();
        else if(now - m_lastaccesstime <= 300)
            return true;
        else
        {
            bool ok = false;
            LDAPMessage* msg = NULL;
            
            TIMEVAL timeOut = {LDAPTIMEOUT,0};
            int err = ldap_search_ext_s(m_ld, NULL, LDAP_SCOPE_BASE, "objectClass=*", NULL, 0, NULL, NULL, &timeOut, 1, &msg);

            if(err == LDAP_SUCCESS)
            {
                ok = true;
            }

            if(msg != NULL)
                ldap_msgfree(msg);
            
            if(!ok)
            {
                if(m_ld != NULL)
                {
                    ldap_unbind(m_ld);
                    m_ld = NULL;
                    m_connected = false;
                }
                DBGLOG("cached connection invalid, creating a new connection");
                return connect();
            }
            else
            {
                time(&m_lastaccesstime);
            }
        }

        return true;
    }
};

class CLdapConnectionPool : public CInterface, implements ILdapConnectionPool
{
private:
    int m_maxsize;
    int m_currentsize;
    IArrayOf<ILdapConnection> m_connections;
    Monitor m_monitor;
    Owned<CLdapConfig> m_ldapconfig;

public:
    IMPLEMENT_IINTERFACE

    CLdapConnectionPool(CLdapConfig* ldapconfig)
    {
        m_ldapconfig.setown(LINK(ldapconfig));
        m_maxsize = m_ldapconfig->getMaxConnections();
        m_currentsize = 0;
        // Set LDAP version to 3
        int version = LDAP_VERSION3;
        ldap_set_option( NULL, LDAP_OPT_PROTOCOL_VERSION, &version);
    }

    virtual ILdapConnection* getConnection()
    {
        synchronized block(m_monitor);
        ForEachItemIn(x, m_connections)
        {
            CLdapConnection* curcon = (CLdapConnection*)&(m_connections.item(x));
            if(curcon != NULL && !curcon->IsShared())
            {
                //PrintLog("Reusing an LDAP connection");
                if(curcon->validate())
                    return LINK(curcon);
                else
                    throw MakeStringException(-1, "Connecting/authenticating to ldap server in re-validation failed");
            }
        }

        //PrintLog("Creating new connection");
        CLdapConnection* newcon = new CLdapConnection(m_ldapconfig.get());
        if(newcon != NULL)
        {
            if(!newcon->connect())
            {
                throw MakeStringException(-1, "Connecting/authenticating to ldap server failed");
            }
            
            if(m_currentsize <= m_maxsize)
            {
                m_connections.append(*newcon);
                m_currentsize++;
                return LINK(newcon);
            }
            else
            {
                return newcon;
            }
        }
        else
        {
            throw MakeStringException(-1, "Failed to create new ldap connection");
        }
    }

    virtual ILdapConnection* getSSLConnection()
    {
        CLdapConnection* newcon = new CLdapConnection(m_ldapconfig.get());
        if(newcon != NULL)
        {
            if(!newcon->connect(true))
            {
                throw MakeStringException(-1, "Connecting/authenticating to ldap server via ldaps failed");
            }
            return newcon;
        }
        else
        {
            throw MakeStringException(-1, "Failed to create new ldap connection");
        }
    }

};

#define LDAP_CONNECTION_TIMEOUT INFINITE

//------------ New Connection Pool Implementation ------------//
class CLdapConnectionManager : public CInterface, implements IResourceFactory<CLdapConnection>
{
    Owned<CLdapConfig>   m_ldapconfig;
    
public:
    IMPLEMENT_IINTERFACE;
    
    CLdapConnectionManager(CLdapConfig* ldapconfig)
    {
        m_ldapconfig.setown(LINK(ldapconfig));
    }

    CLdapConnection* createResource()
    {
        CLdapConnection* newcon = new CLdapConnection(m_ldapconfig.get());
        if(newcon != NULL)
        {
            if(!newcon->connect())
            {
                throw MakeStringException(-1, "Connecting/authenticating to ldap server failed");
            }
            
            return newcon;
        }
        else
        {
            throw MakeStringException(-1, "Failed to create new ldap connection");
        }
    }
};

class CLdapConnectionPool2 : public CInterface, implements ILdapConnectionPool
{
private:
    int m_maxsize;
    Owned<CLdapConfig> m_ldapconfig;
    Owned<CResourcePool<CLdapConnection> > m_connections;
public:
    IMPLEMENT_IINTERFACE

    CLdapConnectionPool2(CLdapConfig* ldapconfig)
    {
        m_ldapconfig.setown(LINK(ldapconfig));
        m_maxsize = m_ldapconfig->getMaxConnections();
        // Set LDAP version to 3
        int version = LDAP_VERSION3;
        ldap_set_option( NULL, LDAP_OPT_PROTOCOL_VERSION, &version);

        m_connections.setown(new CResourcePool<CLdapConnection>);
        Owned<CLdapConnectionManager> poolMgr = new CLdapConnectionManager(ldapconfig);
        m_connections->init(m_maxsize, poolMgr.get());
    }

    virtual ILdapConnection* getConnection()
    {
        Owned<CLdapConnection> con;
        try
        {
            con.setown(m_connections->get(LDAP_CONNECTION_TIMEOUT));
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            DBGLOG("getConnection exception - %s", emsg.str());
            e->Release();
        }
        catch(...)
        {
            DBGLOG("getConnection unknown exception");
        }

        if(con.get())
        {
            if(con->validate())
                return con.getLink();
            else
                throw MakeStringException(-1, "Connecting/authenticating to ldap server in re-validation failed");
        }
        else
                throw MakeStringException(-1, "Failed to get an LDAP Connection.");
    }

    virtual ILdapConnection* getSSLConnection()
    {
        CLdapConnection* newcon = new CLdapConnection(m_ldapconfig.get());
        if(newcon != NULL)
        {
            if(!newcon->connect(true))
            {
                throw MakeStringException(-1, "Connecting/authenticating to ldap server via ldaps failed");
            }
            return newcon;
        }
        else
        {
            throw MakeStringException(-1, "Failed to create new ldap connection");
        }
    }
};


struct ltstr
{
    bool operator()(const char* s1, const char* s2) const { return strcmp(s1, s2) < 0; }
};

//--------------------------------------------
// This helper class ensures memory allocate by
// calls to ldap_get_values gets freed
//--------------------------------------------
class CLDAPGetValuesWrapper
{
private:
    char **values;
public:
    CLDAPGetValuesWrapper(LDAP *ld, LDAPMessage *msg, const char * attr)
    {
#ifdef _WIN32
        values = ldap_get_values(ld, msg, (const PCHAR)attr);
#else
        values = ldap_get_values(ld, msg, attr);
#endif
    }

    ~CLDAPGetValuesWrapper()
    {
        if (values)
            ldap_value_free(values);
    }
    inline bool hasValues()     { return values != NULL  && *values != (char)NULL; }
    inline char **queryValues() { return values; }
};

//--------------------------------------------
// This helper class ensures memory allocate by
// calls to ldap_get_values_len gets freed
//--------------------------------------------
class CLDAPGetValuesLenWrapper
{
private:
    struct berval** bvalues;
public:
    CLDAPGetValuesLenWrapper()
    {
        bvalues = NULL;
    }
    CLDAPGetValuesLenWrapper(LDAP *ld, LDAPMessage *msg, const char * attr)
    {
        bvalues = NULL;
        getValues(ld,msg,attr);
    }

    ~CLDAPGetValuesLenWrapper()
    {
        if (bvalues)
            ldap_value_free_len(bvalues);
    }
    inline bool hasValues()         { return bvalues != NULL  && *bvalues != NULL; }
    inline berval **queryValues()   { return bvalues; }

    //Delayed call to ldap_get_values_len
    void getValues(LDAP *ld, LDAPMessage *msg, const char * attr)
    {
        if (bvalues)
            ldap_value_free_len(bvalues);
#ifdef _WIN32
        bvalues = ldap_get_values_len(ld, msg, (const PCHAR)attr);
#else
        bvalues = ldap_get_values_len(ld, msg, attr);
#endif
    }
};


//--------------------------------------------
// This helper class ensures memory allocate by calls
// to ldap_first_attribute/ldap_next_attribute gets freed
//--------------------------------------------
class CLDAPGetAttributesWrapper
{
private:
        LDAP *          ld;
        LDAPMessage *   entry;
        BerElement *    elem;
        char *          attribute;
public:
    CLDAPGetAttributesWrapper(LDAP * _ld, LDAPMessage * _entry)
        : ld(_ld), entry(_entry)
    {
        elem = NULL;
        attribute = NULL;
    }

    ~CLDAPGetAttributesWrapper()
    {
        if (attribute)
            ldap_memfree(attribute);
        if (elem)
            ber_free(elem, 0);
    }

    inline char * getFirst()
    {
        return attribute = ldap_first_attribute(ld, entry, &elem);
    }

    inline char * getNext()
    {
        if (attribute)
            ldap_memfree(attribute);
        return attribute = ldap_next_attribute(ld, entry, elem);
    }
};

class CLDAPMessage
{
public:
    LDAPMessage *msg;
    CLDAPMessage()       { msg = NULL; }
    ~CLDAPMessage()      { ldapMsgFree(); }
    inline void ldapMsgFree()  { if (msg) { ldap_msgfree(msg); msg = NULL;} }
    inline operator LDAPMessage *() const { return msg; }
};

static CriticalSection  mpaCrit;
static __int64 getMaxPwdAge(Owned<ILdapConnectionPool> _conns, const char * _baseDN)
{
    static time_t   lastPwdAgeCheck = 0;
    static __int64  maxPwdAge = PWD_NEVER_EXPIRES;
    #define HOURLY  ((time_t)(60*60*1000))

    CriticalBlock block(mpaCrit);
    if (lastPwdAgeCheck != 0 && (((msTick() - lastPwdAgeCheck) < HOURLY)))//in case it was retrieved whilst this thread blocked
        return maxPwdAge;

    DBGLOG("Retrieving LDAP 'maxPwdAge'");
    char* attrs[] = {"maxPwdAge", NULL};
    CLDAPMessage searchResult;
    TIMEVAL timeOut = {LDAPTIMEOUT,0};
    Owned<ILdapConnection> lconn = _conns->getConnection();
    LDAP* sys_ld = ((CLdapConnection*)lconn.get())->getLd();
    int result = ldap_search_ext_s(sys_ld, (char*)_baseDN, LDAP_SCOPE_BASE, NULL,
        attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg);
    if(result != LDAP_SUCCESS)
    {
        DBGLOG("ldap_search_ext_s error: %s, when searching maxPwdAge", ldap_err2string( result ));
        return 0;
    }
    unsigned entries = ldap_count_entries(sys_ld, searchResult);
    if(entries == 0)
    {
        DBGLOG("ldap_search_ext_s error: Could not find maxPwdAge");
        return 0;
    }
    maxPwdAge = 0;
    CLDAPGetValuesWrapper vals(sys_ld, searchResult.msg, "maxPwdAge");
    if (vals.hasValues())
    {
        char *val = vals.queryValues()[0];
        if (*val == '-')
            ++val;
        for (int x=0; val[x]; x++)
            maxPwdAge = maxPwdAge * 10 + ( (int)val[x] - '0');
    }
    else
        maxPwdAge = PWD_NEVER_EXPIRES;
    lastPwdAgeCheck = msTick();
    return maxPwdAge;
}

class CLdapClient : public CInterface, implements ILdapClient
{
private:
    Owned<ILdapConnectionPool> m_connections;
    IPermissionProcessor* m_pp;     
    //int                  m_defaultFileScopePermission;
    //int                  m_defaultWorkunitScopePermission;
    Owned<CLdapConfig>   m_ldapconfig;
    StringBuffer         m_pwscheme;
    bool                 m_domainPwdsNeverExpire;//no domain policy for password expiration

public:
    IMPLEMENT_IINTERFACE

    CLdapClient(IPropertyTree* cfg)
    {
        m_ldapconfig.setown(new CLdapConfig(cfg));
        if(cfg && cfg->getPropBool("@useRealConnectionPool", false))
            m_connections.setown(new CLdapConnectionPool2(m_ldapconfig.get())); 
        else
            m_connections.setown(new CLdapConnectionPool(m_ldapconfig.get()));  
        m_pp = NULL;
        //m_defaultFileScopePermission = -2;
        //m_defaultWorkunitScopePermission = -2;
    }

    virtual void init(IPermissionProcessor* pp)
    {
        m_pp = pp;
        if(m_ldapconfig->getServerType() == OPEN_LDAP)
        {
            try
            {
                addDC(m_ldapconfig->getBasedn());
            }
            catch(...)
            {
            }
            try
            {
                addGroup("Directory Administrators", NULL, NULL, m_ldapconfig->getBasedn());
            }
            catch(...)
            {
            }
        }
        createLdapBasedn(NULL, m_ldapconfig->getResourceBasedn(RT_DEFAULT), PT_DEFAULT);
        createLdapBasedn(NULL, m_ldapconfig->getResourceBasedn(RT_FILE_SCOPE), PT_DEFAULT);
        createLdapBasedn(NULL, m_ldapconfig->getResourceBasedn(RT_WORKUNIT_SCOPE), PT_DEFAULT);
        createLdapBasedn(NULL, m_ldapconfig->getResourceBasedn(RT_SUDOERS), PT_DEFAULT);

        createLdapBasedn(NULL, m_ldapconfig->getUserBasedn(), PT_DEFAULT);
        createLdapBasedn(NULL, m_ldapconfig->getGroupBasedn(), PT_DEFAULT);
    }

    virtual LdapServerType getServerType()
    {
        return m_ldapconfig->getServerType();
    }

    virtual ILdapConfig* getLdapConfig()
    {
        return m_ldapconfig.get();
    }

    virtual void setResourceBasedn(const char* rbasedn, SecResourceType rtype)
    {
        m_ldapconfig->setResourceBasedn(rbasedn, rtype);
        createLdapBasedn(NULL, m_ldapconfig->getResourceBasedn(rtype), PT_DEFAULT);
    }

    void calcPWExpiry(CDateTime &dt, unsigned len, char * val)
    {
        __int64 time = 0;
        for (unsigned x=0; x < len; x++)
            time = time * 10 + ( (int)val[x] - '0');
        time += getMaxPwdAge(m_connections,(char*)m_ldapconfig->getBasedn() );
        dt.setFromFILETIME(time);
        dt.adjustTime(dt.queryUtcToLocalDelta());
    }

    virtual bool authenticate(ISecUser& user)
    {
        {
            char        *attribute;
            user.setAuthenticateStatus(AS_UNEXPECTED_ERROR);//assume the worst

            const char* username = user.getName();
            const char* password = user.credentials().getPassword();
            if(!username || !*username || !password || !*password)
                return false;

            if (getMaxPwdAge(m_connections,(char*)m_ldapconfig->getBasedn()) != PWD_NEVER_EXPIRES)
                m_domainPwdsNeverExpire = false;
            else
                m_domainPwdsNeverExpire = true;

            const char* sysuser = m_ldapconfig->getSysUser();
            bool sysUser = false;
            if(sysuser && *sysuser && (strcmp(username, sysuser) == 0))
            {
                if(strcmp(password, m_ldapconfig->getSysUserPassword()) == 0)
                {
                    user.setFullName(m_ldapconfig->getSysUserCommonName());
                    user.setAuthenticateStatus(AS_AUTHENTICATED);
                    if (m_ldapconfig->getServerType() != ACTIVE_DIRECTORY)
                        return true;
                    sysUser = true;
                }
                else
                {
                    user.setAuthenticateStatus(AS_INVALID_CREDENTIALS);
                    return false;
                }
            }

            StringBuffer filter;
            // Retrieve user's dn with system connection
            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                filter.append("sAMAccountName=");
            else
                filter.append("uid=");
            filter.append(username);

            char* attrs[] = {"cn", "userAccountControl", "pwdLastSet", "givenName", "sn", NULL};

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* sys_ld = ((CLdapConnection*)lconn.get())->getLd();
            CLDAPMessage searchResult;
            TIMEVAL timeOut = {LDAPTIMEOUT,0};
            int result = ldap_search_ext_s(sys_ld,
                            (char*)m_ldapconfig->getUserBasedn(), //distinguished name of the entry at which to start the search
                            LDAP_SCOPE_SUBTREE,
                            (char*)filter.str(), //search filter
                            attrs,
                            0, //attribute types and values are to be returned, nonzero if only types are required
                            NULL,
                            NULL,
                            &timeOut,
                            LDAP_NO_LIMIT,
                            &searchResult.msg);

            if(result != LDAP_SUCCESS)
            {
                DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( result ), filter.str(), m_ldapconfig->getUserBasedn());
                return false;
            }

            unsigned entries = ldap_count_entries(sys_ld, searchResult);
            if(entries == 0)
            {
                searchResult.ldapMsgFree();
                TIMEVAL timeOut = {LDAPTIMEOUT,0};
                result = ldap_search_ext_s(sys_ld, (char*)m_ldapconfig->getSysUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg);
                if(result != LDAP_SUCCESS)
                {
                    DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( result ), filter.str(), m_ldapconfig->getSysUserBasedn());
                    user.setAuthenticateStatus(AS_INVALID_CREDENTIALS);
                    return false;
                }

                entries = ldap_count_entries(sys_ld, searchResult);
                if(entries == 0)
                {
                    DBGLOG("LDAP: User %s not found", username);
                    user.setAuthenticateStatus(AS_INVALID_CREDENTIALS);
                    return false;
                }
            }

            LDAPMessage *entry = LdapFirstEntry(sys_ld, searchResult);
            if(entry == NULL)
            {
                DBGLOG("LDAP: Can't find entry for user %s", username);
                return false;
            }
            bool accountPwdNeverExpires = false;

            CLDAPGetAttributesWrapper   atts(sys_ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                if(stricmp(attribute, "cn") == 0)
                {
                    CLDAPGetValuesWrapper vals(sys_ld, entry, attribute);
                    if (vals.hasValues())
                        user.setFullName(vals.queryValues()[0]);
                }
                else if((stricmp(attribute, "givenName") == 0))
                {
                    CLDAPGetValuesWrapper vals(sys_ld, entry, attribute);
                    if (vals.hasValues())
                        user.setFirstName(vals.queryValues()[0]);
                }
                else if((stricmp(attribute, "sn") == 0))
                {
                    CLDAPGetValuesWrapper vals(sys_ld, entry, attribute);
                    if (vals.hasValues())
                        user.setLastName(vals.queryValues()[0]);
                }
                else if((stricmp(attribute, "userAccountControl") == 0))
                {
                    //UF_DONT_EXPIRE_PASSWD 0x10000
                    CLDAPGetValuesWrapper vals(sys_ld, entry, attribute);
                    if (vals.hasValues())
                        if (atoi((char*)vals.queryValues()[0]) & 0x10000)//this can be true at the account level, even if domain policy requires password
                            accountPwdNeverExpires = true;
                }
                else if((stricmp(attribute, "pwdLastSet") == 0))
                {
                    /*pwdLastSet is the date and time that the password for this account was last changed. This
                    value is stored as a large integer that represents the number of 100 nanosecond intervals
                    since January 1, 1601 (UTC), also known as a FILETIME value. If this value is set
                    to 0 and the User-Account-Control attribute does not contain the UF_DONT_EXPIRE_PASSWD
                    flag, then the user must set the password at the next logon.
                    */
                    CLDAPGetValuesLenWrapper valsLen(sys_ld, entry, attribute);
                    if (valsLen.hasValues())
                    {
                        CDateTime expiry;
                        if (!m_domainPwdsNeverExpire && !accountPwdNeverExpires)
                        {
                            struct berval* val = valsLen.queryValues()[0];
                            calcPWExpiry(expiry, (unsigned)val->bv_len, val->bv_val);
                        }
                        else
                        {
                            expiry.clear();
                            DBGLOG("LDAP: Password never expires for user %s", username);
                        }
                        user.setPasswordExpiration(expiry);
                    }
                }
            }

            char *userdn = ldap_get_dn(sys_ld, entry);
            if(userdn == NULL || strlen(userdn) == 0)
            {
                DBGLOG("LDAP: dn not found for user %s", username);
                return false;
            }

            StringBuffer userdnbuf;
            userdnbuf.append(userdn);
            ldap_memfree(userdn);

            if (sysUser)
                return true;//sysuser authenticated above

            StringBuffer hostbuf;
            m_ldapconfig->getLdapHost(hostbuf);
            int rc = LDAP_SERVER_DOWN;
            char *ldap_errstring=NULL;

            for(int retries = 0; retries <= LDAPSEC_MAX_RETRIES; retries++)
            {
                DBGLOG("LdapBind for user %s (retries=%d).", username, retries);
                {
#ifdef NULL_DALIUSER_STACKTRACE
                    //following debug code to be removed
                    if (!username)
                    {
                        DBGLOG("UNEXPECTED USER (NULL) in ldapconnection.cpp authenticate() line %d", __LINE__);
                        PrintStackReport();
                    }
#endif
                    LDAP* user_ld = LdapUtils::LdapInit(m_ldapconfig->getProtocol(), hostbuf.str(), m_ldapconfig->getLdapPort(), m_ldapconfig->getLdapSecurePort());
                    rc = LdapUtils::LdapBind(user_ld, m_ldapconfig->getDomain(), username, password, userdnbuf.str(), m_ldapconfig->getServerType(), m_ldapconfig->getAuthMethod());
                    if(rc != LDAP_SUCCESS)
                        ldap_get_option(user_ld, LDAP_OPT_ERROR_STRING, &ldap_errstring);
                    ldap_unbind(user_ld);
                }
                DBGLOG("finished LdapBind for user %s, rc=%d", username, rc);
                if(!LdapServerDown(rc) || retries > LDAPSEC_MAX_RETRIES)
                    break;
                sleep(LDAPSEC_RETRY_WAIT);
                if(retries < LDAPSEC_MAX_RETRIES)
                    DBGLOG("Server temporarily unreachable, retrying ...");
                // Retrying next ldap sever, might be the same server
                hostbuf.clear();
                m_ldapconfig->getLdapHost(hostbuf);
            }

            if(rc == LDAP_SERVER_DOWN)
            {
                StringBuffer dc;
                LdapUtils::getDcName(NULL, dc);
                if(dc.length() > 0)
                {
                    WARNLOG("Using automatically obtained LDAP Server %s", dc.str());
                    LDAP* user_ld = LdapUtils::LdapInit(m_ldapconfig->getProtocol(), dc.str(), m_ldapconfig->getLdapPort(), m_ldapconfig->getLdapSecurePort());
                    rc = LdapUtils::LdapBind(user_ld, m_ldapconfig->getDomain(), username, password, userdnbuf.str(), m_ldapconfig->getServerType(), m_ldapconfig->getAuthMethod());
                    if(rc != LDAP_SUCCESS)
                        ldap_get_option(user_ld, LDAP_OPT_ERROR_STRING, &ldap_errstring);
                    ldap_unbind(user_ld);
                }
            }
            if(rc != LDAP_SUCCESS)
            {
                if (ldap_errstring && *ldap_errstring && strstr(ldap_errstring, " data "))//if extended error strings are available (they are not in windows clients)
                {
#ifdef _DEBUG
                    DBGLOG("LDAPBIND ERR: RC=%d, - '%s'", rc, ldap_errstring);
#endif
                    if (strstr(ldap_errstring, "data 532"))//80090308: LdapErr: DSID-0C0903A9, comment: AcceptSecurityContext error, data 532, v1db0.
                    {
                        DBGLOG("LDAP: Password Expired(1) for user %s", username);
                        user.setAuthenticateStatus(AS_PASSWORD_VALID_BUT_EXPIRED);
                    }
                    else if (strstr(ldap_errstring, "data 773"))//User must reset password "80090308: LdapErr: DSID-0C0903A9, comment: AcceptSecurityContext error, data 773, v1db1'
                    {
                        DBGLOG("LDAP: User %s Must Reset Password", username);
                        user.setAuthenticateStatus(AS_PASSWORD_VALID_BUT_EXPIRED);
                    }
                    else
                    {
                        DBGLOG("LDAP: Authentication(1) for user %s failed - %s", username, ldap_err2string(rc));
                        user.setAuthenticateStatus(AS_INVALID_CREDENTIALS);
                    }
                }
                else
                {
                    //This path is typical if running ESP on Windows. We have no way
                    //to determine if password entered is valid but expired
                    if (user.getPasswordDaysRemaining() == scPasswordExpired)
                    {
                        DBGLOG("LDAP: Password Expired(2) for user %s", username);
                        user.setAuthenticateStatus(AS_PASSWORD_EXPIRED);
                    }
                    else
                    {
                        DBGLOG("LDAP: Authentication(2) for user %s failed - %s", username, ldap_err2string(rc));
                        user.setAuthenticateStatus(AS_INVALID_CREDENTIALS);
                    }
                }
                return false;
            }
            user.setAuthenticateStatus(AS_AUTHENTICATED);
        }
        //Always retrieve user info(SID, UID, fullname, etc) for Active Directory, when the user first logs in.
        if((m_ldapconfig->getServerType() == ACTIVE_DIRECTORY) && (m_pp != NULL))
            m_pp->retrieveUserInfo(user);

        return true;
    };

    virtual bool authorize(SecResourceType rtype, ISecUser& user, IArrayOf<ISecResource>& resources)
    {
        bool ok = false;
        const char* basedn = m_ldapconfig->getResourceBasedn(rtype);
        if(basedn == NULL || *basedn == '\0')
        {
            DBGLOG("corresponding basedn is not defined for authorize");
            return false;
        }

        const char* username = user.getName();
        if(!username || !*username)
            return false;

        const char* sysuser = m_ldapconfig->getSysUser();
        if(sysuser && *sysuser && (strcmp(username, sysuser) == 0))
        {
            ForEachItemIn(x, resources)
            {
                ISecResource* res = &resources.item(x);
                if(!res)
                    continue;
                if(rtype == RT_MODULE)
                {
                    StringBuffer filter;
                    if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                        filter.append("name=");
                    else
                        filter.append("ou=");
                    filter.append(res->getName());
                    int count = countEntries(m_ldapconfig->getResourceBasedn(rtype), (char*)filter.str(), 10);
                    if(count != 0)
                        res->setAccessFlags(SecAccess_Full);
                    else
                        res->setAccessFlags(-1);
                }
                else
                    res->setAccessFlags(SecAccess_Full);
            }
            return true;
        }

        if(rtype == RT_FILE_SCOPE)
        {
            int defaultFileScopePermission = queryDefaultPermission(user);
            IArrayOf<ISecResource> non_emptylist;
            ForEachItemIn(x, resources)
            {
                ISecResource& res = resources.item(x);
                const char* res_name = res.getName();
                if(res_name == NULL || *res_name == '\0')
                    res.setAccessFlags(defaultFileScopePermission); //res.setAccessFlags(m_defaultFileScopePermission);
                else 
                    non_emptylist.append(*LINK(&res));
            }

            ok = authorizeScope(user, non_emptylist, basedn);
            //if(ok && m_defaultFileScopePermission != -2)
            if(ok && defaultFileScopePermission != -2)
            {
                ForEachItemIn(x, non_emptylist)
                {
                    ISecResource& res = non_emptylist.item(x);
                    if(res.getAccessFlags() == -1)
                        res.setAccessFlags(defaultFileScopePermission); //res.setAccessFlags(m_defaultFileScopePermission);
                }
            }

            return ok;
        }
        else if(rtype == RT_WORKUNIT_SCOPE)
        {
            int defaultWorkunitScopePermission = -2;
            //if(m_defaultWorkunitScopePermission == -2)
            {
                const char* basebasedn = strchr(basedn, ',') + 1;
                StringBuffer baseresource;
                baseresource.append(basebasedn-basedn-4, basedn+3);
                IArrayOf<ISecResource> base_resources;
                base_resources.append(*(new CLdapSecResource(baseresource.str())));
                bool baseok = authorizeScope(user, base_resources, basebasedn);
                if(baseok)
                {
                    defaultWorkunitScopePermission = base_resources.item(0).getAccessFlags();
                }
            }
            IArrayOf<ISecResource> non_emptylist;
            ForEachItemIn(x, resources)
            {
                ISecResource& res = resources.item(x);
                const char* res_name = res.getName();
                if(res_name == NULL || *res_name == '\0')
                    res.setAccessFlags(defaultWorkunitScopePermission);
                else 
                    non_emptylist.append(*LINK(&res));
            }
            ok = authorizeScope(user, non_emptylist, basedn);
            if(ok && defaultWorkunitScopePermission != -2)
            {
                ForEachItemIn(x, non_emptylist)
                {
                    ISecResource& res = non_emptylist.item(x);
                    if(res.getAccessFlags() == -1)
                        res.setAccessFlags(defaultWorkunitScopePermission);
                }
            }

            return ok;
        }
        else
        {
            IArrayOf<CSecurityDescriptor> sdlist;
            ForEachItemIn(x, resources)
            {
                ISecResource& res = resources.item(x);
                const char* resourcename = res.getName();
                CSecurityDescriptor* sd = new CSecurityDescriptor(resourcename);
                sdlist.append(*sd);
            }

            getSecurityDescriptors(rtype, sdlist);
            if(m_pp != NULL)
                ok = m_pp->getPermissions(user, sdlist, resources);
            return ok;
        }
    }

    // Returns true if all resources are correctly added, otherwise returns false.
    virtual bool addResources(SecResourceType rtype, ISecUser& user, IArrayOf<ISecResource>& resources, SecPermissionType ptype, const char* basedn)
    {
        bool ret = true;

        for(unsigned i = 0; i < resources.length(); i++)
        {
            ISecResource* resource = &resources.item(i);
            if(resource != NULL)
            {
                bool oneret = addResource(rtype, user, resource, ptype, basedn);
                ret = ret && oneret; 
            }
        }

        return ret;
    }

    virtual bool getUserInfo(ISecUser& user, const char* infotype)
    {
        char        *attribute;
        LDAPMessage *message;

        const char* username = user.getName();

        if(username == NULL || strlen(username) == 0)
        {
            DBGLOG("LDAP: getUserInfo : username is empty");
            return false;
        }
        
        if(infotype && stricmp(infotype, "sudoers") == 0)
        {
            CLdapSecUser* ldapuser = dynamic_cast<CLdapSecUser*>(&user);

            TIMEVAL timeOut = {LDAPTIMEOUT,0};   
            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

            StringBuffer filter("sudoUser=");
            filter.append(username);
            char  *attrs[] = {"sudoHost", "sudoCommand", "sudoOption", NULL};
            const char* basedn = m_ldapconfig->getResourceBasedn(RT_SUDOERS);
            CLDAPMessage searchResult;
            int rc = ldap_search_ext_s(ld, (char*)basedn, LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg);

            if ( rc != LDAP_SUCCESS )
            {
                DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), basedn);
                ldapuser->setSudoersEnabled(false);
                ldapuser->setInSudoers(false);
                return false;
            }
            
            ldapuser->setSudoersEnabled(true);

            unsigned entries = ldap_count_entries(ld, searchResult);
            if(entries == 0)
            {
                ldapuser->setInSudoers(false);
                return true;
            }

            message = LdapFirstEntry(ld, searchResult);
            if(message == NULL)
            {
                ldapuser->setInSudoers(false);
                return true;
            }

            ldapuser->setInSudoers(true);
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (vals.hasValues())
                {
                    if(stricmp(attribute, "sudoHost") == 0)
                        ldapuser->setSudoHost(vals.queryValues()[0]);
                    else if(stricmp(attribute, "sudoCommand") == 0)
                        ldapuser->setSudoCommand(vals.queryValues()[0]);
                    else if(stricmp(attribute, "sudoOption") == 0)
                        ldapuser->setSudoOption(vals.queryValues()[0]);
                }
            }
            return true;
        }
        else
        {
            StringBuffer filter;
            const char* basedn = m_ldapconfig->getUserBasedn();
            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            {
                filter.append("sAMAccountName=");
            }
            else
            {
                filter.append("uid=");
                if(stricmp(username, m_ldapconfig->getSysUser()) == 0)
                    basedn = m_ldapconfig->getSysUserBasedn();
            }

            filter.append(user.getName());

            TIMEVAL timeOut = {LDAPTIMEOUT,0};   
            
            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

            char        *attrs[] = {"cn", "givenName", "sn", "gidnumber", "uidnumber", "homedirectory", "loginshell", "objectClass", NULL};
            CLDAPMessage searchResult;
            int rc = ldap_search_ext_s(ld, (char*)basedn, LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,   &searchResult.msg );

            if ( rc != LDAP_SUCCESS )
            {
                DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), basedn);
                return false;
            }

            ((CLdapSecUser*)&user)->setPosixenabled(false);
            // Go through the search results by checking message types
            for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
            {
                CLDAPGetAttributesWrapper   atts(ld, searchResult);
                for ( attribute = atts.getFirst();
                      attribute != NULL;
                      attribute = atts.getNext())
                {
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                    {
                        if(stricmp(attribute, "cn") == 0)
                            user.setFullName(vals.queryValues()[0]);
                        else if(stricmp(attribute, "givenName") == 0)
                            user.setFirstName(vals.queryValues()[0]);
                        else if(stricmp(attribute, "sn") == 0)
                            user.setLastName(vals.queryValues()[0]);
                        else if(stricmp(attribute, "gidnumber") == 0)
                            ((CLdapSecUser*)&user)->setGidnumber(vals.queryValues()[0]);
                        else if(stricmp(attribute, "uidnumber") == 0)
                            ((CLdapSecUser*)&user)->setUidnumber(vals.queryValues()[0]);
                        else if(stricmp(attribute, "homedirectory") == 0)
                            ((CLdapSecUser*)&user)->setHomedirectory(vals.queryValues()[0]);
                        else if(stricmp(attribute, "loginshell") == 0)
                            ((CLdapSecUser*)&user)->setLoginshell(vals.queryValues()[0]);
                        else if(stricmp(attribute, "objectClass") == 0)
                        {
                            int valind = 0;
                            char ** values = vals.queryValues();
                            while(values[valind])
                            {
                                if(values[valind] && stricmp(values[valind], "posixAccount") == 0)
                                {
                                    ((CLdapSecUser*)&user)->setPosixenabled(true);
                                    break;
                                }
                                valind++;
                            }
                        }
                    }
                }
            }
            return true;
        }
    }

    ISecUser* lookupUser(unsigned uid)
    {
        StringBuffer sysuser;
        sysuser.append(m_ldapconfig->getSysUser());
        if(sysuser.length() == 0)
        {
#ifdef _WIN32
            char uname[128];
            unsigned long len = 128;
            int rc = GetUserName(uname, &len);
            if(rc != 0)
                sysuser.append(len, uname);
            else
                throw MakeStringException(-1, "Error getting current user's username, error code = %d", rc);
#else
            throw MakeStringException(-1, "systemUser not found in config");
#endif
        }
        
        MemoryBuffer usersidbuf;
        StringBuffer usersidstr;

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            if(m_pp != NULL)
                m_pp->lookupSid(sysuser.str(), usersidbuf);
            if(usersidbuf.length() == 0)
            {
                throw MakeStringException(-1, "system user %s's SID not found", sysuser.str());
            }

            int sidlen = usersidbuf.length();
            char* uidbuf = (char*)&uid;
            for(int i = 0; i < 4; i++)
            {
                usersidbuf.writeDirect(sidlen -4 + i, 1, (uidbuf + 3 - i));
            }
            LdapUtils::bin2str(usersidbuf, usersidstr);
        }
        else
        {
            usersidbuf.append(uid);
        }

        char        *attribute;
        LDAPMessage *message;

        StringBuffer filter;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            filter.append("objectSid=").append(usersidstr.str());
        }
        else
        {
            filter.appendf("entryid=%d", uid);
        }

        TIMEVAL timeOut = {LDAPTIMEOUT,0}; 

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        char* act_fieldname;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            act_fieldname = "sAMAccountName";
        }
        else
        {
            act_fieldname = "uid";
        }
            
        char        *attrs[] = {"cn", act_fieldname, NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
            return NULL;
        }

        if(ldap_count_entries(ld, searchResult) < 1)
        {
            DBGLOG("No entries are found for user with uid %0X", uid);
            return NULL;
        }

        CLdapSecUser* ldapuser = new CLdapSecUser("", "");

        // Go through the search results by checking message types
        for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (vals.hasValues())
                {
                    if(stricmp(attribute, "cn") == 0)
                        ldapuser->setFullName(vals.queryValues()[0]);
                    else if(stricmp(attribute, act_fieldname) == 0)
                        ldapuser->setName(vals.queryValues()[0]);
                }
            }
        }

        ldapuser->setUserID(uid);
        ldapuser->setUserSid(usersidbuf.length(), usersidbuf.toByteArray());
        
        // Since we've got the SID for the user, cache it for later uses.
        MemoryBuffer mb;
        if(m_pp != NULL)
            m_pp->getCachedSid(ldapuser->getName(), mb);
        if(mb.length() == 0)
        {
            m_pp->cacheSid(ldapuser->getName(), usersidbuf.length(), usersidbuf.toByteArray());
        }

        return ldapuser;
    }

    bool lookupAccount(MemoryBuffer& sidbuf, StringBuffer& account_name, ACT_TYPE& act_type)
    {
        char        *attribute;
        LDAPMessage *message;

        char* act_fieldname;

        StringBuffer filter;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            act_fieldname = "sAMAccountName";
            StringBuffer usersidstr;
            LdapUtils::bin2str(sidbuf, usersidstr);
            filter.append("objectSid=").append(usersidstr.str());
        }
        else
        {
            unsigned* uid = (unsigned*)sidbuf.toByteArray();
            filter.appendf("entryid=%d", *uid);
            act_fieldname = "uid";
        }

        TIMEVAL timeOut = {LDAPTIMEOUT,0}; 

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        
        char  *attrs[] = {"cn", act_fieldname, "objectClass", NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
            return false;
        }

        if(ldap_count_entries(ld, searchResult) < 1)
        {
            searchResult.ldapMsgFree();
            rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getGroupBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,   &searchResult.msg );
            if(ldap_count_entries(ld, searchResult) < 1)
            {
                searchResult.ldapMsgFree();
                rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getSysUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );
                //DBGLOG("No entries are found");
                return false;
            }
        }

        StringBuffer act_name;
        StringBuffer cnbuf;

        // Go through the search results by checking message types
        for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (vals.hasValues())
                {
                    if(stricmp(attribute, act_fieldname) == 0)
                        act_name.clear().append(vals.queryValues()[0]);
                    else if(stricmp(attribute, "cn") == 0)
                        cnbuf.clear().append(vals.queryValues()[0]);
                    else if(stricmp(attribute, "objectClass") == 0)
                    {
                        int i = 0;
                        while(vals.queryValues()[i] != NULL)
                        {
                            if(stricmp(vals.queryValues()[i], "person") == 0)
                                act_type = USER_ACT;
                            if(stricmp(vals.queryValues()[i], "group") == 0)
                                act_type = GROUP_ACT;
                            i++;
                        }
                    }
                }
            }
        }

        if(act_type == USER_ACT)
            account_name.append(act_name.str());
        else
            account_name.append(cnbuf.str());

        return true;
    }

    virtual void lookupSid(const char* basedn, const char* filter, MemoryBuffer& act_sid)
    {
        char        *attribute;       
        LDAPMessage *message;

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        char* fieldname;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            fieldname = "objectSid";
        else
            fieldname = "entryid";

        char        *attrs[] = {fieldname, NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)basedn, LDAP_SCOPE_SUBTREE, (char*)filter, attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter, basedn);
            return;
        }

        message = LdapFirstEntry( ld, searchResult);
        if(message != NULL)
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                if(0 == stricmp(attribute, fieldname))
                {
                    CLDAPGetValuesLenWrapper valsLen(ld, message, attribute);
                    if (valsLen.hasValues())
                    {
                        struct berval* val = valsLen.queryValues()[0];
                        if(val != NULL)
                        {
                            int len = val->bv_len;
                            act_sid.append(val->bv_len, val->bv_val);
                        }
                    }
                    break;
                }
            }
        }
    }

    virtual void lookupSid(const char* act_name, MemoryBuffer& act_sid, ACT_TYPE act_type)
    {
        StringBuffer filter;
        const char* basedn;
        if(act_type == USER_ACT)
        {
            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                filter.append("sAMAccountName=").append(act_name);
            else
                filter.append("uid=").append(act_name);

            basedn = m_ldapconfig->getUserBasedn();
            lookupSid(basedn, filter.str(), act_sid);
            if(act_sid.length() == 0)
            {
                StringBuffer basebuf;
                if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                    basebuf.append("cn=Users,").append(m_ldapconfig->getBasedn());
                else if(stricmp(act_name, m_ldapconfig->getSysUser()) == 0)
                    basebuf.append(m_ldapconfig->getSysUserBasedn());
                else
                    basebuf.append("ou=People,").append(m_ldapconfig->getBasedn());

                lookupSid(basebuf.str(), filter.str(), act_sid);
            }
        }
        else
        {
            filter.append("cn=").append(act_name);
            basedn = m_ldapconfig->getGroupBasedn();
            lookupSid(basedn, filter.str(), act_sid);
            if(act_sid.length() == 0)
            {
                StringBuffer basebuf;
                basebuf.append("cn=Users,").append(m_ldapconfig->getBasedn());
                lookupSid(basebuf.str(), filter.str(), act_sid);
                if(act_sid.length() == 0)
                {
                    basebuf.clear();
                    basebuf.append("cn=Builtin,").append(m_ldapconfig->getBasedn());
                    lookupSid(basebuf.str(), filter.str(), act_sid);
                }
            }       
        }

    }

    virtual void setPermissionProcessor(IPermissionProcessor* pp)
    {
        m_pp = pp;
    }

    virtual bool retrieveUsers(IUserArray& users)
    {
        return retrieveUsers("", users);
    }

    virtual bool retrieveUsers(const char* searchstr, IUserArray& users)
    {
        char        *attribute;
        LDAPMessage *message;

        StringBuffer filter;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            filter.append("objectClass=User");
        else
            filter.append("objectClass=inetorgperson");

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        char* act_fieldname;
        char* sid_fieldname;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            act_fieldname = "sAMAccountName";
            sid_fieldname = "objectSid";
        }
        else
        {
            act_fieldname = "uid";
            sid_fieldname = "entryid";
        }

        if(searchstr && *searchstr && strcmp(searchstr, "*") != 0)
        {
            filter.insert(0, "(&(");
            filter.appendf(")(|(%s=*%s*)(%s=*%s*)(%s=*%s*)))", act_fieldname, searchstr, "givenName", searchstr, "sn", searchstr);
        }

        char *attrs[] = {act_fieldname, sid_fieldname, "cn", "userAccountControl", "pwdLastSet", NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );
        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
            return false;
        }


        // Go through the search results by checking message types
        for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            bool accountPwdNeverExpires = false;
            Owned<ISecUser> user = new CLdapSecUser("", "");

            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                if(stricmp(attribute, "cn") == 0)
                {
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                        user->setFullName(vals.queryValues()[0]);
                }
                else if (stricmp(attribute, "userAccountControl") == 0)
                {
                    //UF_DONT_EXPIRE_PASSWD 0x10000
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                        if (atoi((char*)vals.queryValues()[0]) & 0x10000)//this can be true at the account level, even if domain policy requires password
                            accountPwdNeverExpires = true;
                }
                else if(stricmp(attribute, "pwdLastSet") == 0)
                {
                    CDateTime expiry;
                    if (!m_domainPwdsNeverExpire && !accountPwdNeverExpires)
                    {
                        CLDAPGetValuesLenWrapper valsLen(ld, message, attribute);
                        if (valsLen.hasValues())
                        {
                            struct berval* val = valsLen.queryValues()[0];
                            calcPWExpiry(expiry, (unsigned)val->bv_len, val->bv_val);
                        }
                    }
                    else
                        expiry.clear();
                    user->setPasswordExpiration(expiry);
                }
                else if(stricmp(attribute, act_fieldname) == 0)
                {
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                        user->setName(vals.queryValues()[0]);
                }
                else if(stricmp(attribute, sid_fieldname) == 0)
                {
                    if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                    {
                        CLDAPGetValuesLenWrapper valsLen(ld, message, attribute);
                        if (valsLen.hasValues())
                        {
                            struct berval* val = valsLen.queryValues()[0];
                            if(val != NULL)
                            {
                                unsigned uid = val->bv_val[val->bv_len - 4];
                                int i;
                                for(i = 3; i > 0; i--)
                                {
                                    uid = (uid << 8) + val->bv_val[val->bv_len - i];
                                }
                                ((CLdapSecUser*)user.get())->setUserID(uid);
                            }
                        }
                    }
                    else
                    {
                        CLDAPGetValuesWrapper vals(ld, message, attribute);
                        if (vals.hasValues())
                            ((CLdapSecUser*)user.get())->setUserID(atoi(vals.queryValues()[0]));
                    }
                }
            }
            users.append(*LINK(user.get()));
        }
        return true;
    }

    virtual bool userInGroup(const char* userdn, const char* groupdn)
    {
        const char* fldname;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            fldname = "member";
        else
            fldname = "uniquemember";

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        int rc = ldap_compare_s(ld, (char*)groupdn, (char*)fldname, (char*)userdn);
        if(rc == LDAP_COMPARE_TRUE)
            return true;
        else
            return false;
    }

    // Update user's firstname, lastname (plus displayname for active directory).
    virtual bool updateUser(const char* type, ISecUser& user)
    {
        const char* username = user.getName();
        if(!username || !*username)
            return false;

        StringBuffer userdn;
        getUserDN(username, userdn);

        int rc = LDAP_SUCCESS;

        if(!type || !*type || stricmp(type, "names") == 0)
        {
            StringBuffer cnbuf;
            const char* fname = user.getFirstName();
            const char* lname = user.getLastName();
            if(fname && *fname && lname && *lname)
            {
                cnbuf.append(fname).append(" ").append(lname);
            }
            else
                throw MakeStringException(-1, "Please specify both firstname and lastname");

            char *gn_values[] = { (char*)fname, NULL };
            LDAPMod gn_attr = {
                LDAP_MOD_REPLACE,
                "givenName",
                gn_values
            };

            char *sn_values[] = { (char*)lname, NULL };
            LDAPMod sn_attr = {
                LDAP_MOD_REPLACE,
                "sn",
                sn_values
            };

            char *cn_values[] = {(char*)cnbuf.str(), NULL };
            LDAPMod cn_attr = 
            {
                LDAP_MOD_REPLACE,
                "cn",
                cn_values
            };

            char *dispname_values[] = {(char*)cnbuf.str(), NULL };
            LDAPMod dispname_attr = 
            {
                LDAP_MOD_REPLACE,
                "displayName",
                dispname_values
            };

            LDAPMod *attrs[4];
            int ind = 0;
        
            attrs[ind++] = &gn_attr;
            attrs[ind++] = &sn_attr;

            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            {
                attrs[ind++] = &dispname_attr;
            }
            else
            {
                attrs[ind++] = &cn_attr;
            }
            
            attrs[ind] = NULL;
            
            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

            rc = ldap_modify_s(ld, (char*)userdn.str(), attrs);
            if (rc == LDAP_SUCCESS && m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            {
                StringBuffer newrdn("cn=");
                newrdn.append(cnbuf.str());
                rc = LdapRename(ld, (char*)userdn.str(), (char*)newrdn.str(), NULL, true, NULL, NULL);
            }
        }
        else if(stricmp(type, "posixenable") == 0)
        {
            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                throw MakeStringException(-1, "posixAccount isn't applicable to Active Directory");

            CLdapSecUser* ldapuser = dynamic_cast<CLdapSecUser*>(&user);

            char* oc_values[] = {"posixAccount", NULL};
            LDAPMod oc_attr = {
                LDAP_MOD_ADD,
                "objectclass",
                oc_values
            };

            char* oc1_values[] = {"shadowAccount", NULL};
            LDAPMod oc1_attr = {
                LDAP_MOD_ADD,
                "objectclass",
                oc1_values
            };

            char *gidnum_values[] = { (char*)ldapuser->getGidnumber(), NULL };
            LDAPMod gidnum_attr = {
                LDAP_MOD_REPLACE,
                "gidnumber",
                gidnum_values
            };

            char *uidnum_values[] = { (char*)ldapuser->getUidnumber(), NULL };
            LDAPMod uidnum_attr = {
                LDAP_MOD_REPLACE,
                "uidnumber",
                uidnum_values
            };

            char *homedir_values[] = {(char*)ldapuser->getHomedirectory(), NULL };
            LDAPMod homedir_attr = 
            {
                LDAP_MOD_REPLACE,
                "homedirectory",
                homedir_values
            };

            char *loginshell_values[] = {(char*)ldapuser->getLoginshell(), NULL };
            LDAPMod loginshell_attr = 
            {
                LDAP_MOD_REPLACE,
                "loginshell",
                loginshell_values
            };

            LDAPMod *attrs[7];
            int ind = 0;
        
            attrs[ind++] = &gidnum_attr;
            attrs[ind++] = &uidnum_attr;
            attrs[ind++] = &homedir_attr;
            attrs[ind++] = &loginshell_attr;
            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
            int compresult = ldap_compare_s(ld, (char*)userdn.str(), (char*)"objectclass", (char*)"posixAccount");
            if(compresult != LDAP_COMPARE_TRUE)
                attrs[ind++] = &oc_attr;
            compresult = ldap_compare_s(ld, (char*)userdn.str(), (char*)"objectclass", (char*)"shadowAccount");
            if(compresult != LDAP_COMPARE_TRUE)
                attrs[ind++] = &oc1_attr;
            attrs[ind] = NULL;
            rc = ldap_modify_s(ld, (char*)userdn.str(), attrs);
        }
        else if(stricmp(type, "posixdisable") == 0)
        {
            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                throw MakeStringException(-1, "posixAccount isn't applicable to Active Directory");

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
            int compresult = ldap_compare_s(ld, (char*)userdn.str(), (char*)"objectclass", (char*)"posixAccount");
            if(compresult != LDAP_COMPARE_TRUE)
            {
                rc = LDAP_SUCCESS;
            }
            else
            {

                CLdapSecUser* ldapuser = dynamic_cast<CLdapSecUser*>(&user);

                char* oc_values[] = {"posixAccount", NULL};
                LDAPMod oc_attr = {
                    LDAP_MOD_DELETE,
                    "objectclass",
                    oc_values
                };

                char* oc1_values[] = {"shadowAccount", NULL};
                LDAPMod oc1_attr = {
                    LDAP_MOD_DELETE,
                    "objectclass",
                    oc1_values
                };

                char *gidnum_values[] = { NULL };
                LDAPMod gidnum_attr = {
                    LDAP_MOD_DELETE,
                    "gidnumber",
                    gidnum_values
                };

                char *uidnum_values[] = {NULL };
                LDAPMod uidnum_attr = {
                    LDAP_MOD_DELETE,
                    "uidnumber",
                    uidnum_values
                };

                char *homedir_values[] = { NULL };
                LDAPMod homedir_attr = 
                {
                    LDAP_MOD_DELETE,
                    "homedirectory",
                    homedir_values
                };

                char *loginshell_values[] = { NULL };
                LDAPMod loginshell_attr = 
                {
                    LDAP_MOD_DELETE,
                    "loginshell",
                    loginshell_values
                };

                LDAPMod *attrs[7];
                int ind = 0;
            
                attrs[ind++] = &gidnum_attr;
                attrs[ind++] = &uidnum_attr;
                attrs[ind++] = &homedir_attr;
                attrs[ind++] = &loginshell_attr;
                attrs[ind++] = &oc_attr;
                attrs[ind++] = &oc1_attr;
                attrs[ind] = NULL;

                rc = ldap_modify_s(ld, (char*)userdn.str(), attrs);
            }
        }
        else if(stricmp(type, "sudoersadd") == 0)
        {
            CLdapSecUser* ldapuser = dynamic_cast<CLdapSecUser*>(&user);

            char *cn_values[] = {(char*)username, NULL };
            LDAPMod cn_attr = 
            {
                LDAP_MOD_ADD,
                "cn",
                cn_values
            };

            char *oc_values[] = {"sudoRole", NULL };
            LDAPMod oc_attr =
            {
                LDAP_MOD_ADD,
                "objectClass",
                oc_values
            };

            char *user_values[] = {(char*)username, NULL };
            LDAPMod user_attr = 
            {
                LDAP_MOD_ADD,
                "sudoUser",
                user_values
            };

            char* sudoHost = (char*)ldapuser->getSudoHost();
            char* sudoCommand = (char*)ldapuser->getSudoCommand();
            char* sudoOption = (char*)ldapuser->getSudoOption();

            char *host_values[] = {sudoHost, NULL };
            LDAPMod host_attr = 
            {
                LDAP_MOD_ADD,
                "sudoHost",
                host_values
            };
            char *cmd_values[] = {sudoCommand, NULL };
            LDAPMod cmd_attr = 
            {
                LDAP_MOD_ADD,
                "sudoCommand",
                cmd_values
            };
            char *option_values[] = {sudoOption, NULL };
            LDAPMod option_attr = 
            {
                LDAP_MOD_ADD,
                "sudoOption",
                option_values
            };

            LDAPMod *attrs[8];
            int ind = 0;
            
            attrs[ind++] = &cn_attr;
            attrs[ind++] = &oc_attr;
            attrs[ind++] = &user_attr;
            if(sudoHost && *sudoHost)
                attrs[ind++] = &host_attr;
            if(sudoCommand && *sudoCommand)
                attrs[ind++] = &cmd_attr;
            if(sudoOption && *sudoOption)
                attrs[ind++] = &option_attr;

            attrs[ind] = NULL;

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
            StringBuffer dn;
            dn.append("cn=").append(username).append(",").append(m_ldapconfig->getResourceBasedn(RT_SUDOERS));
            int rc = ldap_add_ext_s(ld, (char*)dn.str(), attrs, NULL, NULL);
            if ( rc != LDAP_SUCCESS )
            {
                if(rc == LDAP_ALREADY_EXISTS)
                {
                    throw MakeStringException(-1, "can't add %s to sudoers, an LDAP object with this name already exists", username);
                }
                else
                {
                    DBGLOG("error adding %s to sudoers: %s", username, ldap_err2string( rc ));
                    throw MakeStringException(-1, "error adding %s to sudoers: %s", username, ldap_err2string( rc ));
                }
            }
        }
        else if(stricmp(type, "sudoersdelete") == 0)
        {
            StringBuffer dn;
            dn.append("cn=").append(username).append(",").append(m_ldapconfig->getResourceBasedn(RT_SUDOERS));

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
            
            int rc = ldap_delete_s(ld, (char*)dn.str());

            if ( rc != LDAP_SUCCESS )
            {
                throw MakeStringException(-1, "Error deleting user %s from sudoers: %s", username, ldap_err2string(rc));
            }
        }
        else if(stricmp(type, "sudoersupdate") == 0)
        {
            CLdapSecUser* ldapuser = dynamic_cast<CLdapSecUser*>(&user);
            
            char* sudoHost = (char*)ldapuser->getSudoHost();
            char* sudoCommand = (char*)ldapuser->getSudoCommand();
            char* sudoOption = (char*)ldapuser->getSudoOption();

            char *host_values[] = {(sudoHost&&*sudoHost)?sudoHost:NULL, NULL };
            LDAPMod host_attr = 
            {
                LDAP_MOD_REPLACE,
                "sudoHost",
                host_values
            };
            
            char *cmd_values[] = {(sudoCommand&&*sudoCommand)?sudoCommand:NULL, NULL };
            LDAPMod cmd_attr = 
            {
                LDAP_MOD_REPLACE,
                "sudoCommand",
                cmd_values
            };
            
            char *option_values[] = {(sudoOption&&*sudoOption)?sudoOption:NULL, NULL };
            LDAPMod option_attr = 
            {
                LDAP_MOD_REPLACE,
                "sudoOption",
                option_values
            };

            LDAPMod *attrs[4];
            int ind = 0;
            
            attrs[ind++] = &host_attr;
            attrs[ind++] = &cmd_attr;
            attrs[ind++] = &option_attr;

            attrs[ind] = NULL;

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
            StringBuffer dn;
            dn.append("cn=").append(username).append(",").append(m_ldapconfig->getResourceBasedn(RT_SUDOERS));
            int rc = ldap_modify_ext_s(ld, (char*)dn.str(), attrs, NULL, NULL);
            if ( rc != LDAP_SUCCESS )
            {
                DBGLOG("error modifying sudoers for user %s: %s", username, ldap_err2string( rc ));
                throw MakeStringException(-1, "error modifying sudoers for user %s: %s", username, ldap_err2string( rc ));
            }
        }

        if (rc == LDAP_SUCCESS )
            DBGLOG("User %s successfully updated", username);
        else
            throw MakeStringException(-1, "Error updating user %s - %s", username, ldap_err2string( rc ));

        return true;
    }

    virtual bool changePasswordSSL(const char* username, const char* newPassword)
    {
        Owned<ILdapConnection> lconn;
        try
        {
            lconn.setown(m_connections->getSSLConnection());
        }
        catch(IException*)
        {
            throw MakeStringException(-1, "Failed to set user %s's password because of not being able to create an SSL connection to the ldap server. To set an Active Directory user's password from Linux, you need to enable SSL on the Active Directory ldap server", username);
        }

        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        char        *attribute, **values = NULL;       
        LDAPMessage *message;

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        StringBuffer filter;
        filter.append("sAMAccountName=").append(username);

        char        *attrs[] = {"distinguishedName", NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
            return false;
        }

        StringBuffer userdn;
        message = LdapFirstEntry( ld, searchResult);
        if(message != NULL)
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                if(0 == stricmp(attribute, "distinguishedName"))
                {
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                        userdn.set(vals.queryValues()[0]);
                    break;
                }
            }
        }

        if(userdn.length() == 0)
        {
            throw MakeStringException(-1, "can't find dn for user %s", username);
        }

        LDAPMod modPassword;
        LDAPMod *modEntry[2];
        struct berval pwdBerVal;
        struct berval *pwd_attr[2];
        unsigned short pszPasswordWithQuotes[1024];

        modEntry[0] = &modPassword;
        modEntry[1] = NULL;

        modPassword.mod_op = LDAP_MOD_REPLACE | LDAP_MOD_BVALUES;
        modPassword.mod_type =  "unicodePwd";
        modPassword.mod_vals.modv_bvals = pwd_attr;

        pwd_attr[0] = &pwdBerVal;
        pwd_attr[1]= NULL;

        StringBuffer quotedPasswd("\"");
        quotedPasswd.append(newPassword).append("\"");
        ConvertCToW(pszPasswordWithQuotes, quotedPasswd);

        pwdBerVal.bv_len = quotedPasswd.length() * sizeof(unsigned short);
        pwdBerVal.bv_val = (char*)pszPasswordWithQuotes;

        rc = ldap_modify_s(ld, (char*)userdn.str(), modEntry);

        if (rc == LDAP_SUCCESS )
            DBGLOG("User %s's password has been changed successfully", username);
        else
        {
            StringBuffer errmsg;
            errmsg.appendf("Error setting password for %s - (%d) %s.", username, rc, ldap_err2string( rc ));
            if(rc == LDAP_UNWILLING_TO_PERFORM)
                errmsg.append(" The ldap server refused to change the password. Usually this is because your new password doesn't satisfy the domain policy.");

            throw MakeStringExceptionDirect(-1, errmsg.str());
        }

        return true;
    }

    virtual bool queryPasswordStatus(ISecUser& user, const char* password)
    {
        char *ldap_errstring = NULL;
        const char * username = user.getName();

        StringBuffer userdn;
        getUserDN(user.getName(), userdn);

        StringBuffer hostbuf;
        m_ldapconfig->getLdapHost(hostbuf);

        LDAP* user_ld = LdapUtils::LdapInit(m_ldapconfig->getProtocol(), hostbuf.str(), m_ldapconfig->getLdapPort(), m_ldapconfig->getLdapSecurePort());
        int rc = LdapUtils::LdapBind(user_ld, m_ldapconfig->getDomain(), username, password, userdn, m_ldapconfig->getServerType(), m_ldapconfig->getAuthMethod());
        if(rc != LDAP_SUCCESS)
            ldap_get_option(user_ld, LDAP_OPT_ERROR_STRING, &ldap_errstring);
        ldap_unbind(user_ld);

        //Error string ""80090308: LdapErr: DSID-0C0903A9, comment: AcceptSecurityContext error, data 532, v1db0."
        //is returned if pw valid but expired
        if(rc == LDAP_SUCCESS || strstr(ldap_errstring, "data 532") || strstr(ldap_errstring, "data 773"))//
            return true;
        else
            return false;
    }

    virtual bool updateUserPassword(ISecUser& user, const char* newPassword, const char* currPassword)
    {
        const char* username = user.getName();
        if(!username || !*username)
            return false;

        if (currPassword)
        {
            //User will not be authenticated if their password was expired,
            //so check here that they provided a valid one in the "change
            //password" form (use the one they type, not the one in the secuser)
            bool validated = queryPasswordStatus(user, currPassword);
            if (!validated)
                throw MakeStringException(-1, "Password not changed, invalid credentials");
        }

        return updateUserPassword(username, newPassword);
    }

    virtual bool updateUserPassword(const char* username, const char* newPassword)
    {
        if(!username || !*username)
            return false;
        
        const char* sysuser = m_ldapconfig->getSysUser();
        if(sysuser && *sysuser && strcmp(username, sysuser) == 0)
            throw MakeStringException(-1, "You can't change password of the system user.");

        LdapServerType servertype = m_ldapconfig->getServerType();
        bool ret = true;
        if(servertype == ACTIVE_DIRECTORY)
        {
#ifdef _WIN32
            DWORD nStatus = 0;
            // The application has to run on the same domain as ldap host, and under an administrative user.
            USER_INFO_1003 usriSetPassword;
            StringBuffer fullserver("\\\\");
            StringBuffer server;
            m_ldapconfig->getLdapHost(server);
            fullserver.append(server.str());
            LPWSTR whost = (LPWSTR)alloca((fullserver.length() +1) * sizeof(WCHAR));
            ConvertCToW((unsigned short *)whost, fullserver.str());

            LPWSTR wusername = (LPWSTR)alloca((strlen(username) + 1) * sizeof(WCHAR));
            ConvertCToW((unsigned short *)wusername, username);
            LPWSTR wnewpasswd = (LPWSTR)alloca((strlen(newPassword) + 1) * sizeof(WCHAR));
            ConvertCToW((unsigned short *)wnewpasswd, newPassword);
            usriSetPassword.usri1003_password  = wnewpasswd;
            nStatus = NetUserSetInfo(whost, wusername,  1003, (LPBYTE)&usriSetPassword, NULL);

            if (nStatus == NERR_Success)
            {
                DBGLOG("User %s's password has been changed successfully", username);
                return true;
            }
            else
            {
                StringBuffer errcode, errmsg;

                if(nStatus == ERROR_ACCESS_DENIED)
                {
                    errcode.append("ERROR_ACCESS_DENIED");
                    errmsg.append("The user does not have access to the requested information.");
                }
                else if(nStatus == ERROR_INVALID_PASSWORD)
                {
                    errcode.append("ERROR_INVALID_PASSWORD");
                    errmsg.append("The user has entered an invalid password.");
                }
                else if(nStatus == NERR_InvalidComputer)
                {
                    errcode.append("NERR_InvalidComputer");
                    errmsg.append("The computer name is invalid.");
                }
                else if(nStatus == NERR_NotPrimary)
                {
                    errcode.append("NERR_NotPrimary");
                    errmsg.append("The operation is allowed only on the primary domain controller of the domain.");
                }
                else if(nStatus == NERR_UserNotFound)
                {
                    errcode.append("NERR_UserNotFound");
                    errmsg.append("The user name could not be found.");
                }
                else if(nStatus == NERR_PasswordTooShort)
                {
                    errcode.append("NERR_PasswordTooShort");
                    errmsg.append("The password is shorter than required. ");
                }
                else if(nStatus == ERROR_LOGON_FAILURE)
                {
                    errcode.append("ERROR_LOGON_FAILURE");
                    errmsg.append("To be able to reset password this way, esp has to run under an administrative user on the same domain as the active directory. ");
                }
                else
                {
                    errcode.appendf("%d", nStatus);
                    errmsg.append("");
                }
                // For certain errors just return, other errors try changePasswordSSL.
                if(nStatus == ERROR_INVALID_PASSWORD || nStatus == NERR_UserNotFound || nStatus == NERR_PasswordTooShort)
                    throw MakeStringException(-1, "An error has occurred while setting password with NetUserSetInfo for %s: %s - %s\n", username, errcode.str(), errmsg.str());
                else
                    DBGLOG("An error has occurred while setting password with NetUserSetInfo for %s: %s - %s\n", username, errcode.str(), errmsg.str());
            }
            DBGLOG("Trying changePasswordSSL to change password over regular SSL connection.");
#endif
            changePasswordSSL(username, newPassword);
        }
        else 
        {
            StringBuffer filter;
            filter.append("uid=").append(username);

            char        **values = NULL;       
            LDAPMessage *message;

            TIMEVAL timeOut = {LDAPTIMEOUT,0};   

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

            char        *attrs[] = {LDAP_NO_ATTRS, NULL};
            CLDAPMessage searchResult;
            int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

            if ( rc != LDAP_SUCCESS )
            {
                DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
                return false;
            }

            StringBuffer userdn;
            message = LdapFirstEntry( ld, searchResult);
            
            if(message != NULL)
            {
                char *p = ldap_get_dn(ld, message);
                userdn.append(p);
                ldap_memfree(p);
            }
            char* passwdvalue[] = { (char*)newPassword, NULL };
            LDAPMod pmod = 
            {
                LDAP_MOD_REPLACE,
                "userpassword", 
                passwdvalue
            };

            LDAPMod* pmods[] = {&pmod, NULL};
            rc = ldap_modify_s(ld, (char*)userdn.str(), pmods);

            if (rc == LDAP_SUCCESS )
                DBGLOG("User %s's password has been changed successfully", username);
            else
            {
                StringBuffer errmsg;
                errmsg.appendf("Error changing password for %s - (%d) %s.", username, rc, ldap_err2string( rc ));
                if(rc == LDAP_UNWILLING_TO_PERFORM)
                    errmsg.append(" The ldap server refused to execute the password change action, one of the reasons might be that the new password you entered doesn't satisfy the policy requirement.");

                throw MakeStringExceptionDirect(-1, errmsg.str());
            }
        }
        return true;
    }

    virtual bool getResources(SecResourceType rtype, const char * basedn, const char* prefix, IArrayOf<ISecResource>& resources)
    {
        char        *attribute;
        LDAPMessage *message;

        StringBuffer basednbuf;
        LdapUtils::normalizeDn(basedn, m_ldapconfig->getBasedn(), basednbuf);
        StringBuffer filter("objectClass=*");

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        const char* fldname;
        LdapServerType servertype = m_ldapconfig->getServerType();
        if(servertype == ACTIVE_DIRECTORY && (rtype == RT_DEFAULT || rtype == RT_MODULE || rtype == RT_SERVICE))
            fldname = "name";
        else
            fldname = "ou";
        char        *attrs[] = {(char*)fldname, "description", NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)basednbuf.str(), LDAP_SCOPE_ONELEVEL, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), basednbuf.str());
            return false;
        }

        // Go through the search results by checking message types
        for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            StringBuffer descbuf;
            StringBuffer curname;
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (vals.hasValues())
                {
                    char* val = vals.queryValues()[0];
                    if(val != NULL)
                    {
                        if(stricmp(attribute, fldname) == 0)
                        {
                            curname.append(val);
                        }
                        else if(stricmp(attribute, "description") == 0)
                        {
                            descbuf.append(val);
                        }
                    }
                }
            }
            if(curname.length() == 0)
                continue;
            StringBuffer resourcename;
            if(prefix != NULL && *prefix != '\0')
                resourcename.append(prefix);
            resourcename.append(curname.str());
            CLdapSecResource* resource = new CLdapSecResource(resourcename.str());
            resource->setDescription(descbuf.str());
            resources.append(*resource);
            if(rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE)
            {
                StringBuffer nextbasedn;
                nextbasedn.append("ou=").append(curname.str()).append(",").append(basedn);
                StringBuffer nextprefix;
                if(prefix != NULL && *prefix != '\0')
                    nextprefix.append(prefix);
                nextprefix.append(curname.str()).append("::");
                getResources(rtype, nextbasedn.str(), nextprefix.str(), resources);
            }
        }
        return true;
    }

    virtual bool getResourcesEx(SecResourceType rtype, const char * basedn, const char* prefix, const char* searchstr, IArrayOf<ISecResource>& resources)
    {
        char        *attribute;
        LDAPMessage *message;

        StringBuffer basednbuf;
        LdapUtils::normalizeDn(basedn, m_ldapconfig->getBasedn(), basednbuf);
        StringBuffer filter("objectClass=*");
        if(searchstr && *searchstr && strcmp(searchstr, "*") != 0)
        {
            filter.insert(0, "(&(");
            filter.appendf(")(|(%s=*%s*)))", "uNCName", searchstr);
        }

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        const char* fldname;
        LdapServerType servertype = m_ldapconfig->getServerType();
        if(servertype == ACTIVE_DIRECTORY && (rtype == RT_DEFAULT || rtype == RT_MODULE || rtype == RT_SERVICE))
            fldname = "name";
        else
            fldname = "ou";
        char        *attrs[] = {(char*)fldname, "description", NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)basednbuf.str(), LDAP_SCOPE_ONELEVEL, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), basednbuf.str());
            return false;
        }

        // Go through the search results by checking message types
        for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            StringBuffer descbuf;
            StringBuffer curname;
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (vals.hasValues())
                {
                    char* val = vals.queryValues()[0];
                    if(val != NULL)
                    {
                        if(stricmp(attribute, fldname) == 0)
                        {
                            curname.append(val);
                        }
                        else if(stricmp(attribute, "description") == 0)
                        {
                            descbuf.append(val);
                        }
                    }
                }
            }
            if(curname.length() == 0)
                continue;
            StringBuffer resourcename;
            if(prefix != NULL && *prefix != '\0')
                resourcename.append(prefix);
            resourcename.append(curname.str());
            CLdapSecResource* resource = new CLdapSecResource(resourcename.str());
            resource->setDescription(descbuf.str());
            resources.append(*resource);
            if(rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE)
            {
                StringBuffer nextbasedn;
                nextbasedn.append("ou=").append(curname.str()).append(",").append(basedn);
                StringBuffer nextprefix;
                if(prefix != NULL && *prefix != '\0')
                    nextprefix.append(prefix);
                nextprefix.append(curname.str()).append("::");
                getResources(rtype, nextbasedn.str(), nextprefix.str(), resources);
            }
        }
        return true;
    }

    virtual bool getPermissionsArray(const char* basedn, SecResourceType rtype, const char* name, IArrayOf<CPermission>& permissions)
    {
        StringBuffer basednbuf;
        LdapUtils::normalizeDn(basedn, m_ldapconfig->getBasedn(), basednbuf);
        Owned<CSecurityDescriptor> sd = new CSecurityDescriptor(name);
        IArrayOf<CSecurityDescriptor> sdlist;
        sdlist.append(*LINK(sd));
        if(rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE)
            getSecurityDescriptorsScope(sdlist, basednbuf.str());
        else
            getSecurityDescriptors(sdlist, basednbuf.str());

        m_pp->getPermissionsArray(sd.get(), permissions);

        return true;
    }

    virtual void getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions)
    {
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            groups.append("Authenticated Users");
            managedBy.append("");
            descriptions.append("");
            groups.append("Administrators");
            managedBy.append("");
            descriptions.append("");
        }
        else
        {
            groups.append("Directory Administrators");
            managedBy.append("");
            descriptions.append("");
        }

        char        *attribute;
        LDAPMessage *message;

        StringBuffer filter;

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            filter.append("objectClass=group");
        else
            filter.append("objectClass=groupofuniquenames");

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        char *attrs[] = {"cn", "managedBy", "description", NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getGroupBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,   &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getGroupBasedn());
            return;
        }

        // Go through the search results by checking message types
        for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (!vals.hasValues())
                    continue;
                if(stricmp(attribute, "cn") == 0)
                {
                    groups.append(vals.queryValues()[0]);
                    managedBy.append("");
                    descriptions.append("");
                }
                else if(stricmp(attribute, "managedBy") == 0)
                    managedBy.replace(vals.queryValues()[0], groups.length() - 1);
                else if(stricmp(attribute, "description") == 0)
                    descriptions.replace(vals.queryValues()[0], groups.length() - 1);
            }
        }
    }

    virtual bool changePermission(CPermissionAction& action)
    {
        StringBuffer basednbuf;
        LdapUtils::normalizeDn(action.m_basedn.str(), m_ldapconfig->getBasedn(), basednbuf);
        Owned<CSecurityDescriptor> sd = new CSecurityDescriptor(action.m_rname.str());
        IArrayOf<CSecurityDescriptor> sdlist;
        sdlist.append(*LINK(sd));
        if(action.m_rtype == RT_FILE_SCOPE || action.m_rtype == RT_WORKUNIT_SCOPE)
            getSecurityDescriptorsScope(sdlist, basednbuf.str());
        else
            getSecurityDescriptors(sdlist, basednbuf.str());

        if(m_ldapconfig->getServerType() != ACTIVE_DIRECTORY)
        {
            StringBuffer act_dn;
            if(action.m_account_type == GROUP_ACT)
                getGroupDN(action.m_account_name.str(), act_dn);
            else
                getUserDN(action.m_account_name.str(), act_dn);
            
            action.m_account_name.clear().append(act_dn.str());
        }

        Owned<CSecurityDescriptor> newsd = m_pp->changePermission(sd.get(), action);

        StringBuffer normdnbuf;
        LdapServerType servertype = m_ldapconfig->getServerType();
        name2dn(action.m_rtype, action.m_rname.str(), action.m_basedn.str(), normdnbuf);

        char *empty_values[] = { NULL };

        int numberOfSegs = m_pp->sdSegments(newsd.get());

        LDAPMod *attrs[2];
        LDAPMod sd_attr;
        if(newsd->getDescriptor().length() > 0)
        {
            struct berval** sd_values = (struct berval**)alloca(sizeof(struct berval*)*(numberOfSegs+1));
            MemoryBuffer& sdbuf = newsd->getDescriptor();

            // Active Directory acutally has only one segment.
            if(servertype == ACTIVE_DIRECTORY)
            {
                struct berval* sd_val = (struct berval*)alloca(sizeof(struct berval));
                sd_val->bv_len = sdbuf.length();
                sd_val->bv_val = (char*)sdbuf.toByteArray();
                sd_values[0] = sd_val;
                sd_values[1] = NULL;

                sd_attr.mod_type = "nTSecurityDescriptor";
            }
            else
            {
                const char* bbptr = sdbuf.toByteArray();
                const char* bptr = sdbuf.toByteArray();
                int sdbuflen = sdbuf.length();
                int segind;
                for(segind = 0; segind < numberOfSegs; segind++)
                {
                    if(bptr - bbptr >= sdbuflen)
                        break;
                    while(*bptr == '\0' && (bptr - bbptr) < sdbuflen)
                        bptr++;

                    const char* eptr = bptr;
                    while(*eptr != '\0' && (eptr - bbptr) < sdbuflen)
                        eptr++;

                    struct berval* sd_val = (struct berval*)alloca(sizeof(struct berval));
                    sd_val->bv_len = eptr - bptr;
                    sd_val->bv_val = (char*)bptr;
                    sd_values[segind] = sd_val;

                    bptr = eptr + 1;
                }
                sd_values[segind] = NULL;

                sd_attr.mod_type = (char*)m_ldapconfig->getSdFieldName();
            }
            sd_attr.mod_op = LDAP_MOD_REPLACE | LDAP_MOD_BVALUES;
            sd_attr.mod_vals.modv_bvals = sd_values;

            attrs[0] = &sd_attr;
        }
        else
        {
            if(m_ldapconfig->getServerType() == OPEN_LDAP)
                throw MakeStringException(-1, "removing all permissions for openldap is currently not supported");

            sd_attr.mod_op = LDAP_MOD_DELETE;
            sd_attr.mod_type = (char*)m_ldapconfig->getSdFieldName();
            sd_attr.mod_vals.modv_strvals = empty_values;
            attrs[0] = &sd_attr;
        }

        attrs[1] = NULL;

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        int rc = ldap_modify_s(ld, (char*)normdnbuf.str(), attrs);
        if ( rc != LDAP_SUCCESS )
        {
            throw MakeStringException(-1, "ldap_modify_s error: %d %s", rc, ldap_err2string( rc ));
        }

        return true;
    }

    virtual void getGroups(const char *user, StringArray& groups)
    {
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            char        *attribute;
            LDAPMessage *message;

            if(user == NULL || strlen(user) == 0)
                return;
            StringBuffer filter("sAMAccountName=");
            filter.append(user);

            TIMEVAL timeOut = {LDAPTIMEOUT,0};   

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

            char        *attrs[] = {"memberOf", NULL};
            CLDAPMessage searchResult;
            int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

            if ( rc != LDAP_SUCCESS )
            {
                DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
                return;
            }
            
            unsigned entries = ldap_count_entries(ld, searchResult);
            if(entries == 0)
            {
                searchResult.ldapMsgFree();
                rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getSysUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );

                if ( rc != LDAP_SUCCESS )
                {
                    DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getSysUserBasedn());
                    return;
                }
            }

            // Go through the search results by checking message types
            for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
            {
                CLDAPGetAttributesWrapper   atts(ld, searchResult);
                for ( attribute = atts.getFirst();
                      attribute != NULL;
                      attribute = atts.getNext())
                {
                    if(0 == stricmp(attribute, "memberOf"))
                    {
                        CLDAPGetValuesWrapper vals(ld, message, attribute);
                        if (vals.hasValues())
                        {
                            for (int i = 0; vals.queryValues()[ i ] != NULL; i++ )
                            {
                                char* val = vals.queryValues()[i];
                                char* comma = strchr(val, ',');
                                StringBuffer groupname;
                                groupname.append(comma - val -3, val+3);
                                groups.append(groupname.str());
                            }
                        }
                    }
                }
            }
        }
        else
        {
            StringArray allgroups;
            StringArray allgroupManagedBy;
            StringArray allgroupDescription;
            getAllGroups(allgroups, allgroupManagedBy, allgroupDescription);
            for(unsigned i = 0; i < allgroups.length(); i++)
            {
                const char* grp = allgroups.item(i);
                StringBuffer grpdn, usrdn;
                getUserDN(user, usrdn);
                getGroupDN(grp, grpdn);
                if(userInGroup(usrdn.str(), grpdn.str()))
                {
                    groups.append(grp);
                }
            }
        }       
    }

    virtual void changeUserGroup(const char* action, const char* username, const char* groupname)
    {
        StringBuffer userdn, groupdn;
        getUserDN(username, userdn);
        getGroupDN(groupname, groupdn);
        // Not needed for Active Directory
        // changeUserMemberOf(action, userdn.str(), groupdn.str());
        changeGroupMember(action, groupdn.str(), userdn.str());
    }

    virtual bool deleteUser(ISecUser* user)
    {
        if(user == NULL)
            return false;
        const char* username = user->getName();
        if(username == NULL || *username == '\0')
            return false;

        StringBuffer userdn;
        getUserDN(username, userdn);
        
        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        
        int rc = ldap_delete_s(ld, (char*)userdn.str());

        if ( rc != LDAP_SUCCESS )
        {
            throw MakeStringException(-1, "error deleting user %s: %s", username, ldap_err2string(rc));
        }

        StringArray grps;
        getGroups(username, grps);
        ForEachItemIn(x, grps)
        {
            const char* grp = grps.item(x);
            if(!grp || !*grp)
                continue;
            changeUserGroup("delete", username, grp);
        }

        //Remove tempfile scope for this user
        StringBuffer resName(queryDfsXmlBranchName(DXB_Internal));
        resName.append("::").append(username);
        deleteResource(RT_FILE_SCOPE, resName.str(), m_ldapconfig->getResourceBasedn(RT_FILE_SCOPE));
        
        return true;
    }

    virtual void addGroup(const char* groupname, const char * groupOwner, const char * groupDesc)
    {
        if(groupname == NULL || *groupname == '\0')
            throw MakeStringException(-1, "Can't add group, groupname is empty");

        addGroup(groupname, groupOwner, groupDesc, m_ldapconfig->getGroupBasedn());
    }

    virtual void addGroup(const char* groupname, const char * groupOwner, const char * groupDesc, const char* basedn)
    {
        if(groupname == NULL || *groupname == '\0')
            return;

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            if(stricmp(groupname, "Administrators") == 0)
                throw MakeStringException(-1, "Can't add group %s, it's reserved by the system.", groupname);
        }
        else
        {
            if(stricmp(groupname, "Directory Administrators") == 0)
                throw MakeStringException(-1, "Can't add group %s, it's reserved by the system.", groupname);
        }

        StringBuffer dn;
        dn.append("cn=").append(groupname).append(",").append(basedn);

        char* oc_name;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            oc_name = "group";
        }
        else
        {
            oc_name = "groupofuniquenames";
        }

        char *cn_values[] = {(char*)groupname, NULL };
        LDAPMod cn_attr = 
        {
            LDAP_MOD_ADD,
            "cn",
            cn_values
        };

        char *oc_values[] = {oc_name, NULL };
        LDAPMod oc_attr =
        {
            LDAP_MOD_ADD,
            "objectClass",
            oc_values
        };

        char *member_values[] = {"", NULL};
        LDAPMod member_attr = 
        {
            LDAP_MOD_ADD,
            "uniqueMember",
            member_values
        };

        char *owner_values[] = {(char*)groupOwner, NULL};
        LDAPMod owner_attr =
        {
            LDAP_MOD_ADD,
            "managedBy",
            owner_values
        };
        char *desc_values[] = {(char*)groupDesc, NULL};
        LDAPMod desc_attr =
        {
            LDAP_MOD_ADD,
            "description",
            desc_values
        };
        LDAPMod *attrs[6];
        int ind = 0;
        
        attrs[ind++] = &cn_attr;
        attrs[ind++] = &oc_attr;
        if (groupOwner && *groupOwner)
            attrs[ind++] = &owner_attr;
        if (groupDesc && *groupDesc)
            attrs[ind++] = &desc_attr;
        attrs[ind] = NULL;

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        int rc = ldap_add_ext_s(ld, (char*)dn.str(), attrs, NULL, NULL);
        if ( rc == LDAP_INVALID_SYNTAX  && m_ldapconfig->getServerType() == OPEN_LDAP)//Fedora389 does not 'seem' to need this, openLDAP does
        {
            attrs[ind++] = &member_attr;
            attrs[ind] = NULL;
            rc = ldap_add_ext_s(ld, (char*)dn.str(), attrs, NULL, NULL);
        }
        if ( rc != LDAP_SUCCESS)
        {
            if(rc == LDAP_ALREADY_EXISTS)
            {
                throw MakeStringException(-1, "can't add group %s, an LDAP object with this name already exists", groupname);
            }
            else
            {
                DBGLOG("error addGroup %s, ldap_add_ext_s error: %s", groupname, ldap_err2string( rc ));
                throw MakeStringException(-1, "error addGroup %s, ldap_add_ext_s error: %s", groupname, ldap_err2string( rc ));
            }
        }

    }

    virtual void deleteGroup(const char* groupname)
    {
        if(groupname == NULL || *groupname == '\0')
            throw MakeStringException(-1, "group name can't be empty");

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            if(stricmp(groupname, "Administrators") == 0 || stricmp(groupname, "Authenticated Users") == 0)
                throw MakeStringException(-1, "you can't delete Authenticated Users or Administrators group");
        }
        else
        {
            if(stricmp(groupname, "Directory Administrators") == 0)
                throw MakeStringException(-1, "you can't delete Directory Administrators group");
        }

        StringBuffer dn;
        getGroupDN(groupname, dn);
        
        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        
        int rc = ldap_delete_s(ld, (char*)dn.str());

        if ( rc != LDAP_SUCCESS )
        {
            throw MakeStringException(-1, "error deleting group %s: %s", groupname, ldap_err2string(rc));
        }
    }

    virtual void getGroupMembers(const char* groupname, StringArray & users)
    {
        char        *attribute;
        LDAPMessage *message;

        if(groupname == NULL || strlen(groupname) == 0)
            throw MakeStringException(-1, "group name can't be empty");

        StringBuffer grpdn;
        getGroupDN(groupname, grpdn);
        StringBuffer filter;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            filter.append("distinguishedName=").append(grpdn.str());
        }
        else if(m_ldapconfig->getServerType() == IPLANET)
        {
            filter.append("entrydn=").append(grpdn.str());
        }
        else if(m_ldapconfig->getServerType() == OPEN_LDAP)
        {
            filter.append("cn=").append(groupname);
        }

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        const char* memfieldname;

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            memfieldname = "member";
        }
        else
        {
            memfieldname = "uniquemember";
        }

        char        *attrs[] = {(char*)memfieldname, NULL};
        StringBuffer groupbasedn;
        getGroupBaseDN(groupname, groupbasedn);
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)groupbasedn.str(),LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getBasedn());
            return;
        }
        
        unsigned entries = ldap_count_entries(ld, searchResult);
        if(entries == 0)
        {
            throw MakeStringException(-1, "group %s not found", groupname);
        }

        // Go through the search results by checking message types
        for(message = LdapFirstEntry( ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (vals.hasValues())
                {
                    for (int i = 0; vals.queryValues()[ i ] != NULL; i++ )
                    {
                        char* val = vals.queryValues()[i];
                        StringBuffer uid;
                        getUidFromDN(val, uid);
                        if(uid.length() > 0)
                            users.append(uid.str());
                    }
                }
            }
        }
    }

    virtual void deleteResource(SecResourceType rtype, const char* name, const char* basedn)
    {
        if(basedn == NULL || *basedn == '\0')
            basedn = m_ldapconfig->getResourceBasedn(rtype);

        StringBuffer dn;
        name2dn(rtype, name, basedn, dn);

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        
        int rc = ldap_delete_s(ld, (char*)dn.str());

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("error deleting %s: %s", dn.str(), ldap_err2string(rc));
            //throw MakeStringException(-1, "error deleting %s: %s", dn.str(), ldap_err2string(rc));
        }
        
    }

    virtual void renameResource(SecResourceType rtype, const char* oldname, const char* newname, const char* basedn)
    {
        if(oldname == NULL || *oldname == '\0' || newname == NULL || *newname == '\0')
            throw MakeStringException(-1, "please specfiy old and new names");

        if(basedn == NULL || *basedn == '\0')
            basedn = m_ldapconfig->getResourceBasedn(rtype);

        StringBuffer olddn, newrdn;
        name2dn(rtype, oldname, basedn, olddn);
        name2rdn(rtype, newname, newrdn);
        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY && (rtype == RT_DEFAULT || rtype == RT_MODULE || rtype == RT_SERVICE))
        {
            char* uncname_values[] = {(char*)newname, NULL};
            LDAPMod uncname_attr =
            {
                LDAP_MOD_REPLACE,
                "uNCName",
                uncname_values
            };

            LDAPMod *attrs[2];
            attrs[0] = &uncname_attr;
            attrs[1] = NULL;

            int rc = ldap_modify_s(ld, (char*)olddn.str(), attrs);

            if (rc != LDAP_SUCCESS )
            {
                DBGLOG("Error changing unc %s to %s - %s", oldname, newname, ldap_err2string( rc ));
                //throw MakeStringException(-1, "Error changing unc %s to %s - %s", oldname, newname, ldap_err2string( rc ));
            }
        }

#ifdef _WIN32
        int rc = ldap_rename_ext_s(ld, (char*)olddn.str(), (char*)newrdn.str(), NULL, true, NULL, NULL);
#else
        int rc = ldap_rename_s(ld, (char*)olddn.str(), (char*)newrdn.str(), NULL, true, NULL, NULL);
#endif
        if (rc != LDAP_SUCCESS )
        {
            DBGLOG("Error renaming %s to %s - %s", oldname, newname, ldap_err2string( rc ));
            //throw MakeStringException(-1, "Error renaming %s to %s - %s", oldname, newname, ldap_err2string( rc ));
        }
    }

    virtual void copyResource(SecResourceType rtype, const char* oldname, const char* newname, const char* basedn)
    {
        if(oldname == NULL || *oldname == '\0' || newname == NULL || *newname == '\0')
            throw MakeStringException(-1, "please specfiy old and new names");

        if(basedn == NULL || *basedn == '\0')
            basedn = m_ldapconfig->getResourceBasedn(rtype);

        Owned<CSecurityDescriptor> sd = new CSecurityDescriptor(oldname);
        IArrayOf<CSecurityDescriptor> sdlist;
        sdlist.append(*LINK(sd));
        if(rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE)
            getSecurityDescriptorsScope(sdlist, basedn);
        else
            getSecurityDescriptors(sdlist, basedn);

        if(sd->getDescriptor().length() == 0)
            throw MakeStringException(-1, "error copying %s to %s, %s doesn't exist", oldname, newname, oldname);
        
        ISecUser* user = NULL;
        CLdapSecResource resource(newname);
        addResource(rtype, *user, &resource, PT_DEFAULT, basedn, sd.get(), false);
    }

    void normalizeDn(const char* dn, StringBuffer& ndn)
    {
        LdapUtils::normalizeDn(dn, m_ldapconfig->getBasedn(), ndn);
    }

    virtual bool isSuperUser(ISecUser* user)
    {
        if(user == NULL || user->getName() == NULL)
            return false;
        const char* username = user->getName();
        const char* sysuser = m_ldapconfig->getSysUser();
        if(sysuser != NULL && stricmp(sysuser, username) == 0)
            return true;
        StringBuffer userdn, admingrpdn;
        getUserDN(username, userdn);
        getAdminGroupDN(admingrpdn);
        return userInGroup(userdn.str(), admingrpdn.str());
    }

    virtual ILdapConfig* queryConfig()
    {
        return m_ldapconfig.get();
    }

    virtual int countUsers(const char* searchstr, int limit)
    {
        StringBuffer filter;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            filter.append("objectClass=User");
        else
            filter.append("objectClass=inetorgperson");

        if(searchstr && *searchstr && strcmp(searchstr, "*") != 0)
        {
            filter.insert(0, "(&(");
            filter.appendf(")(|(%s=*%s*)(%s=*%s*)(%s=*%s*)))", (m_ldapconfig->getServerType()==ACTIVE_DIRECTORY)?"sAMAcccountName":"uid", searchstr, "givenName", searchstr, "sn", searchstr);
        }

        return countEntries(m_ldapconfig->getUserBasedn(), filter.str(), limit);
    }

    virtual int countResources(const char* basedn, const char* searchstr, int limit)
    {
        StringBuffer filter;
        filter.append("objectClass=*");

        if(searchstr && *searchstr && strcmp(searchstr, "*") != 0)
        {
            filter.insert(0, "(&(");
            filter.appendf(")(|(%s=*%s*)))", "uNCName", searchstr);
        }

        return countEntries(basedn, filter.str(), limit);
    }

    virtual int countEntries(const char* basedn, const char* filter, int limit)
    {
        TIMEVAL timeOut = {LDAPTIMEOUT,0};

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        char *attrs[] = { LDAP_NO_ATTRS, NULL };
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)basedn, LDAP_SCOPE_SUBTREE, (char*)filter, attrs, 0, NULL, NULL, &timeOut, limit, &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            if(rc == LDAP_SIZELIMIT_EXCEEDED)
                return -1;
            else
                throw MakeStringException(-1, "ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter, basedn);
        }

        int entries = ldap_count_entries(ld, searchResult);
        return entries;
    }

    virtual const char* getPasswordStorageScheme()
    {
        if(m_pwscheme.length() == 0)
        {
            if(m_ldapconfig->getServerType() == IPLANET)
            {
                Owned<ILdapConnection> lconn = m_connections->getConnection();
                LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
                
                char* pw_attrs[] = {"nsslapd-rootpwstoragescheme", NULL};
                CLDAPMessage msg;
                int err = ldap_search_s(ld, "cn=config", LDAP_SCOPE_BASE, "objectClass=*", pw_attrs, false, &msg.msg);
                if(err != LDAP_SUCCESS)
                {
                    DBGLOG("ldap_search_s error: %s", ldap_err2string( err ));
                    return NULL;
                }
                LDAPMessage* entry = LdapFirstEntry(ld, msg);
                if(entry != NULL)
                {
                    CLDAPGetValuesWrapper vals(ld, entry, "nsslapd-rootpwstoragescheme");
                    if (vals.hasValues())
                        m_pwscheme.append(vals.queryValues()[0]);
                }
                ldap_msgfree(msg);
            }
        }
        
        if(m_pwscheme.length() == 0)
            return NULL;
        else
            return m_pwscheme.str();
    }

private:

    virtual void addDC(const char* dc)
    {
        if(dc == NULL || *dc == '\0')
            return;

        StringBuffer dcname;
        LdapUtils::getName(dc, dcname);

        char *dc_values[] = {(char*)dcname.str(), NULL };
        LDAPMod dc_attr = 
        {
            LDAP_MOD_ADD,
            "dc",
            dc_values
        };

        char *o_values[] = {(char*)dcname.str(), NULL };
        LDAPMod o_attr = 
        {
            LDAP_MOD_ADD,
            "o",
            o_values
        };

        char *oc_values[] = {"organization", "dcObject", NULL };
        LDAPMod oc_attr =
        {
            LDAP_MOD_ADD,
            "objectClass",
            oc_values
        };
        
        LDAPMod *attrs[4];
        attrs[0] = &oc_attr;
        attrs[1] = &o_attr;
        attrs[2] = &dc_attr;
        attrs[3] = NULL;

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        int rc = ldap_add_ext_s(ld, (char*)dc, attrs, NULL, NULL);
        if ( rc != LDAP_SUCCESS )
        {
            if(rc == LDAP_ALREADY_EXISTS)
            {
                throw MakeStringException(-1, "can't add dc %s, an LDAP object with this name already exists", dc);
            }
            else
            {
                DBGLOG("error addDC %s, ldap_add_ext_s error: 0x%0x %s", dc, rc, ldap_err2string( rc ));
                throw MakeStringException(-1, "error addDC %s, ldap_add_ext_s error: %s", dc, ldap_err2string( rc ));
            }
        }
    }

    virtual void getUserDN(const char* username, StringBuffer& userdn)
    {
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            StringBuffer filter;
            filter.append("sAMAccountName=");
            filter.append(username);

            char        *attribute;
            LDAPMessage *message;

            TIMEVAL timeOut = {LDAPTIMEOUT,0};   

            char *dn_fieldname;
            dn_fieldname = "distinguishedName";

            Owned<ILdapConnection> lconn = m_connections->getConnection();
            LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

            char        *attrs[] = {dn_fieldname, NULL};
            CLDAPMessage searchResult;
            int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

            if ( rc != LDAP_SUCCESS )
            {
                throw MakeStringException(-1, "ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
            }

            unsigned entries = ldap_count_entries(ld, searchResult);
            if(entries == 0)
            {
                searchResult.ldapMsgFree();
                int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getSysUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );

                if ( rc != LDAP_SUCCESS )
                {
                    throw MakeStringException(-1, "ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getSysUserBasedn());
                }
            }

            message = LdapFirstEntry( ld, searchResult);
            if(message != NULL)
            {

                CLDAPGetAttributesWrapper   atts(ld, searchResult);
                attribute = atts.getFirst();
                if(attribute != NULL)
                {
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                        userdn.append(vals.queryValues()[0]);
                }
            }
            if(userdn.length() == 0)
                throw MakeStringException(-1, "user %s can't be found", username);
        }
        else
        {
            if(stricmp(username, "anyone") == 0)
                userdn.append(username);
            else
                userdn.append("uid=").append(username).append(",").append(m_ldapconfig->getUserBasedn());
        }

    }
    
    virtual void getUidFromDN(const char* dn, StringBuffer& uid)
    {
        if(dn == NULL || *dn == '\0')
            return;

        if(m_ldapconfig->getServerType() != ACTIVE_DIRECTORY)
        {
            if (strncmp(dn,"uid=",4))//Fedora389 returns "cn=Directory Administrators"
                return;
            const char* comma = strchr(dn, ',');
            // DN is in the format of "uid=uuu,ou=ooo,dc=dd"
            uid.append(comma - dn - 4, dn + 4);
            return;
        }

        StringBuffer filter;
        filter.append("distinguishedName=").append(dn);

        char        *attribute;
        LDAPMessage *message;

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        char *uid_fieldname = "sAMAccountName";

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        char        *attrs[] = {uid_fieldname, NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            throw MakeStringException(-1, "ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
        }

        message = LdapFirstEntry( ld, searchResult);
        if(message != NULL)
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            attribute = atts.getFirst();
            if(attribute != NULL)
            {
                CLDAPGetValuesWrapper vals(ld, message, attribute);
                if (vals.hasValues())
                    uid.append(vals.queryValues()[0]);
            }
        }
    }

    virtual void getGroupDN(const char* groupname, StringBuffer& groupdn)
    {
        if(groupname == NULL)
            return;
        LdapServerType stype = m_ldapconfig->getServerType();
        groupdn.append("cn=").append(groupname).append(",");
        if(stype == ACTIVE_DIRECTORY && stricmp(groupname, "Administrators") == 0)
        {
            groupdn.append("cn=Builtin,").append(m_ldapconfig->getBasedn());
        }
        else if((stype == IPLANET || stype == OPEN_LDAP) && stricmp(groupname, "Directory Administrators") == 0)
        {
            groupdn.append(m_ldapconfig->getBasedn());
        }
        else
        {
            groupdn.append(m_ldapconfig->getGroupBasedn());
        }
    }

    virtual void getGroupBaseDN(const char* groupname, StringBuffer& groupbasedn)
    {
        if(groupname == NULL)
            return;
        LdapServerType stype = m_ldapconfig->getServerType();
        if(stype == ACTIVE_DIRECTORY && stricmp(groupname, "Administrators") == 0)
        {
            groupbasedn.append("cn=Builtin,").append(m_ldapconfig->getBasedn());
        }
        else if((stype == IPLANET || stype == OPEN_LDAP) && stricmp(groupname, "Directory Administrators") == 0)
        {
            groupbasedn.append(m_ldapconfig->getBasedn());
        }
        else
        {
            groupbasedn.append(m_ldapconfig->getGroupBasedn());
        }
    }

    virtual void getAdminGroupDN(StringBuffer& groupdn)
    {
        LdapServerType stype = m_ldapconfig->getServerType();
        if(stype == ACTIVE_DIRECTORY)
        {
            groupdn.append("cn=Administrators,cn=Builtin,").append(m_ldapconfig->getBasedn());
        }
        else if(stype == IPLANET)
        {
            groupdn.append("cn=Directory Administrators,").append(m_ldapconfig->getBasedn());
        }
        else if(stype == OPEN_LDAP)
        {
            groupdn.append("cn=Directory Administrators,").append(m_ldapconfig->getBasedn());
        }
    }

    virtual void changeUserMemberOf(const char* action, const char* userdn, const char* groupdn)
    {
        char *grp_values[] = {(char*)groupdn, NULL};
        LDAPMod grp_attr = {
            (action != NULL && stricmp(action, "delete") == 0)?LDAP_MOD_DELETE:LDAP_MOD_ADD,
            "memberOf",
            grp_values
        };

        LDAPMod *grp_attrs[2];
        grp_attrs[0] = &grp_attr;
        grp_attrs[1] = NULL;

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        int rc = ldap_modify_ext_s(ld, (char*)userdn, grp_attrs, NULL, NULL);
        if ( rc != LDAP_SUCCESS )
        {
            throw MakeStringException(-1, "error changing group for user %s, ldap_modify_ext_s error: %s", userdn, ldap_err2string( rc ));
        }
    }

    virtual void changeGroupMember(const char* action, const char* groupdn, const char* userdn)
    {
        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();

        const char* memberfieldname;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            memberfieldname = "member";
        }
        else
        {
            memberfieldname = "uniquemember";
        }

        char *member_values[] = {(char*)userdn, NULL};
        LDAPMod member_attr = {
            (action != NULL && stricmp(action, "delete") == 0)?LDAP_MOD_DELETE:LDAP_MOD_ADD,
            (char*)memberfieldname,
            member_values
        };

        LDAPMod *member_attrs[2];
        member_attrs[0] = &member_attr;
        member_attrs[1] = NULL;

        int rc = ldap_modify_ext_s(ld, (char*)groupdn, member_attrs, NULL, NULL);
        if ( rc != LDAP_SUCCESS )
        {
            if (action != NULL && stricmp(action, "delete") == 0)
                throw MakeStringException(-1, "Failed in deleting member from group: ldap_modify_ext_s error: %s; userdn: %s; groupdn: %s", ldap_err2string( rc ), userdn, groupdn);
            else
                throw MakeStringException(-1, "Failed in adding member to group, ldap_modify_ext_s error: %s; userdn: %s; groupdn: %s", ldap_err2string( rc ), userdn, groupdn);
        }
    }

    void ConvertCToW(unsigned short* pszDest, const char * pszSrc)
    {
        unsigned i = 0;
        for(i = 0; i < strlen(pszSrc); i++)
            pszDest[i] = (unsigned short) pszSrc[i];
        pszDest[i] = (unsigned short)'\0';
    }


    virtual bool authorizeScope(ISecUser& user, IArrayOf<ISecResource>& resources, const char* basedn)
    {
        IArrayOf<CSecurityDescriptor> sdlist;
        std::set<const char*, ltstr> scopeset;
        ForEachItemIn(x, resources)
        {
            ISecResource& res = resources.item(x);
            const char* resourcename = res.getName();
            if(resourcename == NULL || *resourcename == '\0')
                continue;

            // Add one extra Volume type SecurityDescriptor for each resource for ActiveDirectory.
            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
            {
                CSecurityDescriptor* sd = new CSecurityDescriptor(resourcename);
                sd->setObjectClass("Volume");
                sdlist.append(*sd);
            }

            scopeset.insert(resourcename);
            int len = strlen(resourcename);
            const char* curptr = resourcename + len - 1;
            while(curptr > resourcename)
            {
                while(curptr > resourcename && *curptr != ':')
                    curptr--;
                bool foundcolon=false;
                while(curptr >resourcename && *curptr == ':')
                {
                    curptr--;
                    foundcolon = true;
                }
                if(curptr > resourcename || foundcolon)
                {
                    int curlen = curptr - resourcename + 1;
                    char* curscope = (char*)alloca(curlen + 1);
                    strncpy(curscope, resourcename, curlen);
                    curscope[curlen] = 0;
                    scopeset.insert(curscope);
                }
            }
        }

        if(scopeset.size() == 0)
            return true;

        std::set<const char*, ltstr>::iterator iter;
        for(iter = scopeset.begin(); iter != scopeset.end(); iter++)
        {
            const char* curscope = *iter;
            CSecurityDescriptor* sd = new CSecurityDescriptor(*iter);
            sd->setObjectClass("organizationalUnit");
            sdlist.append(*sd);
        }

        getSecurityDescriptorsScope(sdlist, basedn);

        IArrayOf<CSecurityDescriptor> matched_sdlist;
        ForEachItemIn(y, resources)
        {
            ISecResource& res = resources.item(y);
            const char* rname = res.getName();
            if(rname == NULL || *rname == '\0')
                throw MakeStringException(-1, "resource name can't be empty inside authorizeScope");

            CSecurityDescriptor* matchedsd = NULL;
            ForEachItemIn(z, sdlist)
            {
                CSecurityDescriptor& sd = sdlist.item(z);
                const char* sdname = sd.getName();
                if(sdname ==  NULL || *sdname == '\0')
                    continue;
                if(strncmp(sdname, rname, strlen(sdname)) == 0 
                    && (matchedsd == NULL 
                        || ((matchedsd->getDescriptor().length() == 0 && sd.getDescriptor().length() > 0) 
                        || (sd.getDescriptor().length() > 0 && strlen(sdname) > strlen(matchedsd->getName())))))
                    matchedsd = &sd;
            }
            if(matchedsd != NULL)
                matched_sdlist.append(*LINK(matchedsd));
            else
                matched_sdlist.append(*(new CSecurityDescriptor(rname)));
        }

        bool ok = false;
        if(m_pp != NULL)
            ok = m_pp->getPermissions(user, matched_sdlist, resources);
        return ok;
    }

    virtual void getSecurityDescriptors(SecResourceType rtype, IArrayOf<CSecurityDescriptor>& sdlist)
    {
        int len = sdlist.length();
        if(len == 0)
            return;

        const char* rbasedn = m_ldapconfig->getResourceBasedn(rtype);
        if(rbasedn == NULL || *rbasedn == '\0')
        {
            DBGLOG("corresponding resource basedn is not defined");
            return;
        }
            
        std::map<std::string, IArrayOf<CSecurityDescriptor>*> sdmap;

        for(int i = 0; i < len; i++)
        {
            CSecurityDescriptor& sd = sdlist.item(i);
            const char* relativeBasedn = sd.getRelativeBasedn();
            StringBuffer basedn;
            if(relativeBasedn != NULL)
                basedn.append(relativeBasedn).append(",");
            basedn.append(rbasedn);

            std::map<std::string, IArrayOf<CSecurityDescriptor>*>::iterator sdit = sdmap.find(basedn.str());
            if(sdit == sdmap.end())
            {
                IArrayOf<CSecurityDescriptor>* newlist = new IArrayOf<CSecurityDescriptor>;
                newlist->append(*LINK(&sd));
                sdmap[basedn.str()] = newlist;
            }
            else
            {
                (*sdit).second->append(*LINK(&sd));
            }
        }

        for(std::map<std::string, IArrayOf<CSecurityDescriptor>*>::iterator cur = sdmap.begin(); cur != sdmap.end(); cur++)
        {
            getSecurityDescriptors(*((*cur).second), (*cur).first.c_str());
            delete (*cur).second;
        }

    }

    virtual void getSecurityDescriptors(IArrayOf<CSecurityDescriptor>& sdlist, const char* basedn)
    {
        char        *attribute;
        CLDAPGetValuesLenWrapper valsLen;
        LDAPMessage *message;
        int i;
        
        const char *id_fieldname;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            id_fieldname = "name";
        }
        else
        {
            id_fieldname = "ou";
        }
        const char *des_fieldname = m_ldapconfig->getSdFieldName();

        int len = sdlist.length();
        if(len == 0)
            return;

        StringBuffer filter;
        filter.append("(|");
        for(i = 0; i < len; i++)
        {
            CSecurityDescriptor& sd = sdlist.item(i);
            StringBuffer namebuf;
            namebuf.append(sd.getName());
            namebuf.trim();
            if(namebuf.length() > 0)
            {
                filter.append("(").append(id_fieldname).append("=").append(namebuf.str()).append(")");;
            }
        }
        filter.append(")");

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   
        
        char* attrs[] = {(char*)id_fieldname, (char*)des_fieldname, NULL};
        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)basedn, LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );     /* returned results */
        
        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), basedn);
            return;
        }

        // Go through the search results by checking message types
        for(message = LdapFirstEntry(ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            StringBuffer resourcename;
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                if(stricmp(attribute, id_fieldname) == 0)
                {
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                        resourcename.append(vals.queryValues()[0]);
                }
                else if(stricmp(attribute, des_fieldname) == 0) 
                {
                    valsLen.getValues(ld, message, attribute);
                }
            }
            for(i = 0; i < len; i++)
            {
                CSecurityDescriptor& sd = sdlist.item(i);
                if(resourcename.length() > 0 && stricmp(resourcename.str(), sd.getName()) == 0)
                {
                    if (valsLen.hasValues())
                    {
                        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                        {
                            struct berval* val = valsLen.queryValues()[0];
                            if(val != NULL)
                            {
                                CSecurityDescriptor& sd = sdlist.item(i);
                                sd.setDescriptor(val->bv_len, val->bv_val);
                            }
                        }
                        else
                        {
                            MemoryBuffer allvals;
                            int valseq = 0;
                            struct berval* val = valsLen.queryValues()[valseq++];
                            while(val != NULL)
                            {
                                if(val->bv_len > 0)
                                {
                                    allvals.append(val->bv_len, val->bv_val);
                                    allvals.append('\0'); // my separator between ACIs
                                }
                                val = valsLen.queryValues()[valseq++];
                            }
                            if(allvals.length() > 0)
                            {
                                CSecurityDescriptor& sd = sdlist.item(i);
                                sd.setDescriptor(allvals.length(), (void*)allvals.toByteArray());
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    virtual void getSecurityDescriptorsScope(IArrayOf<CSecurityDescriptor>& sdlist, const char* basedn)
    {
        char        *attribute;
        CLDAPGetValuesLenWrapper valsLen;
        LDAPMessage *message;
        int i;

        int len = sdlist.length();
        if(len == 0)
            return;

        LdapServerType servertype = m_ldapconfig->getServerType();

        char *sd_fieldname = (char*)m_ldapconfig->getSdFieldName();

        StringBuffer filter;
        filter.append("(|");
        for(i = 0; i < len; i++)
        {
            CSecurityDescriptor& sd = sdlist.item(i);
            StringBuffer namebuf;
            namebuf.append(sd.getName());
            namebuf.trim();
            if(namebuf.length() > 0)
            {
                const char* resourcename = namebuf.str();
                int len = namebuf.length();
                const char* curptr = resourcename + len - 1;
                StringBuffer dn, cn;
                bool isleaf = true;
                while(curptr > resourcename)
                {
                    const char* lastptr = curptr;
                    while(curptr > resourcename && *curptr != ':')
                        curptr--;
                    int curlen;
                    const char* curscope;
                    if(*curptr == ':')
                    {
                        curlen = lastptr - curptr;
                        curscope = curptr + 1;
                    }
                    else
                    {
                        curlen = lastptr - curptr + 1;
                        curscope = curptr;
                    }
                    if(isleaf && (sd.getObjectClass() != NULL) && (stricmp(sd.getObjectClass(), "Volume") == 0))
                    {
                        cn.append(curlen, curscope);
                        dn.append("cn=").append(curlen, curscope).append(",");
                    }
                    else
                    {
                        cn.append(curlen, curscope);
                        dn.append("ou=").append(curlen, curscope).append(",");
                    }
                    
                    isleaf = false;

                    if (curptr == resourcename) //handle a single char as the top scope, such as x::abc
                        break;

                    while(curptr >resourcename && *curptr == ':')
                        curptr--;

                    if (curptr == resourcename && *curptr != ':') //handle a single char as the top scope, such as x::abc
                    {
                        dn.append("ou=").append(1, curptr).append(",");
                    }
                }
                dn.append(basedn);

                if(servertype == ACTIVE_DIRECTORY)
                {
                    filter.append("(distinguishedName=").append(dn.str()).append(")");
                }
                else if(servertype == IPLANET)
                {
                    filter.append("(entrydn=").append(dn.str()).append(")");
                }
                else if(servertype == OPEN_LDAP)
                {
                    filter.append("(ou=").append(cn.str()).append(")");
                }
                sd.setDn(dn.str());
            }
        }
        filter.append(")");

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   
        
        char* attrs[] = {sd_fieldname, NULL};
        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)basedn, LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT, &searchResult.msg );     /* returned results */
        
        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), basedn);
            return;
        }

        // Go through the search results by checking message types
        for(message = LdapFirstEntry(ld, searchResult); message != NULL; message = ldap_next_entry(ld, message))
        {
            StringBuffer dn;

            char *p = ldap_get_dn(ld, message);
            dn.append(p);
            ldap_memfree(p);

            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                if(stricmp(attribute, sd_fieldname) == 0) 
                {
                    valsLen.getValues(ld, message, attribute);
                }
            }
            for(i = 0; i < len; i++)
            {
                CSecurityDescriptor& sd = sdlist.item(i);
                if(dn.length() > 0 && stricmp(dn.str(), sd.getDn()) == 0)
                {
                    if (valsLen.hasValues())
                    {
                        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
                        {
                            struct berval* val = valsLen.queryValues()[0];
                            if(val != NULL)
                            {
                                CSecurityDescriptor& sd = sdlist.item(i);
                                sd.setDescriptor(val->bv_len, val->bv_val);
                            }
                        }
                        else
                        {
                            MemoryBuffer allvals;
                            int valseq = 0;
                            struct berval* val = valsLen.queryValues()[valseq++];
                            while(val != NULL)
                            {
                                if(val->bv_len > 0)
                                {
                                    allvals.append(val->bv_len, val->bv_val);
                                    allvals.append('\0'); // my separator between ACIs
                                }
                                val = valsLen.queryValues()[valseq++];
                            }
                            if(allvals.length() > 0)
                            {
                                CSecurityDescriptor& sd = sdlist.item(i);
                                sd.setDescriptor(allvals.length(), (void*)allvals.toByteArray());
                            }
                        }

                    }
                    break;
                }
            }
        }
    }

    virtual void createLdapBasedn(ISecUser* user, const char* basedn, SecPermissionType ptype)
    {
        if(basedn == NULL || basedn[0] == '\0')
            return;

        const char* ptr = strstr(basedn, "ou=");
        if(ptr == NULL)
            return;
        ptr += 3;

        StringBuffer oubuf;
        const char* comma = strchr(ptr, ',');
        if(comma == NULL)
        {
            oubuf.append(ptr);
            ptr = NULL;
        }
        else
        {
            oubuf.append(comma - ptr, ptr);
            ptr = comma + 1;
        }

        if(ptr != NULL)
            createLdapBasedn(user, ptr, ptype);

        addOrganizationalUnit(user, oubuf.str(), ptr, ptype);

    }

    virtual bool addOrganizationalUnit(ISecUser* user, const char* name, const char* basedn, SecPermissionType ptype)
    {
        if(name == NULL || basedn == NULL)
            return false;

        if(strchr(name, '/') != NULL || strchr(name, '=') != NULL)
            return false;

        StringBuffer dn;
        dn.append("ou=").append(name).append(",").append(basedn);

        char *ou_values[] = {(char*)name, NULL };
        LDAPMod ou_attr = 
        {
            LDAP_MOD_ADD,
            "ou",
            ou_values
        };

        char *name_values[] = {(char*)name, NULL };
        LDAPMod name_attr = 
        {
            LDAP_MOD_ADD,
            "name",
            name_values
        };

        char *oc_values[] = {"OrganizationalUnit", NULL };
        LDAPMod oc_attr =
        {
            LDAP_MOD_ADD,
            "objectClass",
            oc_values
        };

        MemoryBuffer sdbuf;
        Owned<CSecurityDescriptor> default_sd = NULL;
        if(m_pp !=  NULL)
            default_sd.setown(m_pp->createDefaultSD(user, name, ptype));
        if(default_sd != NULL)
            sdbuf.append(default_sd->getDescriptor());

        LDAPMod *attrs[6];
        int ind = 0;
        attrs[ind++] = &ou_attr;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {       
            attrs[ind++] = &name_attr;
        }
        attrs[ind++] = &oc_attr;

        LDAPMod sd_attr;
        struct berval sd_val;
        sd_val.bv_len = sdbuf.length();
        sd_val.bv_val = (char*)sdbuf.toByteArray();
        struct berval* sd_values[] = {&sd_val, NULL};
        sd_attr.mod_op = LDAP_MOD_ADD | LDAP_MOD_BVALUES;
        
        sd_attr.mod_type = (char*)m_ldapconfig->getSdFieldName();

        sd_attr.mod_vals.modv_bvals = sd_values;

        if(sdbuf.length() > 0)
            attrs[ind++] = &sd_attr;

        attrs[ind] = NULL;

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        int rc = ldap_add_ext_s(ld, (char*)dn.str(), attrs, NULL, NULL);
        if ( rc != LDAP_SUCCESS )
        {
            if(rc == LDAP_ALREADY_EXISTS)
            {
                //WARNLOG("Can't insert ou=%s,%s to Ldap Server, an LDAP object with this name already exists", name, basedn);
                return false;
            }
            else
            {
                throw MakeStringException(-1, "ldap_add_ext_s error for ou=%s,%s: %d %s", name, basedn, rc, ldap_err2string( rc ));
            }
        }

        return true;
    }

    virtual void name2dn(SecResourceType rtype, const char* resourcename, const char* basedn, StringBuffer& ldapname)
    {
        StringBuffer namebuf;

        const char* bptr = resourcename;
        const char* sep = strstr(resourcename, "::");
        while(sep != NULL)
        {
            if(sep > bptr)
            {
                StringBuffer onebuf;
                onebuf.append("ou=").append(sep-bptr, bptr).append(",");
                namebuf.insert(0, onebuf.str());
            }

            bptr = sep + 2;
            sep = strstr(bptr, "::");
        }
        if(*bptr != '\0')
        {
            StringBuffer onebuf;
            if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY && (rtype == RT_DEFAULT || rtype == RT_MODULE || rtype == RT_SERVICE))
                onebuf.append("cn");
            else
                onebuf.append("ou");
            onebuf.append("=").append(bptr).append(",");
            namebuf.insert(0, onebuf.str());
        }

        namebuf.append(basedn);
        LdapUtils::normalizeDn(namebuf.str(), m_ldapconfig->getBasedn(), ldapname);
    }

    virtual void name2rdn(SecResourceType rtype, const char* resourcename, StringBuffer& ldapname)
    {
        if(resourcename == NULL || *resourcename == '\0')
            return;

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY && (rtype == RT_DEFAULT || rtype == RT_MODULE || rtype == RT_SERVICE))
            ldapname.append("cn=");
        else
            ldapname.append("ou=");
        
        const char* prevptr = resourcename;
        const char* nextptr = strstr(resourcename, "::");
        while(nextptr != NULL)
        {
            prevptr = nextptr + 2;
            nextptr = strstr(prevptr, "::");
        }
        if(prevptr != NULL && *prevptr != '\0')
            ldapname.append(prevptr);
    }

    virtual bool addResource(SecResourceType rtype, ISecUser& user, ISecResource* resource, SecPermissionType ptype, const char* basedn)
    {
        Owned<CSecurityDescriptor> template_sd = NULL;
        const char* templatename = m_ldapconfig->getTemplateName();
        if(templatename != NULL && *templatename != '\0')
        {
            IArrayOf<CSecurityDescriptor> sdlist;
            template_sd.setown(new CSecurityDescriptor(templatename));
            sdlist.append(*LINK(template_sd));
            if(basedn && *basedn)
                getSecurityDescriptors(sdlist, basedn);
            else
                getSecurityDescriptors(rtype, sdlist);
        }

        Owned<CSecurityDescriptor> default_sd = NULL;
        if(template_sd != NULL && template_sd->getDescriptor().length() > 0)
        {
            MemoryBuffer template_sd_buf;
            template_sd_buf.append(template_sd->getDescriptor());
            if(m_pp != NULL)
                default_sd.setown(m_pp->createDefaultSD(&user, resource, template_sd_buf));
        }
        else
        {
            if(m_pp !=  NULL)
                default_sd.setown(m_pp->createDefaultSD(&user, resource, ptype));
        }

        return addResource(rtype, user, resource, ptype, basedn, default_sd.get());
    }

    virtual bool addResource(SecResourceType rtype, ISecUser& user, ISecResource* resource, SecPermissionType ptype, const char* basedn, CSecurityDescriptor* default_sd, bool lessException=true)
    {
        if(resource == NULL)
            return true;

        char* resourcename = (char*)resource->getName();
        if(resourcename == NULL)
        {
            DBGLOG("can't add resource, empty resource name");
            return false;
        }

        const char* rbasedn;
        StringBuffer rbasednbuf;
        if(basedn == NULL)
            rbasedn = m_ldapconfig->getResourceBasedn(rtype);
        else
        {
            LdapUtils::normalizeDn(basedn, m_ldapconfig->getBasedn(), rbasednbuf);
            rbasedn = rbasednbuf.str();
        }
        if(rbasedn == NULL || *rbasedn == '\0')
        {
            DBGLOG("Can't add resource, corresponding resource basedn is not defined");
            return false;
        }

        if(strchr(resourcename, '/') != NULL || strchr(resourcename, '=') != NULL)
            return false;

        if(rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE)
        {
            StringBuffer extbuf;
            name2dn(rtype, resourcename, rbasedn, extbuf);
            createLdapBasedn(&user, extbuf.str(), ptype);
            return true;
        }

        LdapServerType servertype = m_ldapconfig->getServerType();

        char* description = (char*)((CLdapSecResource*)resource)->getDescription();

        StringBuffer dn;
        
        char *fieldname, *oc_name;
        if(servertype == ACTIVE_DIRECTORY)
        {
            fieldname = "cn";
            oc_name = "Volume";
        }
        else
        {
            fieldname = "ou";
            oc_name = "OrganizationalUnit";
        }

        dn.append(fieldname).append("=").append(resourcename).append(",");
        dn.append(rbasedn);

        char *cn_values[] = {resourcename, NULL };
        LDAPMod cn_attr = 
        {
            LDAP_MOD_ADD,
            fieldname,
            cn_values
        };
        char *oc_values[] = {oc_name, NULL };
        LDAPMod oc_attr =
        {
            LDAP_MOD_ADD,
            "objectClass",
            oc_values
        };
        char* uncname_values[] = {resourcename, NULL};
        LDAPMod uncname_attr =
        {
            LDAP_MOD_ADD,
            "uNCName",
            uncname_values
        };

        StringBuffer descriptionbuf;
        if(description  && *description)
            descriptionbuf.append(description);
        else
            descriptionbuf.appendf("Access to %s", resourcename);

        char* description_values[] = {(char*)descriptionbuf.str(), NULL};
        LDAPMod description_attr =
        {
            LDAP_MOD_ADD,
            "description",
            description_values
        };

        Owned<CSecurityDescriptor> template_sd = NULL;
        const char* templatename = m_ldapconfig->getTemplateName();
        if(templatename != NULL && *templatename != '\0')
        {
            IArrayOf<CSecurityDescriptor> sdlist;
            template_sd.setown(new CSecurityDescriptor(templatename));
            sdlist.append(*LINK(template_sd));
            getSecurityDescriptors(rtype, sdlist);
        }

        int numberOfSegs = 0;
        if(default_sd != NULL)
        {
            numberOfSegs = m_pp->sdSegments(default_sd);
        }

        LDAPMod **attrs = (LDAPMod**)(alloca((5+numberOfSegs)*sizeof(LDAPMod*)));

        int ind = 0;
        attrs[ind++] = &cn_attr;
        attrs[ind++] = &oc_attr;
        attrs[ind++] = &description_attr;

        if(servertype == ACTIVE_DIRECTORY)
        {
            attrs[ind++] = &uncname_attr;
        }

        LDAPMod sd_attr;

        if(default_sd != NULL)
        {
            struct berval** sd_values = (struct berval**)alloca(sizeof(struct berval*)*(numberOfSegs+1));
            MemoryBuffer& sdbuf = default_sd->getDescriptor();

            // Active Directory acutally has only one segment.
            if(servertype == ACTIVE_DIRECTORY)
            {
                struct berval* sd_val = (struct berval*)alloca(sizeof(struct berval));
                sd_val->bv_len = sdbuf.length();
                sd_val->bv_val = (char*)sdbuf.toByteArray();
                sd_values[0] = sd_val;
                sd_values[1] = NULL;

                sd_attr.mod_type = "ntSecurityDescriptor";
            }
            else
            {
                const char* bbptr = sdbuf.toByteArray();
                const char* bptr = sdbuf.toByteArray();
                int sdbuflen = sdbuf.length();
                int segind;
                for(segind = 0; segind < numberOfSegs; segind++)
                {
                    if(bptr - bbptr >= sdbuflen)
                        break;
                    while(*bptr == '\0' && (bptr - bbptr) < sdbuflen)
                        bptr++;

                    const char* eptr = bptr;
                    while(*eptr != '\0' && (eptr - bbptr) < sdbuflen)
                        eptr++;

                    struct berval* sd_val = (struct berval*)alloca(sizeof(struct berval));
                    sd_val->bv_len = eptr - bptr;
                    sd_val->bv_val = (char*)bptr;
                    sd_values[segind] = sd_val;

                    bptr = eptr + 1;
                }
                sd_values[segind] = NULL;

                sd_attr.mod_type = (char*)m_ldapconfig->getSdFieldName();
            }
            sd_attr.mod_op = LDAP_MOD_ADD | LDAP_MOD_BVALUES;
            sd_attr.mod_vals.modv_bvals = sd_values;

            attrs[ind++] = &sd_attr;
        }
        attrs[ind] = NULL;

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        int rc = ldap_add_ext_s(ld, (char*)dn.str(), attrs, NULL, NULL);
        if ( rc != LDAP_SUCCESS )
        {
            if(rc == LDAP_ALREADY_EXISTS)
            {
                //WARNLOG("Can't insert %s to Ldap Server, an LDAP object with this name already exists", resourcename);
                if(lessException)
                    return false;
                else
                    throw MakeStringException(-1, "Can't insert %s, an LDAP object with this name already exists", resourcename);
            }
            else
            {
                throw MakeStringException(-1, "ldap_add_ext_s error for %s: %d %s", resourcename, rc, ldap_err2string( rc ));
            }
        }

        return true;
    }

    virtual void enableUser(ISecUser* user, const char* dn, LDAP* ld)
    {
        const char* username = user->getName();

        StringBuffer filter;
        filter.append("sAMAccountName=").append(username);

        char        *attribute;
        LDAPMessage *message;

        TIMEVAL timeOut = {LDAPTIMEOUT,0};   

        char        *attrs[] = {"userAccountControl", NULL};
        CLDAPMessage searchResult;
        int rc = ldap_search_ext_s(ld, (char*)m_ldapconfig->getUserBasedn(), LDAP_SCOPE_SUBTREE, (char*)filter.str(), attrs, 0, NULL, NULL, &timeOut, LDAP_NO_LIMIT,    &searchResult.msg );

        if ( rc != LDAP_SUCCESS )
        {
            DBGLOG("ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
            throw MakeStringException(-1, "ldap_search_ext_s error: %s, when searching %s under %s", ldap_err2string( rc ), filter.str(), m_ldapconfig->getUserBasedn());
        }

        StringBuffer act_ctrl;
        message = LdapFirstEntry( ld, searchResult);
        if(message != NULL)
        {
            CLDAPGetAttributesWrapper   atts(ld, searchResult);
            for ( attribute = atts.getFirst();
                  attribute != NULL;
                  attribute = atts.getNext())
            {
                if(0 == stricmp(attribute, "userAccountControl"))
                {
                    CLDAPGetValuesWrapper vals(ld, message, attribute);
                    if (vals.hasValues())
                    {
                        act_ctrl.append(vals.queryValues()[0]);
                        break;
                    }
                }
            }
        }

        if(act_ctrl.length() == 0)
        {
            DBGLOG("enableUser: userAccountControl doesn't exist for user %s",username);
            throw MakeStringException(-1, "enableUser: userAccountControl doesn't exist for user %s",username);
        }

        unsigned act_ctrl_val = atoi(act_ctrl.str());

        // UF_ACCOUNTDISABLE 0x0002
        act_ctrl_val &= 0xFFFFFFFD;
#ifdef _DONT_EXPIRE_PASSWORD
        // UF_DONT_EXPIRE_PASSWD 0x10000
        if (m_domainPwdsNeverExpire)
            act_ctrl_val |= 0x10000;
#endif

        StringBuffer new_act_ctrl;
        new_act_ctrl.append(act_ctrl_val);

        char *ctrl_values[] = {(char*)new_act_ctrl.str(), NULL};
        LDAPMod ctrl_attr = {
            LDAP_MOD_REPLACE,
            "userAccountControl",
            ctrl_values
        };
        LDAPMod *cattrs[2];
        cattrs[0] = &ctrl_attr;
        cattrs[1] = NULL;

        rc = ldap_modify_ext_s(ld, (char*)dn, cattrs, NULL, NULL);
        if ( rc != LDAP_SUCCESS )
        {
            throw MakeStringException(-1, "error enableUser %s, ldap_modify_ext_s error: %s", username, ldap_err2string( rc ));
        }

        // set the password.
        Owned<ISecUser> tmpuser = new CLdapSecUser(user->getName(), "");
        const char* passwd = user->credentials().getPassword();
        if(passwd == NULL || *passwd == '\0')
            passwd = "password";

        if (!updateUserPassword(*tmpuser, passwd, NULL))
        {
            DBGLOG("Error updating password for %s",username);
            throw MakeStringException(-1, "Error updating password for %s",username);
        }
    }


    virtual bool addUser(ISecUser& user)
    {
        const char* username = user.getName();
        if(username == NULL || *username == '\0')
        {
            DBGLOG("Can't add user, username not set");
            throw MakeStringException(-1, "Can't add user, username not set");
        }

        const char* fname = user.getFirstName();
        const char* lname = user.getLastName();
        if((lname == NULL || *lname == '\0') && (fname == NULL || *fname == '\0' || m_ldapconfig->getServerType() == IPLANET))
            lname = username;

        const char* fullname = user.getFullName();
        StringBuffer fullname_buf;
        if(fullname == NULL || *fullname == '\0')
        {
            if(fname != NULL && *fname != '\0')
            {
                fullname_buf.append(fname);
                if(lname != NULL && *lname != '\0')
                    fullname_buf.append(" ");
            }
            if(lname != NULL && *lname  != '\0')
                fullname_buf.append(lname);

            if(fullname_buf.length() == 0)
            {
                fullname_buf.append(username);
            }
            fullname = fullname_buf.str();
        }

        StringBuffer dn;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            dn.append("cn=").append(fullname).append(",");
        }
        else
        {
            dn.append("uid=").append(user.getName()).append(",");
        }
        dn.append(m_ldapconfig->getUserBasedn());

        char* oc_name;
        char* act_fieldname;
        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            oc_name = "User";
            act_fieldname = "sAMAccountName";
        }
        else
        {
            oc_name = "inetorgperson";
            act_fieldname = "uid";
        }

        char *cn_values[] = {(char*)fullname, NULL };
        LDAPMod cn_attr = 
        {
            LDAP_MOD_ADD,
            "cn",
            cn_values
        };

        char *oc_values[] = {oc_name, NULL};
        LDAPMod oc_attr =
        {
            LDAP_MOD_ADD,
            "objectClass",
            oc_values
        };

        char *gn_values[] = { (char*)fname, NULL };
        LDAPMod gn_attr = {
            LDAP_MOD_ADD,
            "givenName",
            gn_values
        };

        char *sn_values[] = { (char*)lname, NULL };
        LDAPMod sn_attr = {
            LDAP_MOD_ADD,
            "sn",
            sn_values
        };

        char* actname_values[] = {(char*)username, NULL};
        LDAPMod actname_attr =
        {
            LDAP_MOD_ADD,
            act_fieldname,
            actname_values
        };

        const char* passwd = user.credentials().getPassword();
        if(passwd == NULL || *passwd == '\0')
            passwd = "password";
        char* passwd_values[] = {(char*)passwd, NULL};
        LDAPMod passwd_attr =
        {
            LDAP_MOD_ADD,
            "userpassword",
            passwd_values
        };

        char *dispname_values[] = {(char*)fullname, NULL };
        LDAPMod dispname_attr = 
        {
            LDAP_MOD_ADD,
            "displayName",
            dispname_values
        };

        char* username_values[] = {(char*)username, NULL};
        LDAPMod username_attr =
        {
            LDAP_MOD_ADD,
            "userPrincipalName",
            username_values
        };

        LDAPMod *attrs[8];
        int ind = 0;
        
        attrs[ind++] = &cn_attr;
        attrs[ind++] = &oc_attr;
        if(fname != NULL && *fname != '\0')
            attrs[ind++] = &gn_attr;
        if(lname != NULL && *lname != '\0')
            attrs[ind++] = &sn_attr;
        attrs[ind++] = &actname_attr;

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            attrs[ind++] = &username_attr;
            attrs[ind++] = &dispname_attr;
        }
        else
        {
            attrs[ind++] = &passwd_attr;
        }

        attrs[ind] = NULL;

        Owned<ILdapConnection> lconn = m_connections->getConnection();
        LDAP* ld = ((CLdapConnection*)lconn.get())->getLd();
        int rc = ldap_add_ext_s(ld, (char*)dn.str(), attrs, NULL, NULL);
        if ( rc != LDAP_SUCCESS )
        {
            if(rc == LDAP_ALREADY_EXISTS)
            {
                DBGLOG("Can't add user %s, an LDAP object with this name already exists", username);
                throw MakeStringException(-1, "Can't add user %s, an LDAP object with this name already exists", username);
            }
            else
            {
                DBGLOG("Error addUser %s, ldap_add_ext_s error: %s", username, ldap_err2string( rc ));
                throw MakeStringException(-1, "Error addUser %s, ldap_add_ext_s error: %s", username, ldap_err2string( rc ));
            }
        }

        if(m_ldapconfig->getServerType() == ACTIVE_DIRECTORY)
        {
            try
            {
                enableUser(&user, dn.str(), ld);
            }
            catch(...)
            {
                deleteUser(&user);
                throw;
            }
        }

        //Add tempfile scope for this user (spill, paused and checkpoint
        //will be created under this user specific scope)
        StringBuffer resName(queryDfsXmlBranchName(DXB_Internal));
        resName.append("::").append(username);
        Owned<ISecResource> resource = new CLdapSecResource(resName.str());
        if (!addResource(RT_FILE_SCOPE, user, resource, PT_ADMINISTRATORS_AND_USER, m_ldapconfig->getResourceBasedn(RT_FILE_SCOPE)))
        {
            throw MakeStringException(-1, "Error adding temp file scope %s",resName.str());
        }

        return true;
    }

    bool createUserScope(ISecUser& user)
    {
        //Add tempfile scope for given user (spill, paused and checkpoint
        //files will be created under this user specific scope)
        StringBuffer resName(queryDfsXmlBranchName(DXB_Internal));
        resName.append("::").append(user.getName());
        Owned<ISecResource> resource = new CLdapSecResource(resName.str());
        return addResource(RT_FILE_SCOPE, user, resource, PT_ADMINISTRATORS_AND_USER, m_ldapconfig->getResourceBasedn(RT_FILE_SCOPE));
    }

    virtual aindex_t getManagedFileScopes(IArrayOf<ISecResource>& scopes)
    {
        getResourcesEx(RT_FILE_SCOPE, m_ldapconfig->getResourceBasedn(RT_FILE_SCOPE), NULL, NULL, scopes);
        return scopes.length();
    }

    virtual int queryDefaultPermission(ISecUser& user)
    {
        const char* basedn = m_ldapconfig->getResourceBasedn(RT_FILE_SCOPE);
        if(basedn == NULL || *basedn == '\0')
        {
            DBGLOG("corresponding basedn is not defined");
            return -2;
        }
        const char* basebasedn = strchr(basedn, ',') + 1;
        StringBuffer baseresource;
        baseresource.append(basebasedn-basedn-4, basedn+3);
        IArrayOf<ISecResource> base_resources;
        base_resources.append(*new CLdapSecResource(baseresource.str()));
        bool baseok = authorizeScope(user, base_resources, basebasedn);
        if(baseok)
            return base_resources.item(0).getAccessFlags();
        else
            return -2;
    }
};

int LdapUtils::getServerInfo(const char* ldapserver, int ldapport, StringBuffer& domainDN, LdapServerType& stype, const char* domainname)
{
    LdapServerType deducedSType = LDAPSERVER_UNKNOWN;
    LDAP* ld = LdapInit("ldap", ldapserver, ldapport, 636); 
    if(ld == NULL)
    {
        ERRLOG("ldap init error");
        return false;
    }
    
    int err = LdapSimpleBind(ld, NULL, NULL);
    if(err != LDAP_SUCCESS)
    {
        DBGLOG("ldap anonymous bind error (%d) - %s", err, ldap_err2string(err));

        // for new versions of openldap, version 2.2.*
        if(err == LDAP_PROTOCOL_ERROR)
            DBGLOG("If you're trying to connect to an OpenLdap server, make sure you have \"allow bind_v2\" enabled in slapd.conf");

        return err;
    }

    LDAPMessage* msg = NULL;
    char* attrs[] = {"namingContexts", NULL};
    err = ldap_search_s(ld, NULL, LDAP_SCOPE_BASE, "objectClass=*", attrs, false, &msg);
    if(err != LDAP_SUCCESS)
    {
        DBGLOG("ldap_search_s error: %s", ldap_err2string( err ));
        if (msg)
            ldap_msgfree(msg);
        return err;
    }
    LDAPMessage* entry = LdapFirstEntry(ld, msg);
    if(entry != NULL)
    {
        CLDAPGetValuesWrapper vals(ld, entry, "namingContexts");
        char** domains = vals.queryValues();
        if(domains != NULL)
        {
            int i = 0;
            char* curdn;
            StringBuffer onedn;
            while((curdn = domains[i]) != NULL)
            {
                if(*curdn != '\0' && (strncmp(curdn, "dc=", 3) == 0 || strncmp(curdn, "DC=", 3) == 0) && strstr(curdn,"DC=ForestDnsZones")==0 && strstr(curdn,"DC=DomainDnsZones")==0 )
                {
                    if(domainDN.length() == 0)
                    {
                        StringBuffer curdomain;
                        LdapUtils::getName(curdn, curdomain);
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
    ldap_unbind(ld);

    if (stype == LDAPSERVER_UNKNOWN)
        stype = deducedSType;
    else if (deducedSType != stype)
        WARNLOG("Ignoring deduced LDAP Server Type, does not match config LDAPServerType");

    return err;
}

#ifdef _WIN32
bool verifyServerCert(LDAP* ld, PCCERT_CONTEXT pServerCert)
{
    return true;
}
#endif

ILdapClient* createLdapClient(IPropertyTree* cfg)
{
    return new CLdapClient(cfg);
}

