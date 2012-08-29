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

#pragma warning( disable : 4786 )

#ifdef _WIN32
#ifdef ESPHTTP_EXPORTS
    #define esp_http_decl __declspec(dllexport)
#endif
#endif

#include "jliball.hpp"
#include "espcontext.hpp"
#include "http/platform/httptransport.ipp"
#include "sechandler.hpp"
#include "espprotocol.hpp"

class CEspContext : public CInterface, implements IEspContext
{
private:
    StringAttr      m_userid;
    StringAttr      m_password;
    StringAttr      m_realm;
    StringAttr      m_path;
    StringAttr      m_peer;
    StringAttr      m_useragent;

    StringBuffer    m_servName;
    StringBuffer    m_servHost;
    short           m_servPort;

    Owned<ISecUser> m_user;
    Owned<ISecResourceList> m_resources;
    Owned<ISecManager> m_secmgr;
    Owned<IAuthMap> m_feature_authmap;
    Owned<ISecPropertyList> m_sec_settings;

    void           *m_bindingValue;
    void           *m_serviceValue;

    bool            m_toBeAuthenticated;
    double          m_clientVer;

    Owned<IProperties> m_queryparams;

    Owned<IProperties> xslParameters;
    Owned<IMapInfo>    m_mapinfo;
    StringArray     m_custom_headers;
    
    unsigned options;

    SecHandler m_SecurityHandler;
    BoolHash  m_optGroups;

    StringArray m_traceValues;
    unsigned    m_active;
    unsigned    m_creationTime;
    unsigned    m_processingTime;
    unsigned    m_exceptionTime;
    bool        m_hasException;
    int         m_exceptionCode;

    ESPSerializationFormat respSerializationFormat;

public:
    IMPLEMENT_IINTERFACE;

    CEspContext() : m_servPort(0), m_bindingValue(0), m_serviceValue(0), m_toBeAuthenticated(false), options(0), m_clientVer(-1)
    {
        m_hasException =  false;
        m_creationTime = msTick();
        m_active=ActiveRequests::getCount();
        respSerializationFormat=ESPSerializationANY;
    }

    ~CEspContext()
    {
        flushTraceSummary();
    }
    virtual void addOptions(unsigned opts){options|=opts;}
    virtual void removeOptions(unsigned opts){opts&=~opts;}
    virtual unsigned queryOptions(){return options;}
    
    // versioning
    virtual double getClientVersion(){return m_clientVer;}
    virtual void setClientVersion(double ver){m_clientVer=ver;}
    virtual bool checkMinVer(double minVer) {  return m_clientVer<0 || m_clientVer >= minVer; }
    virtual bool checkMaxVer(double maxVer) {  return m_clientVer<0 || m_clientVer <= maxVer; }
    virtual bool checkMinMaxVer(double minVer, double maxVer) {  return m_clientVer<0 || m_clientVer>= minVer || m_clientVer <= maxVer; }
    virtual bool checkOptional(const char* option) { return m_queryparams.get() && m_queryparams->hasProp(option); }
    virtual bool isMethodAllowed(double version, const char* optional, const char* security, double maxver, double minver);

    virtual IMapInfo& queryMapInfo() 
    { 
        if (!m_mapinfo.get())
            m_mapinfo.setown(createMapInfo());
        return *m_mapinfo.get();
    }
    virtual bool suppressed(const char* structName, const char* fieldName);

    virtual void addOptGroup(const char* optGroup) {  if (optGroup) m_optGroups.setValue(optGroup,true); }
    virtual BoolHash& queryOptGroups() { return m_optGroups; }

    virtual void setUserID(const char* userid)
    {
        m_userid.set(userid);
    }
    virtual StringBuffer& getUserID(StringBuffer& userid)
    {
        userid.append(m_userid.get());
        return userid;
    }
    virtual const char * queryUserId()
    {
        return m_userid.get();
    }
    virtual void setPassword(const char* password)
    {
        m_password.set(password);
    }
    virtual StringBuffer& getPassword(StringBuffer& password)
    {
        password.append(m_password.get());
        return password;
    }
    virtual const char * queryPassword()
    {
        return m_password.get();
    }

    virtual void setRealm(const char* realm)
    {
        m_realm.set(realm);
    }
    virtual StringBuffer& getRealm(StringBuffer& realm)
    {
        realm.append(m_realm.get());
        return realm;
    }
    virtual const char * queryRealm()
    {
        return m_realm.get();
    }

    virtual void setContextPath(const char* path)
    {
        m_path.set(path);
    }

    virtual const char * getContextPath()
    {
        return m_path.get();
    }

    virtual void setUser(ISecUser* user)
    {
        m_user.setown(user);
        m_SecurityHandler.setUser(user);
    }

    virtual ISecUser* queryUser()
    {
        return m_user.get();
    }
    virtual void setServiceName(const char *name)
    {
        m_servName.clear().append(name).toLowerCase();
    }

    virtual const char * queryServiceName(const char *name)
    {
        return m_servName.str();
    }

    virtual void setCreationTime()
    {
        m_creationTime = msTick();
    }
    virtual const unsigned queryCreationTime()
    {
        return m_creationTime;
    }
    virtual void setProcessingTime()
    {
        m_processingTime = msTick() - m_creationTime;
    }
    virtual const unsigned queryProcessingTime()
    {
        return m_processingTime;
    }
    virtual void setException(int exceptionCode)
    {
        m_hasException = true;
        m_exceptionCode = exceptionCode;
        m_exceptionTime = msTick() - m_creationTime;
    }
    virtual const bool queryException(int& exceptionCode, unsigned& exceptionTime)
    {
        if (m_hasException)
        {
            exceptionCode = m_exceptionCode;
            exceptionTime = m_exceptionTime;
        }
        return m_hasException;
    }
    virtual const bool queryHasException()
    {
        return m_hasException;
    }

    virtual void setResources(ISecResourceList* rlist)
    {
        m_resources.setown(rlist);
        m_SecurityHandler.setResources(rlist);

    }

    virtual ISecResourceList* queryResources()
    {
        return m_resources.get();
    }

    virtual void setSecManger(ISecManager* mgr)
    {
        m_secmgr.setown(mgr);
        m_SecurityHandler.setSecManger(mgr);
    }

    virtual ISecManager* querySecManager()
    {
        return m_secmgr.get();
    }

    virtual void setBindingValue(void * value)
    {
        m_bindingValue=value;
    }

    virtual void * getBindingValue()
    {
        return m_bindingValue;
    }

    virtual void setServiceValue(void * value)
    {
        m_serviceValue=value;
    }

    virtual void * getServiceValue()
    {
        return m_serviceValue;
    }

    virtual void setToBeAuthenticated(bool val)
    {
        m_toBeAuthenticated = val;
    }

    virtual bool toBeAuthenticated()
    {
        return m_toBeAuthenticated;
    }

    virtual void setPeer(const char* peer)
    {
        m_peer.set(peer);
    }
    virtual StringBuffer& getPeer(StringBuffer& peer)
    {
        peer.append(m_peer.get());
        return peer;
    }

    virtual void setUseragent(const char* useragent)
    {
        if(useragent && *useragent)
            m_useragent.set(useragent);
    }
    virtual StringBuffer& getUseragent(StringBuffer& useragent)
    {
        const char* agent = m_useragent.get();
        if(agent && *agent)
            useragent.append(m_useragent.get());
        return useragent;
    }

    virtual IProperties *   queryRequestParameters()
    {
        if (!m_queryparams)
            m_queryparams.setown(createProperties(false));
        return m_queryparams.get();
    }

    virtual void setRequestParameters(IProperties * Parameters)
    {
        m_queryparams.set(Parameters);
    }
    
    virtual void setServAddress(const char * host, short port)
    {
        m_servHost.clear().append(host);
        m_servPort = port;
    }
    
    virtual void getServAddress(StringBuffer & host, short & port)
    {
        host.append(m_servHost);
        port = m_servPort;
    }
    
    virtual void setFeatureAuthMap(IAuthMap * map)
    {
        if(map != NULL)
        {
            m_feature_authmap.setown(map);
            m_SecurityHandler.setFeatureAuthMap(map);
        }
    }
    
    virtual IAuthMap * queryAuthMap()
    {
        return m_feature_authmap.get();
    }

    virtual void setSecuritySettings(ISecPropertyList* slist)
    {
        m_sec_settings.setown(slist);

    }
    virtual ISecPropertyList* querySecuritySettings()
    {
        return m_sec_settings.get();
    }

    virtual bool authorizeFeatures(StringArray & features, IEspStringIntMap & pmap)
    {
        return m_SecurityHandler.authorizeSecReqFeatures(features, pmap, NULL);
    }
    
    virtual bool authorizeFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access)
    {
        SecUserStatus user_status;
        return m_SecurityHandler.authorizeSecFeature(pszFeatureUrl, UserID, CompanyID, access,false,0, user_status);
    }

    virtual bool authorizeFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access,bool bCheckTrial,int DebitUnits, SecUserStatus& user_status)
    {
        return m_SecurityHandler.authorizeSecFeature(pszFeatureUrl, UserID, CompanyID, access,bCheckTrial, DebitUnits, user_status);
    }

    virtual bool authorizeFeature(const char* pszFeatureUrl, SecAccessFlags& access)
    {
        return m_SecurityHandler.authorizeSecFeature(pszFeatureUrl, access);
    }

    virtual bool validateFeatureAccess(const char* pszFeatureUrl, unsigned required, bool throwExcpt)
    {
        return m_SecurityHandler.validateSecFeatureAccess(pszFeatureUrl, required, throwExcpt);
    }

    void AuditMessage(AuditType type, const char *filterType, const char *title, const char *parms, ...);

    IProperties * queryXslParameters()
    {
        if (!xslParameters)
            xslParameters.setown(createProperties(false));
        return xslParameters.get();
    }

    StringArray& queryCustomHeaders()
    {
        return m_custom_headers;
    }

    void addCustomerHeader(const char* name, const char* val)
    {
        if(!name || !*name)
            throw MakeStringException(-1, "Header name can't be empty");
        m_custom_headers.append(StringBuffer(name).appendf(": %s", val?val:"").str());
    }

    virtual void addTraceSummaryValue(const char *name, const char *value)
    {
        StringBuffer str;
        if (name && *name)
            str.append(name).append('=');
        if (value && *value)
            str.append(value);
        m_traceValues.append(str.str());
    }

    virtual void addTraceSummaryValue(const char *name, int value)
    {
        StringBuffer str;
        if (name && *name)
            str.append(name).append('=');
        str.append(value);
        m_traceValues.append(str.str());
    }

    virtual void addTraceSummaryTimeStamp(const char *name)
    {
        if (name && *name)
        {
            unsigned timeval=msTick()-m_creationTime;
            StringBuffer value;
            value.append(name).append('=').appendulong(timeval).append("ms");
            m_traceValues.append(value.str());
        }
    }
    virtual void flushTraceSummary()
    {
        StringBuffer logstr;
        logstr.appendf("activeReqs=").append(m_active).append(';');
        logstr.append("user=").append(queryUserId());
        if (m_peer.length())
            logstr.append('@').append(m_peer.get());
        logstr.append(';');

        if (m_hasException)
        {
            logstr.appendf("exception@%dms=%d;", m_exceptionTime, m_exceptionCode);
        }

        StringBuffer value;
        value.append("total=").appendulong(m_processingTime).append("ms");
        if (m_hasException || (getEspLogLevel() > LogNormal))
        {
            m_traceValues.append(value.str());

            if (m_traceValues.length())
            {
                ForEachItemIn(idx, m_traceValues)
                    logstr.append(m_traceValues.item(idx)).append(";");
                m_traceValues.kill();
            }
        }
        else
        {
            logstr.appendf("%s;", value.str());
        }

        DBGLOG("TxSummary[%s]", logstr.str());
    }

    virtual ESPSerializationFormat getResponseFormat(){return respSerializationFormat;}
    virtual void setResponseFormat(ESPSerializationFormat fmt){respSerializationFormat = fmt;}
};

//---------------------------------------------------------
// implementations

void CEspContext::AuditMessage(AuditType type, const char *filterType, const char *title, const char *parms, ...)
{
    va_list args;
    va_start(args, parms);

    StringBuffer msg;
    StringBuffer format(title);
    format.appendf("\n\tProcess: esp\n\tService: %s\n\tUser: %s", m_servName.str(), queryUserId());
    if (parms)
        format.append("\n\t").append(parms);
    msg.valist_appendf(format.str(), args);
    
    va_end(args);
    AUDIT(type, msg.str());
}

bool CEspContext::suppressed(const char* structName, const char* fieldName) 
{
    if (!m_mapinfo)
        return false;

    double ver = getClientVersion();

    double minver = m_mapinfo->getMinVersion(structName,fieldName);
    if (minver>0 && ver<minver)
        return true;

    double deprver = m_mapinfo->getDeprVersion(structName,fieldName);
    if (deprver>0)
    {
        if (ver>=deprver)
            return true;
    }
    else 
    {
        double maxver = m_mapinfo->getMaxVersion(structName,fieldName);
        if (maxver>0 && ver>maxver)
            return true;
    }

    const char* optional = m_mapinfo->getOptional(structName,fieldName);
    if (optional)
        return !queryRequestParameters()->hasProp(optional);

    return false;
}

bool CEspContext::isMethodAllowed(double version, const char* optional, const char* security, double minver, double maxver)
{
    if (optional)
    {       
        IProperties *props = queryRequestParameters();
        if (props && !props->hasProp(optional))
            return false;
    }

    if (security)
    {
        SecAccessFlags acc;
        if (!authorizeFeature(security, acc) || (acc==SecAccess_None)) 
            return false;
    }
            
    if (minver>0 && version<minver)
        return false;

    if (maxver>0 && version>maxver) 
        return false;

    return true;
}

IEspContext* createEspContext()
{
    return new CEspContext;
}

bool getUrlParams(IProperties *props, StringBuffer& params)
{
    bool hasVersion = false;
    if (props) {
        Owned<IPropertyIterator> it = props->getIterator();
        for (it->first(); it->isValid(); it->next()) {
            const char* key = it->getPropKey();
            if (!key || !*key || stricmp(key,"form")==0 || stricmp(key,"__querystring")==0)
                continue;
            if (params.length()==0)
                params.append("?");
            else
                params.append("&");
            params.append(key);
            if (stricmp(key,"ver_")==0)
                hasVersion = true;
            const char* v = props->queryProp(key);
            if (v && *v)
                params.appendf("=%s",v);
        }
    }
    return hasVersion;
}

void getEspUrlParams(IEspContext& ctx, StringBuffer& params, const char* excludeParams[])
{
    bool hasVersion = false, addAmpersand = false;
    int excludes = 0;
    if (excludeParams) 
        while (excludeParams[excludes]) excludes++;

    IProperties* props = ctx.queryRequestParameters();
    if (props) 
    {
        const char* querystr = props->queryProp("__querystring");
        if (querystr)
        {
            StringArray ps;
            DelimToStringArray(querystr,ps,"&",true);
            for (unsigned int i=0; i<ps.ordinality(); i++)
            {
                const char* item = ps.item(i);
                const char* eq = strchr(item,'=');
                StringAttr key;
                if (eq)
                    key.set(item, eq-item);
                else
                    key.set(item);
    
                bool excluded = false;
                if (*key.get()=='.')
                    excluded = true;
                else for (int i=0; i<excludes; i++)
                {
                    if (stricmp(excludeParams[i],key.get())==0) 
                    {
                        excluded = true;
                        break;
                    }
                }
                
                if (!excluded) 
                {
                    if (addAmpersand) 
                        params.append('&');
                    else
                        addAmpersand = true;
                    params.append(item);
                }
                
                if (stricmp(key,"ver_")==0)
                    hasVersion = true;
            }           
        }
    }

    if (!hasVersion)
        params.appendf("%sver_=%g", addAmpersand?"&":"", ctx.getClientVersion());
}


void addEspNativeArray(StringBuffer& schema, const char* xsdType, const char* arrayType)
{
    schema.appendf("<xsd:complexType name=\"%s\">"
            "<xsd:sequence>"
                "<xsd:element name=\"Item\" type=\"xsd:%s\"  minOccurs=\"0\" maxOccurs=\"unbounded\" />"
            "</xsd:sequence>"
        "</xsd:complexType>\n", arrayType, xsdType);
}

void checkRequest(IEspContext& ctx)
{
#ifdef ENABLE_NEW_SECURITY
    ISecUser* user = ctx.queryUser();
    if (user && user->getStatus()!=SecUserStatus_Unknown) // no user means security is not configured
    {
        BoolHash& groups = ctx.queryOptGroups();
        if (groups.find("internal"))
        {
            if(user->getStatus()!=SecUserStatus_Inhouse)
            {
                ERRLOG("User %s trying to access unauthorized feature: internal", user->getName() ? user->getName() : ctx.queryUserId());
                throw MakeStringException(400,"Bad request");
            }
        }
    }
#elif !defined(DISABLE_NEW_SECURITY)
#error Please include esphttp.hpp in this file.
#endif
}

//--------------------------------
// log level

static IEspContainer*& getContainer()
{
    static IEspContainer* gContainer = NULL;
//  printf("Container: %p\n", gContainer);
    return gContainer;
}

LogLevel getEspLogLevel() { return getEspLogLevel(NULL); }

LogLevel getEspLogLevel(IEspContext* ctx)
{
    if (ctx)
    {
        ISecPropertyList* properties = ctx->querySecuritySettings();
        if (properties)
        {
            ISecProperty* sec = properties->findProperty("DebugMode");
            if (sec)
            {
                const char* mode = sec->getValue();
                if ( mode && (streq(mode,"1") || streq(mode, "true")) )
                    return LogMax;
            }
        }
    }

    if (getContainer())
        return getContainer()->getLogLevel();
    return LogMin;
}

bool getEspLogRequests()
{
    if (getContainer())
        return getContainer()->getLogRequests();
    return false;
}

bool getEspLogResponses()
{
    if (getContainer())
        return getContainer()->getLogResponses();
    return false;
}

unsigned getSlowProcessingTime()
{
    if (getContainer())
        return getContainer()->getSlowProcessingTime();
    return false;
}

void ESPLOG(LogLevel level, const char* fmt, ...)
{
    if (getEspLogLevel(NULL)>=level)
    {
        va_list args;
        va_start(args,fmt);
        VALOG(MCdebugInfo, unknownJob, fmt, args);
        va_end(args);
    }
}

void ESPLOG(IEspContext* ctx, LogLevel level, const char* fmt, ...)
{
    if (getEspLogLevel(ctx)>=level)
    {
        va_list args;
        va_start(args,fmt);
        VALOG(MCdebugInfo, unknownJob, fmt, args);
        va_end(args);
    }
}

void setEspContainer(IEspContainer* container)
{
    getContainer() = container;
}

IEspContainer* getESPContainer()
{
    return getContainer();
}

static StringBuffer g_cfd;

void setCFD(const char* cfd)
{
    g_cfd.clear();
    if(cfd&&*cfd)
        g_cfd.append(cfd);

    g_cfd.trim();
    if(g_cfd.length() > 0)
    {
        char lastChar = g_cfd.charAt(g_cfd.length() - 1);
        if(lastChar != PATHSEPCHAR && lastChar != '/')
            g_cfd.append(PATHSEPCHAR);
    }
}

const char* getCFD()
{
    return g_cfd.str();
}

static StringBuffer g_buildVersion;

void setBuildVersion(const char* buildVersion)
{
    g_buildVersion.clear();
    if(buildVersion&&*buildVersion)
        g_buildVersion.append(buildVersion);

    g_buildVersion.trim();
}

const char* getBuildVersion()
{
    return g_buildVersion.str();
}
static StringBuffer g_buildLevel;

void setBuildLevel(const char* buildLevel)
{
    g_buildLevel.clear();
    if(buildLevel&&*buildLevel)
        g_buildLevel.append(buildLevel);

    g_buildLevel.trim();
}

const char* getBuildLevel()
{
    return g_buildLevel.str();
}
