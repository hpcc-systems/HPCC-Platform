/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#include "esphttp.hpp"

#include "jliball.hpp"
#include "espcontext.hpp"
#include "txsummary.hpp"
#include "http/platform/httptransport.ipp"
#include "sechandler.hpp"
#include "espprotocol.hpp"
#include "espsecurecontext.hpp"
#include "ldapsecurity.ipp"
#include "dasds.hpp"

class CEspContext : public CInterface, implements IEspContext
{
private:
    StringAttr      m_userid;
    StringAttr      m_password;
    StringAttr      m_realm;
    StringAttr      m_path;
    StringAttr      m_peer;
    StringAttr      m_useragent;
    StringAttr      m_acceptLanguage;
    StringAttr      httpMethod;
    StringAttr      servMethod;
    StringAttr      esdlBindingID;

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

    Owned<CTxSummary> m_txSummary;

    unsigned    m_active;
    unsigned    m_creationTime;
    unsigned    m_processingTime;
    unsigned    m_exceptionTime;
    bool        m_hasException;
    int         m_exceptionCode;
    StringAttr  respMsg;
    StringAttr  authStatus = AUTH_STATUS_NA;
    StringAttr  authenticationMethod;
    AuthType    domainAuthType = AuthPerRequestOnly;
    AuthError   authError = EspAuthErrorNone;

    ESPSerializationFormat respSerializationFormat;

    Owned<IEspSecureContext> m_secureContext;

    StringAttr   m_transactionID;
    StringBuffer   m_globalId;
    StringBuffer   m_localId;
    StringBuffer   m_callerId;
    IHttpMessage* m_request;

public:
    IMPLEMENT_IINTERFACE;

    CEspContext(IEspSecureContext* secureContext)
    : m_servPort(0)
    , m_bindingValue(0)
    , m_serviceValue(0)
    , m_toBeAuthenticated(false)
    , m_clientVer(-1)
    , options(0)
    , m_active(ActiveRequests::getCount())
    , m_creationTime(msTick())
    , m_processingTime(0)
    , m_exceptionTime(0)
    , m_hasException(false)
    , m_exceptionCode(0)
    , respSerializationFormat(ESPSerializationANY)
    {
        m_txSummary.setown(new CTxSummary(m_creationTime));
        updateTraceSummaryHeader();
        m_secureContext.setown(secureContext);
        m_SecurityHandler.setSecureContext(secureContext);
        appendGloballyUniqueId(m_localId);
        // use localId as globalId unless we receive another
        m_globalId.set(m_localId);
    }

    ~CEspContext()
    {
        flushTraceSummary();
        if (m_txSummary)
        {
            m_txSummary->tailor(this);
            m_txSummary->log(getTxSummaryLevel(), getTxSummaryGroup(), getTxSummaryStyle());
        }
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
    virtual bool checkOptional(const char* option)
    {
        if (option && *option == '!')
            return !m_queryparams.get() || !m_queryparams->hasProp(option+1);
        else
            return m_queryparams.get() && m_queryparams->hasProp(option);
    }
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
    virtual void setSessionToken(unsigned token)
    {
        if (m_user)
            m_user->credentials().setSessionToken(token);
    }
    virtual unsigned querySessionToken()
    {
        return m_user ? m_user->credentials().getSessionToken() : 0;
    }
    virtual void setSignature(const char * signature)
    {
        if (m_user)
            m_user->credentials().setSignature(signature);
    }
    virtual const char * querySignature()
    {
        return m_user ? m_user->credentials().getSignature() : nullptr;
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

    virtual void setAcceptLanguage(const char* acceptLanguage)
    {
        if(acceptLanguage && *acceptLanguage)
            m_acceptLanguage.set(acceptLanguage);
    }
    virtual StringBuffer& getAcceptLanguage(StringBuffer& acceptLanguage)
    {
        const char* acceptLang = m_acceptLanguage.get();
        if(acceptLang && *acceptLang)
            acceptLanguage.set(m_acceptLanguage.get());
        return acceptLanguage;
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

    virtual bool validateFeaturesAccess(MapStringTo<SecAccessFlags> & pmap, bool throwExcpt)
    {
        return m_SecurityHandler.validateSecFeaturesAccess(pmap, throwExcpt);
    }

    virtual bool validateFeatureAccess(const char* pszFeatureUrl, unsigned required, bool throwExcpt)
    {
        return m_SecurityHandler.validateSecFeatureAccess(pszFeatureUrl, required, throwExcpt);
    }

    virtual void ensureFeatureAccess(const char* pszFeatureUrl, unsigned required, unsigned excCode, const char* excMsg)
    {
        if (!validateFeatureAccess(pszFeatureUrl, required, false))
        {
            setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(excCode, "Resource %s : %s", pszFeatureUrl, excMsg);
        }
    }

    virtual void ensureSuperUser(unsigned excCode, const char* excMsg)
    {
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(m_secmgr.get());
        if (secmgr && !secmgr->isSuperUser(m_user.get()))
        {
            setAuthStatus(AUTH_STATUS_NOACCESS);
            throw makeStringException(excCode, excMsg);
        }
    }

    void AuditMessage(AuditType type, const char *filterType, const char *title, const char *parms, ...) __attribute__((format(printf, 5, 6)));
    void AuditMessage(AuditType type, const char *filterType, const char *title);

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

    virtual void setHTTPMethod(const char *method)
    {
        httpMethod.set(method);
    }

    virtual void setServiceMethod(const char *method)
    {
        servMethod.set(method);
    }

    virtual void setESDLBindingID(const char *id)
    {
        esdlBindingID.set(id);
    }
    virtual const char* queryESDLBindingID()
    {
        return esdlBindingID.get();
    }

    virtual CTxSummary* queryTxSummary()
    {
        return m_txSummary.get();
    }

    virtual void addTraceSummaryValue(LogLevel logLevel, const char *name, const char *value, const unsigned int group = TXSUMMARY_GRP_CORE)
    {
        if (m_txSummary && !isEmptyString(name))
            m_txSummary->append(name, value, logLevel, group);
    }

    virtual void addTraceSummaryValue(LogLevel logLevel, const char *name, __int64 value, const unsigned int group = TXSUMMARY_GRP_CORE)
    {
        if (m_txSummary && !isEmptyString(name))
            m_txSummary->append(name, value, logLevel, group);
    }

    virtual void addTraceSummaryDoubleValue(LogLevel logLevel, const char *name, double value, const unsigned int group = TXSUMMARY_GRP_CORE)
    {
        if (m_txSummary && !isEmptyString(name))
            m_txSummary->append(name, value, logLevel, group);
    }

    virtual void addTraceSummaryTimeStamp(LogLevel logLevel, const char *name, const unsigned int group = TXSUMMARY_GRP_CORE)
    {
        if (m_txSummary && !isEmptyString(name))
            m_txSummary->append(name, m_txSummary->getElapsedTime(), logLevel, group, "ms");
    }
    virtual void flushTraceSummary()
    {
        updateTraceSummaryHeader();
        if (m_txSummary)
        {
            m_txSummary->set("auth", authStatus.get(), LogMin, TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
            m_txSummary->append("total", m_processingTime, LogMin, TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE, "ms");
        }
    }

    virtual void addTraceSummaryCumulativeTime(LogLevel logLevel, const char* name, unsigned __int64 time, const unsigned int group = TXSUMMARY_GRP_CORE)
    {
        if (m_txSummary && !isEmptyString(name))
            m_txSummary->updateTimer(name, time, logLevel, group);
    }

    virtual CumulativeTimer* queryTraceSummaryCumulativeTimer(LogLevel logLevel, const char *name, const unsigned int group = TXSUMMARY_GRP_CORE)
    {
        return (m_txSummary ? m_txSummary->queryTimer(name, logLevel, group) : NULL);
    }
    virtual void cancelTxSummary()
    {
        if (!m_txSummary)
            return;

        m_txSummary->clear();
        m_txSummary.clear();
    }

    virtual void setAuthStatus(const char* status)
    {
        authStatus.set(status);
    }

    virtual const char* queryAuthStatus()
    {
        return authStatus.str();
    }

    virtual void setAuthenticationMethod(const char* method)
    {
        authenticationMethod.set(method);
    }

    virtual const char * getAuthenticationMethod()
    {
        return authenticationMethod.get();
    }

    virtual void setDomainAuthType(AuthType type) { domainAuthType = type; }
    virtual AuthType getDomainAuthType(){ return domainAuthType; }
    virtual void setAuthError(AuthError error) { authError = error; }
    virtual AuthError getAuthError(){ return authError; }
    virtual void setRespMsg(const char* msg)
    {
        respMsg.set(msg);
    }

    virtual const char* getRespMsg()
    {
        return respMsg.get();
    }

    virtual ESPSerializationFormat getResponseFormat(){return respSerializationFormat;}
    virtual void setResponseFormat(ESPSerializationFormat fmt){respSerializationFormat = fmt;}

    void updateTraceSummaryHeader();
    IEspSecureContext* querySecureContext() override
    {
        return m_secureContext.get();
    }

    virtual void setTransactionID(const char * trxid)
    {
        m_transactionID.set(trxid);
    }
    virtual const char * queryTransactionID()
    {
        return m_transactionID.get();
    }
    virtual void setRequest(IHttpMessage* req)
    {
        m_request = req;
    }
    virtual IHttpMessage* queryRequest()
    {
        return m_request;
    }

    virtual void setGlobalId(const char* id)
    {
        m_globalId.set(id);
    }
    virtual const char* getGlobalId()
    {
        return m_globalId.str();
    }
    virtual void setCallerId(const char* id)
    {
        m_callerId.set(id);
    }
    virtual const char* getCallerId()
    {
        return m_callerId.str();
    }
    // No setLocalId() - it should be set once only when constructed
    virtual const char* getLocalId()
    {
        return m_localId.str();
    }
};

//---------------------------------------------------------
// implementations

void CEspContext::AuditMessage(AuditType type, const char *filterType, const char *title, const char *parms, ...)
{
    va_list args;
    va_start(args, parms);

    StringBuffer msg(title);
    msg.appendf("\n\tProcess: esp\n\tService: %s\n\tUser: %s", m_servName.str(), queryUserId());
    if (parms)
        msg.append("\n\t").valist_appendf(parms, args);
    
    va_end(args);
    AUDIT(type, msg.str());
}

void CEspContext::AuditMessage(AuditType type, const char *filterType, const char *title)
{
    VStringBuffer msg("%s\n\tProcess: esp\n\tService: %s\n\tUser: %s", title, m_servName.str(), queryUserId());
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

void CEspContext::updateTraceSummaryHeader()
{
    if (m_txSummary)
    {
        m_txSummary->set("activeReqs", m_active, LogMin, TXSUMMARY_GRP_CORE|TXSUMMARY_GRP_ENTERPRISE);
        VStringBuffer user("%s%s%s", (queryUserId() ? queryUserId() : ""), (m_peer.length() ? "@" : ""), m_peer.str());
        if (!user.isEmpty())
            m_txSummary->set("user", user.str(), LogMin);

        VStringBuffer reqSummary("%s", httpMethod.isEmpty() ? "" : httpMethod.get());
        if (!m_servName.isEmpty() || !servMethod.isEmpty())
        {
            if (!reqSummary.isEmpty())
                reqSummary.append(" ");
            if (!m_servName.isEmpty())
                reqSummary.append(m_servName.str());
            if (!servMethod.isEmpty())
                reqSummary.append(".").append(servMethod.str());
        }
        if (m_clientVer > 0)
        {
            if (!reqSummary.isEmpty())
                reqSummary.append(" ");
            reqSummary.append("v").append(m_clientVer);
        }
        if (!reqSummary.isEmpty())
            m_txSummary->set("req", reqSummary.str(), LogMin, TXSUMMARY_GRP_CORE);
        if (m_hasException)
        {
            m_txSummary->set("excepttime", m_exceptionTime, LogMin, TXSUMMARY_GRP_CORE);
            m_txSummary->set("exceptcode", m_exceptionCode, LogMin, TXSUMMARY_GRP_CORE);
        }
    }
}

IEspContext* createEspContext(IEspSecureContext* secureContext)
{
    return new CEspContext(secureContext);
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
            ps.appendListUniq(querystr, "&");
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
                OWARNLOG("User %s trying to access unauthorized feature: internal", user->getName() ? user->getName() : ctx.queryUserId());
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

LogRequest readLogRequest(char const* req)
{
    if (isEmptyString(req))
        return LogRequestsNever;

    if (strieq(req, "all"))
        return LogRequestsAlways;
    if (strieq(req, "never"))
        return LogRequestsNever;
    if (strieq(req, "only-ones-with-issues"))
        return LogRequestsWithIssuesOnly;
    if (strToBool(req))
        return LogRequestsAlways;
    return LogRequestsNever;
}

StringBuffer& getLogRequestString(LogRequest req, StringBuffer& out)
{
    if (req == LogRequestsAlways)
        out.append("all");
    else if (req == LogRequestsWithIssuesOnly)
        out.append("only-ones-with-issues");
    else
        out.append("never");
    return out;
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

LogLevel getTxSummaryLevel()
{
    if (getContainer())
        return getContainer()->getTxSummaryLevel();
    return LogMin;
}

const unsigned int readTxSummaryStyle(char const* style)
{
    if (isEmptyString(style))
        return TXSUMMARY_OUT_TEXT;

    if (strieq(style, "text"))
        return TXSUMMARY_OUT_TEXT;
    if (strieq(style, "json"))
        return TXSUMMARY_OUT_JSON;
    if (strieq(style, "all"))
        return TXSUMMARY_OUT_TEXT | TXSUMMARY_OUT_JSON;

    return TXSUMMARY_OUT_TEXT;
}

const unsigned int readTxSummaryGroup(char const* group)
{
    if (isEmptyString(group))
        return TXSUMMARY_GRP_CORE;

    if (strieq(group, "core"))
        return TXSUMMARY_GRP_CORE;
    if (strieq(group, "enterprise"))
        return TXSUMMARY_GRP_ENTERPRISE;
    if (strieq(group, "all"))
        return TXSUMMARY_GRP_CORE | TXSUMMARY_GRP_ENTERPRISE;

    return TXSUMMARY_GRP_CORE;
}

const unsigned int getTxSummaryStyle()
{
    if (getContainer())
        return getContainer()->getTxSummaryStyle();
    return TXSUMMARY_OUT_TEXT;
}

const unsigned int getTxSummaryGroup()
{
    if (getContainer())
        return getContainer()->getTxSummaryGroup();
    return TXSUMMARY_GRP_CORE;
}

bool getTxSummaryResourceReq()
{
    if (getContainer())
        return getContainer()->getTxSummaryResourceReq();
    return false;
}

LogRequest getEspLogRequests()
{
    if (getContainer())
        return getContainer()->getLogRequests();
    return LogRequestsNever;
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
    if (g_cfd.length())
        makeAbsolutePath(g_cfd, true);
    if (g_cfd.length())
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
IEspServer* queryEspServer()
{
    return dynamic_cast<IEspServer*>(getESPContainer());
}

IRemoteConnection* getSDSConnectionWithRetry(const char* xpath, unsigned mode, unsigned timeoutMs)
{
    CTimeMon timer(timeoutMs);
    unsigned remaining;
    while (!timer.timedout(&remaining))
    {
        try
        {
            unsigned connTimeoutMs = remaining > SESSION_SDS_LOCK_TIMEOUT ? SESSION_SDS_LOCK_TIMEOUT : remaining;
            Owned<IRemoteConnection> conn = querySDS().connect(xpath, myProcessSession(), mode, connTimeoutMs);
            if (!conn)
                throw MakeStringException(-1, "getSDSConnectionWithRetry() : unabled to establish connection to : %s", xpath);
            return conn.getClear();
        }
        catch (ISDSException* e)
        {
            if (SDSExcpt_LockTimeout != e->errorCode())
                throw;
            IERRLOG(e, "getSDSConnectionWithRetry()");
            e->Release();
        }
    }
    return nullptr;
}
