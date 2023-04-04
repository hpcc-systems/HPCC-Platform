/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

#pragma once

#include "seclib.hpp"
#include "esplog.hpp"
#include "mapinfo.hpp"

interface IEspStringIntMap : extends IInterface
{
    virtual int queryValue(const char * key) = 0;
    virtual void setValue(const char * key, int value) = 0;
};


typedef MapBetween<int, int, StringAttr, const char *> MapIntToStr;
typedef MapStringTo<StringBuffer, StringBuffer&> MapStrToBuf;

#define HTTP_PROTOCOL 2

#if defined(ESP_TIMING)
#define ESP_TIME_SECTION(str) TIME_SECTION(str)
#else
#define ESP_TIME_SECTION(str)
#endif

typedef MapStringTo<bool> BoolHash;

interface IHttpMessage : extends IInterface
{
    virtual int receive(IMultiException * me) = 0;
    virtual int receive(bool alwaysReadContent, IMultiException * me) = 0;
    virtual int send() = 0;
    virtual StringBuffer & getContent(StringBuffer & buf) = 0;
    virtual StringBuffer & getContentType(StringBuffer & contenttype) = 0;
    virtual StringBuffer & getHeader(const char * headername, StringBuffer & headerval) = 0;
    virtual StringBuffer & getStatus(StringBuffer & status) = 0;
};


typedef enum ESPSerializationFormat_
{
    ESPSerializationANY,
    ESPSerializationXML,
    ESPSerializationJSON,
    ESPSerializationCSV,
    ESPSerializationTEXT
} ESPSerializationFormat;

typedef enum AuthType_
{
    AuthTypeMixed,
    AuthPerSessionOnly,
    AuthPerRequestOnly,
    AuthUserNameOnly
} AuthType;

typedef enum AuthError_
{
    EspAuthErrorNone,
    EspAuthErrorEmptyUserID,
    EspAuthErrorUserNotFoundInContext,
    EspAuthErrorNoAuthMechanism,
    EspAuthErrorEmptySecResource,
    EspAuthErrorNotAuthenticated
} AuthError;

typedef enum LogRequest_
{
    LogRequestsNever,
    LogRequestsWithIssuesOnly,
    LogRequestsAlways
} LogRequest;


#define TXSUMMARY_GRP_CORE        0x00000001
#define TXSUMMARY_GRP_ENTERPRISE  0x00000002

#define TXSUMMARY_OUT_TEXT      0x00000001
#define TXSUMMARY_OUT_JSON      0x00000002

#define ESPCTX_NO_NAMESPACES    0x00000001
#define ESPCTX_WSDL             0x00000010
#define ESPCTX_WSDL_EXT         0x00000100
#define ESPCTX_NO_ANNOTATION    0x00001000
#define ESPCTX_ALL_ANNOTATION   0x00010000

class CTxSummary;
interface IEspSecureContext;
class CumulativeTimer;

interface IEspContext : extends IInterface
{
    virtual void setUserID(const char * userid) = 0;
    virtual StringBuffer & getUserID(StringBuffer & userid) = 0;
    virtual const char * queryUserId() = 0;
    virtual void setPassword(const char * password) = 0;
    virtual StringBuffer & getPassword(StringBuffer & password) = 0;
    virtual const char * queryPassword() = 0;
    virtual void setRealm(const char * realm) = 0;
    virtual StringBuffer & getRealm(StringBuffer & realm) = 0;
    virtual const char * queryRealm() = 0;
    virtual void setUser(ISecUser * user) = 0;
    virtual ISecUser * queryUser() = 0;
    virtual void setSessionToken(unsigned token) = 0;
    virtual unsigned querySessionToken() = 0;
    virtual void setSignature(const char * signature) = 0;
    virtual const char * querySignature() = 0;
    virtual void setResources(ISecResourceList * rlist) = 0;
    virtual ISecResourceList * queryResources() = 0;
    virtual void setSecManger(ISecManager * mgr) = 0;
    virtual ISecManager * querySecManager() = 0;
    virtual void setContextPath(const char * path) = 0;
    virtual const char * getContextPath() = 0;
    virtual void setBindingValue(void * value) = 0;
    virtual void * getBindingValue() = 0;
    virtual void setServiceValue(void * value) = 0;
    virtual void * getServiceValue() = 0;
    virtual void setToBeAuthenticated(bool val) = 0;
    virtual bool toBeAuthenticated() = 0;
    virtual void setPeer(const char * peer) = 0;
    virtual StringBuffer & getPeer(StringBuffer & peer) = 0;
    virtual void setUseragent(const char * useragent) = 0;
    virtual StringBuffer & getUseragent(StringBuffer & useragent) = 0;
    virtual void setFeatureAuthMap(IAuthMap * map) = 0;
    virtual IAuthMap * queryAuthMap() = 0;

    virtual void setSecuritySettings(ISecPropertyList * slist) = 0;
    virtual ISecPropertyList * querySecuritySettings() = 0;

    virtual bool authorizeFeature(const char * pszFeatureUrl, SecAccessFlags & access) = 0;
    virtual bool authorizeFeatures(StringArray & features, IEspStringIntMap & pmap) = 0;
    virtual bool authorizeFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access) = 0;

    virtual bool authorizeFeature(const char * pszFeatureUrl, const char* UserID, const char* CompanyID, SecAccessFlags & access,bool bCheckTrial ,int DebitUnits,  SecUserStatus& user_status) = 0;

    virtual bool validateFeaturesAccess(MapStringTo<SecAccessFlags> & pmap, bool throwExcpt) = 0;
    virtual bool validateFeatureAccess(const char * pszFeatureUrl, unsigned required, bool throwExcpt) = 0;
    virtual void ensureFeatureAccess(const char * pszFeatureUrl, unsigned required, unsigned excCode, const char * excMsg) = 0;
    virtual void ensureSuperUser(unsigned excCode, const char * excMsg) = 0;
    virtual void setServAddress(const char * host, short port) = 0;
    virtual void getServAddress(StringBuffer & host, short & port) = 0;
    virtual void AuditMessage(AuditType type, const char * filterType, const char * title, const char * parms, ...) __attribute__((format(printf, 5, 6))) = 0;
    virtual void AuditMessage(AuditType type, const char * filterType, const char * title) = 0;

    virtual void setServiceName(const char *name)=0;
    virtual const char * queryServiceName(const char *name)=0;
    virtual const unsigned queryCreationTime()=0;
    virtual void setProcessingTime()=0;
    virtual const unsigned queryProcessingTime()=0;
    virtual void setException(int exceptionCode)=0;
    virtual const bool queryException(int& exceptionCode, unsigned& exceptionTime)=0;
    virtual const bool queryHasException()=0;

    virtual IProperties *   queryRequestParameters()=0;
    virtual void            setRequestParameters(IProperties * Parameters)=0;

    virtual IProperties * queryXslParameters()=0;
    virtual void addOptions(unsigned opts)=0;
    virtual void removeOptions(unsigned opts)=0;
    virtual unsigned queryOptions()=0;

    // versioning
    virtual double getClientVersion()=0;
    virtual void setClientVersion(double ver)=0;

    virtual bool checkMinVer(double minver) = 0;
    virtual bool checkMaxVer(double maxver) = 0;
    virtual bool checkMinMaxVer(double minver, double maxver) = 0;
    virtual bool checkOptional(const char*) = 0;
    virtual bool isMethodAllowed(double version, const char* optional, const char* security, double minver, double maxver)=0;
//  virtual void setMapInfo(IMapInfo*) = 0;
    virtual IMapInfo& queryMapInfo() = 0;
    virtual bool suppressed(const char* structName, const char* fieldName) = 0;

    virtual void addOptGroup(const char* optGroup) = 0;
    virtual BoolHash& queryOptGroups() = 0;

    virtual StringArray& queryCustomHeaders() = 0;
    virtual void addCustomerHeader(const char* name, const char* val) = 0;

    virtual CTxSummary* queryTxSummary()=0;
    virtual void addTraceSummaryValue(unsigned logLevel, const char *name, const char *value, const unsigned int group = TXSUMMARY_GRP_CORE)=0;
    virtual void addTraceSummaryValue(unsigned logLevel, const char *name, __int64 value, const unsigned int group = TXSUMMARY_GRP_CORE)=0;
    virtual void addTraceSummaryDoubleValue(unsigned logLevel, const char *name, double value, const unsigned int group = TXSUMMARY_GRP_CORE)=0;
    virtual void addTraceSummaryTimeStamp(unsigned logLevel, const char *name, const unsigned int group = TXSUMMARY_GRP_CORE)=0;
    virtual void addTraceSummaryCumulativeTime(unsigned logLevel, const char* name, unsigned __int64 time, const unsigned int group = TXSUMMARY_GRP_CORE)=0;
    virtual CumulativeTimer* queryTraceSummaryCumulativeTimer(unsigned logLevel, const char *name, const unsigned int group = TXSUMMARY_GRP_CORE)=0;
    virtual void cancelTxSummary()=0;


    virtual ESPSerializationFormat getResponseFormat()=0;
    virtual void setResponseFormat(ESPSerializationFormat fmt)=0;

    virtual void setAcceptLanguage(const char * acceptLanguage) = 0;
    virtual StringBuffer& getAcceptLanguage(StringBuffer& acceptLanguage) = 0;

    virtual IEspSecureContext* querySecureContext() = 0;
    virtual void setHTTPMethod(const char *method) = 0;
    virtual void setServiceMethod(const char *method) = 0;

    virtual void setESDLBindingID(const char * id) = 0;
    virtual const char * queryESDLBindingID() = 0;

    virtual void setTransactionID(const char * trxid) = 0;
    virtual const char * queryTransactionID() = 0;

    virtual const char * getAuthenticationMethod()=0;
    virtual void setAuthenticationMethod(const char * method)=0;
    virtual void setDomainAuthType(AuthType type)=0;
    virtual AuthType getDomainAuthType()=0;
    virtual void setAuthError(AuthError error)=0;
    virtual AuthError getAuthError()=0;
    virtual void setAuthStatus(const char * status)=0;
    virtual const char* queryAuthStatus()=0;
    virtual const char * getRespMsg()=0;
    virtual void setRespMsg(const char * msg)=0;
    virtual void setRequest(IHttpMessage* req) = 0;
    virtual IHttpMessage* queryRequest() = 0;

    virtual void setGlobalId(const char* id)=0;
    virtual const char* getGlobalId()=0;
    virtual void setCallerId(const char* id)=0;
    virtual const char* getCallerId()=0;
    virtual const char* getLocalId()=0;
};


typedef unsigned LogLevel;
#define LogNone   0
#define LogMin    1
#define LogNormal 5
#define LogMax    10

interface IEspContainer : extends IInterface
{
    virtual void exitESP() = 0;
    virtual void setLogLevel(LogLevel level) = 0;
    virtual LogLevel getLogLevel() = 0;
    virtual LogRequest getLogRequests() = 0;
    virtual void setLogRequests(LogRequest logReq) = 0;
    virtual bool getLogResponses() = 0;
    virtual void setLogResponses(bool logResp) = 0;
    virtual void setTxSummaryLevel(LogLevel level) = 0;
    virtual LogLevel getTxSummaryLevel() = 0;
    virtual unsigned int getTxSummaryStyle() = 0;
    virtual void setTxSummaryStyle(unsigned int style) = 0;
    virtual unsigned int getTxSummaryGroup() = 0;
    virtual void setTxSummaryGroup(unsigned int group) = 0;
    virtual bool getTxSummaryResourceReq() = 0;
    virtual void setTxSummaryResourceReq(bool req) = 0;
    virtual void log(LogLevel level, const char*,...) __attribute__((format(printf, 3, 4))) = 0;
    virtual unsigned getSlowProcessingTime() = 0;
    virtual void setFrameTitle(const char* title) = 0;
    virtual const char* getFrameTitle() = 0;
    virtual void sendSnmpMessage(const char* msg) = 0;
    virtual bool reSubscribeESPToDali() = 0;
    virtual bool unsubscribeESPFromDali() = 0;
    virtual bool detachESPFromDali(bool force) = 0;
    virtual bool attachESPToDali() = 0;
    virtual bool isAttachedToDali() = 0;
    virtual bool isSubscribedToDali() = 0;
    virtual bool hasCacheClient() = 0;
    virtual const void* queryCacheClient(const char* id) = 0;
    virtual void clearCacheByGroupID(const char* ids, StringArray& errorMsgs) = 0;
    virtual IPropertyTree *queryApplicationConfig() = 0;
    virtual void setApplicationConfig(IPropertyTree *config) = 0;
};

interface IEspRpcBinding;


interface IEspPlugin : extends IInterface
{
    virtual bool isLoaded() = 0;
    virtual bool load() = 0;
    virtual void unload() = 0;
    virtual void * getProcAddress(const char * name) = 0;
    virtual const char * getName() = 0;
};



interface IEspProtocol : extends IInterface
{
    virtual const char * getProtocolName() = 0;
    virtual void addBindingMap(ISocket * sock, IEspRpcBinding * binding, bool isdefault) = 0;
    virtual int removeBindingMap(int port, IEspRpcBinding * binding) = 0;
    virtual void clearBindingMap() = 0;
    virtual void init(IPropertyTree * cfg, const char * process, const char * protocol) = 0;
    virtual void setContainer(IEspContainer * container) = 0;
    virtual int countBindings(int port) = 0;
};



interface IEspStruct : extends IInterface
{
};


interface IEspRequest : extends IEspStruct
{
};


interface IEspClientRpcSettings : extends IEspStruct
{
    virtual void setConnectTimeOutMs(unsigned val) = 0;
    virtual unsigned getConnectTimeOutMs() = 0;
    virtual void setReadTimeOutSecs(unsigned val) = 0;
    virtual unsigned getReadTimeOutSecs() = 0;
    virtual void setMtlsSecretName(const char * name) = 0;
    virtual const char * getMtlsSecretName() = 0;
    virtual void setClientCertificate(const char * certPath, const char * privateKeyPath) = 0;
    virtual void setCACertificates(const char * path) = 0;
    virtual void setAcceptSelfSigned(bool accept) = 0;
};


interface IEspResponse : extends IEspStruct
{
    virtual void setRedirectUrl(const char * url) = 0;
    virtual const IMultiException & getExceptions() = 0;
    virtual void noteException(IException & e) = 0;
};


interface IEspService : extends IInterface
{
    virtual const char * getServiceType() = 0;
    virtual bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process) = 0;
    virtual void setContainer(IEspContainer * container) = 0;
    virtual bool subscribeServiceToDali() = 0;
    virtual bool unsubscribeServiceFromDali() = 0;
    virtual bool detachServiceFromDali() = 0;
    virtual bool attachServiceToDali() = 0;
};



interface IEspRpcBinding : extends IInterface
{
    virtual const char * getRpcType() = 0;
    virtual const char * getTransportType() = 0;
    virtual void addService(const char * name, const char * host, unsigned short port, IEspService & service) = 0;
    virtual void addProtocol(const char * name, IEspProtocol & prot) = 0;
    virtual void getNavigationData(IEspContext & context, IPropertyTree & data) = 0;
    virtual void getDynNavData(IEspContext & context, IProperties * params, IPropertyTree & data) = 0;
    virtual int onGetNavEvent(IEspContext & context, IHttpMessage * req, IHttpMessage * resp) = 0;
    virtual ISocketSelectNotify * queryListener() = 0;
    virtual bool isValidServiceName(IEspContext & context, const char * name) = 0;
    virtual bool qualifyServiceName(IEspContext & context, const char * servname, const char * methname, StringBuffer & servQName, StringBuffer * methQName) = 0;
    virtual int run() = 0;
    virtual int stop() = 0;
    virtual void setContainer(IEspContainer * ic) = 0;
    virtual void setXslProcessor(IInterface * xslp) = 0;
    virtual IEspContainer * queryContainer() = 0;
    virtual unsigned getCacheMethodCount() = 0;
    virtual bool subscribeBindingToDali() = 0;
    virtual bool unsubscribeBindingFromDali() = 0;
    virtual bool detachBindingFromDali() = 0;
    virtual bool attachBindingToDali() = 0;
    virtual bool canDetachFromDali() = 0;
};



interface IEspServer : extends IInterface
{
    virtual void addProtocol(IEspProtocol & prot) = 0;
    virtual void addBinding(const char * name, const char * host, unsigned short port, IEspProtocol & prot, IEspRpcBinding & bind, bool isdefault, IPropertyTree * cfgtree) = 0;
    virtual void removeBinding(unsigned short port, IEspRpcBinding & bind) = 0;
    virtual IEspProtocol * queryProtocol(const char * name) = 0;
    virtual IEspRpcBinding * queryBinding(const char * name) = 0;
    virtual const char * getProcName() = 0;
    virtual IPropertyTree * queryProcConfig() = 0;
    virtual bool addCacheClient(const char * id, const char * initString) = 0;
};


interface IEspServiceCfg : extends IInterface
{
    virtual void init(IPropertyTree & env, const char * proc, const char * service) = 0;
    virtual bool refresh() = 0;
};


interface IEspProtocolCfg : extends IInterface
{
    virtual void init(IPropertyTree & env, const char * proc, const char * service, const char * binding) = 0;
    virtual bool refresh() = 0;
};


interface IEspServiceEntry : extends IInterface
{
    virtual void addRpcBinding(IEspRpcBinding & binding, const char * host, unsigned short port) = 0;
    virtual int run() = 0;
    virtual int stop() = 0;
};



interface IEspNgParameter : extends IInterface
{
    virtual const char * queryName() = 0;
    virtual const char * queryValue() = 0;
    virtual void setValue(const char * val) = 0;
    virtual unsigned getMaxLength() = 0;
    virtual bool isNull() = 0;
    virtual void setNull() = 0;
};


interface IEspNgParameterIterator : extends IInterface
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual bool isValid() = 0;
    virtual IEspNgParameter * query() = 0;
};


interface IEspNgComplexType : extends IInterface
{
    virtual const char * queryName() = 0;
    virtual IEspNgParameterIterator * getParameterIterator() = 0;
};


interface IEspNgRequest : extends IEspNgComplexType
{
};


interface IEspNgResponse : extends IEspNgComplexType
{
};


interface IEspNgServiceBinding : extends IInterface
{
    virtual IEspNgRequest * createRequest(const char * type_name) = 0;
    virtual IEspNgResponse * createResponse(const char * type_name) = 0;
    virtual int processRequest(IEspContext & context, const char * method_name, IEspNgRequest * req, IEspNgResponse * resp) = 0;
    virtual void populateContext(IEspContext & ctx) = 0;
    virtual bool basicAuth(IEspContext * ctx) = 0;
};
