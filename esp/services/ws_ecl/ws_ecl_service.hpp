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

#ifndef _WS_ECL_SERVICE_HPP__
#define _WS_ECL_SERVICE_HPP__


#include "jliball.hpp"
#include "junicode.hpp"
#include "jsmartsock.hpp"
#include "fileview.hpp"

#include "esp.hpp"
#include "SOAP/Platform/soapbind.hpp"

#include "ws_ecl_wuinfo.hpp"

typedef enum wsEclTypes_
{
    wsEclTypeUnknown,
    xsdString,
    xsdBoolean,
    xsdDecimal,
    xsdFloat,
    xsdDouble,
    xsdDuration,
    xsdDateTime,
    xsdTime,
    xsdDate,
    xsdYearMonth,
    xsdYear,
    xsdMonthDay,
    xsdDay,
    xsdMonth,
    xsdHexBinary,
    xsdBase64Binary,
    xsdAnyURI,
    xsdQName,
    xsdNOTATION,
    xsdNormalizedString,
    xsdToken,
    xsdLanguage,
    xsdNMTOKEN,
    xsdNMTOKENS, 
    xsdName,
    xsdNCName,
    xsdID,
    xsdIDREF,
    xsdIDREFS, 
    xsdENTITY,
    xsdENTITIES,
    xsdInteger,
    xsdNonPositiveInteger,
    xsdNegativeInteger,
    xsdLong,
    xsdInt,
    xsdShort,
    xsdByte,
    xsdNonNegativeInteger,
    xsdUnsignedLong,
    xsdUnsignedInt,
    xsdUnsignedShort,
    xsdUnsignedByte,
    xsdPositiveInteger,

    tnsRawDataFile,
    tnsCsvDataFile,
    tnsEspStringArray,
    tnsEspIntArray,
    tnsXmlDataSet,

    maxWsEclType

} wsEclType;

class CWsEclService : public CInterface,
    implements IEspService
{
public:
    MapStringToMyClass<ISmartSocketFactory> connMap;
    StringAttr auth_method;
    StringAttr portal_URL;

public:
    IMPLEMENT_IINTERFACE;

    CWsEclService(){}
    ~CWsEclService();

    virtual const char * getServiceType(){return "ws_ecl";}
    virtual bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process);
    virtual void setContainer(IEspContainer * container){}

};

class CWsEclBinding : public CHttpSoapBinding
{
private:
    CWsEclService *wsecl;

public:
    CWsEclBinding(IPropertyTree *cfg, const char *bindname, const char *procname) : 
        CHttpSoapBinding(cfg, bindname, procname), wsecl(NULL)
    {
    }

    ~CWsEclBinding()
    {
    }

    virtual void addService(const char * name, const char * host, unsigned short port, IEspService & service)
    {
        wsecl = dynamic_cast<CWsEclService*>(&service);
        CEspBinding::addService(name, host, port, service);
    }
    
    virtual void setXslProcessor(IInterface * xslp){}
    
    StringBuffer &generateNamespace(IEspContext &context, CHttpRequest* request, const char *serv, const char *method, StringBuffer &ns);

    virtual int getQualifiedNames(IEspContext& ctx, MethodInfoArray & methods){return 0;}

    void getNavigationData(IEspContext &context, IPropertyTree & data);
    void getRootNavigationFolders(IEspContext &context, IPropertyTree & data);
    void getDynNavData(IEspContext &context, IProperties *params, IPropertyTree & data);
    void addQueryNavLink(IPropertyTree &data, IPropertyTree *query, const char *setname, const char *qname=NULL);
    void getQueryNames(IPropertyTree* settree, const char *id, const char *qname, StringArray& qnames);

    virtual const char* getRootPage() {return "files/esp_app_tree.html";}

    int onGet(CHttpRequest* request, CHttpResponse* response);
    void xsltTransform(const char* xml, unsigned int len, const char* xslFileName, IProperties *params, StringBuffer& ret);

    int getWsEcl2TabView(CHttpRequest* request, CHttpResponse* response, const char *thepath);
    int getGenForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, bool boxform);
    int getWsEcl2Form(CHttpRequest* request, CHttpResponse* response, const char *thepath);

    bool isValidServiceName(IEspContext &context, const char *name){return true;}
    bool qualifyServiceName(IEspContext &context, const char *servname, const char *methname, StringBuffer &servQName, StringBuffer *methQName){servQName.clear().append(servname); if (methQName) methQName->clear().append(methname); return true;}

    int getXsdDefinition(IEspContext &context, CHttpRequest *request, StringBuffer &content, WsEclWuInfo &wsinfo);
    bool getSchema(StringBuffer& schema, IEspContext &ctx, CHttpRequest* req, WsEclWuInfo &wsinfo) ;
    void appendSchemaNamespaces(IPropertyTree *namespaces, IEspContext &ctx, CHttpRequest* req, WsEclWuInfo &wsinfo);
    void appendSchemaNamespaces(IPropertyTree *namespaces, IEspContext &ctx, CHttpRequest* req, const char *service, const char *method);

    void SOAPSectionToXsd(WsEclWuInfo &wsinfo, const char *parmXml, StringBuffer &schema, bool isRequest=true, IPropertyTree *xsdtree=NULL);
    int getXmlTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *formtype, WsEclWuInfo &wsinfo);
    int getXmlTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, const char *formtype);

    void getWsEcl2XmlRequest(StringBuffer& soapmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, const char *xmltype, const char *ns, unsigned flags);
    void buildSampleResponseXml(StringBuffer& msg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo);
    void getSoapMessage(StringBuffer& soapmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, unsigned flags);
    int onGetSoapBuilder(IEspContext &context, CHttpRequest* request, CHttpResponse* response,  WsEclWuInfo &wsinfo);
    int onSubmitQueryOutput(IEspContext &context, CHttpRequest* request, CHttpResponse* response,    WsEclWuInfo &wsinfo, const char *format);
    int onSubmitQueryOutputView(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo);

    int submitWsEclWorkunit(IEspContext & context, WsEclWuInfo &wsinfo, const char *xml, StringBuffer &out, unsigned flags, TextMarkupFormat fmt=MarkupFmt_XML, const char *viewname=NULL, const char *xsltname=NULL);

    void handleHttpPost(CHttpRequest *request, CHttpResponse *response);
    void handleJSONPost(CHttpRequest *request, CHttpResponse *response);

    int HandleSoapRequest(CHttpRequest* request, CHttpResponse* response);
    int getWsEclLinks(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo);
    int getWsEclDefinition(CHttpRequest* request, CHttpResponse* response, const char *thepath);

    int onGetWsdl(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo);
    int onGetXsd(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo);

    int getXsdDefinition(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda);

    int getWsdlMessages(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda);
    int getWsdlPorts(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda);
    int getWsdlBindings(IEspContext &context, CHttpRequest *request, StringBuffer &content, const char *service, const char *method, bool mda);

    int getWsEclExample(CHttpRequest* request, CHttpResponse* response, const char *thepath);

    int getJsonTestForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, WsEclWuInfo &wsinfo, const char *formtype);
    void getWsEclJsonRequest(StringBuffer& soapmsg, IEspContext &context, CHttpRequest* request, WsEclWuInfo &wsinfo, const char *xmltype, const char *ns, unsigned flags);
    void getWsEclJsonResponse(StringBuffer& jsonmsg, IEspContext &context, CHttpRequest *request, const char *xml, WsEclWuInfo &wsinfo);
    
    void sendRoxieRequest(const char *process, StringBuffer &req, StringBuffer &resp, StringBuffer &status, const char *query, const char *contentType="text/xml");
};

#endif //_WS_ECL_SERVICE_HPP__
