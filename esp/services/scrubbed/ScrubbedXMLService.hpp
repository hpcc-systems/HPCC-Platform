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

#ifndef _SCRUBBED_XML_SERVICE_HPP__
#define _SCRUBBED_XML_SERVICE_HPP__

#include "esp.hpp"

class CEspScrubbedXmlService : public CInterface, 
   implements IEspSimpleDataService,
    implements IEspSimpleDataRetrievalService,
    implements IEspWebService,
   implements IEspHtmlForm

{
private:

public:
   IMPLEMENT_IINTERFACE;

    CEspScrubbedXmlService();
    virtual ~CEspScrubbedXmlService();
    
//interface IEspService
   
   virtual const char* getServiceType() 
   {
      return "scrubbed_service";
   }

   virtual bool init(const char * name, const char * type, IPropertyTree * cfg, const char * process);
 
//interface IEspSimpleDataService
    virtual int onSimpleDataRequest(IEspContext &context, IEspSimpleDataRequest &req, IEspSimpleDataResponse &resp);

//interface IEspSimpleDataRetrievalService
    virtual int onSimpleDataByRefRequest(IEspContext &context, IEspSimpleDataRequest & req, IEspSimpleDataRefResponse & resp);
    virtual int onSimpleDataRetrieval(IEspContext &context, IEspSimpleDataRetrievalRequest & req, IEspSimpleDataResponse & resp);

//interface IEspHtmlForm
    virtual bool getHtmlForm(IEspContext &context, const char *path, const char *service, StringBuffer &formStr);
    virtual bool getHtmlResults_Xslt(IEspContext &context, const char * path, const char * service, StringBuffer & xsltStr);
    virtual bool getMetaBlock(IEspContext &context, const char *name, const char * mod, const char * attr, StringBuffer & formStr){return false;}
    virtual bool getParametersXml(IEspContext &context, const char * path, const char * service, StringBuffer & formStr){return false;}
    virtual bool applyXslAttribute(IEspContext &context, const char * path, const char * service, const char * sectionName, const char * input, const char * fullPath, StringBuffer & ret)
    {
        return false;
    }
    virtual bool getDescriptiveXml(IEspContext & context, const char * path, const char * service, StringBuffer & xmlStr)
    {
        return false;
    }

//interface IEspWebService
    virtual bool getWSDL(IEspContext &context, const char * path, const char * service, StringBuffer & wsdlMsg);
    virtual bool getWSDL_Message(IEspContext &context, const char * path, const char * service, StringBuffer & wsdlMsg);
    virtual bool getWSDL_Schema(IEspContext &context, const char * path, const char * service, StringBuffer & wsdlSchema);
    virtual bool getResults_Schema(IEspContext &context, const char * path, const char * service, StringBuffer & dataSchema);
    virtual bool getWsIndex(IEspContext & context, const char * path, bool aliasOnly, StringArray & wsIndex, StringArray & wsInfos, StringBuffer &mode)
    {
        return false;
    }

    virtual bool isValidWsName(IEspContext & context, const char * servname)
    {
        return false;
    }
    virtual bool qualifyWsName(IEspContext & context, const char * servname, const char * methname, StringBuffer & servQName, StringBuffer * methQName)
    {
        return false;
    }


};

#endif //_SCRUBBED_XML_SERVICE_HPP__

