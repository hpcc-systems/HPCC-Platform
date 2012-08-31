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

#pragma warning (disable : 4786)

#include "ws_roxie_esp.ipp"

//ESP Bindings
#include "http/platform/httpprot.hpp"

//ESP Service
#include "ws_roxieService.hpp"

#include "espplugin.hpp"
#include "ScrubbedXmlBind.hpp"

class CRoxieXmlBinding : public CRoxieSoapBinding
{
public:
    CRoxieXmlBinding(IPropertyTree *cfg) : CRoxieSoapBinding(cfg){}

    virtual int processRequest(IRpcMessage* rpc_call, IRpcMessage* rpc_response)
    {
        return CRoxieSoapBinding::processRequest(rpc_call, rpc_response);
    }
    virtual int onGet(CHttpRequest* request, CHttpResponse* response)
    {
        return CRoxieSoapBinding::onGet(request, response);
    }
};

extern "C"
{

//when we aren't loading dynamically
// Change the function names when we stick with dynamic loading.
ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "Roxie_Serv")==0)
   {
      CRoxieEx* service = new CRoxieEx;
        service->init(cfg, process, name);
      return service;
   }
    else
    {
        throw MakeStringException(-1, "Unknown service type %s", type);
    }
   return NULL;
}
 
   

ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "Roxie_Bind")==0)
   {
        return new CRoxieSoapBinding(cfg, name, process);
   }
    else
    {
        throw MakeStringException(-1, "Unknown binding type %s", type);
    }

   return NULL;
}



ESP_FACTORY IEspProtocol * esp_protocol_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
    if (strcmp(type, "http_protocol")==0)
    {
        return new CHttpProtocol;
    }
    else if(strcmp(type, "secure_http_protocol") == 0)
    {
        IPropertyTree *sslSettings;
        sslSettings = cfg->getPropTree(StringBuffer("Software/EspProcess[@name=\"").append(process).append("\"]").append("/EspProtocol[@name=\"").append(name).append("\"]").str());
        if(sslSettings != NULL)
        {
            return new CSecureHttpProtocol(sslSettings);
        }
        else
        {
            throw MakeStringException(-1, "can't find ssl settings in the config file");
        }
    }
    else
    {
        throw MakeStringException(-1, "Unknown protocol %s", name);
    }

    return NULL;
}

};
