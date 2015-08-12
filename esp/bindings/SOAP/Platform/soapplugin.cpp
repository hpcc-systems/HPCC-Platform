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

#pragma warning (disable : 4786)

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "esp.hpp"
#include "IAEsp.hpp"

//ESP Core
#include "espthread.hpp"
#include "espplugin.hpp"

//ESP Bindings
#include "SOAP/Platform/soapbind.hpp"
#include "http/platform/httpprot.hpp"
#include "SOAP/ImageAccess/ImageAccessBind.hpp"


//ESP Services
#ifdef ESPSOAP
#include "ImageAccess/ImageAccessService.h"
#endif

extern "C"
{

//when we aren't loading dynamically
// Change the function names when we stick with dynamic loading.
ESP_FACTORY IEspService * ia_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "ia_service")==0)
   {
      CImageAccessService* service =  new CImageAccessService;
        service->init(name, "ia_service", cfg, process);
        return service;
   }
    else
    {
        throw MakeStringException(-1, "Unknown service %s", type);
    }

   return NULL;
}
   
   
//#ifdef ESP_PLUGIN
ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   return ia_service_factory(name, type, cfg, process);
}
//#endif //ESP_PLUGIN


ESP_FACTORY IEspRpcBinding * ia_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "ia_binding")==0)
   {
        StringBuffer xpath;
        xpath.appendf(".//EspProcess[@name=\"%s\"]", process);
        IPropertyTree* bcfg = cfg->getPropTree(xpath.str());
        if(bcfg != NULL)
        {
          CIASoapBinding* binding = new CIASoapBinding(bcfg);
            return binding;
        }
        else
        {
            CIASoapBinding* binding = new CIASoapBinding;
            //binding->init(cfg, process, name); 
            return binding;
        }

   }
    else
    {
        throw MakeStringException(-1, "Unknown binding %s", type);
    }

   return NULL;
}

//#ifdef ESP_PLUGIN
ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name,  const char* type, IPropertyTree *cfg, const char *process)
{
   return ia_binding_factory(name, type, cfg, process);
}
//#endif //ESP_PLUGIN

ESP_FACTORY IEspProtocol * ia_protocol_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
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
        throw MakeStringException(-1, "Unknown protocol %s", type);
    }

   return NULL;
}

//#ifdef ESP_PLUGIN
ESP_FACTORY IEspProtocol * esp_protocol_factory(const char *name,  const char* type, IPropertyTree *cfg, const char *process)
{
   return ia_protocol_factory(name, type, cfg, process);
}
//#endif //ESP_PLUGIN

};
