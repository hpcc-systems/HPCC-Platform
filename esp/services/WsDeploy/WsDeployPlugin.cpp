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

#include "WsDeploy_esp.ipp"

//ESP Bindings
#include "http/platform/httpprot.hpp"

//ESP Service
#include "WsDeployService.hpp"

#include "espplugin.hpp"

#ifndef HPCCSYSTEMS_CE
  #define createWsDeployInst createWsDeployEE
#else
  #define createWsDeployInst createWsDeployCE 
#endif

extern CWsDeploySoapBinding* createWsDeploySoapBinding(IPropertyTree *cfg, const char *name, const char *process);
extern CWsDeployExCE* createWsDeployInst(IPropertyTree *cfg, const char* name);


extern "C"
{

//when we aren't loading dynamically
// Change the function names when we stick with dynamic loading.
ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "WsDeploy")==0)
   {
      CWsDeployExCE* service = createWsDeployInst(cfg, name);
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
   if (strcmp(type, "WsDeploySoapBinding")==0)
   {
        CWsDeploySoapBinding* binding = createWsDeploySoapBinding(cfg, name, process);
      return binding;
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
