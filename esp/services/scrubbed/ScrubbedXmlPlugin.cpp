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

//ESP Core
#include "espthread.hpp"
#include "espplugin.hpp"

//ESP Bindings
#include "http/platform/httpprot.hpp"
#include "SOAP/Platform/soapbind.hpp"
#include "SOAP/scrubbed/ScrubbedXmlBind.hpp"

//ESP Service
#include "ScrubbedXMLService.hpp"

extern "C"
{

//when we aren't loading dynamically
// Change the function names when we stick with dynamic loading.
ESP_FACTORY IEspService * ecl_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "scrubbed_service")==0)
   {
      CEspScrubbedXmlService* service = new CEspScrubbedXmlService;
        service->init(name, type, cfg, process);
      return (IEspSimpleDataService*)service;
   }
    else
    {
        throw MakeStringException(-1, "Unknown service type %s", type);
    }
   
    return NULL;
}
   
   
//#ifdef ESP_PLUGIN
ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   return ecl_service_factory(name, type, cfg, process);
}
//#endif //ESP_PLUGIN


ESP_FACTORY IEspRpcBinding * ecl_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
    if (strcmp(type, "scrubbed_binding")==0)
    {
        StringBuffer xpath;
        xpath.appendf(".//EspProcess[@name=\"%s\"]/EspBinding[@name=\"%s\"]", process, name);
        IPropertyTree* bcfg = cfg->getPropTree(xpath.str());
        return new CScrubbedXmlBinding(bcfg, name, process);
    }
    else
    {
        throw MakeStringException(-1, "Unknown binding type %s", type);
    }

    return NULL;
}

//#ifdef ESP_PLUGIN
ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   return ecl_binding_factory(name, type, cfg, process);
}
//#endif //ESP_PLUGIN


};
