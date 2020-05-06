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

//Jlib
#include "jliball.hpp"

#include "ws_ecl_service.hpp"

#include "espplugin.hpp"

//ESP Service

extern "C"
{

//when we aren't loading dynamically
// Change the function names when we stick with dynamic loading.
ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
    if (strieq(type, "ws_ecl"))
    {
        CWsEclService* service = new CWsEclService;
        service->init(name, type, cfg, process);
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
    //binding names ending in _http are being added so the names can be made more consistent and can therefore be automatically generated
    //  the name also better reflects that these bindings are for all HTTP based protocols, not just SOAP
    //  both "SoapBinding" and "_http" names instantiate the same objects.
    if (strieq(type, "ws_eclSoapBinding")||strieq(type, "ws_ecl_http"))
    {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name=\"%s\"]", process);
        Owned<IPropertyTree> bcfg = cfg->getPropTree(xpath.str());
        const char* cfgFile = cfg->queryProp("@config");
        if (cfgFile)
            bcfg->addProp("@config", cfgFile);
        return new CWsEclBinding(bcfg.get(), name, process);
    }
    else
    {
        throw MakeStringException(-1, "Unknown binding type %s", type);
    }

    return NULL;
}

};
