/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
