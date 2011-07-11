/*##############################################################################

    Copyright (C) <2011>  <LexisNexis Risk Data Management Inc.>

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
    if (strcmp(type, "ws_ecl")==0)
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
    if (strcmp(type, "ws_eclSoapBinding")==0)
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
