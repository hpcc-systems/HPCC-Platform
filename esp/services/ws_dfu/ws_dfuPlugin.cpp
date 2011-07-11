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

#include "ws_dfu_esp.ipp"
#include "ws_dfuXref_esp.ipp"

//ESP Bindings
#include "http/platform/httpprot.hpp"

//ESP Service
#include "ws_dfuService.hpp"
#include "ws_dfuXRefService.hpp"

#include "espplugin.hpp"

extern "C"
{

//when we aren't loading dynamically
// Change the function names when we stick with dynamic loading.
ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "WsDfu")==0)
   {
      CWsDfuEx* service = new CWsDfuEx;
        service->init(cfg, process, name);
      return service;
   }
   else if (strcmp(type, "WsDfuXRef")==0)
   {
        CWsDfuXRefEx* service = new CWsDfuXRefEx;
        service->init(cfg, process, name);
      return service;

   }
   return NULL;
}



ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
#ifdef _DEBUG
    http_soap_log_level log_level_ = hsl_all;
#else
    http_soap_log_level log_level_ = hsl_none;
#endif
    if (strcmp(type, "ws_dfuSoapBinding")==0)
        return new CWsDfuSoapBindingEx(cfg, name, process, log_level_);
    else if (strcmp(type, "ws_dfuxrefSoapBinding")==0)
        return new CWsDFUXRefSoapBindingEx(cfg, name, process, log_level_);

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
    }

    return NULL;
}

};
