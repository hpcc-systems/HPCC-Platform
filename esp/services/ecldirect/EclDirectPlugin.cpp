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

#include "ecldirect_esp.ipp"
#include "http/platform/httpprot.hpp"
#include "EclDirectService.hpp"
#include "espplugin.hpp"

extern "C"
{

ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "EclDirect")==0)
   {
      CEclDirectEx* service = new CEclDirectEx();
        service->init(cfg, process, name);
      return service;
   }

   ERRLOG("Unknown service type %s", type);
   return NULL;
}

ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
   if (strcmp(type, "EclDirectSoapBinding")==0)
        return new CEclDirectSoapBindingEx(cfg, name, process);

   ERRLOG("Unknown binding type %s", type);
   return NULL;
}

ESP_FACTORY IEspProtocol * esp_protocol_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
{
    if (strcmp(type, "http_protocol")==0)
        return new CHttpProtocol;

    if(strcmp(type, "secure_http_protocol") == 0)
    {
        IPropertyTree *sslSettings;
        sslSettings = cfg->getPropTree(StringBuffer("Software/EspProcess[@name=\"").append(process).append("\"]").append("/EspProtocol[@name=\"").append(name).append("\"]").str());
        if(sslSettings != NULL)
            return new CSecureHttpProtocol(sslSettings);
        ERRLOG("can't find ssl settings in the config file");
        return NULL;
    }

    ERRLOG("Unknown protocol %s", name);
    return NULL;
}

};
