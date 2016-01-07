/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#include "espplugin.hpp"
#include "http/platform/httpprot.hpp"

//ESP Service
#include "loggingservice.hpp"
#include "ws_loggingservice_esp.ipp"

extern "C"
{
    //when we aren't loading dynamically
    // Change the function names when we stick with dynamic loading.
    ESP_FACTORY IEspService * esp_service_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
    {
        if (streq(type, "WsLoggingService"))
        {
            CWsLoggingServiceEx* loggingservice = new CWsLoggingServiceEx;
            loggingservice->init(name, type, cfg, process);
            return loggingservice;
        }
        return NULL;
    }

    ESP_FACTORY IEspRpcBinding * esp_binding_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
    {
        if (streq(type, "loggingservice_binding"))
            return new CWsLoggingServiceSoapBinding(cfg, name, process, hsl_none);

        return NULL;
    }

    ESP_FACTORY IEspProtocol * esp_protocol_factory(const char *name, const char* type, IPropertyTree *cfg, const char *process)
    {
        if (streq(type, "http_protocol"))
            return new CHttpProtocol;
        else if(streq(type, "secure_http_protocol"))
        {
            IPropertyTree *sslSettings;
            sslSettings = cfg->getPropTree(StringBuffer("Software/EspProcess[@name=\"").append(process).append("\"]").append("/EspProtocol[@name=\"").append(name).append("\"]").str());
            if(sslSettings != NULL)
                return new CSecureHttpProtocol(sslSettings);
        }

        return NULL;
    }

};
