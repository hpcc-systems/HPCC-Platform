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

#ifndef __ESPPLUGIN_HPP__
#define __ESPPLUGIN_HPP__

//SCM Interfaces
#include "esp.hpp"


#ifndef ESP_BUILTIN
#ifdef _WIN32
#define ESP_FACTORY __declspec(dllexport)
#endif
#endif

#ifndef ESP_FACTORY
#define ESP_FACTORY
#endif



typedef IEspService * (*esp_service_factory_t)(const char *name,  const char* type, IPropertyTree* cfg, const char *process);
typedef IEspRpcBinding * (*esp_binding_factory_t)(const char *name,  const char* type, IPropertyTree* cfg, const char *process);
typedef IEspProtocol * (*esp_protocol_factory_t)(const char *name,  const char* type, IPropertyTree* cfg, const char *process);


extern "C" {

ESP_FACTORY IEspService *esp_service_factory(const char *name,  const char* type, IPropertyTree* cfg, const char *process);
ESP_FACTORY IEspRpcBinding *esp_binding_factory(const char *name,  const char* type, IPropertyTree* cfg, const char *process);
ESP_FACTORY IEspProtocol *esp_protocol_factory(const char *name,  const char* type, IPropertyTree* cfg, const char *process);

};

#endif //__ESPPLUGIN_HPP__
