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
