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

#ifndef _ESPCONTEXT_HPP__
#define _ESPCONTEXT_HPP__

/*#ifndef ESP_BUILTIN
#ifdef _WIN32
#define ESP_CONTEXT_API __declspec(dllexport)
#endif
#endif

#ifndef ESP_CONTEXT_API
#define ESP_CONTEXT_API
#endif*/

#include "esp.hpp"
#include "esphttp.hpp"

ESPHTTP_API IEspContext* createEspContext();

// Get URL parameters (include these from Content)
// Return: a=b&c=d format. 
ESPHTTP_API bool getUrlParams(IProperties *props, StringBuffer& params);

// Only the original URL (not these from Content: URL form encoded)
// Also remove these params that start with dot (.).
// Return: a=b&c=d format. 
ESPHTTP_API void getEspUrlParams(IEspContext& ctx, StringBuffer& params, const char* excludeParams[]);

ESPHTTP_API void addEspNativeArray(StringBuffer& schema, const char* xsdType, const char* arrayType);
ESPHTTP_API void checkRequest(IEspContext& ctx);

ESPHTTP_API LogLevel getEspLogLevel(IEspContext* );
ESPHTTP_API LogLevel getEspLogLevel();
ESPHTTP_API bool getEspLogRequests();
ESPHTTP_API bool getEspLogResponses();
ESPHTTP_API unsigned getSlowProcessingTime();

ESPHTTP_API void ESPLOG(IEspContext* ctx, LogLevel level, const char* fmt, ...);
ESPHTTP_API void ESPLOG(LogLevel level, const char* fmt, ...);
ESPHTTP_API void setEspContainer(IEspContainer* container);

ESPHTTP_API IEspContainer* getESPContainer();

ESPHTTP_API void setCFD(const char* cfd);
ESPHTTP_API const char* getCFD();

ESPHTTP_API void setBuildVersion(const char* buildVersion);
ESPHTTP_API const char* getBuildVersion();
ESPHTTP_API void setBuildLevel(const char* buildLevel);
ESPHTTP_API const char* getBuildLevel();
#endif

