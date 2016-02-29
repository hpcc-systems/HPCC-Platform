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

static const char* SESSION_ROOT_PATH="Sessions";
#define SESSION_SDS_LOCK_TIMEOUT (30*1000) // 30 seconds

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

ESPHTTP_API void ESPLOG(IEspContext* ctx, LogLevel level, const char* fmt, ...) __attribute__((format(printf, 3, 4)));
ESPHTTP_API void ESPLOG(LogLevel level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
ESPHTTP_API void setEspContainer(IEspContainer* container);

ESPHTTP_API IEspContainer* getESPContainer();

ESPHTTP_API void setCFD(const char* cfd);
ESPHTTP_API const char* getCFD();

ESPHTTP_API void setBuildVersion(const char* buildVersion);
ESPHTTP_API const char* getBuildVersion();
ESPHTTP_API void setBuildLevel(const char* buildLevel);
ESPHTTP_API const char* getBuildLevel();
#endif

