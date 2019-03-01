/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#ifndef _ESPCACHE_IPP__
#define _ESPCACHE_IPP__

#include "esphttp.hpp"

#include "esp.hpp"

enum ESPCacheResult
{
    ESPCacheSuccess,
    ESPCacheError,
    ESPCacheNotFound
};

interface IEspCache : extends IInterface
{
    virtual bool cacheResponse(const char* cacheID, const unsigned cacheSeconds, const char* content, const char* contentType) = 0;
    virtual bool readResponseCache(const char* cacheID, StringBuffer& content, StringBuffer& contentType) = 0;
    virtual void flush(unsigned when) = 0;
};

extern esp_http_decl IEspCache* createESPCache(const char* setting);

#endif
