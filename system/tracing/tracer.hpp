/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2023 HPCC Systems®.

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

#ifndef TRACER_HPP
#define TRACER_HPP

#include "opentelemetry/ext/http/client/http_client_factory.h"
#include "opentelemetry/ext/http/common/url_parser.h"
#include "opentelemetry/trace/semantic_conventions.h"

#include "opentel/tracer_common.hpp"

class Tracer
{
private:
    const char *   globalId;
    const char *   callerId;
    const char *   localId;

    const char *   globalIdHTTPHeaderName = "HPCC-Global-Id";
    const char *   callerIdHTTPHeaderName = "HPCC-Caller-Id";

    const char* assignLocalId();

public:

    Tracer()
    {
        InitTracer();
        CleanupTracer();
    };

    const char* queryGlobalId() const;
    const char* queryCallerId() const;
    const char* queryLocalId() const;
    const char* queryGlobalIdHTTPHeaderName() const { return globalIdHTTPHeaderName; }
    const char* queryCallerIdHTTPHeaderName() const { return callerIdHTTPHeaderName; }

    void setHttpIdHeaderNames(const char *global, const char *caller)
    {
        if (global && *global)
            globalIdHTTPHeaderName = global;
        if (caller && *caller)
            callerIdHTTPHeaderName = caller;
    }

    //can these be private with abstract methods exposed to create/set these values?
    void setGlobalId(const char* id);
    void setCallerId(const char* id);
    void setLocalId(const char* id);
};

#endif
