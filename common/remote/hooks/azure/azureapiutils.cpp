/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#include "platform.h"
#include "azureapiutils.hpp"
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jstring.hpp"
#include "jlog.hpp"
#include <cstdlib>

// Common utility functions shared by both blob and file implementations
//---------------------------------------------------------------------------------------------------------------------

bool areManagedIdentitiesEnabled()
{
    //Use a local static to avoid re-evaluation.  Performance is not critical - so once overhead is acceptable.
    static bool enabled = std::getenv("MSI_ENDPOINT") || std::getenv("IDENTITY_ENDPOINT");
    return enabled;
}

bool isBase64Char(char c)
{
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '+') || (c == '/') || (c == '=');
}

void handleRequestBackoff(const char * message, unsigned attempt, unsigned maxRetries)
{
    OWARNLOG("%s", message);

    if (attempt >= maxRetries)
        throw makeStringException(1234, message);

    // Exponential backoff with jitter
    unsigned backoffMs = (1U << attempt) * 100 + (rand() % 100);
    Sleep(backoffMs);
}

void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s (%d)",
                      op, attempt, maxRetries, filename, pos, len, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s at offset %llu, len %llu: %s",
                      op, attempt, maxRetries, filename, pos, len, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s (%d)",
                      op, attempt, maxRetries, filename, e.ReasonPhrase.c_str(), static_cast<int>(e.StatusCode));

    handleRequestBackoff(msg, attempt, maxRetries);
}

void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename)
{
    VStringBuffer msg("%s failed (attempt %u/%u) for file %s: %s",
                      op, attempt, maxRetries, filename, e.what());

    handleRequestBackoff(msg, attempt, maxRetries);
}
