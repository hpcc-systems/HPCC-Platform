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

#ifndef AZURE_API_UTILS_HPP
#define AZURE_API_UTILS_HPP

#include "jlib.hpp"
#include "jlog.hpp"

#include <azure/core.hpp>
#include <azure/core/http/http.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/files/shares.hpp>

#include <exception>

/*
 * Common utility functions and constants shared by Azure Blob and File implementations
 */

constexpr const char * azureBlobPrefix = "azureblob:";
constexpr const char * azureFilePrefix = "azurefile:";

bool areManagedIdentitiesEnabled();
bool isBase64Char(char c);
void handleRequestBackoff(const char * message, unsigned attempt, unsigned maxRetries);
void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename);
void handleRequestException(const Azure::Core::RequestFailedException& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len);
void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename);
void handleRequestException(const std::exception& e, const char * op, unsigned attempt, unsigned maxRetries, const char * filename, offset_t pos, offset_t len);

#endif
