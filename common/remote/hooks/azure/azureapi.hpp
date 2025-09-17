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

#ifndef AZURE_API_HPP
#define AZURE_API_HPP

#include "jfile.hpp"
#include "azureblob.hpp"
#include "azurefile.hpp"

#ifdef AZURE_API_EXPORTS
#define AZUREAPI_API DECL_EXPORT
#else
#define AZUREAPI_API DECL_IMPORT
#endif

/*
 * Unified Azure API that supports both Blob and File operations
 * Installs hooks into createIFile for both azureblob: and azurefile: prefixes
 */

extern "C" {
  extern AZUREAPI_API void installFileHook();
  extern AZUREAPI_API void removeFileHook();
};

#endif
