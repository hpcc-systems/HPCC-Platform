/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef AZURE_FILE_HPP
#define AZURE_FILE_HPP

#include "jfile.hpp"

#ifdef AZURE_FILE_EXPORTS
#define AZURE_FILE_API DECL_EXPORT
#else
#define AZURE_FILE_API DECL_IMPORT
#endif


/*
 * Direct access to files in s3 buckets
 * Installs hooks into createIFile, spotting filenames of the form s3://url
 */

extern "C" {
  extern AZURE_FILE_API void installFileHook();
  extern AZURE_FILE_API void removeFileHook();
};

#endif
