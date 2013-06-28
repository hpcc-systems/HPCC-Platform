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

#ifndef ARCHIVE_FILE_HPP
#define ARCHIVE_FILE_HPP

#include "jfile.hpp"

#ifdef ARCHIVEFILE_EXPORTS
#define ARCHIVEFILE_API __declspec(dllexport)
#else
#define ARCHIVEFILE_API __declspec(dllimport)
#endif


/*
 * Direct access to files in zip archives, without needing to extract them first
 * Installs hooks into createIFile, spotting filenames of the form /my/directory/myfile.zip/{password}/path/within/archive
 */

extern "C" {
  extern ARCHIVEFILE_API void installFileHook();
  extern ARCHIVEFILE_API void removeFileHook();
};

#endif
