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

#ifndef GITFILE_HPP
#define GITFILE_HPP

#include "jfile.hpp"

#ifdef GITFILE_EXPORTS
#define GITFILE_API DECL_EXPORT
#else
#define GITFILE_API DECL_IMPORT
#endif


/*
 * Direct access to files in git repositories, by revision, without needing to check them out first
 * Installs hooks into createIFile, spotting filenames of the form /my/directory/.git/{revision}/path/within/git
 */

extern "C" {
  extern GITFILE_API void installFileHook();
  extern GITFILE_API void removeFileHook();
};

#endif
