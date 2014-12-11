/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2014 HPCC Systems.

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

#ifndef __ESDL_DECL_HPP__
#define __ESDL_DECL_HPP__

#ifdef _WIN32
 #ifdef ESDLCOMP_EXPORTS
  #define esdlcomp_decl __declspec(dllexport)
 #else
  #define esdlcomp_decl __declspec(dllimport)
 #endif
#else
 #define esdlcomp_decl
#endif

#endif
