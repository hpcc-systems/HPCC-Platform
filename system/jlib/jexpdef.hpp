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


#ifdef _WIN32
 #ifdef JLIB_EXPORTS
  #define jlib_decl __declspec(dllexport)
  #define jlib_thrown_decl __declspec(dllexport)
 #else
  #define jlib_decl __declspec(dllimport)
  #define jlib_thrown_decl __declspec(dllimport)
 #endif
#else
#if __GNUC__ >= 4
  #define jlib_decl  __attribute__ ((visibility("default")))
  #define jlib_thrown_decl __attribute__ ((visibility("default")))
#else
 #define jlib_decl
 #define jlib_thrown_decl 
#endif
#endif
