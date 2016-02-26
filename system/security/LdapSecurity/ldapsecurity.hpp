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

#ifndef _LDAPSECURITY_HPP__
#define _LDAPSECURITY_HPP__

#ifndef LDAPSECURITY_API

#ifdef _WIN32
    #ifndef LDAPSECURITY_EXPORTS
        #define LDAPSECURITY_API __declspec(dllimport)
    #else
        #define LDAPSECURITY_API __declspec(dllexport)
    #endif //LDAPSECURITY_EXPORTS
#else
    #define LDAPSECURITY_API
#endif //_WIN32

#endif 

extern "C" LDAPSECURITY_API ISecManager * newLdapSecManager(const char *serviceName, IPropertyTree &config);
extern "C" LDAPSECURITY_API IAuthMap *newDefaultAuthMap(IPropertyTree* config);

#endif
