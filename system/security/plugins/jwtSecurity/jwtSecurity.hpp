/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#ifndef JWTSECURITY_HPP_
#define JWTSECURITY_HPP_

#ifndef JWTSECURITY_API

#ifndef JWTSECURITY_EXPORTS
    #define JWTSECURITY_API DECL_IMPORT
#else
    #define JWTSECURITY_API DECL_EXPORT
#endif //JWTSECURITY_EXPORTS

#endif

extern "C"
{
    JWTSECURITY_API ISecManager* createInstance(const char* serviceName, IPropertyTree& secMgrCfg, IPropertyTree& bndCfg);
}

#endif // JWTSECURITY_HPP_
