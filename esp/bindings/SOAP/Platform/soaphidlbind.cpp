/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2024 HPCC Systems®.

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

#pragma warning( disable : 4786 )

#include "soaphidlbind.hpp"

void CHttpSoapHidlBinding::registerMethodNames(std::initializer_list<const char *> names)
{
    for (const char *name : names)
    {
        std::string methodNameKey(name);
        std::transform(methodNameKey.cbegin(), methodNameKey.cend(), methodNameKey.begin(),
                       [](unsigned char c){ return std::tolower(c); });
        m_qualifiedMethodNames.emplace(methodNameKey.c_str(), name);
    }
}

bool CHttpSoapHidlBinding::qualifyMethodName(IEspContext &context, const char *methname, StringBuffer *methQName)
{
    if (!methname || !*methname)
    {
        if (methQName != nullptr)
            methQName->clear();
        return true;
    }
    std::string methodName(methname);
    std::transform(methodName.cbegin(), methodName.cend(), methodName.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    auto it = m_qualifiedMethodNames.find(methodName);
    if (it != m_qualifiedMethodNames.end())
    {
        if (methQName != nullptr)
            methQName->set(it->second.c_str());
        return true;
    }
    return false;
}
