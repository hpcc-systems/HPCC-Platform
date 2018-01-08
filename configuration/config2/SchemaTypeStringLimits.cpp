/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#include "SchemaTypeStringLimits.hpp"
#include "EnvironmentValue.hpp"
#include <regex>

std::string SchemaTypeStringLimits::getLimitString() const
{

    return "String limit info";
}


bool SchemaTypeStringLimits::doValueTest(const std::string &testValue) const
{
    bool isValid;
    int len = testValue.length();
    isValid = len >= m_minLength && len <= m_maxLength;

    // test patterns
    for (auto it = m_patterns.begin(); it != m_patterns.end() && isValid; ++it)
    {
        std::regex expr (*it);
        isValid = std::regex_match(testValue, expr);
    }
    return isValid;
}
