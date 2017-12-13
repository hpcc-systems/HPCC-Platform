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

#include "CfgStringLimits.hpp"
#include "EnvValue.hpp"

std::string CfgStringLimits::getString() const
{
    std::string limitStr = "";  // this will turn into an error string describing why a value is invalid
    return limitStr;
}


bool CfgStringLimits::isValueValid(const std::string &testValue) const
{
    bool isValid = true;

    int len = testValue.length();

    isValid = len >= getMin() && len <= getMax();

    if (isValid)
    {
        if (!m_allowedValues.empty())
        {
            bool found = false;
            for (auto valueIt = m_allowedValues.begin(); valueIt != m_allowedValues.end() && !found; ++valueIt)
            {
                found = (*valueIt).m_value == testValue;
            }
            isValid = found;
        }
    }
    return isValid;
}