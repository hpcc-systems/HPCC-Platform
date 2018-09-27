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

#include "SchemaTypeIntegerLimits.hpp"

bool SchemaTypeIntegerLimits::doValueTest(const std::string &value) const
{
    bool isValid = true;
    int testValue;
    try
    {
        testValue = std::stoi(value);
    }
    catch (...)
    {
        isValid = false;  // not even an integer string
    }

    if (isValid)
    {
        //
        // min/max tests
        isValid = (m_minExclusiveTest) ? (testValue > m_min) : (testValue >= m_min);
        isValid = isValid && ((m_maxExclusiveTest) ? (testValue < m_max) : (testValue <= m_max));
    }
    return isValid;
}


std::string SchemaTypeIntegerLimits::getLimitString() const
{
    return "integer limit string";
}
