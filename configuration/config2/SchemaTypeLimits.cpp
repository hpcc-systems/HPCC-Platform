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

#include "SchemaTypeLimits.hpp"

std::vector<AllowedValue> SchemaTypeLimits::getEnumeratedValues() const
{
    return m_enumeratedValues;
}


bool SchemaTypeLimits::isValueValid(const std::string &testValue) const
{
    m_validateMsg = m_validateMsgType = "";
    bool rc = isValidEnumeratedValue(testValue);
    if (rc)
    {
        rc = doValueTest(testValue);
    }
    return rc;
}


bool SchemaTypeLimits::isValidEnumeratedValue(const std::string &testValue) const
{
    bool rc = true;
    if (isEnumerated())  // extra check just in case called by accident
    {
        rc = false;
        for (auto it = m_enumeratedValues.begin(); it != m_enumeratedValues.end() && !rc; ++it)
        {
            if (testValue == it->m_value)
            {
                rc = true;
                m_validateMsg = it->m_userMessage;
                m_validateMsgType = it->m_userMessageType;
            }
        }
    }
    return rc;
}


void SchemaTypeLimits::addDependentAttributeValue(const std::string &value, const std::string &depAttr, const std::string &depAttrVal)
{
    for (auto it = m_enumeratedValues.begin(); it != m_enumeratedValues.end(); ++it)
    {
        if ((*it).m_value == value)
        {
            (*it).addDependentValue(depAttr, depAttrVal);
            break;
        }
    }
}


void AllowedValue::addDependentValue(const std::string &attribute, const std::string &value)
{
    m_dependencies.push_back(DependentValue(attribute, value));
}
