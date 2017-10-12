/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#include <algorithm>

#include "ConfigItemValueSet.hpp"
#include "ConfigExceptions.hpp"


void ConfigItemValueSet::addCfgValue(const std::shared_ptr<CfgValue> pNewValue)
{
    std::shared_ptr<CfgValue> pValue = findValue(pNewValue->getName(), false);
    if (!pValue)
    {
		m_cfgValues.push_back(pNewValue);
    }
    else 
    {
        std::string msg = "A value already exists for " + pNewValue->getName();
        throw(new ParseException(msg));
    }
}


void ConfigItemValueSet::addCfgValue(const std::shared_ptr<ConfigItemValueSet> &valueSet)
{
    const std::vector<std::shared_ptr<CfgValue>> &values = valueSet->getCfgValues();
    for (auto it=values.begin(); it!=values.end(); ++it)
        addCfgValue(*it);
}


const std::vector<std::shared_ptr<CfgValue>> &ConfigItemValueSet::getCfgValues() const
{
    return m_cfgValues;
}


std::shared_ptr<CfgValue> ConfigItemValueSet::findValue(const std::string &valueName, bool throwIfNotFound) const
{
    std::shared_ptr<CfgValue> pValue;
    auto it = std::find_if(m_cfgValues.begin(), m_cfgValues.end(), [valueName](const std::shared_ptr<CfgValue> &pTestValue) -> bool {return pTestValue->getName() == valueName;} );
    if (it != m_cfgValues.end())
    {
        pValue = *it;
    }
    else if (throwIfNotFound)
    {
        std::string msg = "Unable to find valueName(" + valueName + ")";
        throw(new ValueException(msg));
    }
    return pValue;
}
