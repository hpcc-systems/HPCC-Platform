/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "Variable.hpp"
#include "TemplateException.hpp"
#include "IPAddressRangeVariable.hpp"
#include "IPAddressVariable.hpp"
#include "HostNameVariable.hpp"
#include <memory>

// todo add more types that correspond to XSD types so that modification template inputs can be validated earlier

std::shared_ptr<Variable> variableFactory(const std::string &type, const std::string &name)
{
    std::shared_ptr<Variable> pInput;
    if (type == "string")
    {
        pInput = std::make_shared<Variable>(name);
    }
    else if (type == "iprange")
    {
        pInput = std::make_shared<IPAddressRangeVariable>(name);
    }
    else if (type == "ipaddress")
    {
        pInput = std::make_shared<IPAddressVariable>(name);
    }
    else if (type == "hostname")
    {
        pInput = std::make_shared<HostNameVariable>(name);
    }
    else
    {
        throw TemplateException("Invalid input type '" + type + "'");
    }
    return pInput;
}



std::string Variable::getValue(size_t idx) const
{
    //
    // If no value assigned yet, throw an exception
    if (m_values.empty())
    {
        std::string msg = "Attempt to get value of uninitialized input '" + m_name + "'";
        throw TemplateException(msg, false);
    }

    //
    // If there is only one value, then it's a single value input, so just return the first index regardless
    if (m_values.size() == 1)
    {
        idx = 0;
    }

    //
    // Otherwise, make sure the requested index is in range
    else if (idx >= m_values.size())
    {
        std::string msg = "Attempt to get value out of range (" + std::to_string(idx) + "), size = " + std::to_string(m_values.size()) + " for '" + m_name +"'";
        throw TemplateException(msg, false);
    }

    return m_values[idx];
}


void Variable::addValue(const std::string &value)
{
    m_values.emplace_back(value);
}
