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

#include "EnvironmentValue.hpp"
#include "EnvironmentNode.hpp"

bool EnvironmentValue::setValue(const std::string &value, Status *pStatus, bool forceSet)
{
    bool rc = true;
    std::string oldValue = m_value;
    if (m_pSchemaValue)
    {
        m_forcedSet = false;
        if (m_pSchemaValue->isValueValid(value))
        {
            m_value = value;
        }
        else if (forceSet)
        {
            m_value = value;
            m_forcedSet = true;
            if (pStatus != nullptr)
            {
                std::string msg = "Attribute forced to invalid value (" + m_pSchemaValue->getType()->getLimitString() + ")";
                pStatus->addMsg(statusMsg::info, m_pMyEnvNode.lock()->getId(), m_name, msg);
            }
        }
        else
        {
            rc = false;
            if (pStatus != nullptr)
            {
                std::string msg = "Value not set. New value(" + value + ") not valid (" + m_pSchemaValue->getType()->getLimitString() + ")";
                pStatus->addMsg(statusMsg::error, m_pMyEnvNode.lock()->getId(), m_name, msg);
            }
        }

        if (rc)
            m_pSchemaValue->mirrorValueToEnvironment(oldValue, value);

    }
    return rc;
}


void EnvironmentValue::getAllValuesForSiblings(std::vector<std::string> &result) const
{
    std::shared_ptr<EnvironmentNode> pEnvNode = m_pMyEnvNode.lock();
    return pEnvNode->getAttributeValueForAllSiblings(m_pSchemaValue->getName(), result);
}


bool EnvironmentValue::isValueValid(const std::string &value) const
{
    bool rc = m_pSchemaValue->isValueValid(value, this);
    return rc;
}


void EnvironmentValue::validate(Status &status, const std::string &myId) const
{
    //
    // Only validate if value is set, otherwise, it's valid
    if (isValueSet())
    {
        if (!m_pSchemaValue->isDefined())
            status.addMsg(statusMsg::warning, myId, m_name, "No type information exists");

        if (m_forcedSet)
            status.addMsg(statusMsg::warning, myId, m_name, "Current value was force set to an invalid value");

        // Will generate status based on current value and type
        m_pSchemaValue->validate(status, myId, this);
    }
}


// Called when a new value has been created, not read from existing environment, but created and added
void EnvironmentValue::initialize()
{
    //
    // Is there an auto generated value we should process:
    const std::string &type = m_pSchemaValue->getAutoGenerateType();
    if (!type.empty())
    {
        //
        // type "prefix" means to use the auto generate value as a name prefix and to append numbers until a new unique name is
        // found
        if (type == "prefix")
        {
            std::string newName;
            const std::string &prefix = m_pSchemaValue->getAutoGenerateValue();
            std::vector<std::string> curValues;
            m_pMyEnvNode.lock()->getAttributeValueForAllSiblings(m_name, curValues);
            size_t count = curValues.size();
            newName = prefix;
            size_t n = 0;
            while (n <= count + 1)
            {
                bool found = false;
                for (auto it = curValues.begin(); it != curValues.end() && !found; ++it)
                {
                    if ((*it) == newName)
                        found = true;
                }

                if (!found)
                {
                    setValue(newName, nullptr);
                    break;
                }
                ++n;
                newName = prefix + std::to_string(n);
            }
        }

        //
        // If type is configProperty, then the autogenerate value is the name of the value's node's schema item
        // property. Set the value to this
        else if (type == "configProperty")
        {
            const std::string &propertyName = m_pSchemaValue->getAutoGenerateValue();
            std::string value;
            value = m_pMyEnvNode.lock()->getSchemaItem()->getProperty(propertyName);
            setValue(value, nullptr);
        }
    }
}


std::string EnvironmentValue::getNodeId() const
{
    return m_pMyEnvNode.lock()->getId();
}