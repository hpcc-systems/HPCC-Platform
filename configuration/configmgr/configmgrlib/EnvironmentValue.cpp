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


EnvironmentValue::~EnvironmentValue()
{
    //
    // Tell the schema value that we are going away
    m_pSchemaValue->removeEnvironmentValue(this);
}


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


        //
        // If the value was set successfull...
        if (rc)
        {
            //
            // See if the new value has dependent values, set them
            std::vector<AllowedValue> allowedValues;
            std::shared_ptr<EnvironmentNode> pMyEnvNode = m_pMyEnvNode.lock();
            m_pSchemaValue->getAllowedValues(allowedValues, pMyEnvNode);
            for (auto &allowedValue: allowedValues)
            {
                if (value == allowedValue.m_value)
                {
                    if (allowedValue.hasDependencies())
                    {
                        Status tempStatus;  // assumpition is that dependent values will set properly
                        pMyEnvNode->setAttributeValues(allowedValue.getDependencies(), tempStatus, false, false);
                        break;   // done!
                    }
                }
            }

            //
            // Mirro this value throughout the environment if needed
            m_pSchemaValue->mirrorValueToEnvironment(oldValue, value, pStatus);
        }
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
    else
    {
        if (m_pSchemaValue->isRequired())
        {
            status.addMsg(statusMsg::error, myId, m_name, "Required value has not been set");
        }

        if (m_pSchemaValue->hasForcedValue())
        {
            std::string msg = "No value provided, default value of " + m_pSchemaValue->getForcedValue() + " will be used.";
            status.addMsg(statusMsg::warning, myId, m_name, msg);
        }
    }
}


void EnvironmentValue::initialize()
{
    //
    // Is there an auto generated value we should create?
    const std::string &type = m_pSchemaValue->getAutoGenerateType();
    if (!type.empty())
    {
        //
        // The "prefix" type uses the auto generate value to generate a unique value by appending a number until it is
        // unique. There are three variations:
        //   prefix & prefix_ - Use the value first by itself, then append an incrementing number, starting with 1,
        //      until unique. prefix_ uses an _ between value and the number (value_3)
        //   prefix# - always appends a number thus creating an incrementing unique value starting at 1
        //      (value1, value2, value3, ...)
        if (type == "prefix" || type == "prefix_" || type == "prefix#")
        {
            std::string connector = (type == "prefix_") ? "_" : "";
            std::string newName;
            const std::string &prefix = m_pSchemaValue->getAutoGenerateValue();
            std::vector<std::string> curValues;
            m_pMyEnvNode.lock()->getAttributeValueForAllSiblings(m_name, curValues);
            size_t count = curValues.size();
            size_t n = (type == "prefix#") ? 1 : 0;
            newName = ((type == "prefix#") ? (prefix + connector + std::to_string(n)) : prefix);
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
                newName = prefix + connector + std::to_string(n);
            }
        }

        //
        // If type is configProperty, then the autogenerated value is the taken from the value of the node's indicated property
        else if (type == "configProperty")
        {
            const std::string &propertyName = m_pSchemaValue->getAutoGenerateValue();
            std::string value = m_pMyEnvNode.lock()->getSchemaItem()->getProperty(propertyName);
            setValue(value, nullptr);
        }

        //
        // Fixed value?
        else if (type == "fixedValue")
        {
            setValue(m_pSchemaValue->getAutoGenerateValue(), nullptr);
        }

        //
        // Sibling value? Used to copy the value of a sibling attribute to this attribute
        else if (type == "siblingValue")
        {
            setValue(getEnvironmentNode()->getAttribute(m_pSchemaValue->getAutoGenerateValue())->getValue(), nullptr);
        }
    }
}


std::string EnvironmentValue::getNodeId() const
{
    return m_pMyEnvNode.lock()->getId();
}
