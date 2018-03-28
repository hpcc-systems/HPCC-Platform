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

#include "SchemaValue.hpp"
#include "EnvironmentValue.hpp"

SchemaValue::SchemaValue(const std::string &name, bool isDefined) :
    m_name(name), m_displayName(name)
{
    bitMask.m_required = 0;
    bitMask.m_readOnly = 0;
    bitMask.m_hidden = 0;
    bitMask.m_deprecated = 0;
    bitMask.m_isUnique = 0;
    bitMask.m_isDefined = isDefined;
}


bool SchemaValue::isValueValid(const std::string &value, const EnvironmentValue *pEnvValue) const
{
    bool isValid = true;   // assume valid

    //
    // Check the type
    isValid = m_pType->isValueValid(value);

    //
    // Keyed ?, then value must NOT be in the current list.
    if (isValid && isUniqueValue() && pEnvValue != nullptr)
    {
        bool found = false;
        std::vector<std::string> allValues;
        pEnvValue->getAllValuesForSiblings(allValues);
        for (auto it = allValues.begin(); it != allValues.end() && !found; ++it)
            found = *it == value;

        if (found)
        {
            return false;
        }
    }

    //
    // Keyref ?, then the value must be from another set
    if (isValid && isFromUniqueValueSet() && pEnvValue != nullptr)
    {
        bool found = false;
        std::vector<std::string> allValues;
        getAllKeyRefValues(allValues);
        for (auto it = allValues.begin(); it != allValues.end() && !found; ++it)
            found = *it == value;
        isValid = found;
    }
    return isValid;
}


void SchemaValue::validate(Status &status, const std::string &id, const EnvironmentValue *pEnvValue) const
{
    std::string curValue = pEnvValue->getValue();
    bool isValid;

    if (!m_pType->isValueValid(curValue))
    {
        if (pEnvValue)
        {
            std::string msg;
            if (pEnvValue->wasForced())
                msg = "Value was forced to an invalid value (" + curValue + ").";
            else
                msg = "Value is invalid (" + curValue + ").";
            msg += "Valid value (" + m_pType->getLimitString() + ")";

            status.addMsg(pEnvValue->wasForced() ? statusMsg::warning : statusMsg::error, pEnvValue->getNodeId(), pEnvValue->getName(), msg);
        }
        isValid = false;
    }

    // get currentvalue from pEnvValue
    // for keyed, make sure all values are unique
    // call pType with value to see if good
    // call pType->limits->toString(value) if bad to get message about whats bad
    // add to status

}


void SchemaValue::resetEnvironment()
{
    m_envValues.clear();
}


// replicates the new value throughout the environment
void SchemaValue::mirrorValueToEnvironment(const std::string &oldValue, const std::string &newValue)
{
    for (auto mirrorCfgIt = m_mirrorToSchemaValues.begin(); mirrorCfgIt != m_mirrorToSchemaValues.end(); ++mirrorCfgIt)
    {
        (*mirrorCfgIt)->setMirroredEnvironmentValues(oldValue, newValue);
    }
}


// Worker method for replicating a mirrored value to the environment values for this config value
void SchemaValue::setMirroredEnvironmentValues(const std::string &oldValue, const std::string &newValue)
{
    for (auto envIt = m_envValues.begin(); envIt != m_envValues.end(); ++envIt)
    {
        std::shared_ptr<EnvironmentValue> pEnvValue = (*envIt).lock();
        if (pEnvValue && pEnvValue->getValue() == oldValue)
        {
            pEnvValue->setValue(newValue, nullptr, true);
        }
    }
}


void SchemaValue::getAllEnvironmentValues(std::vector<std::string> &values) const
{
    for (auto it = m_envValues.begin(); it != m_envValues.end(); ++it)
    {
        values.push_back((*it).lock()->getValue());
    }
}


void SchemaValue::getAllowedValues(std::vector<AllowedValue> &allowedValues, const EnvironmentValue *pEnvValue) const
{
    //
    // Either the type is enumerated, or there is a keyref.
    if (m_pType->isEnumerated())
    {
        allowedValues = m_pType->getEnumeratedValues();
    }
    else if (isFromUniqueValueSet() && pEnvValue != nullptr)
    {
        std::vector<std::string> refValues;
        getAllKeyRefValues(refValues);
        for (auto it = refValues.begin(); it != refValues.end(); ++it)
        {
            allowedValues.push_back({ *it, "" });
        }
    }
}


void SchemaValue::getAllKeyRefValues(std::vector<std::string> &keyRefValues) const
{
    std::vector<std::weak_ptr<SchemaValue>> refCfgValues = getUniqueValueSetRefs();
    for (auto refCfgValueIt = refCfgValues.begin(); refCfgValueIt != refCfgValues.end(); ++refCfgValueIt)
    {
        std::shared_ptr<SchemaValue> pRefCfgValue = (*refCfgValueIt).lock();
        std::vector<std::string> allValues;
        pRefCfgValue->getAllEnvironmentValues(allValues);
        keyRefValues.insert(keyRefValues.end(), allValues.begin(), allValues.end());
    }
}