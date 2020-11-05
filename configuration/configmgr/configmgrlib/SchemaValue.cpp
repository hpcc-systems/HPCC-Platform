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
#include "EnvironmentNode.hpp"
#include <algorithm>
#include "ConfigPath.hpp"
#include "Utils.hpp"
#include "Exceptions.hpp"

SchemaValue::SchemaValue(const std::string &name, bool isDefined) :
    m_name(name), m_displayName(name), m_invertHiddenIf(false)
{
    bitMask.m_required = 0;
    bitMask.m_readOnly = 0;
    bitMask.m_hidden = 0;
    bitMask.m_deprecated = 0;
    bitMask.m_isUnique = 0;
    bitMask.m_isDefined = isDefined;
    bitMask.m_noOutput = 0;
    m_ordinal = 0;
}


SchemaValue::SchemaValue(const SchemaValue &value)
{
    // Primary purpose of the copy constructor is for use when a complexType is referenced. A copy is made which includes a copy
    // of each SchemaValue in the complexType SchemaItem.
    m_pType = value.m_pType;
    m_name = value.m_name;
    m_displayName = value.m_displayName;
    m_mirrorFromPath = value.m_mirrorFromPath;
    m_autoGenerateValue = value.m_autoGenerateValue;
    m_autoGenerateType = value.m_autoGenerateType;
    bitMask = value.bitMask;
    m_forcedValue = value.m_forcedValue;
    m_tooltip = value.m_tooltip;
    m_modifiers = value.m_modifiers;
    m_valueLimitRuleType = value.m_valueLimitRuleType;
    m_valueLimitRuleData = value.m_valueLimitRuleData;
    m_requiredIf = value.m_requiredIf;
    m_groupByName = value.m_groupByName;
    m_hiddenIf = value.m_hiddenIf;
    m_invertHiddenIf = value.m_invertHiddenIf;

    // special processing? Maybe after inserting?
    std::vector<std::shared_ptr<SchemaValue>> m_mirrorToSchemaValues;
    std::vector<std::weak_ptr<SchemaValue>> m_pUniqueValueSetRefs;    // this value serves as the key from which values are valid

    m_ordinal = 0;  // assigned when inserted
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
    bool isValid = true;

    if (pEnvValue == nullptr)
    {
        std::string msg = "Attempt to validate schema value w/o an environment value.";
        throw(ParseException(msg));
    }

    //
    // If we have an environment value, more specific information can be provided
    if (pEnvValue)
    {
        std::string curValue = pEnvValue->getValue();
        isValid = m_pType->isValueValid(curValue);

        //
        // See if there is a dependency on another value being set.
        if (!m_requiredIf.empty() && isValid)
        {
            //
            // Required if string format is an xpath. Search this environment value's owning node
            // for a match.
            std::vector<std::shared_ptr<EnvironmentNode>> nodes;
            pEnvValue->getEnvironmentNode()->fetchNodes(m_requiredIf, nodes);
            if (!nodes.empty())
            {
                //
                // Since here is a match for a requiredIf, this value MUST be set
                if (pEnvValue->getValue().empty())
                {
                    isValid = false;
                    std::string msg = "Environment value required based on requiredIf rule " + m_requiredIf + " being set.";
                    status.addMsg(statusMsg::error, pEnvValue->getNodeId(), pEnvValue->getName(), msg);
                }
            }
        }

        //
        // If not valid, provide the reason
        if (!isValid)
        {
            std::string msg;
            if (pEnvValue->wasForced())
                msg = "Value was forced to an invalid value (" + curValue + ").";
            else
                msg = "Value is invalid (" + curValue + ").";
            msg += " Valid value (" + m_pType->getLimitString() + ")";

            status.addMsg(pEnvValue->wasForced() ? statusMsg::warning : statusMsg::error, pEnvValue->getNodeId(), pEnvValue->getName(), msg);
        }

        //
        // Otherwise, the value is valid, but there could be a validate message
        else
        {
            const std::string &validateMsg = m_pType->getValidateMsg();
            if (!validateMsg.empty())
            {
                status.addMsg(status.getMsgLevelFromString(m_pType->getValidateMsgType()), pEnvValue->getNodeId(), pEnvValue->getName(), validateMsg);
            }
        }
    }
}


void SchemaValue::resetEnvironment()
{
    m_envValues.clear();
}


// replicates the new value throughout the environment
void SchemaValue::mirrorValueToEnvironment(const std::string &oldValue, const std::string &newValue, Status *pStatus)
{
    for (auto &pSchemaValue: m_mirrorToSchemaValues)
    {
        pSchemaValue->doMirroroToEnvironmentValues(oldValue, newValue, pStatus);
    }
}


void SchemaValue::doMirroroToEnvironmentValues(const std::string &oldValue, const std::string &newValue, Status *pStatus)
{
    std::string msg = "Value automatically changed from " + oldValue + " to " + newValue;
    for (auto &envValueIt: m_envValues)
    {
        std::shared_ptr<EnvironmentValue> pEnvValue = envValueIt.lock();
        if (pEnvValue && pEnvValue->getValue() == oldValue)
        {
            pEnvValue->setValue(newValue, nullptr, true);
            if (pStatus != nullptr)
            {
                pStatus->addMsg(statusMsg::change, msg, pEnvValue->getEnvironmentNode()->getId(), pEnvValue->getSchemaValue()->getDisplayName());
            }
        }
    }
}


void SchemaValue::getAllEnvironmentValues(std::vector<std::shared_ptr<EnvironmentValue>> &envValues) const
{
    for (auto it = m_envValues.begin(); it != m_envValues.end(); ++it)
    {
        std::shared_ptr<EnvironmentValue> pEnvValue = it->lock();
        if (pEnvValue)
        {
            envValues.push_back(pEnvValue);
        }
    }
}


bool SchemaValue::getAllowedValues(std::vector<AllowedValue> &allowedValues, const std::shared_ptr<const EnvironmentNode> &pEnvNode) const
{
    bool rc = false;

    //
    // If enumerated, get the allowed values
    if (m_pType->isEnumerated())
    {
        allowedValues = m_pType->getEnumeratedValues();
        rc = true;
    }

    //
    // Or, keyed? (note that the keyed check MUST be last since a more restrictive rule may be defined for logical purposes
    // while a keyed reference is present for XML schema validation)
    else if (isFromUniqueValueSet())
    {
        std::shared_ptr<EnvironmentValue> pEnvValue = pEnvNode->getAttribute(getName());
        std::vector<std::string> curValues;
        pEnvValue->getAllValuesForSiblings(curValues);

        std::vector<std::string> refValues;
        getAllKeyRefValues(refValues);

        //
        // For each key reference value (the full set of allowed values), remove any that are already in use (curValues)
        for (auto &refIt: refValues)
        {
            std::vector<std::string>::const_iterator inUseIt = std::find_if(curValues.begin(), curValues.end(), [&](const std::string &curValueIt) {
                return curValueIt == refIt;
            });

            if (inUseIt == curValues.end())
            {
                allowedValues.emplace_back(AllowedValue(refIt, ""));
            }
        }

        if (!allowedValues.empty() && m_valueLimitRuleType == "addDependencies_FromSiblingAttributeValue")
        {
            std::vector<std::string> params = splitString(m_valueLimitRuleData, ",");

            if (params.size() != 4)
            {
                std::string msg = "Applying rule " + m_valueLimitRuleType + ", expected 4 parameters in rule data";
                throw(ParseException(msg));
            }

            std::string matchPath = params[0];
            std::string matchAttribute = params[1];
            std::string depAttrSource = params[2];
            std::string depAttrTarget = params[3];

            //
            // Get an environment node pointer using the first entry in the env values vector. We know it's not empty because
            // we have at least one allowed value. Use this for fetching
            std::shared_ptr<EnvironmentNode> pEnvValuesNode = m_envValues[0].lock()->getEnvironmentNode();

            //
            // Loop through each allowed value and find it's environment node (by value). Then add a dependency
            // based on the dependent attribute source.
            for (auto &allowedValue: allowedValues)
            {
                std::string path = matchPath;
                path.append("[@").append(matchAttribute).append("='").append(allowedValue.m_value).append("']");
                std::vector<std::shared_ptr<EnvironmentNode>> envNodes;
                pEnvValuesNode->fetchNodes(path, envNodes);
                if (!envNodes.empty())
                {
                    std::shared_ptr<EnvironmentValue> pAttr = envNodes[0]->getAttribute(depAttrSource);
                    if (pAttr)
                    {
                        allowedValue.addDependentValue(depAttrTarget, pAttr->getValue());
                    }
                }
            }
        }
        rc = true;
    }
    return rc;
}


void SchemaValue::getAllKeyRefValues(std::vector<std::string> &keyRefValues) const
{
    std::vector<std::weak_ptr<SchemaValue>> refCfgValues = getUniqueValueSetRefs();
    for (auto refCfgValueIt = refCfgValues.begin(); refCfgValueIt != refCfgValues.end(); ++refCfgValueIt)
    {
        std::shared_ptr<SchemaValue> pRefCfgValue = (*refCfgValueIt).lock();
        std::vector<std::shared_ptr<EnvironmentValue>> allEnvValues;
        pRefCfgValue->getAllEnvironmentValues(allEnvValues);
        for (auto &envIt: allEnvValues)
        {
            keyRefValues.push_back(envIt->getValue());
        }
    }
}


bool SchemaValue::hasModifier(const std::string &modifier) const
{
    return std::find(m_modifiers.begin(), m_modifiers.end(), modifier) != m_modifiers.end();
}


bool SchemaValue::isHidden(const EnvironmentValue *pEnvValue) const
{
    bool hidden = bitMask.m_hidden;
    if (!hidden && !m_hiddenIf.empty() && pEnvValue != nullptr)
    {
        //
        // Hidden if string format is an xpath. Search this environment value's owning node
        // for a match.
        std::vector<std::shared_ptr<EnvironmentNode>> nodes;
        pEnvValue->getEnvironmentNode()->fetchNodes(m_hiddenIf, nodes);
        hidden = !nodes.empty();
        hidden = m_invertHiddenIf == !hidden;
    }
    return hidden;
}


void SchemaValue::removeEnvironmentValue(const EnvironmentValue *pEnvValue)
{
    for (auto it = m_envValues.begin(); it != m_envValues.end(); ++it)
    {
        if ((*it).lock().get() == nullptr)
        {
            m_envValues.erase(it);
            break;
        }
    }
}
