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
#include "Utils.hpp"

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
    m_default = value.m_default;
    m_tooltip = value.m_tooltip;
    m_modifiers = value.m_modifiers;
    m_valueLimitRuleType = value.m_valueLimitRuleType;
    m_valueLimitRuleData = value.m_valueLimitRuleData;
    m_requiredIf = value.m_requiredIf;
    m_group = value.m_group;

    // special processing? Maybe after inserting?
    std::vector<std::shared_ptr<SchemaValue>> m_mirrorToSchemaValues;
    std::vector<std::weak_ptr<SchemaValue>> m_pUniqueValueSetRefs;    // this value serves as the key from which values are valid
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
    bool isValid = m_pType->isValueValid(curValue);

    //
    // If we have an environment value, more specific information can be provided
    if (pEnvValue)
    {
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
    std::string msg = "Value automatically changed from " + oldValue + " to " + newValue;
    for (auto mirrorCfgIt = m_mirrorToSchemaValues.begin(); mirrorCfgIt != m_mirrorToSchemaValues.end(); ++mirrorCfgIt)
    {
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
}


void SchemaValue::getAllEnvironmentValues(std::vector<std::shared_ptr<EnvironmentValue>> &envValues) const
{
    for (auto it = m_envValues.begin(); it != m_envValues.end(); ++it)
    {
        envValues.push_back(it->lock());
    }
}


void SchemaValue::getAllowedValues(std::vector<AllowedValue> &allowedValues, const std::shared_ptr<const EnvironmentNode> &pEnvNode) const
{
    //
    // If enumerated, get the allowed values
    if (m_pType->isEnumerated())
    {
        allowedValues = m_pType->getEnumeratedValues();
    }

    //
    // Is there a specialized rule that limits the values?
    else if (!m_valueLimitRuleType.empty())
    {
        //
        // uniqueItemType_1 - value is based on a unique item type described by the data for the rule. This is version 1
        if (m_valueLimitRuleType == "uniqueItemType_1")
        {
            std::vector<std::string> params = splitString(m_valueLimitRuleData, ",");
            std::vector<std::string> parts;

            //
            // First parameter is the source values for an attribute search. The two parts of the parameter are the path to the
            // node set where the atttribute, the second part, name is found (not that there may be no entries). Find all the nodes
            // for the path (parts[0]), then get all of the values for the attribute (parts[1]). This serves as the list of existing
            // values that are eliminated from the final list of allowable values.
            parts = splitString(params[0], "@");
            std::vector<std::shared_ptr<EnvironmentNode>> existingSourceNodes;
            pEnvNode->fetchNodes(parts[0], existingSourceNodes);
            std::vector<std::string> existingSourceAttributeValues;
            for (auto &existingNodeIt: existingSourceNodes)
            {
                existingSourceAttributeValues.push_back( existingNodeIt->getAttributeValue(parts[1]));
            }

            //
            // Get the full set of possible values using the params[1] values. From its parts, parts[0] is the path
            // to find the set of all possible nodes that could serve as an allowable value.
            std::vector<std::shared_ptr<EnvironmentNode>> allSourceNodes;
            parts = splitString(params[1], "@");
            std::string sourceAttributeName = parts[1];  // for use below in case parts is reused later
            pEnvNode->fetchNodes(parts[0], allSourceNodes);

            //
            // For each exising source node, using the existingSourceAttributeValues, matching the name to the value in
            // sourceAttributeName, and collect the itemType values found.
            std::vector<std::string> existingItemTypes;
            for (auto &existingValueIt: existingSourceAttributeValues)
            {
                std::vector<std::shared_ptr<EnvironmentNode>>::iterator sourceIt = std::find_if(allSourceNodes.begin(), allSourceNodes.end(),
                    [&](std::shared_ptr<EnvironmentNode> &srcIt) {
                        return srcIt->getAttributeValue(sourceAttributeName) == existingValueIt;
                });

                if (sourceIt != allSourceNodes.end())
                {
                    existingItemTypes.push_back((*sourceIt)->getSchemaItem()->getItemType());
                }
            }

            //
            // Build the allowable value list by only adding itmes from the all sources list that don't hvae
            // an entry in the existing item type vector
            for (auto &sourceIt: allSourceNodes)
            {
                std::vector<std::string>::const_iterator itemTypeIt = std::find_if(existingItemTypes.begin(), existingItemTypes.end(), [&](const std::string &itemIt) {
                    return itemIt == sourceIt->getSchemaItem()->getItemType();
                });

                if (itemTypeIt == existingItemTypes.end())
                {
                    allowedValues.push_back({ sourceIt->getAttributeValue(sourceAttributeName), "" });
                }
            }
        }
    }

    //
    // Or, keyed? (note that the keyed check MUST be last since a more restrictive rule may be defined for UI purposes
    // while a keyed reference is present for XML schema validation)
    else if (isFromUniqueValueSet())
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
        std::vector<std::shared_ptr<EnvironmentValue>> allEnvValues;
        pRefCfgValue->getAllEnvironmentValues(allEnvValues);
        for (auto &envIt: allEnvValues)
        {
            keyRefValues.push_back(envIt->getValue());
        }
    }
}
