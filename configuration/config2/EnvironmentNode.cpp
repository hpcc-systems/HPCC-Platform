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

#include "EnvironmentNode.hpp"

void EnvironmentNode::addChild(std::shared_ptr<EnvironmentNode> pNode)
{
    m_children.insert(std::make_pair(pNode->getName(), pNode));
}


bool EnvironmentNode::removeChild(const std::shared_ptr<EnvironmentNode> pNode)
{
    bool removed = false;
    for (auto it=m_children.begin(); it!= m_children.end() && !removed; ++it)
    {
        if (pNode == it->second)
        {
            m_children.erase(it);
            removed = true;
        }
    }
    return removed;
}


void EnvironmentNode::addAttribute(const std::string &name, std::shared_ptr<EnvironmentValue> pValue)
{
    auto retValue = m_attributes.insert(std::make_pair(name, pValue));
}


std::vector<std::shared_ptr<EnvironmentNode>> EnvironmentNode::getChildren(const std::string &name) const
{
    std::vector<std::shared_ptr<EnvironmentNode>> childNodes;
    if (name == "")
    {
        for (auto nodeIt = m_children.begin(); nodeIt != m_children.end(); ++nodeIt)
        {
            childNodes.push_back(nodeIt->second);
        }
    }
    else
    {
        auto rangeIt = m_children.equal_range(name);
        for (auto it = rangeIt.first; it != rangeIt.second; ++it)
        {
            childNodes.push_back(it->second);
        }
    }

    return childNodes;
}


std::map<std::string, std::vector<std::shared_ptr<EnvironmentNode>>> EnvironmentNode::getChildrenByName() const
{
    std::map<std::string, std::vector<std::shared_ptr<EnvironmentNode>>> results;
    for (auto childIt = m_children.begin(); childIt != m_children.end(); ++childIt)
    {
        auto it = results.find(childIt->second->getName());
        if (it == results.end())
        {
            std::vector<std::shared_ptr<EnvironmentNode>> nodes;
            nodes.push_back(childIt->second);
            results[childIt->second->getName()] = nodes;
        }
        else
        {
            it->second.push_back(childIt->second);
        }
    }
    return results;
}


std::map<std::string, std::vector<std::shared_ptr<EnvironmentNode>>> EnvironmentNode::getChildrenByConfigType() const
{
    std::map<std::string, std::vector<std::shared_ptr<EnvironmentNode>>> results;
    for (auto childIt = m_children.begin(); childIt != m_children.end(); ++childIt)
    {
        auto it = results.find(childIt->second->getSchemaItem()->getItemType());
        if (it == results.end())
        {
            std::vector<std::shared_ptr<EnvironmentNode>> nodes;
            nodes.push_back(childIt->second);
            results[childIt->second->getSchemaItem()->getItemType()] = nodes;
        }
        else
        {
            it->second.push_back(childIt->second);
        }
    }
    return results;
}


std::shared_ptr<EnvironmentNode> EnvironmentNode::getParent() const
{
    std::shared_ptr<EnvironmentNode> pParent;
    if (!m_pParent.expired())
    {
        pParent = m_pParent.lock();
    }
    return pParent;
}


std::vector<std::shared_ptr<EnvironmentValue>> EnvironmentNode::getAttributes() const
{
    std::vector<std::shared_ptr<EnvironmentValue>> attributes;

    for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
    {
        attributes.push_back(attrIt->second);
    }
    return attributes;
}


void EnvironmentNode::addMissingAttributesFromConfig()
{
    std::map<std::string, std::shared_ptr<SchemaValue>> configuredAttributes = m_pSchemaItem->getAttributes();

    //
    // go through all the configured attrubutes and for each that is not present in our list, add it
    for (auto it = configuredAttributes.begin(); it != configuredAttributes.end(); ++it)
    {
        auto attrIt = m_attributes.find(it->first);
        if (attrIt == m_attributes.end())
        {
            std::shared_ptr<SchemaValue> pCfgValue = it->second;
            std::shared_ptr<EnvironmentValue> pEnvValue = std::make_shared<EnvironmentValue>(shared_from_this(), pCfgValue, it->first);
            pCfgValue->addEnvironmentValue(pEnvValue);
            addAttribute(it->first, pEnvValue);
        }
    }

}


void EnvironmentNode::setAttributeValues(const std::vector<ValueDef> &values, Status &status, bool allowInvalid, bool forceCreate)
{
    for (auto it = values.begin(); it != values.end(); ++it)
    {
        setAttributeValue((*it).name, (*it).value, status, allowInvalid, forceCreate);
    }
}


void EnvironmentNode::setAttributeValue(const std::string &attrName, const std::string &value, Status &status, bool allowInvalid, bool forceCreate)
{
    std::shared_ptr<EnvironmentValue> pEnvValue;

    auto it = m_attributes.find(attrName);
    if (it != m_attributes.end())
    {
        pEnvValue = it->second;
    }

    //
    // Not found on this node. See if the configuration defines the attribute. If so, set the value and move on.
    // If not and the forceCreate flag is set, create it. 
    else if (forceCreate)
    {
        std::shared_ptr<SchemaValue> pCfgValue = m_pSchemaItem->getAttribute(attrName);
        pEnvValue = std::make_shared<EnvironmentValue>(shared_from_this(), pCfgValue, attrName);
        addAttribute(attrName, pEnvValue);
        if (!pCfgValue->isDefined())
        {
            status.addStatusMsg(statusMsg::warning, getId(), attrName, "", "Undefined attribute did not exist in configuration, was created");
        }
    }

    if (pEnvValue)
    {
        pEnvValue->setValue(value, &status, allowInvalid); 
    }
    else
    {
        status.addStatusMsg(statusMsg::error, getId(), attrName, "", "The attribute does not exist and was not created");
    }

}


std::string EnvironmentNode::getAttributeValue(const std::string &name) const
{
    std::string value;
    std::shared_ptr<EnvironmentValue> pAttribute = getAttribute(name);
    if (pAttribute)
        value = pAttribute->getValue();
    return value;
}


bool EnvironmentNode::setNodeValue(const std::string &value, Status &status, bool force)
{
    bool rc = false;

    //
    // If no environment value is present, create one first
    if (!m_pNodeValue)
    {
        std::shared_ptr<SchemaValue> pCfgValue = m_pSchemaItem->getItemSchemaValue();
        m_pNodeValue = std::make_shared<EnvironmentValue>(shared_from_this(), pCfgValue, "");  // node's value has no name
    }

    rc = m_pNodeValue->setValue(value, &status, force);
    return rc;
}


void EnvironmentNode::validate(Status &status, bool includeChildren) const
{
    //
    // Check node value
    if (m_pNodeValue)
    {
        m_pNodeValue->validate(status, "");
    }

    //
    // Check any attributes
    for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
    {
        attrIt->second->validate(status, m_id);

        //
        // If this value must be unique, make sure it is
        if (attrIt->second->getSchemaValue()->isUniqueValue())
        {
            bool found = false;
            std::vector<std::string> allValues = attrIt->second->getAllValues();
            std::set<std::string> unquieValues;
            for (auto it = allValues.begin(); it != allValues.end() && !found; ++it)
            {
                auto ret = unquieValues.insert(*it);
                found = ret.second;
            }

            if (found)
            {
                status.addUniqueStatusMsg(statusMsg::error, m_id, attrIt->second->getName(), "", "Attribute value must be unique");
            }
        }

        //
        // Does this value need to be from another set of values?
        if (attrIt->second->getSchemaValue()->isFromUniqueValueSet())
        {
            bool found = false;
            std::vector<std::string> allValues = attrIt->second->getSchemaValue()->getAllKeyRefValues();
            for (auto it = allValues.begin(); it != allValues.end() && !found; ++it)
                found = *it == attrIt->second->getValue();
            if (!found)
            {
                status.addStatusMsg(statusMsg::error, m_id, attrIt->second->getName(), "", "Attribute value must be from a unique set");
            }
        }
    }

    //
    // Now check all children
    if (includeChildren)
    {
        for (auto childIt = m_children.begin(); childIt != m_children.end(); ++childIt)
        {
            childIt->second->validate(status);
        }
    }
}


std::vector<std::string> EnvironmentNode::getAllAttributeValues(const std::string &attrName) const
{
    std::vector<std::string> values;
    std::shared_ptr<EnvironmentNode> pParentNode = m_pParent.lock();
    if (pParentNode)
    {
        std::vector<std::shared_ptr<EnvironmentNode>> nodes = pParentNode->getChildren(m_name);
        for (auto it = nodes.begin(); it != nodes.end(); ++it)
        {
            values.push_back((*it)->getAttributeValue(attrName));
        }
    }
    return values;
}


const std::shared_ptr<EnvironmentValue> EnvironmentNode::getAttribute(const std::string &name) const
{
    std::shared_ptr<EnvironmentValue> pValue;
    auto it = m_attributes.find(name);
    if (it != m_attributes.end())
    {
        pValue = it->second;
    }
    return pValue;
}


std::vector<std::shared_ptr<SchemaItem>> EnvironmentNode::getInsertableItems() const
{
    std::vector<std::shared_ptr<SchemaItem>> insertableItems;
    std::map<std::string, unsigned> childCounts;

    //
    // Iterate over the children and for each, create a childCount entry based on the
    // child node's configuration type
    for (auto childIt = m_children.begin(); childIt != m_children.end(); ++childIt)
    {
        std::string itemType = childIt->second->getSchemaItem()->getItemType();
        auto findIt = childCounts.find(itemType);
        if (findIt != childCounts.end())
        {
            ++findIt->second;
        }
        else
        {
            childCounts.insert({ itemType, 1 });
        }
    }

    //
    // Now get the full list of configurable items, then resolve it against the child counts from
    // above to build a vector of insertable items
    std::vector<std::shared_ptr<SchemaItem>> configChildren = m_pSchemaItem->getChildren();
    for (auto cfgIt = configChildren.begin(); cfgIt != configChildren.end(); ++cfgIt)
    {
        auto findIt = childCounts.find((*cfgIt)->getItemType());
        if (findIt != childCounts.end())
        {
            if (findIt->second < (*cfgIt)->getMaxInstances())
            {
                insertableItems.push_back(*cfgIt);
            }
        }
        else
        {
            insertableItems.push_back(*cfgIt);
        }
    }
    return insertableItems;
}


//
// Called to initialize a newly added node to the environment (not just read from the environment)
void EnvironmentNode::initialize()
{
    //
    // Add any attributes that are requried
    addMissingAttributesFromConfig();

    //
    // If we are a comonent and there is a buildSet attribute, set the value to the configItem's type
    if (m_pSchemaItem->getProperty("componentName") != "" && hasAttribute("buildSet"))
    {
        Status status;
        setAttributeValue("buildSet", m_pSchemaItem->getProperty("category"), status);
    }


    //
    // Initilize each attribute
    for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
    {
        attrIt->second->initialize();
    }
}