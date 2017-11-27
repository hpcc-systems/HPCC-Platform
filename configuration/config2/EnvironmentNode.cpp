/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems�.

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


void EnvironmentNode::addAttribute(const std::string &name, std::shared_ptr<EnvValue> pValue)
{
    auto retValue = m_attributes.insert(std::make_pair(name, pValue));
    // todo: add check to make sure no duplicate attributes, use retValue to see if value was inserted or not
}


std::vector<std::shared_ptr<EnvironmentNode>> EnvironmentNode::getChildren(const std::string &name) const
{
    std::vector<std::shared_ptr<EnvironmentNode>> childNodes;
    if (name == "") {
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


std::shared_ptr<EnvironmentNode> EnvironmentNode::getParent() const
{
	std::shared_ptr<EnvironmentNode> pParent;
	if (!m_pParent.expired())
	{
		pParent = m_pParent.lock();
	}
	return pParent;
}


std::vector<std::shared_ptr<EnvValue>> EnvironmentNode::getAttributes() const
{
    std::vector<std::shared_ptr<EnvValue>> attributes;

    for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
    {
        attributes.push_back(attrIt->second);
    }
    return attributes;
}


void EnvironmentNode::setAttributeValues(const std::vector<ValueDef> &values, Status &status, bool allowInvalid, bool forceCreate)
{
    for (auto it = values.begin(); it != values.end(); ++it)
    {
        setAttributeValue((*it).name, (*it).value, status, allowInvalid, forceCreate);
    }
}


// should probably return a status object, and put path/valueName in there
void EnvironmentNode::setAttributeValue(const std::string &attrName, const std::string &value, Status &status, bool allowInvalid, bool forceCreate)
{
    std::shared_ptr<EnvValue> pEnvValue;

    auto it = m_attributes.find(attrName);
    if (it != m_attributes.end())
    {
        pEnvValue = it->second;
    }

    //
    // Not found on this node. See if the configuration defines the attribute. If so, set the value and move on.
    // If not and the forceCreate flag is set, create it. 
    else
    {
        std::shared_ptr<CfgValue> pCfgValue = m_pConfigItem->getAttribute(attrName);
        pEnvValue = std::make_shared<EnvValue>(shared_from_this(), pCfgValue, attrName);
        addAttribute(attrName, pEnvValue);
        if (!pCfgValue->isConfigured())
        {
            status.addStatusMsg(statusMsg::warning, getId(), attrName, "", "Undefined attribute did not exist in configuration, was created");
        }
    }


    //
    // If we have a value, set it to the new value. If that passes, see if there is any post processing to do. Note that
    // a forced create value can never have an invalid value.
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
    std::shared_ptr<EnvValue> pAttribute = getAttribute(name);
    if (pAttribute)
        value = pAttribute->getValue();
    return value;
}


bool EnvironmentNode::setValue(const std::string &value, Status &status, bool force)
{
    bool rc = false;

    if (m_pNodeValue)
    {
        rc = m_pNodeValue->setValue(value, &status, force);
    }
    else
    {
        std::shared_ptr<CfgValue> pCfgValue = m_pConfigItem->getItemCfgValue();
        
        m_pNodeValue = std::make_shared<EnvValue>(shared_from_this(), pCfgValue, "");  // node's value has no name
        rc = m_pNodeValue->setValue(value, &status, force);
    }
    return rc;
}


void EnvironmentNode::validate(Status &status, bool includeChildren) const
{
    //
    // Check node value
    if (m_pNodeValue)
    {
        if (!m_pNodeValue->checkCurrentValue())
        {
            m_pNodeValue->validate(status, "");
            //status.addStatusMsg(statusMsg::warning, getId(), "", "", "The node value is not valid");
        }
    }

    //
    // Check any attributes
    for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
    {
        if (!attrIt->second->checkCurrentValue())
        {
            attrIt->second->validate(status, m_id);
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


std::vector<std::string> EnvironmentNode::getAllFieldValues(const std::string &fieldName) const
{
    std::vector<std::string> values;
    std::shared_ptr<EnvironmentNode> pParentNode = m_pParent.lock();
    if (pParentNode)
    {
        std::vector<std::shared_ptr<EnvironmentNode>> nodes = pParentNode->getChildren(m_name);
        for (auto it = nodes.begin(); it != nodes.end(); ++it)
        {
            values.push_back((*it)->getAttributeValue(fieldName));
        }
    }
    return values;
}


const std::shared_ptr<EnvValue> EnvironmentNode::getAttribute(const std::string &name) const
{
    std::shared_ptr<EnvValue> pValue;
    auto it = m_attributes.find(name);
    if (it != m_attributes.end())
    {
        pValue = it->second;
    }
    return pValue;
}