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


std::vector<std::shared_ptr<EnvValue>> EnvironmentNode::getAttributes() const
{
	std::vector<std::shared_ptr<EnvValue>> attributes;

	for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
	{
		attributes.push_back(attrIt->second);
	}
	return attributes;
}


// should probably return a status object, and put path/valueName in there
bool EnvironmentNode::setAttributeValue(const std::string &name, const std::string &value, bool allowInvalid, bool forceCreate)
{
	bool rc = false;
	std::shared_ptr<EnvValue> pEnvValue;
	auto it = m_attributes.find(name);
	if (it != m_attributes.end())
	{
		pEnvValue = it->second;
	}

	//
	// This is a non-defined attribute. If force is true, create a new non-config backed attribute
	else if (forceCreate)
	{
		std::shared_ptr<CfgValue> pCfgValue = std::make_shared<CfgValue>(name, false);
		std::shared_ptr<EnvValue> pEnvValue = std::make_shared<EnvValue>(shared_from_this(), pCfgValue, name);
		//addStatus(NodeStatus::warning, "Attribute " + name + " not defined in configuration schema, unable to validate value.");
		addAttribute(name, pEnvValue);
		rc = false;
	}
	else
	{
		// todo: a message here that value does not exist and force was not set?
		rc = false;
	}

	//
	// If we have a value, set it to the new value. If that passes, see if there is any post processint to do
	if (pEnvValue)
	{
		rc = pEnvValue->setValue(value); 
		if (rc)
		{

		}
	}
	return rc;
}


std::string EnvironmentNode::getAttributeValue(const std::string &name) const
{
	std::string value;
	std::shared_ptr<EnvValue> pAttribute = getAttribute(name);
	if (pAttribute)
		value = pAttribute->getValue();
	return value;
}


bool EnvironmentNode::setValue(const std::string &value, bool force)
{
	bool rc = false;

	if (m_pNodeValue)
	{
		rc = m_pNodeValue->setValue(value, force);
	}
	else
	{
		std::shared_ptr<CfgValue> pCfgValue = m_pConfigItem->getItemCfgValue();
		
		m_pNodeValue = std::make_shared<EnvValue>(shared_from_this(), pCfgValue, "");  // node's value has no name
		rc = m_pNodeValue->setValue(value, force);
	}
	return rc;
}


bool EnvironmentNode::validate()
{
	//
	// Check node value
	if (m_pNodeValue)
	{
		m_pNodeValue->checkCurrentValue();
	}

	//
	// Check any attributes
	for (auto attrIt = m_attributes.begin(); attrIt != m_attributes.end(); ++attrIt)
	{
		attrIt->second->checkCurrentValue();
	}

	//
	// Now check all children
	for (auto childIt = m_children.begin(); childIt != m_children.end(); ++childIt)
	{
		childIt->second->validate();
	}

	return true;
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