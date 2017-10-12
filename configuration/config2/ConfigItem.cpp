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

#include "ConfigItem.hpp"
#include "ConfigExceptions.hpp"
#include "CfgStringLimits.hpp"
#include "CfgIntegerLimits.hpp"
#include <algorithm>


ConfigItem::ConfigItem(const std::string &name, const std::string &className, const std::shared_ptr<ConfigItem> &pParent) :
	m_className(className), 
	m_name(name), 
	m_pParent(pParent), 
	m_displayName(name),
	m_minInstances(1), 
	m_maxInstances(1), 
	m_isConfigurable(false), 
	m_version(-1) 
{
	//
	// If this is a root node (no parent), then do some additional init so that default nodes
	// throughout the configuration can present a set of objects, even for non configured elements
	if (m_pParent.expired())
	{
		std::shared_ptr<CfgType> pDefaultType = std::make_shared<CfgType>("none");
		std::shared_ptr<CfgLimits> pDefaultLimits = std::make_shared<CfgLimits>();
		pDefaultType->setLimits(pDefaultLimits);
		addType(pDefaultType);
	}
}


void ConfigItem::addType(const std::shared_ptr<CfgType> &pType)
{
    m_types[pType->getName()] = pType;
}


const std::shared_ptr<CfgType> &ConfigItem::getType(const std::string &typeName) const
{
    auto it = m_types.find(typeName);
    if (it != m_types.end())
    {
        return it->second;
    }
    else
    {
        std::shared_ptr<ConfigItem> pParent = m_pParent.lock();
        if (pParent)
        {
            return pParent->getType(typeName);
        }
    }

    //
    // Did not find the type
    std::string msg = "Unable to find type: " + typeName;
    throw(new ParseException(msg));
}


bool ConfigItem::addUniqueName(const std::string keyName)
{
    auto result = m_keys.insert(keyName);
    return result.second;
}


void ConfigItem::addConfigType(const std::shared_ptr<ConfigItem> &pItem, const std::string &typeName)
{
    auto it = m_configTypes.find(typeName);
    if (it == m_configTypes.end())
    {
        m_configTypes[typeName] = pItem;
    }
    else
    {
        throw(new ParseException("Duplicate config type found: " + pItem->getName()));
    }
}


std::shared_ptr<ConfigItem> ConfigItem::getConfigType(const std::string &name, bool throwIfNotPresent) const
{
    std::shared_ptr<ConfigItem> pItem;
    auto it = m_configTypes.find(name);
    if (it != m_configTypes.end())
    {
        return it->second;
    }
    else
    {
        std::shared_ptr<ConfigItem> pParent = m_pParent.lock();
        if (pParent)
        {
            return pParent->getConfigType(name);
        }
    }

    //
    // Did not find the type
    if (throwIfNotPresent)
    {
        std::string msg = "Unable to find config type: " + name;
        throw(new ParseException(msg));
    }
    return pItem;
}


void ConfigItem::addAttribute(const std::shared_ptr<CfgValue> &pCfgValue)
{
	auto retVal = m_attributes.insert({ pCfgValue->getName(), pCfgValue });
	if (!retVal.second)
	{
		throw(new ParseException("Duplicate attribute (" + pCfgValue->getName() + ") found for element " + m_name));
	}
}


void ConfigItem::addAttribute(const std::vector<std::shared_ptr<CfgValue>> &attributes)
{
	for (auto it = attributes.begin(); it != attributes.end(); ++it)
		addAttribute((*it));
}


std::shared_ptr<CfgValue> ConfigItem::getAttribute(const std::string &name) const
{
	std::shared_ptr<CfgValue> pCfgValue;  
	auto it = m_attributes.find(name);
	if (it != m_attributes.end())
		pCfgValue = it->second;
	else
	{
		pCfgValue = std::make_shared<CfgValue>(name);
		pCfgValue->setType(getType("none"));
	}
	return pCfgValue;
}


void ConfigItem::addKey(const std::string &keyName, const std::string &elementName, const std::string &attributeName)
{
    std::shared_ptr<ConfigItem> pCfgItem = getChild(elementName);  // todo: validate pCfgItem found
    std::shared_ptr<CfgValue> pAttribute = pCfgItem->getAttribute(attributeName);  
    if (pAttribute)
    {
        pAttribute->setKey(true);
        auto result = m_keyDefs.insert({ keyName, pAttribute });
        if (!result.second)
        {
            throw(new ParseException("Duplicate key (" + keyName + ") found for element " + m_name));
        }
    }
}


void ConfigItem::addKeyRef(const std::string &keyName, const std::string &elementName, const std::string &attributeName)
{
    auto keyIt = m_keyDefs.find(keyName);
    if (keyIt != m_keyDefs.end())
    {
        std::shared_ptr<CfgValue> pKeyRefAttribute = keyIt->second;
        std::shared_ptr<ConfigItem> pCfgItem = getChild(elementName);   // todo: validate pCfgItem
        std::shared_ptr<CfgValue> pAttribute = pCfgItem->getAttribute(attributeName); 
        if (pAttribute)
        {
            pAttribute->setKeyRef(pKeyRefAttribute);
        }
    }
}


std::vector<std::shared_ptr<ConfigItem>> ConfigItem::getChildren() const
{
    std::vector<std::shared_ptr<ConfigItem>> children;
    
    for (auto it = m_children.begin(); it != m_children.end(); ++it)
        children.push_back(it->second);

    return children;
}


std::shared_ptr<ConfigItem> ConfigItem::getChild(const std::string &name)
{
	std::shared_ptr<ConfigItem> pItem;
	auto it = m_children.find(name);
	if (it != m_children.end())
	{
		pItem = it->second;
	}
	else
	{
		pItem = std::make_shared<ConfigItem>(name, "undefined", shared_from_this());
	}
	return pItem;
}


void ConfigItem::resetEnvironment()
{
    for (auto it = m_attributes.begin(); it != m_attributes.end(); ++it)
    {
        it->second->resetEnvironment();
    }
}


std::shared_ptr<CfgValue> ConfigItem::findCfgValue(const std::string &path)
{
    bool rootPath = path[0] == '/';
    //
    // If path is from the root, move on up
    if (rootPath && !m_pParent.expired())
    {   
        std::shared_ptr<ConfigItem> pParent = m_pParent.lock();
        if (pParent)
        {
            return pParent->findCfgValue(path);
        }
    }

    size_t start = rootPath ? 1 : 0;
    size_t end = path.find_first_of("/@", start);
    if (end != std::string::npos)
    {
        std::string elem = path.substr(start, end - start);

        if (rootPath)
        {
            if (m_name == elem)
            {
                return findCfgValue(path.substr(end + 1));
            }
            else
            {
                // todo: throw? the root is not correct
            }
        }

        if (path[0] == '@')
        {
            std::string attrName = path.substr(1);
            auto attrIt = m_attributes.find(attrName);
            if (attrIt != m_attributes.end())
            {
                return attrIt->second;
            }
            else
            {
                // todo: throw
            }
        }

        else
        {
            auto cfgChild = m_children.find(elem);
            if (cfgChild != m_children.end())
            {
                return cfgChild->second->findCfgValue(path.substr(end + ((path[end]=='/') ? 1 : 0)));
            }
            else
            {
                // todo: throw
            }
        }
        //else if (path[end] == '@')
        //{
        //    return cfgChild->second->findCfgValue(path.substr(end1));
        //    std::string attrName = path.substr(end);
        //    auto attrIt = m_attributes.find(attrName);
        //    if (attrIt != m_attributes.end())
        //    {
        //        return attrIt->second;
        //    }
        //    else
        //    {
        //        // todo: throw
        //    }
        //}
    }

    return std::shared_ptr<CfgValue>(); 
}


void ConfigItem::postProcessConfig()
{
    for (auto it = m_attributes.begin(); it != m_attributes.end(); ++it)
    {
        if (it->second->isMirroredValue())
        {
            std::shared_ptr<CfgValue> pSrcCfgValue = findCfgValue(it->second->getMirrorFromPath());
            if (pSrcCfgValue)
            {
                pSrcCfgValue->addMirroredCfgValue(it->second);
            }
        }
    }

    for (auto it = m_children.begin(); it!= m_children.end(); ++it)
    {
        it->second->postProcessConfig();
    }
}