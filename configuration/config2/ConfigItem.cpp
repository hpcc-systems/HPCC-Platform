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

#include "ConfigItem.hpp"
#include "ConfigExceptions.hpp"
#include "CfgStringLimits.hpp"
#include "CfgIntegerLimits.hpp"
#include <algorithm>


// static class variables
std::map<std::string, std::vector<std::shared_ptr<CfgValue>>> ConfigItem::m_uniqueAttributeValueSets;
//std::map<std::string, ConfigItem::KeyRef> ConfigItem::m_keyRefs;


ConfigItem::ConfigItem(const std::string &name, const std::string &className, const std::shared_ptr<ConfigItem> &pParent) :
	m_className(className), 
	m_name(name), 
	m_pParent(pParent), 
	m_displayName(name),
	m_minInstances(1), 
	m_maxInstances(1), 
	m_version(-1) 
{
	//
	// If this is a root node (no parent), then do some additional init 
	if (m_pParent.expired())
	{
        //
        // Create a default type so that all values have a type
		std::shared_ptr<CfgType> pDefaultType = std::make_shared<CfgType>("default");
		std::shared_ptr<CfgLimits> pDefaultLimits = std::make_shared<CfgLimits>();
		pDefaultType->setLimits(pDefaultLimits);
		addType(pDefaultType);
	}
}


void ConfigItem::addType(const std::shared_ptr<CfgType> &pType)
{
    m_types[pType->getName()] = pType;
}


std::shared_ptr<CfgType> ConfigItem::getType(const std::string &typeName, bool throwIfNotPresent) const
{
    std::shared_ptr<CfgType> pType;
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
            return pParent->getType(typeName, throwIfNotPresent);
        }
    }

    //
    // Did not find the type
    if (throwIfNotPresent)
    {
        std::string msg = "Unable to find type: " + typeName;
        throw(ParseException(msg));
    }
    return pType;
}


bool ConfigItem::addUniqueName(const std::string keyName)
{
    auto result = m_keys.insert(keyName);
    return result.second;  // true if keyName did not already exist
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
        throw(ParseException("Duplicate config type found: " + pItem->getName()));
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
            return pParent->getConfigType(name, throwIfNotPresent);
        }
    }

    //
    // Did not find the type
    if (throwIfNotPresent)
    {
        std::string msg = "Unable to find config type: " + name;
        throw(ParseException(msg));
    }
    return pItem;
}


//
// Inserts a previously added configType to the config item
void ConfigItem::insertConfigType(const std::shared_ptr<ConfigItem> pTypeItem)
{
    //
    // To insert a config type (most likely previously defined by a complexType name="" XSD definition)
    // loop through each set of configurable pieces of the input type, make a copy of each, and add it to 
    // this element.

    //
    // Children
    std::multimap<std::string, std::shared_ptr<ConfigItem>> typeChildren = pTypeItem->getChildren();
    for (auto childIt = typeChildren.begin(); childIt != typeChildren.end(); ++childIt)
    {
        std::shared_ptr<ConfigItem> pNewItem = std::make_shared<ConfigItem>(*(childIt->second));
        addChild(pNewItem);
    }

    //
    // Attributes
    const std::map<std::string, std::shared_ptr<CfgValue>> &typeAttributes = pTypeItem->getAttributes();
    for (auto attrIt = typeAttributes.begin(); attrIt != typeAttributes.end(); ++attrIt)
    {
        std::shared_ptr<CfgValue> pNewAttr = std::make_shared<CfgValue>(*(attrIt->second));
        addAttribute(pNewAttr);
    }

    //
    // Type main value
    if (pTypeItem->getItemCfgValue() != nullptr)
    {
        std::shared_ptr<CfgValue> pNewItemCfgValue = std::make_shared<CfgValue>(*(pTypeItem->getItemCfgValue()));
        setItemCfgValue(pNewItemCfgValue);
    }

    //
    // Unique attribute sets
    for (auto setIt = pTypeItem->m_uniqueAttributeValueSetDefs.begin(); setIt != pTypeItem->m_uniqueAttributeValueSetDefs.end(); ++setIt)
    {
        m_uniqueAttributeValueSetDefs.insert({ setIt->first, setIt->second });
    }

    //
    // Unique attribute reference sets
    for (auto it = pTypeItem->m_uniqueAttributeValueSetReferences.begin(); it != pTypeItem->m_uniqueAttributeValueSetReferences.end(); ++it)
    {
        m_uniqueAttributeValueSetReferences.insert({ it->first, it->second });
    }
}


void ConfigItem::addAttribute(const std::shared_ptr<CfgValue> &pCfgValue)
{
	auto retVal = m_attributes.insert({ pCfgValue->getName(), pCfgValue });
	if (!retVal.second)
	{
		throw(ParseException("Duplicate attribute (" + pCfgValue->getName() + ") found for element " + m_name));
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
        // not found, build a default cfg value for the undefined attribute
		pCfgValue = std::make_shared<CfgValue>(name, false); 
		pCfgValue->setType(getType("default"));
	}
	return pCfgValue;
}


void ConfigItem::addUniqueAttributeValueSetDefinition(const std::string &setName, const std::string &elementPath, const std::string &attributeName, bool duplicateOk)
{
    m_uniqueAttributeValueSetDefs.insert({ setName, SetInfo(setName, elementPath, attributeName, duplicateOk) });   // these are processed later
}


void ConfigItem::addReferenceToUniqueAttributeValueSet(const std::string &setName, const std::string &elementPath, const std::string &attributeName)
{
    m_uniqueAttributeValueSetReferences.insert({ setName, SetInfo(setName, elementPath, attributeName) });   // these are processed later
}



void ConfigItem::processUniqueAttributeValueSetReferences()
{
    for (auto setRefIt = m_uniqueAttributeValueSetReferences.begin(); setRefIt != m_uniqueAttributeValueSetReferences.end(); ++setRefIt)
    {  
        auto keyIt = m_uniqueAttributeValueSets.find(setRefIt->second.m_setName);
        if (keyIt != m_uniqueAttributeValueSets.end())
        {
            for (auto cfgIt = keyIt->second.begin(); cfgIt != keyIt->second.end(); ++cfgIt)
            {
                std::shared_ptr<CfgValue> pKeyRefAttribute = *cfgIt;     // this is the reference attribute from which attributeName must be a member
                std::string cfgValuePath = ((setRefIt->second.m_elementPath != ".") ? setRefIt->second.m_elementPath : "") + "@" + setRefIt->second.m_attributeName;
                std::vector<std::shared_ptr<CfgValue>> cfgValues;
                findCfgValues(cfgValuePath, cfgValues);
                if (!cfgValues.empty())
                {
                    for (auto attrIt = cfgValues.begin(); attrIt != cfgValues.end(); ++attrIt)
                    {
                        (*attrIt)->setUniqueValueSetRef(pKeyRefAttribute);
                    }
                }
                else
                {
                    throw(ParseException("Attribute " + (setRefIt->second.m_attributeName + " not found when adding keyRef for key " + (setRefIt->second.m_setName))));
                }
            }
        }
        else
        {
            throw(ParseException("Keyref to key '" + (setRefIt->second.m_setName + "' was not found")));
        }
    }
}


//std::vector<std::shared_ptr<ConfigItem>> ConfigItem::getChildren() const
//{
//    std::vector<std::shared_ptr<ConfigItem>> children;
//    for (auto it = m_children.begin(); it != m_children.end(); ++it)
//        children.push_back(it->second);
//    return children;
//}


std::shared_ptr<ConfigItem> ConfigItem::getChild(const std::string &name)
{
	std::shared_ptr<ConfigItem> pItem = std::make_shared<ConfigItem>(name, "default", shared_from_this());
	auto it = m_children.find(name);  // only return the first one
	if (it != m_children.end())
	{
		pItem = it->second;
	}
	return pItem;
}


std::shared_ptr<ConfigItem> ConfigItem::getChildByComponent(const std::string &name, std::string &componentName)
{
    std::shared_ptr<ConfigItem> pItem = std::make_shared<ConfigItem>(name, "default", shared_from_this());  
    auto childItRange = m_children.equal_range(name);
    for (auto childIt = childItRange.first; childIt != childItRange.second; ++childIt)
    {
        if (childIt->second->getComponentName() == componentName)
        {
            pItem = childIt->second;
            break;
        }
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



void ConfigItem::findCfgValues(const std::string &path, std::vector<std::shared_ptr<CfgValue>> &cfgValues)
{
    bool rootPath = path[0] == '/';
    //
    // If path is from the root, move on up
    if (rootPath && !m_pParent.expired())
    {   
        std::shared_ptr<ConfigItem> pParent = m_pParent.lock();
        if (pParent)
        {
            return pParent->findCfgValues(path, cfgValues);
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
                return findCfgValues(path.substr(end + 1), cfgValues);
            }
            else
            {
                // todo: throw? the root is not correct
            }
        }

        if (path[0] == '@')
        {
            std::string attrName = path.substr(1);
            auto rangeIt = m_attributes.equal_range(attrName);
            for (auto it = rangeIt.first; it != rangeIt.second; ++it)
            {
                cfgValues.push_back(it->second);
            }
        }

        else
        {
            auto rangeIt = m_children.equal_range(elem);
            for (auto it = rangeIt.first; it != rangeIt.second; ++it)
            {
                return it->second->findCfgValues(path.substr(end + ((path[end] == '/') ? 1 : 0)), cfgValues);
            }
        }
    }

    return;
}


void ConfigItem::processUniqueAttributeValueSets()
{
    for (auto setIt = m_uniqueAttributeValueSetDefs.begin(); setIt != m_uniqueAttributeValueSetDefs.end(); ++setIt)
    {
        auto it = m_uniqueAttributeValueSets.find(setIt->first);
        bool keyDefExists = it != m_uniqueAttributeValueSets.end();
        if (!keyDefExists || setIt->second.m_duplicateOk)
        {
            //std::shared_ptr<ConfigItem> pCfgItem = getChild(elementName);  // todo: validate pCfgItem found
            std::string cfgValuePath = ((setIt->second.m_elementPath != ".") ? setIt->second.m_elementPath : "") + "@" + setIt->second.m_attributeName;
            std::vector<std::shared_ptr<CfgValue>> cfgValues;
            findCfgValues(cfgValuePath, cfgValues);
            if (!cfgValues.empty())
            {
                //
                // For each attribute, if it does not already exist in the list of attributes making up this
                // key value, add it.
                for (auto attrIt = cfgValues.begin(); attrIt != cfgValues.end(); ++attrIt)
                {
                    (*attrIt)->setUniqueValue(true);

                    if (!keyDefExists)
                    {
                        std::vector<std::shared_ptr<CfgValue>> values;
                        values.push_back(*attrIt);
                        it = m_uniqueAttributeValueSets.insert({ setIt->second.m_setName, values }).first;  // so the else condition will work
                        keyDefExists = true;  // Now, it does exist
                    }
                    else
                    {
                        std::vector<std::shared_ptr<CfgValue>> &values = it->second;
                        bool found = false;
                        for (auto cfgIt = values.begin(); cfgIt != values.end() && !found; ++cfgIt)
                        {
                            found = (*cfgIt) == (*attrIt);
                        }
                        if (!found)
                            values.push_back(*attrIt);
                    }
                }
            }
            else
            {
                throw(ParseException("Attribute " + setIt->second.m_attributeName + " not found for key " + setIt->second.m_setName));
            }
        }
        else
        {
            throw(ParseException("Duplicate key (" + setIt->second.m_setName + ") found for element " + m_name));
        }
    }

    //
    // Post process all of our children now
    for (auto it = m_children.begin(); it != m_children.end(); ++it)
    {
        it->second->processUniqueAttributeValueSets();
    }
}



void ConfigItem::postProcessConfig()
{

    processUniqueAttributeValueSetReferences();


    //
    // Post process the attributes
    for (auto it = m_attributes.begin(); it != m_attributes.end(); ++it)
    {
        //
        // If this is a mirroed value, go find the source and attach ourselves so that if that value changes,
        // it is replicated to us.
        if (it->second->isMirroredValue())
        {
            std::vector<std::shared_ptr<CfgValue>> cfgValues;
            findCfgValues(it->second->getMirrorFromPath(), cfgValues);
            if (!cfgValues.empty() && cfgValues.size() == 1)
            {
                if (cfgValues.size() == 1)
                {
                    it->second->addMirroredCfgValue(cfgValues[0]);
                }
                else
                {
                    throw(ParseException("Multiple sources found for mirror from path for attribute " + it->second->getName() + " (path=" + it->second->getMirrorFromPath()));
                }
            }
            else
            {
                throw(ParseException("Mirrored from source not found for attribute " + it->second->getName() + " path=" + it->second->getMirrorFromPath()));
            }
        }
    }

    //
    // Post process all of our children now
    for (auto it = m_children.begin(); it!= m_children.end(); ++it)
    {
        it->second->postProcessConfig();
    }
}