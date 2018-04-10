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

#include "SchemaItem.hpp"
#include "Exceptions.hpp"
#include "SchemaTypeStringLimits.hpp"
#include "SchemaTypeIntegerLimits.hpp"
#include <algorithm>


// static class variables
//std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> SchemaItem::m_uniqueAttributeValueSets;

SchemaItem::SchemaItem(const std::string &name, const std::string &className, const std::shared_ptr<SchemaItem> &pParent) :
    m_pParent(pParent),
    m_hidden(false),
    m_minInstances(1),
    m_maxInstances(1)
{
    //
    // Set property defaults
    m_properties["name"] = name;
    m_properties["displayName"] = name;
    m_properties["className"] = className;

    //
    // If this is a root node (no parent), then do some additional init
    if (m_pParent.expired())
    {
        //
        // Create a default type so that all values have a type
        std::shared_ptr<SchemaType> pDefaultType = std::make_shared<SchemaType>("default");
        std::shared_ptr<SchemaTypeLimits> pDefaultLimits = std::make_shared<SchemaTypeLimits>();
        pDefaultType->setLimits(pDefaultLimits);
        addSchemaValueType(pDefaultType);
    }
}


void SchemaItem::addSchemaValueType(const std::shared_ptr<SchemaType> &pType)
{
    m_types[pType->getName()] = pType;
}


std::shared_ptr<SchemaType> SchemaItem::getSchemaValueType(const std::string &typeName, bool throwIfNotPresent) const
{
    std::shared_ptr<SchemaType> pType;
    auto it = m_types.find(typeName);
    if (it != m_types.end())
    {
        return it->second;
    }
    else
    {
        std::shared_ptr<SchemaItem> pParent = m_pParent.lock();
        if (pParent)
        {
            return pParent->getSchemaValueType(typeName, throwIfNotPresent);
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


bool SchemaItem::addUniqueName(const std::string keyName)
{
    auto result = m_keys.insert(keyName);
    return result.second;  // true if keyName did not already exist
}


void SchemaItem::addSchemaType(const std::shared_ptr<SchemaItem> &pItem, const std::string &typeName)
{
    auto it = m_schemaTypes.find(typeName);
    if (it == m_schemaTypes.end())
    {
        m_schemaTypes[typeName] = pItem;
    }
    else
    {
        throw(ParseException("Duplicate config type found: " + m_properties["name"]));
    }
}


std::shared_ptr<SchemaItem> SchemaItem::getSchemaType(const std::string &name, bool throwIfNotPresent) const
{
    std::shared_ptr<SchemaItem> pItem;
    auto it = m_schemaTypes.find(name);
    if (it != m_schemaTypes.end())
    {
        return it->second;
    }
    else
    {
        std::shared_ptr<SchemaItem> pParent = m_pParent.lock();
        if (pParent)
        {
            return pParent->getSchemaType(name, throwIfNotPresent);
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
// This method is used to insert a named type into the current schema item. This is done by making copies
// of the relevant members and inserting them into this instance
void SchemaItem::insertSchemaType(const std::shared_ptr<SchemaItem> pTypeItem)
{
    //
    // To insert a schema type (for example a previously defined complexType name="" XSD definition)
    // loop through each set of configurable pieces of the input type, make a copy of each, and add it to
    // this element.

    //
    // Children
    std::vector<std::shared_ptr<SchemaItem>> typeChildren;
    pTypeItem->getChildren(typeChildren);
    for (auto childIt = typeChildren.begin(); childIt != typeChildren.end(); ++childIt)
    {
        std::shared_ptr<SchemaItem> pNewItem = std::make_shared<SchemaItem>(*(*childIt));
        addChild(pNewItem);
    }

    //
    // Attributes
    std::vector< std::shared_ptr<SchemaValue>> typeAttributes;
    pTypeItem->getAttributes(typeAttributes);
    for (auto attrIt = typeAttributes.begin(); attrIt != typeAttributes.end(); ++attrIt)
    {
        std::shared_ptr<SchemaValue> pNewAttr = std::make_shared<SchemaValue>(*(*attrIt));
        addAttribute(pNewAttr);
    }

    //
    // Type main value
    if (pTypeItem->getItemSchemaValue() != nullptr)
    {
        std::shared_ptr<SchemaValue> pNewItemCfgValue = std::make_shared<SchemaValue>(*(pTypeItem->getItemSchemaValue()));
        setItemSchemaValue(pNewItemCfgValue);
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


void SchemaItem::addAttribute(const std::shared_ptr<SchemaValue> &pCfgValue)
{
    auto retVal = m_attributes.insert({ pCfgValue->getName(), pCfgValue });
    if (!retVal.second)
    {
        throw(ParseException("Duplicate attribute (" + pCfgValue->getName() + ") found for element " + m_properties["name"]));
    }
}


void SchemaItem::addAttribute(const std::map<std::string, std::shared_ptr<SchemaValue>> &attributes)
{
    for (auto it = attributes.begin(); it != attributes.end(); ++it)
        addAttribute(it->second);
}


void SchemaItem::addAttribute(const std::vector<std::shared_ptr<SchemaValue>> &attributes)
{
    for (auto it=attributes.begin(); it!=attributes.end(); ++it)
        addAttribute(*it);
}


void SchemaItem::getAttributes(std::vector<std::shared_ptr<SchemaValue>> &attributes) const
{
    for (auto it = m_attributes.begin(); it != m_attributes.end(); ++it)
        attributes.push_back(it->second);
}


std::shared_ptr<SchemaValue> SchemaItem::getAttribute(const std::string &name, bool createIfDoesNotExist) const
{
    std::shared_ptr<SchemaValue> pCfgValue;
    auto it = m_attributes.find(name);
    if (it != m_attributes.end())
    {
        pCfgValue = it->second;
    }
    else if (createIfDoesNotExist)
    {
        // not found, build a default cfg value for the undefined attribute
        pCfgValue = std::make_shared<SchemaValue>(name, false);
        pCfgValue->setType(getSchemaValueType("default"));
    }
    return pCfgValue;
}


void SchemaItem::addUniqueAttributeValueSetDefinition(const std::string &setName, const std::string &elementPath, const std::string &attributeName, bool duplicateOk)
{
    m_uniqueAttributeValueSetDefs.insert({ setName, SetInfo(setName, elementPath, attributeName, duplicateOk) });   // these are processed later
}


void SchemaItem::addReferenceToUniqueAttributeValueSet(const std::string &setName, const std::string &elementPath, const std::string &attributeName)
{
    m_uniqueAttributeValueSetReferences.insert({ setName, SetInfo(setName, elementPath, attributeName) });   // these are processed later
}


void SchemaItem::processUniqueAttributeValueSetReferences(const std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> &uniqueAttributeValueSets)
{
    for (auto setRefIt = m_uniqueAttributeValueSetReferences.begin(); setRefIt != m_uniqueAttributeValueSetReferences.end(); ++setRefIt)
    {
        auto keyIt = uniqueAttributeValueSets.find(setRefIt->second.m_setName);
        if (keyIt != uniqueAttributeValueSets.end())
        {
            for (auto cfgIt = keyIt->second.begin(); cfgIt != keyIt->second.end(); ++cfgIt)
            {
                std::shared_ptr<SchemaValue> pKeyRefAttribute = *cfgIt;     // this is the reference attribute from which attributeName must be a member
                std::string cfgValuePath = ((setRefIt->second.m_elementPath != ".") ? setRefIt->second.m_elementPath : "") + "@" + setRefIt->second.m_attributeName;
                std::vector<std::shared_ptr<SchemaValue>> cfgValues;
                findSchemaValues(cfgValuePath, cfgValues);
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


void SchemaItem::getChildren(std::vector<std::shared_ptr<SchemaItem>> &children)
{
    for (auto it = m_children.begin(); it != m_children.end(); ++it)
    {
        children.push_back(it->second);
    }
}


std::shared_ptr<SchemaItem> SchemaItem::getChild(const std::string &name)
{
    std::shared_ptr<SchemaItem> pItem = std::make_shared<SchemaItem>(name, "default", shared_from_this());
    auto it = m_children.find(name);  // only return the first one
    if (it != m_children.end())
    {
        pItem = it->second;
    }
    return pItem;
}


std::shared_ptr<SchemaItem> SchemaItem::getChildByComponent(const std::string &name, std::string &componentName)
{
    std::shared_ptr<SchemaItem> pItem = std::make_shared<SchemaItem>(name, "default", shared_from_this());
    auto childItRange = m_children.equal_range(name);
    for (auto childIt = childItRange.first; childIt != childItRange.second; ++childIt)
    {
        if (childIt->second->getProperty("componentName") == componentName)
        {
            pItem = childIt->second;
            break;
        }
    }

    return pItem;
}


void SchemaItem::resetEnvironment()
{
    for (auto it = m_attributes.begin(); it != m_attributes.end(); ++it)
    {
        it->second->resetEnvironment();
    }
}



void SchemaItem::findSchemaValues(const std::string &path, std::vector<std::shared_ptr<SchemaValue>> &schemaValues)
{
    bool rootPath = path[0] == '/';

    //
    // If path is from the root, and we aren't the root, pass the request to our parent
    if (rootPath && !m_pParent.expired())
    {
        std::shared_ptr<SchemaItem> pParent = m_pParent.lock();
        if (pParent)
        {
            return pParent->findSchemaValues(path, schemaValues);
        }
    }

    //
    // Break the path down and process it
    size_t start = rootPath ? 1 : 0;    // skip leading slash if we are at the root
    size_t end = path.find_first_of("/@", start);
    if (end != std::string::npos)
    {
        std::string elem = path.substr(start, end - start);

        if (rootPath)
        {
            if (m_properties["name"] == elem)
            {
                return findSchemaValues(path.substr(end + 1), schemaValues);
            }
            else
            {
                throw(ParseException("Unable to find root element '" + elem + "' when searching path '" + path + "'"));
            }
        }

        if (path[0] == '@')
        {
            std::string attrName = path.substr(1);
            auto rangeIt = m_attributes.equal_range(attrName);
            for (auto it = rangeIt.first; it != rangeIt.second; ++it)
            {
                schemaValues.push_back(it->second);
            }
        }

        else
        {
            auto rangeIt = m_children.equal_range(elem);
            for (auto it = rangeIt.first; it != rangeIt.second; ++it)
            {
                it->second->findSchemaValues(path.substr(end + ((path[end] == '/') ? 1 : 0)), schemaValues);
            }
        }
    }

    return;
}


void SchemaItem::processDefinedUniqueAttributeValueSets(std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> &uniqueAttributeValueSets)
{
    for (auto setIt = m_uniqueAttributeValueSetDefs.begin(); setIt != m_uniqueAttributeValueSetDefs.end(); ++setIt)
    {
        auto it = uniqueAttributeValueSets.find(setIt->first);
        bool keyDefExists = it != uniqueAttributeValueSets.end();
        if (!keyDefExists || setIt->second.m_duplicateOk)
        {
            std::string cfgValuePath = ((setIt->second.m_elementPath != ".") ? setIt->second.m_elementPath : "") + "@" + setIt->second.m_attributeName;
            std::vector<std::shared_ptr<SchemaValue>> cfgValues;
            findSchemaValues(cfgValuePath, cfgValues);
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
                        std::vector<std::shared_ptr<SchemaValue>> values;
                        values.push_back(*attrIt);
                        it = uniqueAttributeValueSets.insert({ setIt->second.m_setName, values }).first;  // so the else condition will work
                        keyDefExists = true;  // Now, it does exist
                    }
                    else
                    {
                        std::vector<std::shared_ptr<SchemaValue>> &values = it->second;
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
            throw(ParseException("Duplicate key (" + setIt->second.m_setName + ") found for element " + m_properties["name"]));
        }
    }

    //
    // Post process all of our children now
    for (auto it = m_children.begin(); it != m_children.end(); ++it)
    {
        it->second->processDefinedUniqueAttributeValueSets(uniqueAttributeValueSets);
    }
}



void SchemaItem::postProcessConfig(const std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> &uniqueAttributeValueSets)
{
    //
    // Make sure that the item type value for all children that are insertable (minRequired = 0 or maxAllowed > minRequired)
    std::set<std::string> itemTypes;
    for (auto it = m_children.begin(); it != m_children.end(); ++it)
    {
        if (it->second->isInsertable())
        {
            auto rc = itemTypes.insert(it->second->getItemType());
            if (!rc.second)
            {
                throw(ParseException("Duplicate itemType(" + it->second->getItemType() + ") found for element " + m_properties["name"]));
            }
        }
    }

    processUniqueAttributeValueSetReferences(uniqueAttributeValueSets);

    //
    // Post process the attributes
    for (auto it = m_attributes.begin(); it != m_attributes.end(); ++it)
    {
        //
        // If this is a mirroed value, go find the source and attach ourselves so that if that value changes,
        // it is replicated to us.
        if (it->second->isMirroredValue())
        {
            std::vector<std::shared_ptr<SchemaValue>> cfgValues;
            findSchemaValues(it->second->getMirrorFromPath(), cfgValues);
            if (!cfgValues.empty() && cfgValues.size() == 1)
            {
                if (cfgValues.size() == 1)
                {
                    it->second->addMirroredSchemaValue(cfgValues[0]);
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
        it->second->postProcessConfig(uniqueAttributeValueSets);
    }
}


std::string SchemaItem::getItemType() const
{
    //
    // Return itemType based on this set of rules
    if (!getProperty("itemType").empty())
        return getProperty("itemType");
    else if (!getProperty("componentName").empty())
        return getProperty("componentName");

    return getProperty("name");
}


std::string SchemaItem::getProperty(const std::string &name, const std::string &dflt) const
{
    std::string retVal = dflt;
    auto it = m_properties.find(name);
    if (it != m_properties.end())
    {
        retVal = it->second;
    }
    return retVal;
}
