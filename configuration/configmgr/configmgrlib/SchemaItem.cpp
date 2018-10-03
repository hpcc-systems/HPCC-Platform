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
#include "Utils.hpp"


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
        pDefaultType->setBaseType("string");
        std::shared_ptr<SchemaTypeLimits> pDefaultLimits = std::make_shared<SchemaTypeLimits>();
        pDefaultType->setLimits(pDefaultLimits);
        addSchemaValueType(pDefaultType);
    }
}


SchemaItem::SchemaItem(const SchemaItem &item)
{
    //
    // Copy stuff that doesn't have to be unique
    m_hidden = item.m_hidden;
    m_maxInstances = item.m_maxInstances;
    m_minInstances = item.m_minInstances;
    m_properties = item.m_properties;
    m_types = item.m_types;
    m_schemaTypes = item.m_schemaTypes;

    if (m_pItemValue)
        m_pItemValue = std::make_shared<SchemaValue>(*(item.m_pItemValue));  // copy constructed

    //
    // Make a copy of the children now
    for (auto childIt = item.m_children.begin(); childIt != item.m_children.end(); ++childIt)
    {
        addChild(std::make_shared<SchemaItem>(*(childIt->second)));
    }

    //
    // Copy the attributes
    for (auto attrIt = item.m_attributes.begin(); attrIt != item.m_attributes.end(); ++attrIt)
    {
        addAttribute(std::make_shared<SchemaValue>(*(attrIt->second)));
    }

    //
    // Event handlers
    m_eventHandlers = item.m_eventHandlers;

    m_uniqueAttributeValueSetReferences = item.m_uniqueAttributeValueSetReferences;
    m_uniqueAttributeValueSetDefs = item.m_uniqueAttributeValueSetDefs;
    m_requiredInstanceComponents = item.m_requiredInstanceComponents;
}


void SchemaItem::addSchemaValueType(const std::shared_ptr<SchemaType> &pType)
{
    auto it = m_types.find(pType->getName());
    if (it == m_types.end())
    {
        m_types[pType->getName()] = pType;
    }
    else
    {
        throw(ParseException("Element: " + getProperty("name") + ", duplicate schema value type found: " + pType->getName()));
    }
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
        throw(ParseException("Element: " + getProperty("name") + ", duplicate schema complex type found: " + typeName));
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

    //
    // Children - note that making a copy will also create copies of all descendants
    std::vector<std::shared_ptr<SchemaItem>> typeChildren;
    pTypeItem->getChildren(typeChildren);
    for (auto childIt = typeChildren.begin(); childIt != typeChildren.end(); ++childIt)
    {
        std::shared_ptr<SchemaItem> pNewItem = std::make_shared<SchemaItem>(*(*childIt));
        addChild(pNewItem);
    }
}


void SchemaItem::addAttribute(const std::shared_ptr<SchemaValue> &pCfgValue)
{
    auto retVal = m_attributes.insert({ pCfgValue->getName(), pCfgValue });
    if (!retVal.second)
    {
        throw(ParseException("Element: " + getProperty("name") + ", duplicate attribute (" + pCfgValue->getName() + ") found"));
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


void SchemaItem::addUniqueAttrValueSetDefsAndRefs(const std::shared_ptr<SchemaItem> &pSourceSchemaItem)
{
    for (auto &ref: pSourceSchemaItem->m_uniqueAttributeValueSetReferences)
    {
        addReferenceToUniqueAttributeValueSet(ref.first, ref.second.m_elementPath, ref.second.m_attributeName);
    }

    for (auto &def: pSourceSchemaItem->m_uniqueAttributeValueSetDefs)
    {
        addUniqueAttributeValueSetDefinition(def.first, def.second.m_elementPath, def.second.m_attributeName, true);
    }
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
                std::string cfgValuePath = setRefIt->second.m_elementPath + "[@" + setRefIt->second.m_attributeName + "]";
                std::vector<std::shared_ptr<SchemaValue>> cfgValues;
                fetchSchemaValues(cfgValuePath, cfgValues);
                if (!cfgValues.empty())
                {
                    for (auto attrIt = cfgValues.begin(); attrIt != cfgValues.end(); ++attrIt)
                    {
                        (*attrIt)->setUniqueValueSetRef(pKeyRefAttribute);
                    }
                }
                else
                {
                    throw(ParseException("Element: " + getProperty("name") + ", Attribute '" + (setRefIt->second.m_attributeName + "' not found when adding keyRef for key " + (setRefIt->second.m_setName))));
                }
            }
        }
        else
        {
            throw(ParseException("Element: " + getProperty("name") + ", Keyref to key '" + (setRefIt->second.m_setName + "' was not found")));
        }
    }
}


void SchemaItem::getChildren(std::vector<std::shared_ptr<SchemaItem>> &children, const std::string &name, const std::string &itemType) const
{
    for (auto it = m_children.begin(); it != m_children.end(); ++it)
    {
        if (name.empty() || it->first == name)
        {
            if (itemType.empty() || it->second->getProperty("itemType") == itemType)
            {
                children.emplace_back(it->second);
            }
        }
    }
}


void SchemaItem::resetEnvironment()
{
    for (auto it = m_attributes.begin(); it != m_attributes.end(); ++it)
    {
        it->second->resetEnvironment();
    }
}


void SchemaItem::fetchSchemaValues(const std::string &path, std::vector<std::shared_ptr<SchemaValue>> &schemaValues)
{
    ConfigPath configPath(path);
    doFetchSchemaValues(configPath, schemaValues);
}


void SchemaItem::doFetchSchemaValues(ConfigPath &configPath, std::vector<std::shared_ptr<SchemaValue>> &schemaValues)
{

    std::shared_ptr<ConfigPathItem> pPathItem = configPath.getNextPathItem();

    if (pPathItem)
    {
        if (pPathItem->isRoot())
        {
            getSchemaRoot()->doFetchSchemaValues(configPath, schemaValues);
        }
        else if (pPathItem->isParentPathtItem())
        {
            if (!m_pParent.expired())
            {
                m_pParent.lock()->doFetchSchemaValues(configPath, schemaValues);
            }
        }
        else if (pPathItem->isCurrentPathItem())
        {
            doFetchSchemaValues(configPath, schemaValues);
        }
        else
        {
            //
            // Get the items to check for this path element. If there is no element name, then use
            // this item, otherwise get children matching the element name
            std::vector<std::shared_ptr<SchemaItem>> items;
            if (pPathItem->getElementName().empty())
            {
                items.push_back(std::const_pointer_cast<SchemaItem>(shared_from_this()));
            }
            else
            {
                getChildren(items, pPathItem->getElementName());
            }

            if (!pPathItem->getAttributeName().empty())  // note that attribute values are not supported for schema xpaths
            {
                auto it = items.begin();
                while (it != items.end())
                {
                    std::shared_ptr<SchemaValue> pAttribute = (*it)->getAttribute(pPathItem->getAttributeName());
                    if (!pAttribute)
                    {
                        it = items.erase(it);
                    }
                    else
                    {
                        ++it;
                    }
                }
            }

            //
            // If path elements remain, fetch schema values from each match at this level, otherwise push the results to
            // the result schema value vector
            if (configPath.isPathRemaining())
            {
                //
                // For all the matching nodes at this element, call each to continue the search
                for (auto itemIt = items.begin(); itemIt != items.end(); ++itemIt)
                {
                    (*itemIt)->doFetchSchemaValues(configPath, schemaValues);
                }
            }
            else
            {
                for (auto itemIt = items.begin(); itemIt != items.end(); ++itemIt)
                {
                    schemaValues.push_back((*itemIt)->getAttribute(pPathItem->getAttributeName()));
                }
            }
        }
    }
}


std::shared_ptr<SchemaItem> SchemaItem::getSchemaRoot()
{
    if (!m_pParent.expired())
    {
        return m_pParent.lock()->getSchemaRoot();
    }

    std::shared_ptr<SchemaItem> ptr = shared_from_this();
    return ptr;
}


void SchemaItem::getPath(std::string &path) const
{
    path = getProperty("name") + path;
    if (!m_pParent.expired())
    {
        path = "/" + path;
        m_pParent.lock()->getPath(path);
    }
}


void SchemaItem::processDefinedUniqueAttributeValueSets(std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> &uniqueAttributeValueSets)
{
    for (auto setIt = m_uniqueAttributeValueSetDefs.begin(); setIt != m_uniqueAttributeValueSetDefs.end(); ++setIt)
    {
        auto it = uniqueAttributeValueSets.find(setIt->first);
        bool keyDefExists = it != uniqueAttributeValueSets.end();
        if (!keyDefExists || setIt->second.m_duplicateOk)
        {
            std::string cfgValuePath = setIt->second.m_elementPath + "[@" + setIt->second.m_attributeName + "]";
            std::vector<std::shared_ptr<SchemaValue>> cfgValues;
            fetchSchemaValues(cfgValuePath, cfgValues);
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
                throw(ParseException("Element: " + getProperty("name", "unknown") + ", attribute " + setIt->second.m_attributeName + " not found for key " + setIt->second.m_setName));
            }
        }
        else
        {
            throw(ParseException("Element: " + getProperty("name", "unknown") + ", duplicate key (" + setIt->second.m_setName + ") found for element " + m_properties["name"]));
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
                throw(ParseException("Element: " + getProperty("name") + ", duplicate itemType(" + it->second->getItemType() + ") found"));
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
            fetchSchemaValues(it->second->getMirrorFromPath(), cfgValues);
            if (!cfgValues.empty() && cfgValues.size() == 1)
            {
                if (cfgValues.size() == 1)
                {
                    it->second->addMirroredSchemaValue(cfgValues[0]);
                }
                else
                {
                    throw(ParseException("Element: " + getProperty("name") + ", multiple sources found for mirror from path for attribute " + it->second->getName() + " (path=" + it->second->getMirrorFromPath()));
                }
            }
            else
            {
                throw(ParseException("Element: " + getProperty("name") + ", mirrored from source not found for attribute '" + it->second->getName() + "' path=" + it->second->getMirrorFromPath()));
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


void SchemaItem::processEvent(const std::string &eventType, const std::shared_ptr<EnvironmentNode> &pEventSourceNode) const
{
    //
    // Loop through any event handlers we may have
    for (auto eventIt = m_eventHandlers.begin(); eventIt != m_eventHandlers.end(); ++eventIt)
    {
        (*eventIt)->processEvent(eventType, pEventSourceNode);
    }

    //
    // Pass the event on because events are broadcast
    std::vector<std::shared_ptr<SchemaItem>> children;
    getChildren(children);
    for (auto childIt = children.begin(); childIt != children.end(); ++childIt)
    {
        (*childIt)->processEvent(eventType, pEventSourceNode);
    }
}


void SchemaItem::setRequiredInstanceComponents(const std::string list)
{
    m_requiredInstanceComponents = splitString(list, "|");
}
