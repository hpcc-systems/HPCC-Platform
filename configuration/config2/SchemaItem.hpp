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


#ifndef _CONFIG2_CONFIGITEM_HPP_
#define _CONFIG2_CONFIGITEM_HPP_

#include <string>
#include <memory>
#include <set>
#include <map>
#include "SchemaType.hpp"
#include "SchemaValue.hpp"
#include "platform.h"


class DECL_EXPORT SchemaItem : public std::enable_shared_from_this<SchemaItem>
{
    public:

        SchemaItem(const std::string &name, const std::string &className = "category", const std::shared_ptr<SchemaItem> &pParent = nullptr);
        ~SchemaItem() { }
        std::string getItemType() const;
        void setMinInstances(unsigned num) { m_minInstances = num; }
        unsigned getMinInstances() const { return m_minInstances; }
        void setMaxInstances(unsigned num) { m_maxInstances = num; }
        unsigned getMaxInstances() const { return m_maxInstances; }
        void addSchemaValueType(const std::shared_ptr<SchemaType> &pType);
        std::shared_ptr<SchemaType> getSchemaValueType(const std::string &typeName, bool throwIfNotPresent = true) const;
        void addSchemaType(const std::shared_ptr<SchemaItem> &pItem, const std::string &typeName);
        std::shared_ptr<SchemaItem> getSchemaType(const std::string &name, bool throwIfNotPresent=true) const;
        void insertSchemaType(const std::shared_ptr<SchemaItem> pTypeItem);
        void addChild(const std::shared_ptr<SchemaItem> &pItem) { m_children.insert({ pItem->getProperty("name"), pItem }); }
        void addChild(const std::shared_ptr<SchemaItem> &pItem, const std::string &name) { m_children.insert({ name, pItem }); }
        void getChildren(std::vector<std::shared_ptr<SchemaItem>> &children);
        std::shared_ptr<SchemaItem> getChild(const std::string &name);
        std::shared_ptr<SchemaItem> getChildByComponent(const std::string &name, std::string &componentName);
        void setItemSchemaValue(const std::shared_ptr<SchemaValue> &pValue) { m_pItemValue = pValue; }
        std::shared_ptr<SchemaValue> getItemSchemaValue() const { return m_pItemValue; }
        bool isItemValueDefined() { return m_pItemValue != nullptr; }
        void findSchemaValues(const std::string &path, std::vector<std::shared_ptr<SchemaValue>> &schemaValues);
        void addAttribute(const std::shared_ptr<SchemaValue> &pCfgValue);
        void addAttribute(const std::vector<std::shared_ptr<SchemaValue>> &attributes);
        void addAttribute(const std::map<std::string, std::shared_ptr<SchemaValue>> &attributes);
        std::shared_ptr<SchemaValue> getAttribute(const std::string &name, bool createIfDoesNotExist=true) const;
        void getAttributes(std::vector<std::shared_ptr<SchemaValue>> &attributes) const;
        bool addUniqueName(const std::string keyName);
        void addUniqueAttributeValueSetDefinition(const std::string &setName, const std::string &elementPath, const std::string &attributeName, bool duplicateOk = false);
        void addReferenceToUniqueAttributeValueSet(const std::string &setName, const std::string &elementPath, const std::string &attributeName);
        void processDefinedUniqueAttributeValueSets(std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> &uniqueAttributeValueSets);
        void processUniqueAttributeValueSetReferences(const std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> &uniqueAttributeValueSets);
        void resetEnvironment();
        void postProcessConfig(const std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> &uniqueAttributeValueSets);
        bool isInsertable() const { return (m_minInstances == 0) || (m_maxInstances > m_minInstances); }
        bool isRequired() const { return m_minInstances > 0; }

        std::string getProperty(const std::string &name, const std::string &dfault = std::string("")) const;
        void setProperty(const std::string &name, const std::string &value) { m_properties[name] = value; }
        void setHidden(bool hidden) { m_hidden = hidden; }
        bool isHidden() const { return m_hidden; }


    protected:

        SchemaItem() { };

    protected:

        std::map<std::string, std::string> m_properties;
        bool m_hidden;
        unsigned m_minInstances;
        unsigned m_maxInstances;
        std::multimap<std::string, std::shared_ptr<SchemaItem>> m_children;
        std::shared_ptr<SchemaValue> m_pItemValue;   // value for this item (think of it as the VALUE for an element <xx attr= att1>VALUE</xx>)
        std::map<std::string, std::shared_ptr<SchemaValue>> m_attributes;   // attributes for this item (think in xml terms <m_name attr1="val" attr2="val" .../> where attrN is in this vector
        std::set<std::string> m_keys;   // generic set of key values for use by any component to prevent duplicate operations
        std::weak_ptr<SchemaItem> m_pParent;
        std::map<std::string, std::shared_ptr<SchemaType>> m_types;
        std::map<std::string, std::shared_ptr<SchemaItem>> m_schemaTypes;                // reusable types

        // This struct handles both unique attribute sets and references to same
        struct SetInfo {
            SetInfo(const std::string &setName, const std::string &elementPath, const std::string &attributeName) :
                m_setName(setName), m_elementPath(elementPath), m_attributeName(attributeName), m_duplicateOk(false) { }
            SetInfo(const std::string &setName, const std::string &elementPath, const std::string &attributeName, bool duplicateOk) :
                m_setName(setName), m_elementPath(elementPath), m_attributeName(attributeName), m_duplicateOk(duplicateOk) { }
            std::string m_setName;
            std::string m_elementPath;
            std::string m_attributeName;
            bool m_duplicateOk;
        };

        // Attribute unique sets and references to unique sets are stored during parsing and post processed
        std::map<std::string, SetInfo> m_uniqueAttributeValueSetReferences;
        std::map<std::string, SetInfo> m_uniqueAttributeValueSetDefs;

        // These are the attribute value sets whose members must be unique
        //static std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> m_uniqueAttributeValueSets;
};

#endif // _CONFIG2_CONFIGITEM_HPP_
