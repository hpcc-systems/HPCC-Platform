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
#include "CfgType.hpp"
#include "CfgValue.hpp"


class ConfigItemValueSet;


class ConfigItem : public std::enable_shared_from_this<ConfigItem>
{
    public:

        ConfigItem(const std::string &name, const std::string &className = "category", const std::shared_ptr<ConfigItem> &pParent = nullptr);
        virtual ~ConfigItem() { }

        virtual const std::string &getClassName() const { return m_className; }
        virtual void setClassName(const std::string &className) { m_className = className;  }

        virtual const std::string &getComponentName() const { return m_componentName; }
        virtual void setComponentName(const std::string &componentName) { m_componentName = componentName; }

        virtual const std::string &getName() const { return m_name; }
        virtual void setName(const std::string &name) { m_name = name; }      

        virtual void setDisplayName(const std::string &displayName) { m_displayName = displayName; }
        virtual const std::string &getDisplayName() const { return m_displayName.length() ? m_displayName : m_name; }

        virtual void setCategory(const std::string &category) { m_category = category; }
        virtual const std::string &getCategory() const { return m_category; }

        void setMinInstances(int num) { m_minInstances = num; }
        int getMinInstances() const { return m_minInstances; }
        
        void setMaxInstances(int num) { m_maxInstances = num; }
        int getMaxInstances() const { return m_maxInstances; }

        virtual void addType(const std::shared_ptr<CfgType> &pType);
        virtual std::shared_ptr<CfgType> getType(const std::string &typeName, bool throwIfNotPresent = true) const;

        void setVersion(int version) { m_version = version;  }
        int getVersion() const { return m_version; }

        virtual void addConfigType(const std::shared_ptr<ConfigItem> &pItem, const std::string &typeName);
        virtual std::shared_ptr<ConfigItem> getConfigType(const std::string &name, bool throwIfNotPresent=true) const;
        virtual void insertConfigType(const std::shared_ptr<ConfigItem> pTypeItem);

        virtual void addChild(const std::shared_ptr<ConfigItem> &pItem) { m_children.insert({ pItem->getName(), pItem }); }
        virtual void addChild(const std::shared_ptr<ConfigItem> &pItem, const std::string &name) { m_children.insert({ name, pItem }); }
        //virtual std::vector<std::shared_ptr<ConfigItem>> getChildren() const;
        const std::multimap<std::string, std::shared_ptr<ConfigItem>> &getChildren() const { return m_children; }
        std::shared_ptr<ConfigItem> getChild(const std::string &name);
        std::shared_ptr<ConfigItem> getChildByComponent(const std::string &name, std::string &componentName);
        
        virtual void setItemCfgValue(const std::shared_ptr<CfgValue> &pValue) { m_pItemCfgValue = pValue; }
        virtual std::shared_ptr<CfgValue> getItemCfgValue() const { return m_pItemCfgValue; }
        virtual bool isItemValueDefined() { return m_pItemCfgValue != nullptr; }
        void findCfgValues(const std::string &path, std::vector<std::shared_ptr<CfgValue>> &cfgValues);

        virtual void addAttribute(const std::shared_ptr<CfgValue> &pCfgValue);
        virtual void addAttribute(const std::vector<std::shared_ptr<CfgValue>> &attributes);
        virtual std::shared_ptr<CfgValue> getAttribute(const std::string &name) const;
        virtual const std::map<std::string, std::shared_ptr<CfgValue>> &getAttributes() const { return m_attributes;  }

        virtual bool addUniqueName(const std::string keyName);
        virtual void addUniqueAttributeValueSetDefinition(const std::string &setName, const std::string &elementPath, const std::string &attributeName, bool duplicateOk = false);
        virtual void addReferenceToUniqueAttributeValueSet(const std::string &setName, const std::string &elementPath, const std::string &attributeName);
        virtual void processUniqueAttributeValueSets();
        virtual void processUniqueAttributeValueSetReferences();

        virtual void resetEnvironment(); 

        virtual void postProcessConfig();

        ConfigItem() { };


    protected:

        std::string m_name;
        std::string m_displayName;
        std::string m_className;
        std::string m_category;  // used for further subdividing to the user
        std::string m_componentName;   
        std::multimap<std::string, std::shared_ptr<ConfigItem>> m_children; 
        std::shared_ptr<CfgValue> m_pItemCfgValue;   // value for this item (think of it as the VALUE for an element <xx attr= att1=>VALUE</xx>)
        std::map<std::string, std::shared_ptr<CfgValue>> m_attributes;   // attributes for this item (think in xml terms <m_name attr1="val" attr2="val" .../> where attrN is in this vector
        std::set<std::string> m_keys;   // generic set of key values for use by any component to prevent duplicate operations
        std::weak_ptr<ConfigItem> m_pParent;

        std::map<std::string, std::shared_ptr<CfgType>> m_types;
        std::map<std::string, std::shared_ptr<ConfigItem>> m_configTypes;                // reusable types

        int m_minInstances;
        int m_maxInstances;
        int m_version;

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
        static std::map<std::string, std::vector<std::shared_ptr<CfgValue>>> m_uniqueAttributeValueSets;    

    private:

        
        

};


#endif // _CONFIG2_CONFIGITEM_HPP_
