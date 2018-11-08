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

#ifndef _CONFIG2_ENVIRONMENTNODE_HPP_
#define _CONFIG2_ENVIRONMENTNODE_HPP_

#include <memory>
#include <map>
#include "SchemaItem.hpp"
#include "EnvironmentValue.hpp"
#include "SchemaValue.hpp"
#include "Status.hpp"
#include "InsertableItem.hpp"
#include "NameValue.hpp"
#include "platform.h"
#include "ConfigPath.hpp"


class DECL_EXPORT EnvironmentNode : public std::enable_shared_from_this<EnvironmentNode>
{
    public:

        EnvironmentNode(const std::shared_ptr<SchemaItem> &pCfgItem, const std::string &elemName, const std::shared_ptr<EnvironmentNode> &pParent = nullptr) :
            m_pSchemaItem(pCfgItem), m_name(elemName), m_pParent(pParent) { }
        ~EnvironmentNode() { }
        const std::string &getName() const { return m_name;  }
        void addChild(std::shared_ptr<EnvironmentNode> pNode);
        bool removeChild(std::shared_ptr<EnvironmentNode> pNode, std::vector<std::string> &removedNodeIds);
        void removeAllChildren(std::vector<std::string> &removedNodeIds);
        void getChildren(std::vector<std::shared_ptr<EnvironmentNode>> &children, const std::string &name=std::string("")) const;
        bool hasChildren() const { return m_children.size() != 0; }
        int getNumChildren() const { return (int)m_children.size(); }
        std::shared_ptr<EnvironmentNode> getParent() const;
        void setParent(const std::shared_ptr<EnvironmentNode> &pParent) { m_pParent = pParent; }
        bool addAttribute(const std::string &name, std::shared_ptr<EnvironmentValue> pValue);
        void setAttributeValues(const std::vector<NameValue> &values, Status &status, bool allowInvalid, bool forceCreate);
        void setAttributeValue(const std::string &name, const std::string &value, Status &status, bool allowInvalid=false, bool forceCreate=false);   // candidate for a variant?
        std::string getAttributeValue(const std::string &name) const;                                  // candidate for a variant?
        void addMissingAttributesFromConfig();
        bool setLocalValue(const std::string &newValue, Status &status, bool force = false);
        std::string getLocalValue() const;
        void setLocalEnvValue(const std::shared_ptr<EnvironmentValue> &pEnvValue) { m_pLocalValue = pEnvValue;  }
        const std::shared_ptr<EnvironmentValue> &getLocalEnvValue() const { return m_pLocalValue;  }
        bool isLocalValueSet() const { return m_pLocalValue != nullptr; }
        void getAttributes(std::vector<std::shared_ptr<EnvironmentValue>> &attrs) const;
        const std::shared_ptr<EnvironmentValue> getAttribute(const std::string &name) const;
        bool hasAttribute(const std::string &name) const { return m_attributes.find(name) != m_attributes.end();  }
        bool hasAttributes() const { return m_attributes.size() != 0; }
        void setId(const std::string &id) { m_id = id; }
        const std::string &getId() const { return m_id;  }
        void validate(Status &status, bool includeChildren=false, bool includeHiddenNodes=false) const;
        void getAttributeValueForAllSiblings(const std::string &attrName, std::vector<std::string> &result) const;
        const std::shared_ptr<SchemaItem> &getSchemaItem() const { return m_pSchemaItem; }
        void getInsertableItems(std::vector<InsertableItem> &items) const;
        void initialize();
        void fetchNodes(const std::string &path, std::vector<std::shared_ptr<EnvironmentNode>> &nodes) const;
        std::shared_ptr<const EnvironmentNode> getRoot() const;
        void addEnvironmentInsertData(const std::string &envData) { m_insertData = envData; }
        const std::string &getEnvironmentInsertData() const { return m_insertData; }
        void clearEnvironmentInsertData() { m_insertData.clear();  }


    protected:

        void doFetchNodes(ConfigPath &configPath, std::vector<std::shared_ptr<EnvironmentNode>> &nodes) const;


    protected:

        std::string m_name;
        std::shared_ptr<SchemaItem> m_pSchemaItem;
        std::weak_ptr<EnvironmentNode> m_pParent;
        std::multimap<std::string, std::shared_ptr<EnvironmentNode>> m_children;
        std::shared_ptr<EnvironmentValue> m_pLocalValue;   // not normal because values usually in attributes
        std::map<std::string, std::shared_ptr<EnvironmentValue>> m_attributes;
        std::string m_id;
        std::string m_insertData;
};

#endif
