/*##############################################################################

HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems�.

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
#include "ConfigItem.hpp"
#include "EnvValue.hpp"
#include "CfgValue.hpp"
#include "NodeStatus.hpp"

class EnvironmentNode : public NodeStatus, public std::enable_shared_from_this<EnvironmentNode>
{
	public:

		EnvironmentNode(const std::shared_ptr<ConfigItem> &pCfgItem, const std::string &elemName, const std::shared_ptr<EnvironmentNode> &pParent = nullptr) : 
			m_pConfigItem(pCfgItem), m_name(elemName), m_pParent(pParent) { }
		~EnvironmentNode() { }
		const std::string &getName() const { return m_name;  }
		void addChild(std::shared_ptr<EnvironmentNode> pNode);
		std::vector<std::shared_ptr<EnvironmentNode>> getChildren(const std::string &name="") const;
        std::map<std::string, std::vector<std::shared_ptr<EnvironmentNode>>> getChildrenByName() const;
		bool hasChildren() const { return m_children.size() != 0; }
		int getNumChildren() const { return m_children.size(); }
		void addAttribute(const std::string &name, std::shared_ptr<EnvValue> pValue);
		bool setAttributeValue(const std::string &name, const std::string &value, bool allowInvalid=false, bool forceCreate=false);   // candidate for a variant?
		std::string getAttributeValue(const std::string &name) const;                                  // candidate for a variant?
		bool setValue(const std::string &value, bool force = false);   
		void setNodeEnvValue(const std::shared_ptr<EnvValue> &pEnvValue) { m_pNodeValue = pEnvValue;  }
		const std::shared_ptr<EnvValue> &getNodeEnvValue() const { return m_pNodeValue;  }
		std::vector<std::shared_ptr<EnvValue>> getAttributes() const;
		const std::shared_ptr<EnvValue> getAttribute(const std::string &name) const;
		bool hasAttributes() const { return m_attributes.size() != 0; }
		void setPath(const std::string &path) { m_path = path; } 
		const std::string &getPath() const { return m_path;  }
		const std::string &getMessage() const { return m_msg; }
		void setMessage(const std::string &msg) { m_msg = msg; }
		bool validate();
		std::vector<std::string> getAllFieldValues(const std::string &fieldName) const;
		const std::shared_ptr<ConfigItem> &getConfigItem() const { return m_pConfigItem; }


	protected:

		std::string m_msg;           // error or warning message
		std::string m_name;   
		std::shared_ptr<ConfigItem> m_pConfigItem;  
		std::weak_ptr<EnvironmentNode> m_pParent;
		std::multimap<std::string, std::shared_ptr<EnvironmentNode>> m_children;
		std::shared_ptr<EnvValue> m_pNodeValue;   // the node's value (not normal)
		std::map<std::string, std::shared_ptr<EnvValue>> m_attributes;
		std::string m_path;
};


#endif