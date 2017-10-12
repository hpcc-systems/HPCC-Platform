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

#ifndef _CONFIG2_ENVVALUE_HPP_
#define _CONFIG2_ENVVALUE_HPP_

#include <string>
#include "CfgValue.hpp"
#include "NodeStatus.hpp"

class EnvironmentNode;

class EnvValue : public NodeStatus
{
	public:
		EnvValue(const std::shared_ptr<EnvironmentNode> &pMyNode, const std::shared_ptr<CfgValue> &pCfgValue, const std::string &name="") : m_pMyEnvNode(pMyNode), m_pCfgValue(pCfgValue), m_name(name) { }
		~EnvValue() { }
		bool setValue(const std::string &value, bool force=false);
		bool checkCurrentValue();
		const std::string &getValue() const { return m_value;  }
		const std::string &getDefaultValue() const { return m_pCfgValue->getDefaultValue(); }
		bool hasDefaultValue() const { return m_pCfgValue->hasDefaultValue(); }
		const std::shared_ptr<CfgValue> &getCfgValue() const { return m_pCfgValue;  }
		const std::string &getName() const { return m_name;  }
		bool isValueValid(const std::string &value) const;
        bool validate() const;
	

	private:

		std::string m_name;
		std::string m_value;
		std::shared_ptr<CfgValue> m_pCfgValue;   
		std::weak_ptr<EnvironmentNode> m_pMyEnvNode;
};


#endif