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

#ifndef _CONFIG2_ENVVALUE_HPP_
#define _CONFIG2_ENVVALUE_HPP_

#include <string>
#include "SchemaValue.hpp"
#include "Status.hpp"
#include "platform.h"

class EnvironmentNode;

class DECL_EXPORT EnvironmentValue
{
    public:

        EnvironmentValue(const std::shared_ptr<EnvironmentNode> &pMyNode, const std::shared_ptr<SchemaValue> &pCfgValue, const std::string &name="") :
            m_pMyEnvNode(pMyNode), m_pSchemaValue(pCfgValue), m_name(name), m_forcedSet(false) { }
        EnvironmentValue(const std::shared_ptr<EnvironmentNode> &pMyNode, const std::shared_ptr<SchemaValue> &pCfgValue, const std::string &name, const std::string initValue) :
            EnvironmentValue(pMyNode, pCfgValue, name) { m_value = initValue; }

        ~EnvironmentValue() { }
        bool setValue(const std::string &value, Status *pStatus, bool forceSet=false);
        bool isValueSet() const { return !m_value.empty(); }
        const std::string &getValue() const { return m_value.empty() ? getForcedValue() : m_value;  }
        const std::string &getForcedValue() const { return m_pSchemaValue->getForcedValue(); }
        bool hasForcedValue() const { return m_pSchemaValue->hasForcedValue(); }
        const std::shared_ptr<SchemaValue> &getSchemaValue() const { return m_pSchemaValue;  }
        const std::string &getName() const { return m_name;  }
        bool wasForced() const { return m_forcedSet; }
        bool isValueValid(const std::string &value) const;
        void validate(Status &status, const std::string &myId) const;
        void getAllValuesForSiblings(std::vector<std::string> &result) const;
        std::string getNodeId() const;
        std::shared_ptr<EnvironmentNode> getEnvironmentNode() const { return m_pMyEnvNode.lock(); }
        void initialize();

    private:

        bool m_forcedSet;     // true when last set value was a forced set
        std::string m_name;
        std::string m_value;
        std::shared_ptr<SchemaValue> m_pSchemaValue;
        std::weak_ptr<EnvironmentNode> m_pMyEnvNode;
};

#endif
