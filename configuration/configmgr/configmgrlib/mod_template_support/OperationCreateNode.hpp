/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#ifndef HPCCSYSTEMS_PLATFORM_OPERATIONCREATENODE_HPP
#define HPCCSYSTEMS_PLATFORM_OPERATIONCREATENODE_HPP

#include "Operation.hpp"

#include "../../RapidJSON/include/rapidjson/document.h"


class OperationCreateNode : public Operation
{
    public:

        OperationCreateNode() = default;
        ~OperationCreateNode() = default;
        void setNodeType(const std::string &type) { m_nodeType = type; }
        std::string getNodeType() const { return m_nodeType; }
        void setSaveNodeIdName(const std::string &name) { m_saveNodeIdName = name; }
        std::string getSaveNodeIdName() const { return m_saveNodeIdName; }


    protected:

        void doExecute(EnvironmentMgr *pEnvMgr, Inputs *pInputs);

    private:

        std::string m_nodeType;
        std::string m_saveNodeIdName;
};


#endif //HPCCSYSTEMS_PLATFORM_OPERATIONCREATENODE_HPP
