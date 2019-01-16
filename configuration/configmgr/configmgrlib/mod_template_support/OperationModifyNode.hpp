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

#ifndef HPCCSYSTEMS_PLATFORM_OPERATIONMODIFYNODE_HPP
#define HPCCSYSTEMS_PLATFORM_OPERATIONMODIFYNODE_HPP

#include "Operation.hpp"

class OperationModifyNode : public Operation
{
    public:

        OperationModifyNode() = default;
        ~OperationModifyNode() override = default;
        void addAttributeForDeletion(std::string name) { m_deleteAttributes.emplace_back(name); }


    protected:

        void doExecute(EnvironmentMgr *pEnvMgr, Variables *pInputs) override;


    protected:

        std::vector<std::string> m_deleteAttributes;

};


#endif //HPCCSYSTEMS_PLATFORM_OPERATIONMODIFYNODE_HPP
