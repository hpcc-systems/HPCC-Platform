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

#include "OperationModifyNode.hpp"

#include "Operation.hpp"
#include "TemplateExecutionException.hpp"


void OperationModifyNode::doExecute(EnvironmentMgr *pEnvMgr, Variables *pInputs)
{
    std::shared_ptr<EnvironmentNode> pEnvNode;
    getParentNodeIds(pEnvMgr, pInputs);

    //
    // Execute for each parent node
    if (!m_parentNodeIds.empty())
    {
        for (auto &parentNodeId: m_parentNodeIds)
        {
            Status status;

            std::string nodeId = pInputs->doValueSubstitution(parentNodeId);
            pEnvNode = pEnvMgr->findEnvironmentNodeById(nodeId);
            if (pEnvNode)
            {
                //
                // Set the indicated attribute values
                std::vector<NameValue> attrValues;
                for (auto &attr: m_attributes)
                {
                    if (!attr.doNotSet)
                    {
                        attrValues.emplace_back(NameValue(attr.getName(), attr.cookedValue));
                    }
                }
                pEnvNode->setAttributeValues(attrValues, status, true, true);
            }
            else
            {
                // todo base execute method in Opeeration class should catch this exception and rethrow a standard exception
                throw TemplateExecutionException("There was an error retrieving a node for modification");
            }
        }
    }
    else if (m_throwOnEmpty)
    {
        throw TemplateExecutionException("Unable to find parent node");
    }

}
