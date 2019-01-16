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


#include "OperationCreateNode.hpp"
#include "EnvironmentMgr.hpp"
#include "EnvironmentNode.hpp"
#include "EnvironmentValue.hpp"
#include "TemplateExecutionException.hpp"
#include "Status.hpp"


void OperationCreateNode::doExecute(EnvironmentMgr *pEnvMgr, Variables *pInputs)
{
    std::shared_ptr<EnvironmentNode> pNewEnvNode;

    //
    // Find the parent node(s). Either was input, or based on a path, which may match more than one node
    getParentNodeIds(pEnvMgr, pInputs);

    //
    // Create an input to hold the newly created node ID(s) if indicated. The IDs are saved as the node(s) is/are
    // created.
    std::shared_ptr<Variable> pSaveNodeIdInput;
    if (!m_saveNodeIdName.empty())
    {
        pSaveNodeIdInput = createInput(m_saveNodeIdName, "string", pInputs, m_duplicateSaveNodeIdInputOk);
    }

    //
    // Create any attribute save inputs
    createAttributeSaveInputs(pInputs);

    //
    // Execute for each parent node
    if (!m_parentNodeIds.empty())
    {
        for (auto &parentNodeId: m_parentNodeIds)
        {
            Status status;

            std::string nodeId = pInputs->doValueSubstitution(parentNodeId);

            //
            // Get a new node for insertion (this does not insert the node, but rather returns an orphaned node that
            // can be inserted)
            pNewEnvNode = pEnvMgr->getNewEnvironmentNode(parentNodeId, m_nodeType, status);
            if (pNewEnvNode)
            {
                //
                // Build a vector of name value pairs of the attributes to set for the node. Also, if any attribute
                // indicates that its set value shall be saved, ensure an input exists.
                std::vector<NameValue> attrValues;
                for (auto &attr: m_attributes)
                {
                    if (!attr.doNotSet)
                    {
                        attrValues.emplace_back(NameValue(attr.getName(), attr.cookedValue));
                    }
                }

                //
                // Add the new node to the environment
                pNewEnvNode = pEnvMgr->addNewEnvironmentNode(parentNodeId, m_nodeType, attrValues, status, true, true);
                if (pNewEnvNode)
                {
                    // construct a status for just this new node's ID so we can see if there is an error
                    Status newNodeStatus(status, pNewEnvNode->getId());
                    if (newNodeStatus.isError())
                    {
                        throw TemplateExecutionException("There was a problem adding the new node, status returned an error");
                    }

                    //
                    // save the node id if indicated
                    if (pSaveNodeIdInput)
                    {
                        pSaveNodeIdInput->addValue(pNewEnvNode->getId());
                    }

                    //
                    // Save any attribute values to inputs for use later
                    saveAttributeValues(pInputs, pNewEnvNode);
                }
                else
                {
                    throw TemplateExecutionException("There was a problem adding the new node");
                }
            }
            else
            {
                throw TemplateExecutionException("Unable to get new node");
            }
        }
    }
    else
    {
        throw TemplateExecutionException("Unable to find parent node");
    }
}
