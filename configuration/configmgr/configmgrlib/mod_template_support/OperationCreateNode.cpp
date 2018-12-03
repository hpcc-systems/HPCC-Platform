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
#include "TemplateException.hpp"
#include "Status.hpp"
#include <vector>



void OperationCreateNode::doExecute(EnvironmentMgr *pEnvMgr, Inputs *pInputs)
{
    std::shared_ptr<EnvironmentNode> pNewEnvNode;
    std::vector<std::string> parentNodeIds;

    //
    // Find the parent node(s). Either was input, or based on a path, which may match more than one node
    if (!m_parentNodeId.empty())
    {
        parentNodeIds.emplace_back(m_parentNodeId);
    }
    else
    {
        std::vector<std::shared_ptr<EnvironmentNode>> envNodes;
        pEnvMgr->fetchNodes(m_path, envNodes);
        for (auto &envNode: envNodes)
            parentNodeIds.emplace_back(envNode->getId());
    }

    //
    // Create an input to hold the newly created node ID(s) if indicated. The IDs are saved as the node(s) is/are
    // created.
    std::shared_ptr<Input> pSaveNodeIdInput;
    if (!m_saveNodeIdName.empty())
    {
        pSaveNodeIdInput = inputValueFactory("string", m_saveNodeIdName);
        pInputs->add(pSaveNodeIdInput);
    }

    //
    // If any attribute values are to be saved from the created node(s), create the inputs.
    for (auto &attr: m_attributes)
    {
        //
        // If this is a saved attribute value, make sure the input exists
        if (!attr.saveValue.empty())
        {
            std::shared_ptr<Input> pInput = pInputs->getInput(attr.saveValue, false);
            if (pInput)
            {
                throw TemplateException("Attribute '" + attr.name + "' save value '" + attr.saveValue + "' already exists.", false);
            }
            pInput = inputValueFactory("string", attr.saveValue);
            pInputs->add(pInput);
        }
    }


    //
    // Execute for each parent node
    if (!parentNodeIds.empty())
    {
        for (auto parentNodeId: parentNodeIds)
        {
            Status status;

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
                        attrValues.emplace_back(NameValue(attr.name, attr.cookedValue));
                    }
                }

                //
                // Add the new node to the environment
                pNewEnvNode = pEnvMgr->addNewEnvironmentNode(parentNodeId, m_nodeType, attrValues, status);
                if (pNewEnvNode)
                {
                    // construct a status for just this new node's ID so we can see if there is an error
                    Status newNodeStatus(status, pNewEnvNode->getId());
                    if (newNodeStatus.isError())
                    {
                        throw TemplateException("There was a problem adding the new node, status returned an error",
                                                false);
                    }

                    //
                    // save the node id if indicated
                    if (pSaveNodeIdInput)
                    {
                        pSaveNodeIdInput->addValue(pNewEnvNode->getId());
                    }

                    //
                    // Save any required attribute values
                    for (auto &attr: m_attributes)
                    {
                        if (!attr.saveValue.empty())
                        {
                            std::shared_ptr<Input> pInput = pInputs->getInput(attr.saveValue);
                            pInput->addValue(pNewEnvNode->getAttribute(attr.name)->getValue());
                        }
                    }
                }
                else
                {
                    throw TemplateException("There was a problem adding the new node", false);
                }
            }
            else
            {
                throw TemplateException("Unable to get new node", false);
            }
        }
    }
    else
    {
        throw TemplateException("Unable to find parent node", false);
    }
}
