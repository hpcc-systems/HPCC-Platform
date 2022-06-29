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

#include <algorithm>
#include "AddRequiredInstancesSupport.hpp"

extern "C" DECL_EXPORT HPCCConfigSupport* getCfgMgrSupportInstance(EnvironmentMgr *pEnvMgr)
{
    HPCCConfigSupport *pInstance = new AddRequiredInstancesSupport(pEnvMgr);
    return pInstance;
}


void  AddRequiredInstancesSupport::processEvent(const std::string &event, const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEventNode, Status &status) const
{
    if (event == "create")
    {
        //
        // Determin if an instance copy has to be added to another component
        // - The newly created node (pEventNode) is a hardware instance (hwinstance)
        // - The new node's parent schema element has a required instance component list that includes add_required_instances
        if (pEventNode->getSchemaItem()->getItemType() == "hwinstance")
        {
            const std::vector<std::string> &reqList = pEventNode->getSchemaItem()->getRequiredInstanceComponents();
            if (!reqList.empty())
            {
                for (auto &req: reqList)
                {
                    //
                    // Get the nodes where an instance may need to be added
                    std::vector<std::shared_ptr<EnvironmentNode>> targetNodes;
                    pEventNode->fetchNodes(req, targetNodes);

                    //
                    // For each node, if a matching instance doesn't exist, add one
                    for (auto &targetNode: targetNodes)
                    {
                        std::string nodeName = pEventNode->getAttribute("computer")->getValue();
                        std::string existingPath = "Instance/[@computer='" + nodeName + "']";
                        std::vector<std::shared_ptr<EnvironmentNode>> instanceNodes;
                        targetNode->fetchNodes(existingPath, instanceNodes);
                        if (instanceNodes.empty())
                        {
                            addInstance(pEventNode, targetNode, status);
                        }
                    }
                }
            }
        }
    }
}


void  AddRequiredInstancesSupport::validate(const std::shared_ptr<SchemaItem> &pSchema, const std::shared_ptr<EnvironmentNode> &pEnvironment, Status &status) const
{
}


void AddRequiredInstancesSupport::addInstance(const std::shared_ptr<EnvironmentNode> &pEventNode, const std::shared_ptr<EnvironmentNode> &pTargetNode, Status &status) const
{
    std::pair<std::string, std::string> attrValues;
    std::vector<NameValue> initAttributeValues;

    //
    // Generate a list of attribute values for each schema attribute in the node being inserted that has a value in the
    // event node, then insert it.
    std::vector<std::shared_ptr<SchemaItem>> children;
    pTargetNode->getSchemaItem()->getChildren(children, "Instance");
    if (!children.empty())
    {
        std::vector<std::shared_ptr<SchemaValue>> attributes;
        children[0]->getAttributes(attributes);
        for (auto &daInstAttr: attributes)
        {
            std::shared_ptr<EnvironmentValue> pEventAttr = pEventNode->getAttribute(daInstAttr->getName());
            if (pEventAttr)
            {
                std::string eventAttrValue = pEventAttr->getValue();
                if (!eventAttrValue.empty())
                {
                    initAttributeValues.emplace_back(NameValue(daInstAttr->getName(), eventAttrValue));
                }
            }
        }
        m_pEnvMgr->addNewEnvironmentNode(pTargetNode, children[0], initAttributeValues, status);
    }
}
