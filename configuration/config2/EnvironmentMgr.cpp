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

#include "EnvironmentMgr.hpp"
#include "Exceptions.hpp"
#include "XMLEnvironmentMgr.hpp"


EnvironmentMgr *getEnvironmentMgrInstance(const EnvironmentType envType)
{
    EnvironmentMgr *pEnvMgr = nullptr;
    if (envType == XML)
    {
        pEnvMgr = new XMLEnvironmentMgr();
    }
    return pEnvMgr;
}


EnvironmentMgr::EnvironmentMgr() :
    m_key(1)  // ID 0 is reserved for the root node
{
    m_pSchema = std::make_shared<SchemaItem>("root");  // make the root
}


bool EnvironmentMgr::loadSchema(const std::string &configPath, const std::string &masterConfigFile, const std::vector<std::string> &cfgParms)
{
    bool rc = false;
    if (createParser())
    {
        rc = m_pSchemaParser->parse(configPath, masterConfigFile, cfgParms);
        if (rc)
        {
            // unique attribure value sets are global across a schema. Allocate one here and pass it in
            // for use in building the necessary references and dependencies across the schema, then pass
            // it to the post processing for finalization. Once referencs and dependencies are built, the
            // attribute value sets are no longer needed.
            std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> uniqueAttributeValueSets;
            m_pSchema->processDefinedUniqueAttributeValueSets(uniqueAttributeValueSets);  // This must be done first
            m_pSchema->postProcessConfig(uniqueAttributeValueSets);
        }
    }
    return rc;
}


std::string EnvironmentMgr::getLastSchemaMessage() const
{
    if (m_pSchemaParser)
        return m_pSchemaParser->getLastMessage();
    return "";
}


bool EnvironmentMgr::loadEnvironment(const std::string &qualifiedFilename)
{
    bool rc = false;

    if (m_pSchema)
    {
        std::ifstream in;

        in.open(qualifiedFilename);
        if (in.is_open())
        {
            rc = doLoadEnvironment(in);
        }
        else
        {
            m_message = "Unable to open environment file '" + qualifiedFilename + "'";
        }
    }
    else
    {
        m_message = "No schema loaded";
    }
    return rc;
}


bool EnvironmentMgr::saveEnvironment(const std::string &qualifiedFilename)
{
    bool rc = false;
    if (m_pRootNode)
    {
        std::ofstream out;

        out.open(qualifiedFilename);
        if (out.is_open())
        {
            rc = save(out);
        }
    }
    else
    {
        m_message = "No environment loaded";
    }
    return rc;
}


void EnvironmentMgr::addPath(const std::shared_ptr<EnvironmentNode> pNode)
{
    auto retVal = m_nodeIds.insert({pNode->getId(), pNode });
    if (!retVal.second)
    {
        throw (ParseException("Attempted to insert duplicate path name " + pNode->getId() + " for node "));
    }
}


std::shared_ptr<EnvironmentNode> EnvironmentMgr::getEnvironmentNode(const std::string &nodeId)
{
    std::shared_ptr<EnvironmentNode> pNode;
    auto pathIt = m_nodeIds.find(nodeId);
    if (pathIt != m_nodeIds.end())
        pNode = pathIt->second;
    return pNode;
}



std::shared_ptr<EnvironmentNode> EnvironmentMgr::addNewEnvironmentNode(const std::string &parentNodeId, const std::string &configType, Status &status)
{
    std::shared_ptr<EnvironmentNode> pNewNode;
    std::shared_ptr<EnvironmentNode> pParentNode = getEnvironmentNode(parentNodeId);
    if (pParentNode)
    {
        std::shared_ptr<SchemaItem> pNewCfgItem;
        std::vector<std::shared_ptr<SchemaItem>> insertableItems;
        pParentNode->getInsertableItems(insertableItems);
        for (auto it = insertableItems.begin(); it != insertableItems.end(); ++it)
        {
            if ((*it)->getItemType() == configType)
            {
                pNewNode = addNewEnvironmentNode(pParentNode, *it, status);
                break;
            }
            if (pNewNode == nullptr)
            {
                status.addMsg(statusMsg::error, "Configuration type (" + configType + ") not found");
            }
        }
    }
    else
    {
        status.addMsg(statusMsg::error, parentNodeId, "", "Unable to find indicated parent node");
    }
    return pNewNode;
}


std::shared_ptr<EnvironmentNode> EnvironmentMgr::addNewEnvironmentNode(const std::shared_ptr<EnvironmentNode> &pParentNode, const std::shared_ptr<SchemaItem> &pNewCfgItem, Status &status)
{
    std::shared_ptr<EnvironmentNode> pNewNode;

    //
    // Create the new node and add it to the parent
    pNewNode = std::make_shared<EnvironmentNode>(pNewCfgItem, pNewCfgItem->getProperty("name"), pParentNode);
    pNewNode->setId(getUniqueKey());
    pParentNode->addChild(pNewNode);
    addPath(pNewNode);
    pNewNode->initialize();


    //
    // Look through the children and add any that are necessary
    std::vector<std::shared_ptr<SchemaItem>> cfgChildren;
    pNewCfgItem->getChildren(cfgChildren);
    for (auto childIt = cfgChildren.begin(); childIt != cfgChildren.end(); ++childIt)
    {
        int numReq = (*childIt)->getMinInstances();
        for (int i=0; i<numReq; ++i)
        {
            addNewEnvironmentNode(pNewNode, *childIt, status);
        }
    }

    return pNewNode;
}


bool EnvironmentMgr::removeEnvironmentNode(const std::string &nodeId)
{
    bool rc = false;
    std::shared_ptr<EnvironmentNode> pNode = getEnvironmentNode(nodeId);

    if (pNode)
    {
        std::shared_ptr<EnvironmentNode> pParentNode = pNode->getParent();
        if (pParentNode->removeChild(pNode))
        {
            m_nodeIds.erase(nodeId);
            rc = true;
        }
    }

    return rc;
}


std::string EnvironmentMgr::getUniqueKey()
{
    return std::to_string(m_key++);
}


void EnvironmentMgr::validate(Status &status, bool includeHiddenNodes) const
{
    if (m_pRootNode)
    {
        m_pRootNode->validate(status, true, includeHiddenNodes);
    }
    else
    {
        status.addMsg(statusMsg::error, "No environment loaded");
    }
}