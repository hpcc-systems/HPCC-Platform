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


EnvironmentMgr *getEnvironmentMgrInstance(const std::string &envType)
{
    EnvironmentMgr *pEnvMgr = NULL;
    if (envType == "XML")
    {
        pEnvMgr = new XMLEnvironmentMgr();
    }
    return pEnvMgr;
}


EnvironmentMgr::EnvironmentMgr() :
    m_key(0)
{
    m_pSchema = std::make_shared<SchemaItem>("root");  // make the root
}


bool EnvironmentMgr::loadSchema(const std::string &configPath, const std::string &masterConfigFile, const std::vector<std::string> &cfgParms)  // todo: add a status object here for return
{
    bool rc = false;
    if (createParser(configPath, masterConfigFile, cfgParms))
    {
        rc = m_pSchemaParser->parse(configPath, masterConfigFile, cfgParms);
        if (rc)
        {
            m_pSchema->processUniqueAttributeValueSets();  // This must be done first
            m_pSchema->postProcessConfig();
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


bool EnvironmentMgr::loadEnvironment(const std::string &filename)
{
    bool rc = false;
    std::ifstream in;
    std::string fpath = filename;

    in.open(fpath);
    if (in.is_open())
    {
        rc = doLoadEnvironment(in);
    }
    else
    {
        m_message = "Unable to open environment file '" + filename + "'";
    }
    return rc;
}


void EnvironmentMgr::saveEnvironment(const std::string &filename)
{
    std::ofstream out;

    out.open(filename);
    if (out.is_open())
    {
        save(out);
    }
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



std::shared_ptr<EnvironmentNode> EnvironmentMgr::addNewEnvironmentNode(const std::string &parentNodeId, const std::string &elementType, Status &status)
{
    std::shared_ptr<EnvironmentNode> pNewNode;
    std::shared_ptr<EnvironmentNode> pParentNode = getEnvironmentNode(parentNodeId);
    if (pParentNode)
    {
        std::shared_ptr<SchemaItem> pNewCfgItem;
        std::vector<std::shared_ptr<SchemaItem>> insertableItems = pParentNode->getInsertableItems();  // configured items under the parent
        for (auto it = insertableItems.begin(); it != insertableItems.end(); ++it)
        {
            if ((*it)->getItemType() == elementType)
            {
                pNewNode = addNewEnvironmentNode(pParentNode, *it, status);
                break;
            }
        }
    }
    else
    {
        status.addStatusMsg(statusMsg::error, parentNodeId, "", "", "Unable to find indicated parent node");
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
    auto cfgChildren = pNewCfgItem->getChildren();
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


bool EnvironmentMgr::removeEnvironmentNode(const std::string &nodeId, Status &status)
{
    bool rc = false;
    std::shared_ptr<EnvironmentNode> pNode = getEnvironmentNode(nodeId);

    if (pNode)
    {
        std::shared_ptr<EnvironmentNode> pParentNode = pNode->getParent();
        if (pParentNode->removeChild(pNode))
        {
            m_nodeIds.erase(nodeId);
        }
        else
        {
            status.addStatusMsg(statusMsg::error, nodeId, "", "", "Unable to remove the node");
        }
    }
    else
    {
        status.addStatusMsg(statusMsg::error, nodeId, "", "", "Unable to find indicated node");
    }

    return rc;
}


std::string EnvironmentMgr::getUniqueKey()
{
    return std::to_string(m_key++);
}


void EnvironmentMgr::validate(Status &status) const
{
    if (m_pRootNode)
    {
        m_pRootNode->validate(status, true);
    }
}