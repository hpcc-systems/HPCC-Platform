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
#include "InsertableItem.hpp"

std::atomic_int EnvironmentMgr::m_key(1);

EnvironmentMgr *getEnvironmentMgrInstance(const EnvironmentType envType)
{
    EnvironmentMgr *pEnvMgr = nullptr;
    if (envType == XML)
    {
        pEnvMgr = new XMLEnvironmentMgr();
    }
    return pEnvMgr;
}


EnvironmentMgr::EnvironmentMgr()
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
            try
            {
                std::vector<std::shared_ptr<EnvironmentNode>> rootNodes = doLoadEnvironment(in, m_pSchema);  // root
                if (rootNodes.size() == 1)
                {
                    m_pRootNode = rootNodes[0];
                    assignNodeIds(m_pRootNode);
                    rc = true;
                }
                else
                {
                    m_message = "There was an unknown error loading the environment";
                }
            }
            catch (ParseException &pe)
            {
                m_message = pe.what();
            }
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


std::string EnvironmentMgr::getRootNodeId() const
{
    std::string nodeId;
    if (m_pRootNode != nullptr)
    {
        nodeId = m_pRootNode->getId();
    }
    return nodeId;
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
        std::vector<InsertableItem> insertableItems;
        std::string itemType = configType;
        std::pair<std::string, std::string> initAttributeValue;
        size_t atPos = itemType.find_first_of('@');
        if (atPos != std::string::npos)
        {
            std::string attrNameValue = itemType.substr(atPos + 1);
            itemType.erase(atPos, std::string::npos);

            size_t equalPos = attrNameValue.find_first_of('=');
            if (equalPos != std::string::npos)
            {
                initAttributeValue.first = attrNameValue.substr(0, equalPos);
                initAttributeValue.second = attrNameValue.substr(equalPos + 1);
            }
        }
        pParentNode->getInsertableItems(insertableItems);
        for (auto it = insertableItems.begin(); it != insertableItems.end(); ++it)
        {
            if ((*it).m_pSchemaItem->getItemType() == itemType)
            {
                pNewNode = addNewEnvironmentNode(pParentNode, (*it).m_pSchemaItem, status, initAttributeValue);
                break;
            }
        }
        if (pNewNode == nullptr)
        {
            status.addMsg(statusMsg::error, "Configuration type (" + configType + ") not found");
        }
    }
    else
    {
        status.addMsg(statusMsg::error, parentNodeId, "", "Unable to find indicated parent node");
    }
    pNewNode->validate(status, true, false);
    return pNewNode;
}


std::shared_ptr<EnvironmentNode> EnvironmentMgr::addNewEnvironmentNode(const std::shared_ptr<EnvironmentNode> &pParentNode, const std::shared_ptr<SchemaItem> &pCfgItem, Status &status,
                                                                       const std::pair<std::string, std::string> &initAttribute)
{
    std::shared_ptr<EnvironmentNode> pNewEnvNode;

    //
    // Create the new node and add it to the parent
    pNewEnvNode = std::make_shared<EnvironmentNode>(pCfgItem, pCfgItem->getProperty("name"), pParentNode);
    pNewEnvNode->setId(EnvironmentMgr::getUniqueKey());

    addPath(pNewEnvNode);
    pNewEnvNode->initialize();
    if (!initAttribute.first.empty())
    {
        std::shared_ptr<EnvironmentValue> pAttr = pNewEnvNode->getAttribute(initAttribute.first);
        if (pAttr)
        {
            pAttr->setValue(initAttribute.second, nullptr);
        }
    }
    pParentNode->addChild(pNewEnvNode);

    //
    // Send a create event now that it's been added to the environment
    pCfgItem->getSchemaRoot()->processEvent("create", pNewEnvNode);
    insertExtraEnvironmentData(m_pRootNode);

    //
    // Look through the children and add any that are necessary
    std::vector<std::shared_ptr<SchemaItem>> cfgChildren;
    pCfgItem->getChildren(cfgChildren);
    for (auto childIt = cfgChildren.begin(); childIt != cfgChildren.end(); ++childIt)
    {
        int numReq = (*childIt)->getMinInstances();
        for (int i = 0; i<numReq; ++i)
        {
            std::pair<std::string, std::string> empty;
            addNewEnvironmentNode(pNewEnvNode, *childIt, status, empty);
        }
    }

    return pNewEnvNode;
}


void EnvironmentMgr::insertExtraEnvironmentData(std::shared_ptr<EnvironmentNode> pParentNode)
{
    std::string insertData = pParentNode->getEnvironmentInsertData();
    if (!insertData.empty())
    {
        std::istringstream extraData(insertData);
        std::vector<std::shared_ptr<EnvironmentNode>> extraNodes = doLoadEnvironment(extraData, pParentNode->getSchemaItem());  // not root
        for (auto &&envNode : extraNodes)
        {
            assignNodeIds(envNode);
            pParentNode->addChild(envNode);  // link extra node data to the newly created node
            pParentNode->clearEnvironmentInsertData();
        }
    }

    std::vector<std::shared_ptr<EnvironmentNode>> childNodes;
    pParentNode->getChildren(childNodes);
    for (auto &&child : childNodes)
    {
        insertExtraEnvironmentData(child);
    }
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


void EnvironmentMgr::assignNodeIds(const std::shared_ptr<EnvironmentNode> &pNode)
{
    pNode->setId(getUniqueKey());
    addPath(pNode);
    std::vector<std::shared_ptr<EnvironmentNode>> children;
    pNode->getChildren(children);
    for (auto it=children.begin(); it!=children.end(); ++it)
    {
        assignNodeIds(*it);
    }
}


void EnvironmentMgr::fetchNodes(const std::string path, std::vector<std::shared_ptr<EnvironmentNode>> &nodes, const std::shared_ptr<EnvironmentNode> &pStartNode) const
{
    const std::shared_ptr<EnvironmentNode> pStart = (pStartNode != nullptr) ? pStartNode : m_pRootNode;
    pStart->fetchNodes(path, nodes);
}
