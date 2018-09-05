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
#include "Utils.hpp"
#include <dlfcn.h>

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


EnvironmentMgr::EnvironmentMgr() :
    m_message("Unknown error")
{
    m_pSchema = std::make_shared<SchemaItem>("root");  // make the root
}


bool EnvironmentMgr::loadSchema(const std::string &configPath, const std::string &masterConfigFile, const std::map<std::string, std::string> &cfgParms)
{
    bool rc = false;
    if (createParser())
    {
        rc = m_pSchemaParser->parse(configPath, masterConfigFile, cfgParms);
        if (rc)
        {
            try {
                // unique attribure value sets are global across a schema. Allocate one here and pass it in
                // for use in building the necessary references and dependencies across the schema, then pass
                // it to the post processing for finalization. Once references and dependencies are built, the
                // attribute value sets are no longer needed.
                std::map<std::string, std::vector<std::shared_ptr<SchemaValue>>> uniqueAttributeValueSets;
                m_pSchema->processDefinedUniqueAttributeValueSets(uniqueAttributeValueSets);  // This must be done first
                m_pSchema->postProcessConfig(uniqueAttributeValueSets);
            }
            catch (ParseException &pe)
            {
                m_message = pe.what();
                rc = false;
            }
        }

        //
        // Load any support libs that are environment specific
        auto findIt = cfgParms.find("support_libs");
        if (findIt != cfgParms.end())
        {
            std::vector<std::string> supportLibNames = splitString(findIt->second, ",");
            for (auto &libName: supportLibNames)
            {
                std::string fullName = libName + ".so";

                try
                {
                    std::shared_ptr<EnvSupportLib> pLib = std::make_shared<EnvSupportLib>(fullName, this);
                    m_supportLibs.push_back(pLib);
                }
                catch (ParseException &pe)
                {
                    m_message = pe.what();
                    rc = false;
                }
            }
        }

    }
    return rc;
}


std::string EnvironmentMgr::getLastSchemaMessage() const
{
    std::string msg;
    if (m_pSchemaParser)
        msg = m_pSchemaParser->getLastMessage();
    if (msg.empty())
        msg = m_message;
    return msg;
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


std::shared_ptr<EnvironmentNode> EnvironmentMgr::findEnvironmentNodeById(const std::string &nodeId) const
{
    std::shared_ptr<EnvironmentNode> pNode;
    auto pathIt = m_nodeIds.find(nodeId);
    if (pathIt != m_nodeIds.end())
        pNode = pathIt->second;
    return pNode;
}


std::shared_ptr<EnvironmentNode> EnvironmentMgr::getNewEnvironmentNode(const std::string &parentNodeId, const std::string &inputItemType, Status &status) const
{
    std::shared_ptr<EnvironmentNode> pNewEnvNode;
    std::shared_ptr<EnvironmentNode> pParentNode = findEnvironmentNodeById(parentNodeId);
    if (pParentNode)
    {
        std::string itemType;
        std::vector<NameValue> initAttributeValues;
        getInitAttributesFromItemType(inputItemType, itemType, initAttributeValues);
        std::shared_ptr<SchemaItem> pNewCfgItem = findInsertableItem(pParentNode, itemType);
        if (pNewCfgItem)
        {
            pNewEnvNode = std::make_shared<EnvironmentNode>(pNewCfgItem, pNewCfgItem->getProperty("name"), pParentNode);
            pNewEnvNode->initialize();
            pNewEnvNode->setAttributeValues(initAttributeValues, status, false, false);
        }
        else
        {
            status.addMsg(statusMsg::error, "Configuration type (" + inputItemType + ") not found");
        }
    }
    return pNewEnvNode;
}



std::shared_ptr<EnvironmentNode> EnvironmentMgr::addNewEnvironmentNode(const std::string &parentNodeId, const std::string &inputItemType,
                                                                       std::vector<NameValue> &initAttributes, Status &status)
{
    std::shared_ptr<EnvironmentNode> pNewNode;
    std::shared_ptr<EnvironmentNode> pParentNode = findEnvironmentNodeById(parentNodeId);
    if (pParentNode)
    {
        std::string itemType;
        std::vector<NameValue> initAttributeValues;
        getInitAttributesFromItemType(inputItemType, itemType, initAttributeValues);
        std::shared_ptr<SchemaItem> pNewCfgItem = findInsertableItem(pParentNode, itemType);
        if (pNewCfgItem)
        {
            pNewNode = addNewEnvironmentNode(pParentNode, pNewCfgItem, initAttributes, status);
            if (pNewNode == nullptr)
            {
                status.addMsg(statusMsg::error, "Unable to create new node for itemType: " + inputItemType);
            }
            else
            {
                pNewNode->validate(status, true, false);
            }
        }
        else
        {
            status.addMsg(statusMsg::error, "Configuration type (" + inputItemType + ") not found");
        }
    }
    else
    {
        status.addMsg(statusMsg::error, parentNodeId, "", "Unable to find indicated parent node");
    }
    return pNewNode;
}


std::shared_ptr<EnvironmentNode> EnvironmentMgr::addNewEnvironmentNode(const std::shared_ptr<EnvironmentNode> &pParentNode, const std::shared_ptr<SchemaItem> &pCfgItem,
                                                                       std::vector<NameValue> &initAttributes, Status &status)
{
    std::shared_ptr<EnvironmentNode> pNewEnvNode;

    //
    // Create the new node and add it to the parent
    pNewEnvNode = std::make_shared<EnvironmentNode>(pCfgItem, pCfgItem->getProperty("name"), pParentNode);
    pNewEnvNode->setId(EnvironmentMgr::getUniqueKey());

    addPath(pNewEnvNode);
    pNewEnvNode->initialize();
    pNewEnvNode->setAttributeValues(initAttributes, status, false, false);
    pParentNode->addChild(pNewEnvNode);

    //
    // Send a create event now that it's been added to the environment
    pCfgItem->getSchemaRoot()->processEvent("create", pNewEnvNode);

    //
    // Call any registered support libs with the event
    for (auto &libIt: m_supportLibs)
    {
        libIt->processEvent("create", m_pSchema, pNewEnvNode, status);
    }

    insertExtraEnvironmentData(m_pRootNode);

    //
    // Look through the children and add any that are necessary
    std::vector<std::shared_ptr<SchemaItem>> cfgItemChildren;
    pCfgItem->getChildren(cfgItemChildren);
    std::vector<NameValue> empty;
    for (auto &pCfgChild: cfgItemChildren)
    {
        for (int i = 0; i<pCfgChild->getMinInstances(); ++i)
        {
            addNewEnvironmentNode(pNewEnvNode, pCfgChild, empty, status);
        }
    }

    return pNewEnvNode;
}


std::shared_ptr<SchemaItem> EnvironmentMgr::findInsertableItem(const std::shared_ptr<EnvironmentNode> &pNode, const std::string &itemType) const
{
    std::shared_ptr<SchemaItem> pItem;
    std::vector<InsertableItem> insertableItems;
    pNode->getInsertableItems(insertableItems);
    for (auto &pInsertableIt: insertableItems)
    {
        if (pInsertableIt.m_pSchemaItem->getItemType() == itemType)
        {
            pItem = pInsertableIt.m_pSchemaItem;
            break;  // we found the insertable item we wanted, so time to get out
        }
    }
    return pItem;
}


void EnvironmentMgr::getInitAttributesFromItemType(const std::string &inputItemType, std::string &itemType, std::vector<NameValue> &initAttributes) const
{
    //
    // In case nothing specifed
    itemType = inputItemType;

    size_t atPos = itemType.find_first_of('@');
    if (atPos != std::string::npos)
    {
        std::vector<std::string> initAttrs = splitString(inputItemType.substr(atPos + 1), ",");
        for (auto &initAttr: initAttrs)
        {
            std::vector<std::string> kvPair = splitString(initAttr, "=");
            if (kvPair.size() == 2)
            {
                initAttributes.emplace_back(NameValue(kvPair[0], kvPair[1]));
            }
            else
            {
                throw (ParseException("Invalid attribute initialization detected: " + initAttr));
            }
        }
    }
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
    std::shared_ptr<EnvironmentNode> pNode = findEnvironmentNodeById(nodeId);

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
