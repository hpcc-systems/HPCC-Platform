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

#include "ws_config2Service.hpp"
#include "jfile.hpp"
#include "SchemaItem.hpp"
#include "InsertableItem.hpp"
#include "jexcept.hpp"
#include "ws_config2Error.hpp"
#include "Exceptions.hpp"

static const std::string CFG2_MASTER_CONFIG_FILE = "environment.xsd";
static const std::string CFG2_CONFIG_DIR = COMPONENTFILES_DIR PATHSEPSTR "configschema" PATHSEPSTR "xsd" PATHSEPSTR;
static const std::string CFG2_PLUGINS_DIR = COMPONENTFILES_DIR PATHSEPSTR "configschema" PATHSEPSTR "xsd" PATHSEPSTR "plugins" PATHSEPSTR;
static const std::string CFG2_SOURCE_DIR = CONFIG_SOURCE_DIR;
static const std::string ACTIVE_ENVIRONMENT_FILE = CONFIG_DIR PATHSEPSTR ENV_XML_FILE;

Cws_config2Ex::Cws_config2Ex()
{
    m_sessionKey = 0;
}


Cws_config2Ex::~Cws_config2Ex()
{
}


bool Cws_config2Ex::onOpenSession(IEspContext &context, IEspOpenSessionRequest &req, IEspOpenSessionResponse &resp)
{
    bool loaded = false;
    ConfigMgrSession *pNewSession = new ConfigMgrSession();

    std::string inputMasterFile = req.getMasterSchemaFile();
    std::string inputSchemaPath = req.getSchemaPath();
    std::string inputSourcePath = req.getSourcePath();
    std::string inputActivePath = req.getActivePath();
    pNewSession->masterConfigFile = !inputMasterFile.empty() ? inputMasterFile : CFG2_MASTER_CONFIG_FILE;
    pNewSession->username = req.getUsername();
    pNewSession->schemaPath = !inputSchemaPath.empty() ? inputSchemaPath : CFG2_CONFIG_DIR;
    pNewSession->sourcePath = !inputSourcePath.empty() ? inputSourcePath : CFG2_SOURCE_DIR;
    pNewSession->activePath = !inputActivePath.empty() ? inputActivePath : ACTIVE_ENVIRONMENT_FILE;

    //
    // Make sure paths end with a separator
    if (std::string(1, pNewSession->schemaPath.back()) != PATHSEPSTR)
        pNewSession->schemaPath += PATHSEPSTR;

    if (std::string(1, pNewSession->sourcePath.back()) != PATHSEPSTR)
        pNewSession->sourcePath += PATHSEPSTR;

    //
    // Only XML supported at this time
    pNewSession->configType = XML;

    //
    // Open the session by loading the schema, which is done during session init (build cfg parms as well)
    std::map<std::string, std::string> cfgParms;
    cfgParms["buildset"] = "buildset.xml";  // Note that this is hardcoded for now, when other types suppored, must be passed in

    std::string inputSupportLibs = req.getSupportLibs();
    cfgParms["support_libs"] = inputSupportLibs.empty() ? "libcfgsupport_addrequiredinstances" : inputSupportLibs;
    std::string pluginsPath = req.getPluginPaths();
    cfgParms["plugin_paths"] = !pluginsPath.empty() ? pluginsPath : CFG2_PLUGINS_DIR;

    if (pNewSession->initializeSession(cfgParms))
    {
        std::string sessionId = std::to_string(m_sessionKey);
        resp.setSessionId(sessionId.c_str());
        m_sessions[sessionId] = pNewSession;
        m_sessionKey++;
    }
    else
    {
        std::string errMsg = pNewSession->getLastMsg();
        delete pNewSession;
        throw MakeStringException(CFGMGR_ERROR_SESSION_NOT_CREATED, "Error creating session, error: %s", errMsg.c_str());
    }
    return true;
}


bool Cws_config2Ex::onCloseSession(IEspContext &context, IEspCloseSessionRequest &req, IEspEmptyResponse &resp)
{
    std::string sessionId = req.getSessionId();
    ConfigMgrSession *pSession = getConfigSession(sessionId);

    if (pSession->modified)
    {
        if (!req.getForceClose())
        {
            throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_MODIFIED, "Current environment is modified, either save or close it first");
        }
    }

    deleteConfigSession(sessionId);
    return true;
}


bool Cws_config2Ex::onGetOpenSessions(IEspContext &context, IEspListOpenSessionsRequest &req, IEspListOpenSessionsResponse &resp)
{
    IArrayOf<IEspOpenSessionInfo> openSessions;
    for (auto sessionIt=m_sessions.begin(); sessionIt != m_sessions.end(); ++sessionIt)
    {
        ConfigMgrSession *pSession = sessionIt->second;
        Owned<IEspOpenSessionInfo> pSessionInfo = createOpenSessionInfo();
        pSessionInfo->setUsername(pSession->username.c_str());
        pSessionInfo->setCurEnvironmentFile(pSession->curEnvironmentFile.c_str());
        pSessionInfo->setLocked(pSession->locked);
        pSessionInfo->setModified(pSession->modified);
        openSessions.append(*pSessionInfo.getLink());
    }
    resp.setOpenSessions(openSessions);
    return true;
}


bool Cws_config2Ex::onGetEnvironmentFileList(IEspContext &context, IEspGetEnvironmentFileListRequest &req, IEspGetEnvironmentListResponse &resp)
{
    std::string sessionId = req.getSessionId();
    ConfigMgrSession *pSession = getConfigSession(sessionId);

    //
    // Calculate md5 checksum of the current active environment for use in comparing against the available list
    // of environments so the active environment is identified in the returned list
    StringBuffer activeConfig_md5sum;
    md5_filesum(pSession->activePath.c_str(), activeConfig_md5sum);
    IArrayOf<IEspEnvironmentFileType> environmentFiles;
    Owned<IFile> pDir = createIFile(pSession->sourcePath.c_str());
    if (pDir->exists())
    {
        Owned<IDirectoryIterator> it = pDir->directoryFiles(nullptr, false, true);
        ForEach(*it)
        {
            StringBuffer filename;
            it->getName(filename);
            String str(filename);
            str.toLowerCase();
            if (str.endsWith(pSession->getEnvironmentFileExtension().c_str()))
            {
                Owned<IEspEnvironmentFileType> pEnvFile = createEnvironmentFileType();
                pEnvFile->setFilename(filename.str());
                //
                // See if active
                StringBuffer curEnvFile_md5sum;
                std::string fullPath;
                std::string fname = filename.str();
                pSession->getEnvironmentFullyQualifiedPath(fname, fullPath);
                md5_filesum(fullPath.c_str(), curEnvFile_md5sum);
                if (strcmp(curEnvFile_md5sum.str(),activeConfig_md5sum.str()) == 0)
                {
                    pEnvFile->setIsActive(true);
                }
                environmentFiles.append(*pEnvFile.getLink());
            }
        }
        resp.setEnvironmentFiles(environmentFiles);
    }

    return true;
}


bool Cws_config2Ex::onOpenEnvironmentFile(IEspContext &context, IEspOpenEnvironmentFileRequest &req, IEspOpenEnvironmentFileResponse &resp)
{
    bool doOpen = false;
    ConfigMgrSession *pSession = getConfigSession(req.getSessionId());

    //
    // See if modified (which can only be true if an environment is currently loaded)
    if (pSession->modified)
    {
        throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_MODIFIED, "Current environment is modified, either save or close it first");
    }

    std::string newEnvFile = req.getFilename();
    if (!pSession->loadEnvironment(newEnvFile))
    {
        throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_NOT_LOADED, "Unable to load environment, error = %s", pSession->getLastMsg().c_str());
    }

    resp.setRootNodeId(pSession->m_pEnvMgr->getRootNodeId().c_str());

    return true;
}


bool Cws_config2Ex::onCloseEnvironmentFile(IEspContext &context, IEspCloseEnvironmentFileRequest &req, IEspEmptyResponse &resp)
{
    bool doClose = false;
    ConfigMgrSession *pSession = getConfigSession(req.getSessionId());

    if (pSession->modified)
    {
        pSession = getConfigSessionForUpdate(req.getSessionId(), req.getSessionLockKey());  // forces the lock key check

        //
        // Since modified, only allow close if discard changes is set
        if (!req.getDiscardChanges())
        {
            throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_MODIFIED, "Current environment is modified, either save, close, or set discardChanges");
        }
    }

    pSession->closeEnvironment();
    return true;
}


bool Cws_config2Ex::onSaveEnvironmentFile(IEspContext &context, IEspSaveEnvironmentFileRequest &req, IEspEmptyResponse &resp)
{
    // todo: If this envronment file is loaded by any other session, go mark that session that the environment has changed
    // and don't allow a save w/o reloading first. maybe add a reload request. Add relevant errors.
    // also make sure if filename is specified that it
    //  1. does not match the current loaded environment if current environment is modified and lock key is missing or does not match
    //  2. does not match the name of any other open session's loaded and locked environment.
    std::string sessionId = req.getSessionId();
    ConfigMgrSession *pSession = getConfigSession(sessionId);  // only verify session ID, lock key verified below

    std::string saveFilename = req.getFilename();
    bool doSave = true;
    bool wasSaved = false;

    //
    // If a new filename is given, then search existing sessions to see if the filename matches any open
    // environments that have locked it. If so, prevent the save.
    if (!saveFilename.empty())
    {
        for (auto sessionIt = m_sessions.begin(); sessionIt != m_sessions.end(); ++sessionIt)
        {
            if (sessionIt->second->curEnvironmentFile == saveFilename && sessionIt->second->locked)
            {
                throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_LOCKED, "Target environment is currently locked by another session");
            }
        }
    }

    //
    // If session if modified and saving over exiting environment, check lock key
    if (saveFilename.empty() && pSession->modified)
    {
        pSession = getConfigSessionForUpdate(req.getSessionLockKey(), req.getSessionLockKey());
    }

    if (!pSession->saveEnvironment(saveFilename))
    {
        throw MakeStringException(CFGMGR_ERROR_SAVE_ENVIRONMENT, "There was a problem saving the environment");
    }

    //
    // Save was completed, if a new filename was given, then search existing sessions to see if the filename
    // matches any open environments. If so, mark the session as having the environment externally modified
    // which prevents that session from locking the environment. Note that another session having the
    // environment locked would have prevented this session from locking the same environment.
    if (!saveFilename.empty())
    {
        for (auto sessionIt = m_sessions.begin(); sessionIt != m_sessions.end(); ++sessionIt)
        {
            if (sessionIt->second->curEnvironmentFile == saveFilename)
            {
                sessionIt->second->externallyModified = true;
            }
        }
    }

    return true;
}


bool Cws_config2Ex::onLockSession(IEspContext &context, IEspLockSessionRequest &req, IEspLockSessionResponse &resp)
{
    std::string sessionId = req.getSessionId();
    ConfigMgrSession *pSession = getConfigSession(sessionId, true);
    if (pSession->locked)
    {
        throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_LOCKED, "Current enironment already locked");
    }

    if (pSession->externallyModified)
    {
        throw MakeStringException(CFGMGR_ERROR_ENV_EXTERNAL_CHANGE, "No environment loaded");
    }

    //
    // Search existing sessions to see if any currently have this environment file locked
    for (auto sessionIt = m_sessions.begin(); sessionIt != m_sessions.end(); ++sessionIt)
    {
        if (sessionIt->second != pSession && sessionIt->second->locked && pSession->curEnvironmentFile == sessionIt->second->curEnvironmentFile)
        {
            throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_LOCKED, "Environment is locked by another session");
        }
    }

    if (pSession->lock())
    {
        resp.setSessionLockKey(pSession->lockKey.c_str());
    }
    else
    {
        throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_LOCKING, "Error locking the session");
    }

    return true;
}


bool Cws_config2Ex::onUnlockSession(IEspContext &context, IEspUnlockSessionRequest &req, IEspEmptyResponse &resp)
{
    ConfigMgrSession *pSession = getConfigSessionForUpdate(req.getSessionId(), req.getSessionLockKey());

    if (pSession->modified && req.getRejectIfModified())
    {
        throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_LOCKING, "Error unlocking the session, environment has been modified. Please save first.");
    }

    if (!pSession->unlock(req.getSessionLockKey()))
    {
        throw MakeStringException(CFGMGR_ERROR_ENVIRONMENT_LOCKING, "Error unlocking the session");
    }

    return true;
}


bool Cws_config2Ex::onGetNode(IEspContext &context, IEspGetNodeRequest &req, IEspGetNodeResponse &resp)
{
    std::string sessionId = req.getSessionId();
    std::string id = req.getNodeId();
    ConfigMgrSession *pSession = getConfigSession(sessionId, true);
    Status status;

    EnvironmentMgr *pEnvMgr = pSession->m_pEnvMgr;
    std::shared_ptr<EnvironmentNode> pNode = pEnvMgr->findEnvironmentNodeById(id);
    if (pNode == nullptr)
    {
        throw MakeStringException(CFGMGR_ERROR_NODE_INVALID, "Environment node ID is not valid");
    }

    getNodeResponse(pNode, resp);
    pNode->validate(status, false);  // validate this node only
    buildStatusResponse(status, pSession, resp.updateStatus());

    //
    // Finalize the response
    resp.setNodeId(id.c_str());
    return true;
}


bool Cws_config2Ex::onGetCreateNodeInfo(IEspContext &context, IEspGetCreateNodeInfoRequest &req, IEspGetCreateNodeInfoResponse &resp)
{
    ConfigMgrSession *pSession = getConfigSession(req.getSessionId());
    Status status;

    std::string parentNodeId = req.getParentNodeId();
    std::shared_ptr<EnvironmentNode> pNode = pSession->m_pEnvMgr->findEnvironmentNodeById(parentNodeId);

    if (pNode)
    {
        std::shared_ptr<EnvironmentNode> pNewNode = pSession->m_pEnvMgr->getNewEnvironmentNode(parentNodeId, req.getNodeType(), status);
        if (pNewNode)
        {
            getCreateNodeInfoResponse(pNewNode, resp);
            pSession->modified = true;
        }
    }
    else
    {
        throw MakeStringException(CFGMGR_ERROR_NODE_INVALID, "Environment node ID is not valid");
    }

    return true;
}


bool Cws_config2Ex::onInsertNode(IEspContext &context, IEspInsertNodeRequest &req, IEspGetNodeResponse &resp)
{
    ConfigMgrSession *pSession = getConfigSessionForUpdate(req.getSessionId(), req.getSessionLockKey());
    Status status;

    std::string parentNodeId = req.getParentNodeId();

    if (pSession->m_pEnvMgr->findEnvironmentNodeById(parentNodeId))
    {
        std::vector<NameValue> initAttributes;

        bool forceCreate = req.getForceCreate();
        bool allowInvalid = req.getAllowInvalid();
        IArrayOf<IConstAttributeValueType> &attrbuteValues = req.getAttributeValues();

        ForEachItemIn(i, attrbuteValues)
        {
            IConstAttributeValueType& attrVal = attrbuteValues.item(i);
            initAttributes.emplace_back(NameValue(attrVal.getName(), attrVal.getValue()));
        }

        std::shared_ptr<EnvironmentNode> pNewNode = pSession->m_pEnvMgr->addNewEnvironmentNode(parentNodeId, req.getNodeType(), initAttributes, status);

        if (pNewNode)
        {
            getNodeResponse(pNewNode, resp);
            resp.setNodeId(pNewNode->getId().c_str());
            pSession->modified = true;
        }
    }
    else
    {
        throw MakeStringException(CFGMGR_ERROR_NODE_INVALID, "Environment node ID is not valid");
    }

    buildStatusResponse(status, pSession, resp.updateStatus());
    return true;
}


bool Cws_config2Ex::onRemoveNode(IEspContext &context, IEspRemoveNodeRequest &req, IEspStatusResponse &resp)
{
    std::string sessionId = req.getSessionId();
    std::string key = req.getSessionLockKey();
    ConfigMgrSession *pSession = getConfigSessionForUpdate(sessionId, key);
    Status status;

    std::string nodeId = req.getNodeId();

    if (!pSession->m_pEnvMgr->removeEnvironmentNode(nodeId))
    {
        throw MakeStringException(CFGMGR_ERROR_NODE_INVALID, "Environment node ID is not valid");
    }

    pSession->modified = true;
    pSession->m_pEnvMgr->validate(status, false);
    buildStatusResponse(status, pSession, resp.updateStatus());
    resp.setEnvironmentModified(pSession->modified);
    return true;
}


bool Cws_config2Ex::onValidateEnvironment(IEspContext &context, IEspValidateEnvironmentRequest &req, IEspStatusResponse &resp)
{
    Status status;
    std::string sessionId = req.getSessionId();
    ConfigMgrSession *pSession = getConfigSession(sessionId, true);

    pSession->m_pEnvMgr->validate(status, req.getIncludeHiddenNodes());
    buildStatusResponse(status, pSession, resp.updateStatus());
    resp.setEnvironmentModified(pSession->modified);
    return true;
}


bool Cws_config2Ex::onSetValues(IEspContext &context, IEspSetValuesRequest &req, IEspStatusResponse &resp)
{
    Status status;
    std::string sessionId = req.getSessionId();
    std::string key = req.getSessionLockKey();
    ConfigMgrSession *pSession = getConfigSessionForUpdate(sessionId, key);

    std::string id = req.getNodeId();
    std::shared_ptr<EnvironmentNode> pNode = pSession->m_pEnvMgr->findEnvironmentNodeById(id);
    if (pNode == nullptr)
    {
        throw MakeStringException(CFGMGR_ERROR_NODE_INVALID, "Environment node ID is not valid");
    }

    bool forceCreate = req.getForceCreate();
    bool allowInvalid = req.getAllowInvalid();
    IArrayOf<IConstAttributeValueType> &attrbuteValues = req.getAttributeValues();
    std::vector<NameValue> values;

    ForEachItemIn(i, attrbuteValues)
    {
        IConstAttributeValueType& attrVal = attrbuteValues.item(i);
        values.emplace_back(NameValue(attrVal.getName(), attrVal.getValue()));
    }

    pNode->setAttributeValues(values, status, allowInvalid, forceCreate);
    pSession->modified = true;
    buildStatusResponse(status, pSession, resp.updateStatus());
    resp.setEnvironmentModified(pSession->modified);
    return true;
}


bool Cws_config2Ex::onGetParents(IEspContext &context, IEspGetParentsRequest &req, IEspGetParentsResponse &resp)
{
    std::string nodeId = req.getNodeId();
    std::string sessionId = req.getSessionId();

    ConfigMgrSession *pSession = getConfigSession(sessionId, true);

    StringArray ids;
    std::shared_ptr<EnvironmentNode> pNode = pSession->m_pEnvMgr->findEnvironmentNodeById(nodeId);
    if (pNode == nullptr)
    {
        throw MakeStringException(CFGMGR_ERROR_NODE_INVALID, "Environment node ID is not valid");
    }

    while (pNode)
    {
        pNode = pNode->getParent();
        if (pNode)
        {
            ids.append(pNode->getId().c_str());
        }
    }
    resp.setParentIdList(ids);

    return true;
}


bool Cws_config2Ex::onGetNodeTree(IEspContext &context, IEspGetTreeRequest &req, IEspGetTreeResponse &resp)
{
    std::string nodeId = req.getNodeId();
    std::string sessionId = req.getSessionId();

    ConfigMgrSession *pSession = getConfigSession(sessionId, true);

    if (nodeId.empty())
        nodeId = pSession->m_pEnvMgr->getRootNodeId();

    std::shared_ptr<EnvironmentNode> pNode = pSession->m_pEnvMgr->findEnvironmentNodeById(nodeId);
    if (pNode)
    {
        getNodeTree(pNode, resp.updateTree(), req.getNumLevels(), req.getIncludeAttributes());
    }

    return true;
}


bool Cws_config2Ex::onFetchNodes(IEspContext &context, IEspFetchNodesRequest &req, IEspFetchNodesResponse &resp)
{
    std::string sessionId = req.getSessionId();
    std::string path = req.getPath();
    std::string startingNodeId = req.getStartingNodeId();
    std::shared_ptr<EnvironmentNode> pStartingNode;
    ConfigMgrSession *pSession = getConfigSession(sessionId, true);

    if (startingNodeId != "")
    {
        if (path[0] == '/')
        {
            throw MakeStringException(CFGMGR_ERROR_PATH_INVALID, "Path may not begin at root if starting node specified");
        }

        pStartingNode = pSession->m_pEnvMgr->findEnvironmentNodeById(startingNodeId);
        if (!pStartingNode)
        {
            throw MakeStringException(CFGMGR_ERROR_NODE_INVALID, "The starting node ID is not valid");
        }
    }
    else if (path[0] != '/')
    {
        throw MakeStringException(CFGMGR_ERROR_PATH_INVALID, "Path must begin at root (/) if no starting node is specified");
    }

    try
    {
        std::vector<std::shared_ptr<EnvironmentNode>> nodes;
        pSession->m_pEnvMgr->fetchNodes(path, nodes, pStartingNode);
        StringArray ids;
        for ( auto &&pNode : nodes)
        {
            if (!pNode->getSchemaItem()->isHidden())
            {
                ids.append(pNode->getId().c_str());
            }
        }
        resp.setNodeIds(ids);
    }
    catch (ParseException &pe)
    {
        throw MakeStringException(CFGMGR_ERROR_PATH_INVALID, "%s", pe.what());
    }

    return true;
}


void Cws_config2Ex::buildStatusResponse(const Status &status, ConfigMgrSession *pSession, IEspStatusType &respStatus) const
{
    std::vector<statusMsg> statusMsgs = status.getMessages();

    IArrayOf<IEspStatusMsgType> msgs;
    for (auto msgIt=statusMsgs.begin(); msgIt!=statusMsgs.end(); ++msgIt)
    {
        Owned<IEspStatusMsgType> pStatusMsg = createStatusMsgType();
        pStatusMsg->setNodeId((*msgIt).nodeId.c_str());
        pStatusMsg->setMsg((*msgIt).msg.c_str());
        pStatusMsg->setMsgLevel(status.getStatusTypeString((*msgIt).msgLevel).c_str());
        pStatusMsg->setAttribute((*msgIt).attribute.c_str());

        if (!(*msgIt).nodeId.empty() && pSession != nullptr)
        {
            StringArray ids;
            getNodeParents((*msgIt).nodeId, pSession, ids);
            std::shared_ptr<EnvironmentNode> pNode = pSession->m_pEnvMgr->findEnvironmentNodeById((*msgIt).nodeId);
            pStatusMsg->setNodeName(pNode->getName().c_str());
            pStatusMsg->setParentIdList(ids);
        }
        msgs.append(*pStatusMsg.getLink());
    }

    respStatus.setStatusMessages(msgs);
    respStatus.setError(status.isError());
}


ConfigMgrSession *Cws_config2Ex::getConfigSession(const std::string &sessionId, bool environmentRequired)
{
    ConfigMgrSession *pSession = nullptr;

    if (sessionId.empty())
        throw MakeStringException(CFGMGR_ERROR_MISSING_SESSION_ID, "Session ID required");

    auto it = m_sessions.find(sessionId);
    if (it != m_sessions.end())
    {
        pSession = (it->second);
    }

    if (pSession == nullptr)
        throw MakeStringException(CFGMGR_ERROR_INVALID_SESSION_ID, "Session ID not valid");

    if (environmentRequired && pSession->curEnvironmentFile.empty())
        throw MakeStringException(CFGMGR_ERROR_NO_ENVIRONMENT, "No environment loaded");

    return pSession;
}


ConfigMgrSession *Cws_config2Ex::getConfigSessionForUpdate(const std::string &sessionId, const std::string &lockKey)
{
    ConfigMgrSession *pSession = getConfigSession(sessionId, true);
    if (!pSession->doesKeyFit(lockKey))
    {
        throw MakeStringException(CFGMGR_ERROR_LOCK_KEY_INVALID, "Session lock key is missing or invalid");
    }
    return pSession;
}


bool Cws_config2Ex::deleteConfigSession(const std::string &sessionId)
{
    bool rc = false;
    ConfigMgrSession *pSession = getConfigSession(sessionId);
    if (pSession)
    {
        m_sessions.erase(sessionId);
        delete pSession;
        rc = true;
    }
    return rc;
}


void Cws_config2Ex::getNodeResponse(const std::shared_ptr<EnvironmentNode> &pNode, IEspGetNodeResponse &resp) const
{
    const std::shared_ptr<SchemaItem> &pNodeSchemaItem = pNode->getSchemaItem();
    std::string nodeDisplayName;

    resp.setNodeId(pNode->getId().c_str());

    //
    // Fill in base node info struct
    getNodeInfo(pNode, resp.updateNodeInfo());

    //
    // Handle the attributes
    IArrayOf<IEspAttributeType> nodeAttributes;
    if (pNode->hasAttributes())
    {
        getAttributes(pNode, nodeAttributes, true);
    }
    resp.setAttributes(nodeAttributes);

    //
    // Now the children
    IArrayOf<IEspNode> childNodes;
    if (pNode->hasChildren())
    {
        std::vector<std::shared_ptr<EnvironmentNode>> children;
        pNode->getChildren(children);
        for (auto it=children.begin(); it!=children.end(); ++it)
        {
            std::shared_ptr<EnvironmentNode> pChildEnvNode = *it;
            const std::shared_ptr<SchemaItem> pSchemaItem = pChildEnvNode->getSchemaItem();
            Owned<IEspNode> pChildNode = createNode();
            getNodeInfo(pChildEnvNode, pChildNode->updateNodeInfo());
            pChildNode->setNodeId(pChildEnvNode->getId().c_str());
            pChildNode->setNumChildren(pChildEnvNode->getNumChildren());
            childNodes.append(*pChildNode.getLink());
        }
    }
    resp.setChildren(childNodes);

    //
    // Build a list of items that can be inserted under this node
    IArrayOf<IEspInsertItemType> newNodes;
    std::vector<InsertableItem> insertableList;
    pNode->getInsertableItems(insertableList);
    for (auto &insertableItem: insertableList)
    {
        bool addItem = true;
        std::shared_ptr<SchemaItem> pSchemaItem = insertableItem.m_pSchemaItem;
        Owned<IEspInsertItemType> pInsertInfo = createInsertItemType();
        pInsertInfo->setName(pSchemaItem->getProperty("displayName").c_str());
        pInsertInfo->setNodeType(pSchemaItem->getItemType().c_str());
        pInsertInfo->setClass(pSchemaItem->getProperty("className").c_str());
        pInsertInfo->setCategory(pSchemaItem->getProperty("category").c_str());
        pInsertInfo->setRequired(pSchemaItem->isRequired());
        pInsertInfo->setTooltip(pSchemaItem->getProperty("tooltip").c_str());
        if (insertableItem.m_limitChoices)
        {
            pInsertInfo->setFixedChoices(true);
            IArrayOf<IEspChoiceLimitType> fixedChoices;
            for (auto &fc: insertableItem.m_itemLimits)
            {
                Owned<IEspChoiceLimitType> pChoice = createChoiceLimitType();
                pChoice->setDisplayName(fc.itemName.c_str());
                std::string itemType = pSchemaItem->getItemType() + "@" + fc.attributeName + "=" + fc.attributeValue;
                pChoice->setItemType(itemType.c_str());
                fixedChoices.append(*pChoice.getLink());
            }
            pInsertInfo->setChoiceList(fixedChoices);
            addItem = fixedChoices.ordinality() != 0;
        }

        if (addItem)
        {
            newNodes.append(*pInsertInfo.getLink());
        }
    }
    resp.setInsertable(newNodes);

    if (pNodeSchemaItem->isItemValueDefined())
    {
        resp.setLocalValueDefined(true);

        const std::shared_ptr<SchemaValue> &pNodeSchemaValue = pNodeSchemaItem->getItemSchemaValue();
        const std::shared_ptr<SchemaType> &pType = pNodeSchemaValue->getType();
        resp.updateValue().updateType().setBaseType(pType->getBaseType().c_str());
        resp.updateValue().updateType().setSubType(pType->getSubType().c_str());
        resp.updateValue().setRequired(pNodeSchemaValue->isRequired());
        resp.updateValue().setReadOnly(pNodeSchemaValue->isReadOnly());
        resp.updateValue().setHidden(pNodeSchemaValue->isHidden());

        std::shared_ptr<SchemaTypeLimits> &pLimits = pType->getLimits();
        if (pLimits->isMaxSet())
        {
            resp.updateValue().updateType().updateLimits().setMaxValid(true);
            resp.updateValue().updateType().updateLimits().setMax(pLimits->getMax());
        }
        if (pLimits->isMinSet())
        {
            resp.updateValue().updateType().updateLimits().setMinValid(true);
            resp.updateValue().updateType().updateLimits().setMin(pLimits->getMin());
        }

        if (pNode->isLocalValueSet())
        {
            const std::shared_ptr<EnvironmentValue> &pLocalValue = pNode->getLocalEnvValue();
            resp.updateValue().setCurrentValue(pLocalValue->getValue().c_str());
        }
    }
}


void Cws_config2Ex::getCreateNodeInfoResponse(const std::shared_ptr<EnvironmentNode> &pNode, IEspGetCreateNodeInfoResponse &resp) const
{
    const std::shared_ptr<SchemaItem> &pNodeSchemaItem = pNode->getSchemaItem();
    std::string nodeDisplayName;

    //
    // Fill in base node info struct
    getNodeInfo(pNode, resp.updateNodeInfo());

    //
    // Handle the attributes
    IArrayOf<IEspAttributeType> nodeAttributes;
    if (pNode->hasAttributes())
    {
        getAttributes(pNode, nodeAttributes, true);
    }
    resp.setAttributes(nodeAttributes);

    if (pNodeSchemaItem->isItemValueDefined())
    {
        resp.setLocalValueDefined(true);

        const std::shared_ptr<SchemaValue> &pNodeSchemaValue = pNodeSchemaItem->getItemSchemaValue();
        const std::shared_ptr<SchemaType> &pType = pNodeSchemaValue->getType();
        resp.updateValue().updateType().setBaseType(pType->getBaseType().c_str());
        resp.updateValue().updateType().setSubType(pType->getSubType().c_str());
        resp.updateValue().setRequired(pNodeSchemaValue->isRequired());
        resp.updateValue().setReadOnly(pNodeSchemaValue->isReadOnly());
        resp.updateValue().setHidden(pNodeSchemaValue->isHidden());

        std::shared_ptr<SchemaTypeLimits> &pLimits = pType->getLimits();
        if (pLimits->isMaxSet())
        {
            resp.updateValue().updateType().updateLimits().setMaxValid(true);
            resp.updateValue().updateType().updateLimits().setMax(pLimits->getMax());
        }
        if (pLimits->isMinSet())
        {
            resp.updateValue().updateType().updateLimits().setMinValid(true);
            resp.updateValue().updateType().updateLimits().setMin(pLimits->getMin());
        }

        if (pNode->isLocalValueSet())
        {
            const std::shared_ptr<EnvironmentValue> &pLocalValue = pNode->getLocalEnvValue();
            resp.updateValue().setCurrentValue(pLocalValue->getValue().c_str());
        }
    }
}



void Cws_config2Ex::getNodeInfo(const std::shared_ptr<EnvironmentNode> &pNode, IEspNodeInfoType &nodeInfo) const
{
    const std::shared_ptr<SchemaItem> &pNodeSchemaItem = pNode->getSchemaItem();
    std::string nodeDisplayName;
    //
    // Fill in base node info struct
    getNodeInfo(pNodeSchemaItem, nodeInfo);      // fill it in based on schema
    getNodeDisplayName(pNode, nodeDisplayName);  // possibly override the displayname
    nodeInfo.setDisplayName(nodeDisplayName.c_str());
    nodeInfo.setName(pNode->getName().c_str());
}


void Cws_config2Ex::getNodeInfo(const std::shared_ptr<SchemaItem> &pNodeSchemaItem, IEspNodeInfoType &nodeInfo) const
{
    //
    // Fill in base node info struct
    std::string displayName = pNodeSchemaItem->getProperty("displayName");
    nodeInfo.setDisplayName(displayName.c_str());
    nodeInfo.setNodeType(pNodeSchemaItem->getItemType().c_str());
    nodeInfo.setClass(pNodeSchemaItem->getProperty("className").c_str());
    nodeInfo.setTooltip(pNodeSchemaItem->getProperty("tooltip").c_str());
    nodeInfo.setHidden(pNodeSchemaItem->isHidden());

    std::string category = pNodeSchemaItem->getProperty("category");
    nodeInfo.setCategory((!category.empty()) ? category.c_str() : displayName.c_str());
}


void Cws_config2Ex::getAttributes(const std::shared_ptr<EnvironmentNode> &pEnvNode, IArrayOf<IEspAttributeType> &nodeAttributes, bool includeMissing) const
{
    std::vector<std::shared_ptr<SchemaValue>> schemaValues;
    pEnvNode->getSchemaItem()->getAttributes(schemaValues);

    for (auto it=schemaValues.begin(); it!=schemaValues.end(); ++it)
    {
        const std::shared_ptr<SchemaValue> pSchemaValue = *it;
        std::shared_ptr<EnvironmentValue> pEnvValue = pEnvNode->getAttribute(pSchemaValue->getName());
        Owned<IEspAttributeType> pAttribute = createAttributeType();

        if (pEnvValue || includeMissing)
        {
            pAttribute->setName(pSchemaValue->getName().c_str());
            pAttribute->setDisplayName(pSchemaValue->getDisplayName().c_str());
            pAttribute->setTooltip(pSchemaValue->getTooltip().c_str());

            const std::shared_ptr<SchemaType> &pType = pSchemaValue->getType();
            std::shared_ptr<SchemaTypeLimits> &pLimits = pType->getLimits();
            pAttribute->updateType().setBaseType(pType->getBaseType().c_str());
            pAttribute->updateType().setSubType(pType->getSubType().c_str());
            if (pLimits->isMaxSet())
            {
                pAttribute->updateType().updateLimits().setMaxValid(true);
                pAttribute->updateType().updateLimits().setMax(pLimits->getMax());
            }
            if (pLimits->isMinSet())
            {
                pAttribute->updateType().updateLimits().setMinValid(true);
                pAttribute->updateType().updateLimits().setMin(pLimits->getMin());
            }
            pAttribute->setRequired(pSchemaValue->isRequired());
            pAttribute->setReadOnly(pSchemaValue->isReadOnly());
            pAttribute->setHidden(pSchemaValue->isHidden(pEnvValue.get()));
            pAttribute->setDeprecated(pSchemaValue->isDeprecated());
            std::string groupName = pSchemaValue->getGroupByName();
            pAttribute->setGroup(groupName.empty() ? "Attributes" : groupName.c_str());

            //
            // Only add allowed values if attribute is not read only
            if (!pSchemaValue->isReadOnly())
            {
                std::vector<AllowedValue> allowedValues;
                if (pSchemaValue->getAllowedValues(allowedValues, pEnvNode))
                {
                    pAttribute->updateType().updateLimits().setHasChoices(true);
                    IArrayOf<IEspChoiceType> choices;
                    if (!allowedValues.empty())
                    {
                        for (auto valueIt = allowedValues.begin(); valueIt != allowedValues.end(); ++valueIt)
                        {
                            Owned<IEspChoiceType> pChoice = createChoiceType();
                            pChoice->setDisplayName((*valueIt).m_displayName.c_str());
                            pChoice->setValue((*valueIt).m_value.c_str());
                            pChoice->setDesc((*valueIt).m_description.c_str());
                            pChoice->setMsg((*valueIt).m_userMessage.c_str());
                            pChoice->setMsgType((*valueIt).m_userMessageType.c_str());

                            //
                            // Add dependencies
                            if ((*valueIt).hasDependencies())
                            {
                                IArrayOf<IEspDependentValueType> dependencies;
                                for (auto &depIt: (*valueIt).getDependencies())
                                {
                                    Owned<IEspDependentValueType> pDep = createDependentValueType();
                                    pDep->setAttributeName(depIt.m_attribute.c_str());
                                    pDep->setAttributeValue(depIt.m_value.c_str());
                                    dependencies.append(*pDep.getLink());
                                }
                                pChoice->setDependencies(dependencies);
                            }

                            //
                            // Add optional/required attributes.
                            if (!(*valueIt).m_optionalAttributes.empty())
                            {
                                StringArray attributeNames;
                                StringBuffer atrrname;
                                for (auto &attr: (*valueIt).m_optionalAttributes)
                                {
                                    atrrname = attr.c_str();
                                    attributeNames.append(atrrname);
                                }
                                pChoice->setOptionalAttributes(attributeNames);
                            }

                            if (!(*valueIt).m_requiredAttributes.empty())
                            {
                                StringArray attributeNames;
                                StringBuffer atrrname;
                                for (auto &attr: (*valueIt).m_requiredAttributes)
                                {
                                    atrrname = attr.c_str();
                                    attributeNames.append(atrrname);
                                }
                                pChoice->setRequiredAttributes(attributeNames);
                            }

                            choices.append(*pChoice.getLink());
                        }
                    }
                    pAttribute->updateType().updateLimits().setChoiceList(choices);
                }
            }

            const std::vector<std::string> &mods = pSchemaValue->getModifiers();
            if (!mods.empty())
            {
                StringArray modifiers;
                StringBuffer modifier;
                for (auto &modIt: mods)
                {
                    modifier.set(modIt.c_str());
                    modifiers.append(modifier);
                }
                pAttribute->setModifiers(modifiers);
            }

            if (pEnvValue)
            {
                pAttribute->setCurrentValue(pEnvValue->getValue().c_str());
            }

            pAttribute->setForcedValue(pSchemaValue->getForcedValue().c_str());
            pAttribute->setPresetValue(pSchemaValue->getPresetValue().c_str());

            pAttribute->setIsPresentInEnvironment(static_cast<bool>(pEnvValue));
            nodeAttributes.append(*pAttribute.getLink());
        }
    }
}


void Cws_config2Ex::getNodeDisplayName(const std::shared_ptr<EnvironmentNode> &pNode, std::string &nodeDisplayName) const
{
    const std::shared_ptr<SchemaItem> &pNodeSchemaItem = pNode->getSchemaItem();
    nodeDisplayName = pNodeSchemaItem->getProperty("displayName");
    if (pNode->hasAttributes())
    {
        std::shared_ptr<EnvironmentValue> pAttr = pNode->getAttribute("name");
        if (pAttr && pAttr->isValueSet())
        {
            nodeDisplayName = pAttr->getValue();  // better usability value
        }
    }
}


void Cws_config2Ex::getNodeParents(const std::string &nodeId, ConfigMgrSession *pSession, StringArray &parentNodeIds) const
{
    std::shared_ptr<EnvironmentNode> pNode = pSession->m_pEnvMgr->findEnvironmentNodeById(nodeId);
    if (pNode)
    {
        while (pNode)
        {
            pNode = pNode->getParent();
            if (pNode)
            {
                parentNodeIds.append(pNode->getId().c_str());
            }
        }
    }
}

void Cws_config2Ex::getNodeTree(const std::shared_ptr<EnvironmentNode> &pNode, IEspTreeElementType &treeElement, int levels, bool includeAttributes) const
{
    //
    // Fill in this element
    treeElement.setNodeId(pNode->getId().c_str());
    getNodeInfo(pNode, treeElement.updateNodeInfo());
    if (includeAttributes && pNode->hasAttributes())
    {
        IArrayOf<IEspAttributeType> nodeAttributes;
        getAttributes(pNode, nodeAttributes, true);
        treeElement.setAttributes(nodeAttributes);
    }

    //
    // If we need to descend more levels, do so
    if (levels > 0)
    {
        --levels;

        IArrayOf<IEspTreeElementType> childNodes;
        if (pNode->hasChildren())
        {
            std::vector<std::shared_ptr<EnvironmentNode>> children;
            pNode->getChildren(children);
            for (auto it=children.begin(); it!=children.end(); ++it)
            {
                Owned<IEspTreeElementType> pTreeElement = createTreeElementType();
                getNodeTree(*it, *pTreeElement, levels, includeAttributes);
                childNodes.append(*pTreeElement.getLink());
            }
        }
        treeElement.setChildren(childNodes);
    }
}
