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

#ifndef _WSCONFIGMGR_HPP_
#define _WSCONFIGMGR_HPP_

#include "ws_configmgr.hpp"
#include "ws_configmgr_esp.ipp"
#include <string>
#include "XSDSchemaParser.hpp"
#include "EnvironmentMgr.hpp"
#include "ws_configmgrSession.hpp"


class Status;

class Cws_configMgrEx : public Cws_configmgr
{
public:
    IMPLEMENT_IINTERFACE

    Cws_configMgrEx();
    virtual ~Cws_configMgrEx();

    virtual bool onGetNode(IEspContext &context, IEspGetNodeRequest &req, IEspGetNodeResponse &resp);
    virtual bool onSetValues(IEspContext &context, IEspSetValuesRequest &req, IEspStatusResponse &resp);
    virtual bool onGetParents(IEspContext &context, IEspGetParentsRequest &req, IEspGetParentsResponse &resp);
    virtual bool onInsertNode(IEspContext &context, IEspInsertNodeRequest &req, IEspGetNodeResponse &resp);
    virtual bool onGetCreateNodeInfo(IEspContext &context, IEspGetCreateNodeInfoRequest &req, IEspGetCreateNodeInfoResponse &resp);
    virtual bool onRemoveNode(IEspContext &context, IEspRemoveNodeRequest &req, IEspStatusResponse &resp);

    virtual bool onOpenSession(IEspContext &context, IEspOpenSessionRequest &req, IEspOpenSessionResponse &resp);
    virtual bool onCloseSession(IEspContext &context, IEspCloseSessionRequest &req, IEspEmptyResponse &resp);
    virtual bool onGetEnvironmentFileList(IEspContext &context, IEspGetEnvironmentFileListRequest &req, IEspGetEnvironmentListResponse &resp);
    virtual bool onOpenEnvironmentFile(IEspContext &context, IEspOpenEnvironmentFileRequest &req, IEspOpenEnvironmentFileResponse &resp);
    virtual bool onCloseEnvironmentFile(IEspContext &context, IEspCloseEnvironmentFileRequest &req, IEspEmptyResponse &resp);
    virtual bool onSaveEnvironmentFile(IEspContext &context, IEspSaveEnvironmentFileRequest &req, IEspEmptyResponse &resp);
    virtual bool onLockSession(IEspContext &context, IEspLockSessionRequest &req, IEspLockSessionResponse &resp);
    virtual bool onUnlockSession(IEspContext &context, IEspUnlockSessionRequest &req, IEspEmptyResponse &resp);
    virtual bool onValidateEnvironment(IEspContext &context, IEspValidateEnvironmentRequest &req, IEspStatusResponse &resp);
    virtual bool onGetOpenSessions(IEspContext &context, IEspListOpenSessionsRequest &req, IEspListOpenSessionsResponse &resp);
    virtual bool onGetNodeTree(IEspContext &context, IEspGetTreeRequest &req, IEspGetTreeResponse &resp);
    virtual bool onFetchNodes(IEspContext &context, IEspFetchNodesRequest &req, IEspFetchNodesResponse &resp);
    virtual bool onGetNodeCopy(IEspContext &context, IEspGetNodeCopyRequest &req, IEspGetNodeCopyResponse &resp);
    virtual bool onInsertNodeCopy(IEspContext &context, IEspPasteNodeCopyRequest &req, IEspPasteNodeCopyResponse &resp);


private:

    void buildStatusResponse(const Status &status, ConfigMgrSession *pSession, IEspStatusType &respStatus) const;
    ConfigMgrSession *getConfigSession(const std::string &sessionId, bool environmentRequired = false);
    ConfigMgrSession *getConfigSessionForUpdate(const std::string &sessionId, const std::string &lockKey);
    bool deleteConfigSession(const std::string &sessionId);
    void getNodeResponse(const std::shared_ptr<EnvironmentNode> &pNode, IEspGetNodeResponse &resp) const;
    void getCreateNodeInfoResponse(const std::shared_ptr<EnvironmentNode> &pNode, IEspGetCreateNodeInfoResponse &resp) const;
    void getNodeInfo(const std::shared_ptr<EnvironmentNode> &pNode, IEspNodeInfoType &nodeInfo) const;
    void getNodeInfo(const std::shared_ptr<SchemaItem> &pNodeSchemaItem, IEspNodeInfoType &nodeInfo) const;
    void getAttributes(const std::shared_ptr<EnvironmentNode> &pEnvNode, IArrayOf<IEspAttributeType> &nodeAttributes, bool includeMissing = false) const;
    void getNodeDisplayName(const std::shared_ptr<EnvironmentNode> &pNode, std::string &nodeDisplayName) const;
    void getNodeParents(const std::string &nodeId, ConfigMgrSession *pSession, StringArray &parentNodeIds) const;
    void getNodeTree(const std::shared_ptr<EnvironmentNode> &pNode, IEspTreeElementType &treeElement, int levels, bool includeAttributes) const;


private:

    std::map<std::string, ConfigMgrSession *> m_sessions;
    unsigned m_sessionKey;
};

#endif // _WSCONFIGMGR_HPP_
