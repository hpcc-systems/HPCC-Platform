/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifndef _ESPWIZ_ws_access_HPP__
#define _ESPWIZ_ws_access_HPP__

#pragma warning( disable : 4786)
#include "ldapsecurity.ipp"

#include "ws_access.hpp"
#include "ws_access_esp.ipp"

class Cws_accessSoapBindingEx : public Cws_accessSoapBinding
{
    StringBuffer m_authType;

public:
    Cws_accessSoapBindingEx(IPropertyTree *cfg, const char *name, const char *process, http_soap_log_level llevel=hsl_none) : Cws_accessSoapBinding(cfg, name, process, llevel)
    {
        StringBuffer xpath;
        xpath.appendf("Software/EspProcess[@name='%s']/Authentication/@method", process);
        const char* method = cfg->queryProp(xpath);
        if (method && *method)
            m_authType.append(method);
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        IPropertyTree *folder = ensureNavFolder(data, "Users/Permissions", "Permissions");

        if (!stricmp(m_authType.str(), "none") || !stricmp(m_authType.str(), "local"))
        {
            ensureNavLink(*folder, "Users", "/ws_access/SecurityNotEnabled?form_", "Users");
            ensureNavLink(*folder, "Groups", "/ws_access/SecurityNotEnabled?form_", "Groups");
            ensureNavLink(*folder, "Permissions", "/ws_access/SecurityNotEnabled?form_", "Permissions");
        }
        else
        {
            ensureNavLink(*folder, "Users", "/ws_access/Users", "Users");
            ensureNavLink(*folder, "Groups", "/ws_access/Groups", "Groups");
            ensureNavLink(*folder, "Permissions", "/ws_access/Permissions", "Permissions");
            ensureNavLink(*folder, "FileScopes", "/ws_access/Resources?rtype=file&rtitle=FileScope", "FileScopes");
        }
    }

    virtual int onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method);
};

class Cws_accessEx : public Cws_access
{
    Owned<IPropertyTree> m_servicecfg;
    IArrayOf<IEspDnStruct> m_basedns;
    IArrayOf<IEspDnStruct> m_rawbasedns;
    SecResourceType str2type(const char* rtstr);

    void setBasedns(IEspContext &context);
    const char* getBaseDN(IEspContext &context, const char* rtype, StringBuffer& baseDN);
    bool permissionAddInputOnResource(IEspContext &context, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp);
    bool permissionAddInputOnAccount(IEspContext &context, const char* accountName, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp);
    bool getNewFileScopePermissions(ISecManager* secmgr, IEspResourceAddRequest &req, StringBuffer& existingResource, StringArray& newResources);
    bool setNewFileScopePermissions(ISecManager* secmgr, IEspResourceAddRequest &req, StringBuffer& existingResource, StringArray& newResources);
    bool permissionsReset(CLdapSecManager* ldapsecmgr, const char* basedn, const char* rtype, const char* prefix,
        const char* resourceName, ACT_TYPE accountType, const char* accountName,
        bool allow_access, bool allow_read, bool allow_write, bool allow_full,
        bool deny_access, bool deny_read, bool deny_write, bool deny_full);
    void getBaseDNsForAddingPermssionToAccount(CLdapSecManager* secmgr, const char* prefix, const char* accountName, 
        int accountType, StringArray& basednNames);
    int enableDisableScopeScans(IEspContext &context, bool doEnable, StringBuffer &retMsg);
    CLdapSecManager* queryLDAPSecurityManager(IEspContext &context);

public:
    IMPLEMENT_IINTERFACE;

    virtual void init(IPropertyTree *cfg, const char *process, const char *service);

    virtual bool onUsers(IEspContext &context, IEspUserRequest &req, IEspUserResponse &resp);
    virtual bool onUserQuery(IEspContext &context, IEspUserQueryRequest &req, IEspUserQueryResponse &resp);
    virtual bool onUserEdit(IEspContext &context, IEspUserEditRequest &req, IEspUserEditResponse &resp);
    virtual bool onGroups(IEspContext &context, IEspGroupRequest &req, IEspGroupResponse &resp);
    virtual bool onGroupQuery(IEspContext &context, IEspGroupQueryRequest &req, IEspGroupQueryResponse &resp);
    virtual bool onGroupMemberQuery(IEspContext &context, IEspGroupMemberQueryRequest &req, IEspGroupMemberQueryResponse &resp);
    virtual bool onAddUser(IEspContext &context, IEspAddUserRequest &req, IEspAddUserResponse &resp);
    virtual bool onUserAction(IEspContext &context, IEspUserActionRequest &req, IEspUserActionResponse &resp);
    virtual bool onPermissions(IEspContext &context, IEspBasednsRequest &req, IEspBasednsResponse &resp);
    virtual bool onResources(IEspContext &context, IEspResourcesRequest &req, IEspResourcesResponse &resp);
    virtual bool onResourceQuery(IEspContext &context, IEspResourceQueryRequest &req, IEspResourceQueryResponse &resp);
    virtual bool onResourceAdd(IEspContext &context, IEspResourceAddRequest &req, IEspResourceAddResponse &resp);
    virtual bool onResourceAddInput(IEspContext &context, IEspResourceAddInputRequest &req, IEspResourceAddInputResponse &resp);
    virtual bool onResourcePermissions(IEspContext &context, IEspResourcePermissionsRequest &req, IEspResourcePermissionsResponse &resp);
    virtual bool onPermissionAddInput(IEspContext &context, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp);
    virtual bool onPermissionAction(IEspContext &context, IEspPermissionActionRequest &req, IEspPermissionActionResponse &resp);
    virtual bool onUserGroupEditInput(IEspContext &context, IEspUserGroupEditInputRequest &req, IEspUserGroupEditInputResponse &resp);
    virtual bool onUserGroupEdit(IEspContext &context, IEspUserGroupEditRequest &req, IEspUserGroupEditResponse &resp);
    virtual bool onGroupAdd(IEspContext &context, IEspGroupAddRequest &req, IEspGroupAddResponse &resp);
    virtual bool onGroupAction(IEspContext &context, IEspGroupActionRequest &req, IEspGroupActionResponse &resp);
    virtual bool onGroupEdit(IEspContext &context, IEspGroupEditRequest &req, IEspGroupEditResponse &resp);
    virtual bool onGroupMemberEditInput(IEspContext &context, IEspGroupMemberEditInputRequest &req, IEspGroupMemberEditInputResponse &resp);
    virtual bool onGroupMemberEdit(IEspContext &context, IEspGroupMemberEditRequest &req, IEspGroupMemberEditResponse &resp);
    virtual bool onResourceDelete(IEspContext &context, IEspResourceDeleteRequest &req, IEspResourceDeleteResponse &resp);
    virtual bool onUserResetPass(IEspContext &context, IEspUserResetPassRequest &req, IEspUserResetPassResponse &resp);
    virtual bool onUserResetPassInput(IEspContext &context, IEspUserResetPassInputRequest &req, IEspUserResetPassInputResponse &resp);
    virtual bool onUserPosix(IEspContext &context, IEspUserPosixRequest &req, IEspUserPosixResponse &resp);
    virtual bool onUserPosixInput(IEspContext &context, IEspUserPosixInputRequest &req, IEspUserPosixInputResponse &resp);
    virtual bool onUserInfoEdit(IEspContext &context, IEspUserInfoEditRequest &req, IEspUserInfoEditResponse &resp);
    virtual bool onUserInfoEditInput(IEspContext &context, IEspUserInfoEditInputRequest &req, IEspUserInfoEditInputResponse &resp);
    virtual bool onUserSudoersInput(IEspContext &context, IEspUserSudoersInputRequest &req, IEspUserSudoersInputResponse &resp);
    virtual bool onUserSudoers(IEspContext &context, IEspUserSudoersRequest &req, IEspUserSudoersResponse &resp);
    virtual bool onAccountPermissions(IEspContext &context, IEspAccountPermissionsRequest &req, IEspAccountPermissionsResponse &resp);
    virtual bool onFilePermission(IEspContext &context, IEspFilePermissionRequest &req, IEspFilePermissionResponse &resp);
    virtual bool onPermissionsResetInput(IEspContext &context, IEspPermissionsResetInputRequest &req, IEspPermissionsResetInputResponse &resp);
    virtual bool onPermissionsReset(IEspContext &context, IEspPermissionsResetRequest &req, IEspPermissionsResetResponse &resp);
    virtual bool onUserAccountExport(IEspContext &context, IEspUserAccountExportRequest &req, IEspUserAccountExportResponse &resp);
    virtual bool onClearPermissionsCache(IEspContext &context, IEspClearPermissionsCacheRequest &req, IEspClearPermissionsCacheResponse &resp);
    virtual bool onQueryScopeScansEnabled(IEspContext &context, IEspQueryScopeScansEnabledRequest &req, IEspQueryScopeScansEnabledResponse &resp);
    virtual bool onEnableScopeScans(IEspContext &context, IEspEnableScopeScansRequest &req, IEspEnableScopeScansResponse &resp);
    virtual bool onDisableScopeScans(IEspContext &context, IEspDisableScopeScansRequest &req, IEspDisableScopeScansResponse &resp);
};

#endif //_ESPWIZ_ws_access_HPP__
