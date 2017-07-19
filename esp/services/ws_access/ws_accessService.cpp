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

#pragma warning (disable : 4786)

#include <stdlib.h>

#include "ws_accessService.hpp"
#include "exception_util.hpp"
#include "dasess.hpp"

#include <set>

#define MSG_SEC_MANAGER_IS_NULL "Security manager is not found. Please check if the system authentication is set up correctly"
#define MSG_SEC_MANAGER_ISNT_LDAP "LDAP Security manager is required for this feature. Please enable LDAP in the system configuration"

#define FILE_SCOPE_URL "FileScopeAccess"
#define FILE_SCOPE_RTYPE "file"
#define FILE_SCOPE_RTITLE "FileScope"

#define MAX_USERS_DISPLAY 400
#define MAX_RESOURCES_DISPLAY 3000
static const long MAXXLSTRANSFER = 5000000;

void checkUser(IEspContext& context, const char* rtype = NULL, const char* rtitle = NULL, unsigned int SecAccessFlags = SecAccess_Full)
{
    CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    if (rtype && rtitle && strieq(rtype, FILE_SCOPE_RTYPE) && strieq(rtitle, FILE_SCOPE_RTITLE))
    {
        if (!context.validateFeatureAccess(FILE_SCOPE_URL, SecAccessFlags, false))
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to File Scope is denied.");
        return;
    }

    if(!secmgr->isSuperUser(context.queryUser()))
        throw MakeStringException(ECLWATCH_ADMIN_ACCESS_DENIED, "Access denied, administrators only.");
}

void Cws_accessEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(cfg == NULL)
        throw MakeStringException(-1, "can't initialize Cws_accessEx, cfg is NULL");

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    IPropertyTree* servicecfg = cfg->getPropTree(xpath.str());
    if(servicecfg == NULL)
    {
        WARNLOG(-1, "config not found for service %s/%s",process, service);
        return;
    }
    m_servicecfg.setown(servicecfg);

    /* Config is like -
    <Modules basedn="ou=le,ou=ecl,dc=le">
        <Eclserver name="eclserver" basedn="ou=le,ou=ecl,dc=le" templateName="repository.newmoduletemplate"/>
    </Modules>
    <Files basedn="ou=Files,ou=ecl"/>
    <Resources>
        <Binding name="EspBinding" service="espsmc" port="8010" basedn="ou=SMC,ou=EspServices,ou=ecl" workunitsBasedn="ou=workunits,ou=ecl"/>
    </Resources>
    */

    Owned<IPropertyTreeIterator> eclservers = m_servicecfg->getElements("Modules/Eclserver");
    for (eclservers->first(); eclservers->isValid(); eclservers->next())
    {
        const char *templatename = eclservers->query().queryProp("@templateName");
        const char* basedn = eclservers->query().queryProp("@basedn");
        if(basedn && *basedn)
        {
            StringBuffer name, head;
            const char* eclservername = eclservers->query().queryProp("@name");
            name.append("Repository Modules for ").append(eclservername);

            Owned<IEspDnStruct> onedn = createDnStruct();
            onedn->setBasedn(basedn);
            onedn->setName(name.str());
            onedn->setRtype("module");
            onedn->setRtitle("Module");
            if(templatename != NULL)
            {
                onedn->setTemplatename(templatename);
            }
            m_rawbasedns.append(*onedn.getLink());
        }
    }
    const char* modules_basedn = m_servicecfg->queryProp("Modules/@basedn");
    if(modules_basedn && *modules_basedn)
    {
        Owned<IEspDnStruct> onedn = createDnStruct();
        onedn->setBasedn(modules_basedn);
        onedn->setName("Repository Modules");
        onedn->setRtype("module");
        onedn->setRtitle("Module");
        m_rawbasedns.append(*onedn.getLink());
    }

    const char* files_basedn = m_servicecfg->queryProp("Files/@basedn");
    if(files_basedn && *files_basedn)
    {
        Owned<IEspDnStruct> onedn = createDnStruct();
        onedn->setBasedn(files_basedn);
        onedn->setName("File Scopes");
        onedn->setRtype(FILE_SCOPE_RTYPE);
        m_rawbasedns.append(*onedn.getLink());
        onedn->setRtitle(FILE_SCOPE_RTITLE);
    }

    StringBuffer workunits_basedn;
    Owned<IPropertyTreeIterator> bindings = m_servicecfg->getElements("Resources/Binding");

    for (bindings->first(); bindings->isValid(); bindings->next())
    {
        const char *service = bindings->query().queryProp("@service");
        const char* basedn = bindings->query().queryProp("@basedn");
        if(workunits_basedn.length() == 0)
        {
            const char* wubasedn = bindings->query().queryProp("@workunitsBasedn");
            if(wubasedn != NULL)
                workunits_basedn.append(wubasedn);
        }

        if(basedn && *basedn)
        {
            StringBuffer name, head;
            name.append("Esp Features for ");
            const char* bptr = basedn;
            while(*bptr != '\0' && *bptr != '=')
                bptr++;
            if(*bptr != '\0')
                bptr++;
            const char* colon = strstr(bptr, ",");
            if(colon == NULL)
                head.append(bptr);
            else
                head.append(colon - bptr, bptr);

            if(stricmp(head.str(), "WsAttributesAccess") == 0)
                continue;

            Owned<IEspDnStruct> onedn = createDnStruct();
            onedn->setBasedn(basedn);
            name.append(head.str());
            onedn->setName(name.str());
            onedn->setRtype("service");
            head.append(" Feature");
            onedn->setRtitle(head.str());
            m_rawbasedns.append(*onedn.getLink());
        }
    }

    if(workunits_basedn.length() > 0)
    {
        Owned<IEspDnStruct> onedn = createDnStruct();
        onedn->setBasedn(workunits_basedn.str());
        onedn->setName("Workunit Scopes");
        onedn->setRtype("workunit");
        onedn->setRtitle("WorkunitScope");
        m_rawbasedns.append(*onedn.getLink());
    }

}

CLdapSecManager* Cws_accessEx::queryLDAPSecurityManager(IEspContext &context)
{
    ISecManager* secMgr = context.querySecManager();
    if(secMgr && secMgr->querySecMgrType() != SMT_LDAP)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_ISNT_LDAP);
    return dynamic_cast<CLdapSecManager*>(secMgr);
}

void Cws_accessEx::setBasedns(IEspContext &context)
{
    CLdapSecManager* secmgr = (CLdapSecManager*)(context.querySecManager());

    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    set<string> alreadythere;
    ForEachItemInRev(x, m_rawbasedns)
    {
        IEspDnStruct* basedn = &(m_rawbasedns.popGet());
        const char* tname = basedn->getTemplatename();
        StringBuffer nbasedn;
        secmgr->normalizeDn(basedn->getBasedn(), nbasedn);
        if(alreadythere.find(nbasedn.str()) == alreadythere.end())
        {
            alreadythere.insert(nbasedn.str());
            Owned<IEspDnStruct> onedn = createDnStruct();
            onedn->setBasedn(nbasedn.str());
            onedn->setName(basedn->getName());
            onedn->setRtype(basedn->getRtype());
            onedn->setRtitle(basedn->getRtitle());
            if(tname != NULL && *tname != '\0')
                onedn->setTemplatename(tname);
            m_basedns.append(*onedn.getLink());
        }
        else
        {
            ForEachItemIn(y, m_basedns)
            {
                IEspDnStruct* curbasedn = &(m_basedns.item(y));
                if(stricmp(curbasedn->getBasedn(), nbasedn.str()) == 0)
                {
                    const char* curtname = curbasedn->getTemplatename();
                    if((curtname == NULL || *curtname == '\0') && (tname != NULL && *tname != '\0'))
                        curbasedn->setTemplatename(tname);
                    break;
                }
            }
        }
    }

    return;
}

bool Cws_accessEx::getNewFileScopePermissions(ISecManager* secmgr, IEspResourceAddRequest &req, StringBuffer& existingResource, StringArray& newResources)
{
    if (!secmgr)
        return false;

    const char* name0 = req.getName();
    if (!name0 || !*name0)
        return false;

    char* pStr0 = (char*) name0;
    while (pStr0[0] == ':') //in case of some ':' by mistake
        pStr0++;

    if (pStr0[0] == 0)
        return false;

    StringBuffer lastFileScope;
    char* pStr = strstr(pStr0, "::");
    while (pStr)
    {
        char fileScope[10240];
        strncpy(fileScope, pStr0, pStr-pStr0);
        fileScope[pStr-pStr0] = 0;

        if (lastFileScope.length() < 1)
            lastFileScope.append(fileScope);
        else
            lastFileScope.appendf("::%s", fileScope);
        newResources.append(lastFileScope.str());

        pStr0 = pStr+2;
        while (pStr0[0] == ':') //in case of more than two ':' by mistake
            pStr0++;
        if (pStr0[0] == 0)
            break;

        pStr = strstr(pStr0, "::");
    }

    if (pStr0[0] != 0)
    {
        if (lastFileScope.length() < 1)
            lastFileScope.append(pStr0);
        else
            lastFileScope.appendf("::%s", pStr0);
        newResources.append(lastFileScope.str());
    }

    CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
    while (newResources.ordinality())
    {
        StringBuffer namebuf = newResources.item(0);
        try
        {
            IArrayOf<CPermission> permissions;
            ldapsecmgr->getPermissionsArray(req.getBasedn(), str2type(req.getRtype()), namebuf.str(), permissions);
            if (!permissions.ordinality())
            {
                break;
            }
        }
        catch(IException* e) //exception may be thrown when no permission for the resource
        {
            e->Release();
            break;
        }

        existingResource.clear().append(namebuf);
        newResources.remove(0);
    }

    return true;
}

bool Cws_accessEx::setNewFileScopePermissions(ISecManager* secmgr, IEspResourceAddRequest &req, StringBuffer& existingResource, StringArray& newResources)
{
    if (!secmgr || !newResources.ordinality())
    {
        return false;
    }

    const char* basedn = req.getBasedn();
    if (!basedn || !*basedn)
    {
        return false;
    }

    StringBuffer basednBuf;
    basednBuf.append(basedn);

    if (existingResource.length() < 1)
    {
        existingResource.append("files");

        const char* comma = strchr(basedn, ',');
        const char* eqsign = strchr(basedn, '=');
        if(eqsign && comma && (strlen(comma) > 1))
        {
            basednBuf.clear().append(comma + 1);
        }
    }

    IArrayOf<CPermission> requiredPermissions;
    CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
    ldapsecmgr->getPermissionsArray(basednBuf, str2type(req.getRtype()), existingResource.str(), requiredPermissions);
    if (!requiredPermissions.ordinality())
    {
        return false;
    }

    ForEachItemIn(x, requiredPermissions)
    {
        CPermission& perm = requiredPermissions.item(x);

        int accType = perm.getAccount_type(); //0-individual, 1 - group
        const char* actname = perm.getAccount_name();
        if (!actname || !*actname)
            continue;

        CPermissionAction paction;
        paction.m_basedn.append(req.getBasedn());
        paction.m_rtype = str2type(req.getRtype());
        paction.m_account_type = (ACT_TYPE)accType;
        paction.m_account_name.append(actname);
        paction.m_allows = perm.getAllows();
        paction.m_denies = perm.getDenies();
        if ((accType != GROUP_ACT) || ((stricmp(actname, "Administrators") != 0) && (stricmp(actname, "Authenticated Users") != 0)))
        {
            paction.m_action.append("add");

        }
        else
        {
            paction.m_action.append("update");
        }

        ForEachItemIn(y, newResources)
        {
            StringBuffer namebuf = newResources.item(y);
            paction.m_rname.clear().append(namebuf.str());
            ldapsecmgr->changePermission(paction);
        }

    }

    return true;
}

bool Cws_accessEx::onUsers(IEspContext &context, IEspUserRequest &req, IEspUserResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);

        double version = context.getClientVersion();
        if (version > 1.03)
        {
            if(secmgr == NULL)
            {
                resp.setNoSecMngr(true);
                return true;
            }
        }
        else
        {
            if(secmgr == NULL)
                throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        }

        checkUser(context);

        const char* searchstr = req.getSearchinput();
        int numusers = secmgr->countUsers(searchstr, MAX_USERS_DISPLAY);

        if(numusers == -1)
        {
            resp.setToomany(true);
            return true;
        }

        resp.setToomany(false);

        /*
        LdapServerType servertype = secmgr->getLdapServerType();
        if(servertype != ACTIVE_DIRECTORY)
            resp.setPosixok(true);
        else
            resp.setPosixok(false);
        */
        resp.setPosixok(false);

        IArrayOf<IEspUserInfo> espusers;
        IUserArray users;
        secmgr->searchUsers(searchstr, users);
        ForEachItemIn(x, users)
        {
            ISecUser* usr = &users.item(x);
            if(usr)
            {
                Owned<IEspUserInfo> oneusr = createUserInfo();
                oneusr->setUsername(usr->getName());
                oneusr->setFullname(usr->getFullName());

                double version = context.getClientVersion();

                if (version >= 1.10)
                {
                    oneusr->setEmployeeID(usr->getEmployeeID());
                }

                if (version >= 1.07)
                {
                    StringBuffer sb;
                    switch (usr->getPasswordDaysRemaining())//-1 if expired, -2 if never expires
                    {
                    case scPasswordExpired:
                        sb.set("Expired");
                        break;
                    case scPasswordNeverExpires:
                        sb.set("Never");
                        break;
                    default:
                        {
                            CDateTime dt;
                            usr->getPasswordExpiration(dt);
                            dt.getDateString(sb);
                            break;
                        }
                    }
                    oneusr->setPasswordexpiration(sb.str());
                }
                espusers.append(*oneusr.getLink());
            }
        }

        resp.setUsers(espusers);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool Cws_accessEx::onUserQuery(IEspContext &context, IEspUserQueryRequest &req, IEspUserQueryResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }
        checkUser(context);

        __int64 pageStartFrom = 0;
        unsigned pageSize = 100;
        if (!req.getPageSize_isNull())
            pageSize = req.getPageSize();
        if (!req.getPageStartFrom_isNull())
            pageStartFrom = req.getPageStartFrom();

        UserField sortOrder[2] = {UFName, UFterm};
        CUserSortBy sortBy = req.getSortBy();
        switch (sortBy)
        {
        case CUserSortBy_FullName:
            sortOrder[0] = UFFullName;
            break;
        case CUserSortBy_PasswordExpiration:
            sortOrder[0] = UFPasswordExpiration;
            break;
        case CUserSortBy_EmployeeID:
            sortOrder[0] = UFEmployeeID;
            break;
        default:
            break;
        }
        sortOrder[0] = (UserField) (sortOrder[0] | UFnocase);
        bool descending = req.getDescending();
        if (descending)
            sortOrder[0] = (UserField) (sortOrder[0] | UFreverse);

        unsigned total;
        __int64 cacheHint;
        IArrayOf<IEspUserInfo> espUsers;
        Owned<ISecItemIterator> it = secmgr->getUsersSorted(req.getName(), sortOrder, (const __int64) pageStartFrom, (const unsigned) pageSize, &total, &cacheHint);
        ForEach(*it)
        {
            IPropertyTree& usr = it->query();
            const char* userName = usr.queryProp(getUserFieldNames(UFName));
            if (!userName || !*userName)
                continue;

            Owned<IEspUserInfo> userInfo = createUserInfo();
            userInfo->setUsername(userName);
            const char* fullName = usr.queryProp(getUserFieldNames(UFFullName));
            if (fullName && *fullName)
                userInfo->setFullname(fullName);
            const char* passwordExpiration = usr.queryProp(getUserFieldNames(UFPasswordExpiration));
            if (passwordExpiration && *passwordExpiration)
                userInfo->setPasswordexpiration(passwordExpiration);

            const char* employeeID = usr.queryProp(getUserFieldNames(UFEmployeeID));
            if (employeeID && *employeeID)
                userInfo->setEmployeeID(employeeID);

            espUsers.append(*userInfo.getClear());
        }

        resp.setUsers(espUsers);
        resp.setTotalUsers(total);
        resp.setCacheHint(cacheHint);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool Cws_accessEx::onUserEdit(IEspContext &context, IEspUserEditRequest &req, IEspUserEditResponse &resp)
{
    try
    {
        checkUser(context);

        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
        resp.setUsername(req.getUsername());

        StringArray groupnames;
        ldapsecmgr->getGroups(req.getUsername(), groupnames);
        IArrayOf<IEspGroupInfo> groups;
        for(unsigned i = 0; i < groupnames.length(); i++)
        {
            const char* grpname = groupnames.item(i);
            if(grpname == NULL || grpname[0] == '\0')
                continue;
            Owned<IEspGroupInfo> onegrp = createGroupInfo();
            onegrp->setName(grpname);
            groups.append(*onegrp.getLink());
        }
        resp.setGroups(groups);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserGroupEditInput(IEspContext &context, IEspUserGroupEditInputRequest &req, IEspUserGroupEditInputResponse &resp)
{
    try
    {
        checkUser(context);

        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
        resp.setUsername(req.getUsername());

        set<string> ogrps;
        ogrps.insert("Authenticated Users");
        StringArray grps;
        ldapsecmgr->getGroups(req.getUsername(), grps);
        unsigned i = 0;
        for(i = 0; i < grps.length(); i++)
        {
            const char* grp = grps.item(i);
            if(grp != NULL && *grp != '\0')
            {
                ogrps.insert(grp);
            }
        }

        StringArray groupnames;
        StringArray managedBy;
        StringArray descriptions;
        ldapsecmgr->getAllGroups(groupnames, managedBy, descriptions);
        IArrayOf<IEspGroupInfo> groups;
        for(i = 0; i < groupnames.length(); i++)
        {
            const char* grpname = groupnames.item(i);
            if(grpname == NULL || grpname[0] == '\0')
                continue;
            if(ogrps.find(grpname) == ogrps.end())
            {
                Owned<IEspGroupInfo> onegrp = createGroupInfo();
                onegrp->setName(grpname);
                onegrp->setGroupDesc(descriptions.item(i));
                onegrp->setGroupOwner(managedBy.item(i));
                groups.append(*onegrp.getLink());
            }
        }
        resp.setGroups(groups);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserGroupEdit(IEspContext &context, IEspUserGroupEditRequest &req, IEspUserGroupEditResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)(context.querySecManager());

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("username can't be empty");
            return false;
        }

        StringArray& groupnames = req.getGroupnames();
        try
        {
            for(unsigned i = 0; i < groupnames.length(); i++)
            {
                const char* grpname = groupnames.item(i);
                if(grpname == NULL || *grpname == '\0')
                    continue;
                    secmgr->changeUserGroup(req.getAction(), username, grpname);
            }
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            e->errorMessage(errmsg);
            DBGLOG("error changing user's group membership: %s", errmsg.str());
            resp.setRetcode(e->errorCode());
            resp.setRetmsg(errmsg.str());
            return false;
        }

        resp.setRetcode(0);
        resp.setUsername(username);
        resp.setAction(req.getAction());
        if(stricmp(req.getAction(), "add") == 0)
            resp.setRetmsg("user successfully added to groups");
        else
            resp.setRetmsg("user successfully deleted from groups");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroups(IEspContext &context, IEspGroupRequest &req, IEspGroupResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr0 = queryLDAPSecurityManager(context);

        double version = context.getClientVersion();
        if (version > 1.03)
        {
            if(secmgr0 == NULL)
            {
                //throw MakeStringException(-1, "SecManager is NULL, please check if the binding's authentication is set up correctly");
                resp.setNoSecMngr(true);
                return true;
            }
        }

        checkUser(context);

        StringArray groupnames;
        StringArray groupManagedBy;
        StringArray groupDescriptions;
        ISecManager* secmgr = context.querySecManager();
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        secmgr->getAllGroups(groupnames, groupManagedBy, groupDescriptions);
        ///groupnames.append("Administrators");
        ///groupnames.append("Full_Access_TestingOnly");
        //groupnames.kill();
        if (groupnames.length() > 0)
        {
            IArrayOf<IEspGroupInfo> groups;
            for(unsigned i = 0; i < groupnames.length(); i++)
            {
                const char* grpname = groupnames.item(i);
                //if(grpname == NULL || grpname[0] == '\0' || stricmp(grpname, "Authenticated Users") == 0)
                if(grpname == NULL || grpname[0] == '\0')
                    continue;
                Owned<IEspGroupInfo> onegrp = createGroupInfo();
                onegrp->setName(grpname);
                onegrp->setGroupDesc(groupDescriptions.item(i));
                onegrp->setGroupOwner(groupManagedBy.item(i));
                groups.append(*onegrp.getLink());
            }

            resp.setGroups(groups);
        }
/*
    IArrayOf<IEspGroupInfo> groups;
            Owned<IEspGroupInfo> onegrp = createGroupInfo();
            onegrp->setName("grpname");
            groups.append(*onegrp.getLink());

    resp.setGroups(groups);
*/
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroupQuery(IEspContext &context, IEspGroupQueryRequest &req, IEspGroupQueryResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }

        checkUser(context);

        __int64 pageStartFrom = 0;
        unsigned pageSize = 100;
        if (!req.getPageSize_isNull())
            pageSize = req.getPageSize();
        if (!req.getPageStartFrom_isNull())
            pageStartFrom = req.getPageStartFrom();

        GroupField sortOrder[2] = {GFName, GFterm};
        CGroupSortBy sortBy = req.getSortBy();
        switch (sortBy)
        {
        case CGroupSortBy_ManagedBy:
            sortOrder[0] = GFManagedBy;
            break;
        default:
            break;
        }
        sortOrder[0] = (GroupField) (sortOrder[0] | UFnocase);
        bool descending = req.getDescending();
        if (descending)
            sortOrder[0] = (GroupField) (sortOrder[0] | UFreverse);

        unsigned total;
        __int64 cacheHint;
        IArrayOf<IEspGroupInfo> groups;
        Owned<ISecItemIterator> it = secmgr->getGroupsSorted(sortOrder, (const __int64) pageStartFrom, (const unsigned) pageSize, &total, &cacheHint);
        ForEach(*it)
        {
            IPropertyTree& g = it->query();
            const char* groupName = g.queryProp(getGroupFieldNames(GFName));
            if (!groupName || !*groupName)
                continue;

            Owned<IEspGroupInfo> groupInfo = createGroupInfo();
            groupInfo->setName(groupName);
            const char* managedBy = g.queryProp(getGroupFieldNames(GFManagedBy));
            if (managedBy && *managedBy)
                groupInfo->setGroupOwner(managedBy);
            const char* desc = g.queryProp(getGroupFieldNames(GFDesc));
            if (desc && *desc)
                groupInfo->setGroupDesc(desc);
            groups.append(*groupInfo.getClear());
        }

        resp.setGroups(groups);
        resp.setTotalGroups(total);
        resp.setCacheHint(cacheHint);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onAddUser(IEspContext &context, IEspAddUserRequest &req, IEspAddUserResponse &resp)
{
    try
    {
        checkUser(context);

        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("username can't be empty");
            return false;
        }
        if(strchr(username, ' '))
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Username can't contain spaces");
            return false;
        }

        CLdapSecManager* secmgr0 = (CLdapSecManager*)secmgr;
        if((secmgr0->getLdapServerType() == ACTIVE_DIRECTORY) && (strlen(username) > 20))
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Username can't be more than 20 characters.");
            return false;
        }

        const char* pass1 = req.getPassword1();
        const char* pass2 = req.getPassword2();
        if(pass1 == NULL || pass2 == NULL || *pass1 == '\0' || *pass2 == '\0' || strcmp(pass1, pass2) != 0)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("password and retype can't be empty and must match.");
            return false;
        }

        const char * employeeID = NULL;
        if (context.getClientVersion() >= 1.10)
        {
            employeeID = req.getEmployeeID();
        }

        Owned<ISecUser> user = secmgr->createUser(username);
        ISecCredentials& cred = user->credentials();
        const char* firstname = req.getFirstname();
        const char* lastname = req.getLastname();
        if(firstname != NULL)
            user->setFirstName(firstname);
        if(lastname != NULL)
            user->setLastName(lastname);
        if(employeeID != NULL)
            user->setEmployeeID(employeeID);
        if(pass1 != NULL)
            cred.setPassword(pass1);
        try
        {
            if (user.get())
                secmgr->addUser(*user.get());
        }
        catch(IException* e)
        {
            resp.setRetcode(-1);
            StringBuffer errmsg;
            resp.setRetmsg(e->errorMessage(errmsg).str());
            return false;
        }

        resp.setRetcode(0);
        resp.setRetmsg("User successfully added");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserAction(IEspContext &context, IEspUserActionRequest &req, IEspUserActionResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)(context.querySecManager());

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* action = req.getActionType();
        if (!action || !*action)
            throw MakeStringException(ECLWATCH_INVALID_ACTION, "Action not specified.");

        if (!stricmp(action, "delete"))
        {
            StringArray& usernames = req.getUsernames();

            for(unsigned i = 0; i < usernames.length(); i++)
            {
                const char* username = usernames.item(i);
                Owned<ISecUser> user = secmgr->createUser(username);
                secmgr->deleteUser(user.get());
            }
        }
        else if (!stricmp(action, "export"))
        {
            StringBuffer users;
            StringArray& usernames = req.getUsernames();
            for(unsigned i = 0; i < usernames.length(); i++)
            {
                const char* username = usernames.item(i);
                if (i > 0)
                    users.appendf("&usernames_i%d=%s", i+1, username);
                else
                    users.append(username);
            }
            resp.setRedirectUrl(StringBuffer("/ws_access/UserAccountExport?usernames_i1=").append(users).str());
        }
        resp.setAction(action);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroupAdd(IEspContext &context, IEspGroupAddRequest &req, IEspGroupAddResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)(context.querySecManager());

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* groupname = req.getGroupname();

        if(groupname == NULL || *groupname == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Group name can't be empty");
            return false;
        }

        resp.setGroupname(groupname);

        double version = context.getClientVersion();
        const char * groupDesc = NULL;
        const char * groupOwner = NULL;
        if (version >= 1.09)
        {
            groupDesc = req.getGroupDesc();
            groupOwner = req.getGroupOwner();
        }

        try
        {
            secmgr->addGroup(groupname, groupOwner, groupDesc);
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            resp.setRetcode(e->errorCode());
            resp.setRetmsg(emsg.str());
            return false;
        }
        catch(...)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Unknown error");
            return false;
        }

        resp.setRetcode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroupAction(IEspContext &context, IEspGroupActionRequest &req, IEspGroupActionResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* action = req.getActionType();
        if (!action || !*action)
            throw MakeStringException(ECLWATCH_INVALID_ACTION, "Action not specified.");

        if (!stricmp(action, "export"))
        {
            StringBuffer groups;
            StringArray& groupnames = req.getGroupnames();
            for(unsigned i = 0; i < groupnames.length(); i++)
            {
                const char* group = groupnames.item(i);
                if (i > 0)
                    groups.appendf("&groupnames_i%d=%s", i+1, group);
                else
                    groups.append(group);
            }
            resp.setRedirectUrl(StringBuffer("/ws_access/UserAccountExport?groupnames_i1=").append(groups).str());
        }
        else if (!stricmp(action, "delete"))
        {
            CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;

            StringArray& groupnames = req.getGroupnames();

            IArrayOf<IEspAccountPermission> accountPermissions;
            double version = context.getClientVersion();
            if (version > 1.01)
            {
                bool bDeletePermission = false;
                if(!req.getDeletePermission_isNull())
                    bDeletePermission = req.getDeletePermission();

                if(m_basedns.length() == 0)
                {
                    setBasedns(context);
                }

                ForEachItemIn(y, m_basedns)
                {
                    IEspDnStruct* curbasedn = &(m_basedns.item(y));
                    const char *aBasedn = curbasedn->getBasedn();
                    const char *aRtype = curbasedn->getRtype();
                    if (!aBasedn || !*aBasedn ||!aRtype || !*aRtype)
                        continue;

                    SecResourceType rtype = str2type(aRtype);

                    IArrayOf<IEspResource> ResourceArray;
                    if(rtype == RT_WORKUNIT_SCOPE)
                    {
                        StringBuffer deft_basedn, deft_name;
                        const char* comma = strchr(aBasedn, ',');
                        const char* eqsign = strchr(aBasedn, '=');
                        if(eqsign != NULL)
                        {
                            if(comma == NULL)
                                deft_name.append(eqsign+1);
                            else
                            {
                                deft_name.append(comma - eqsign - 1, eqsign+1);
                                deft_basedn.append(comma + 1);
                            }
                        }

                        if (deft_name.length() > 0)
                        {
                            Owned<IEspResource> oneresource = createResource();
                            oneresource->setName(deft_name);
                            oneresource->setDescription(deft_basedn);
                            ResourceArray.append(*oneresource.getLink());
                        }
                    }

                    IArrayOf<ISecResource> resources;
                    if(secmgr->getResources(rtype, aBasedn, resources))
                    {
                        ForEachItemIn(y1, resources)
                        {
                            ISecResource& r = resources.item(y1);
                            const char* rname = r.getName();
                            if(rname == NULL || *rname == '\0')
                                continue;

                            Owned<IEspResource> oneresource = createResource();
                            oneresource->setName(rname);
                            oneresource->setDescription(aBasedn);
                            ResourceArray.append(*oneresource.getLink());
                        }
                    }

                    ForEachItemIn(y2, ResourceArray)
                    {
                        IEspResource& r = ResourceArray.item(y2);
                        const char* rname = r.getName();
                        const char* bnname = r.getDescription();
                        if(rname == NULL || *rname == '\0')
                            continue;


                        StringBuffer namebuf(rname);
                        //const char* prefix = req.getPrefix();
                        //if(prefix && *prefix)
                        //  namebuf.insert(0, prefix);

                        try
                        {
                            IArrayOf<CPermission> permissions;
                            ldapsecmgr->getPermissionsArray(bnname, rtype, namebuf.str(), permissions);
                            ForEachItemIn(x, permissions)
                            {
                                CPermission& perm = permissions.item(x);
                                const char* actname = perm.getAccount_name();
                                int accountType = perm.getAccount_type(); //0-individual, 1 - group
                                //if ((bGroupAccount && accountType < 1) || (!bGroupAccount && accountType > 0))
                                if (accountType < 1 || !actname || !*actname) //Support Group only
                                    continue;

                                ForEachItemIn(x1, groupnames)
                                {
                                    const char* groupname = groupnames.item(x1);
                                    if (groupname &&    !strcmp(actname, groupname))
                                    {
                                        ///bDeletePermission = true;
                                        if (!bDeletePermission)
                                        {
                                            Owned<IEspAccountPermission> onepermission = createAccountPermission();
                                            onepermission->setBasedn(bnname);
                                            onepermission->setRType(aRtype);
                                            onepermission->setResourceName(namebuf.str());
                                            onepermission->setPermissionName(groupname);
                                            accountPermissions.append(*onepermission.getLink());
                                        }
                                        else
                                        {
                                            CPermissionAction paction;
                                            paction.m_basedn.append(bnname);
                                            paction.m_rtype = rtype;
                                            paction.m_rname.append(namebuf.str());
                                            paction.m_account_name.append(actname);
                                            paction.m_account_type = (ACT_TYPE) accountType;
                                            paction.m_allows = perm.getAllows();
                                            paction.m_denies = perm.getDenies();
                                            paction.m_action.append("delete");

                                            if(!ldapsecmgr->changePermission(paction))
                                            {
                                                resp.setRetcode(-1);
                                                resp.setRetmsg("Unknown error");
                                                return false;
                                            }
                                        }

                                        break;
                                    }
                                }
                            }
                        }
                        catch(IException* e)
                        {
                            e->Release();
                        }
                    }
                }
            }

            try
            {
                if (accountPermissions.length() < 1)
                {
                    ForEachItemIn(x1, groupnames)
                    {
                        const char* groupname = groupnames.item(x1);
                        secmgr->deleteGroup(groupname);
                    }
                }
                else
                {
                    StringBuffer groupnamestr;
                    groupnamestr.append("DeletePermission=1");
                    ForEachItemIn(x1, groupnames)
                    {
                        const char* groupname = groupnames.item(x1);
                        groupnamestr.appendf("&groupnames_i%d=%s", x1+1, groupname);
                    }

                    resp.setPermissions(accountPermissions);
                    resp.setGroupnames(groupnamestr.str());
                    resp.setRetcode(0);
                }
            }
            catch(IException* e)
            {
                StringBuffer emsg;
                e->errorMessage(emsg);
                resp.setRetcode(e->errorCode());
                resp.setRetmsg(emsg.str());
                return false;
            }
            catch(...)
            {
                resp.setRetcode(-1);
                resp.setRetmsg("Unknown error");
                return false;
            }
        }
        resp.setRetcode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroupEdit(IEspContext &context, IEspGroupEditRequest &req, IEspGroupEditResponse &resp)
{
    try
    {
        checkUser(context);

        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
        resp.setGroupname(req.getGroupname());

        StringArray usernames;
        ldapsecmgr->getGroupMembers(req.getGroupname(), usernames);
        IArrayOf<IEspUserInfo> users;
        unsigned i = 0;
        for(i = 0; i < usernames.length(); i++)
        {
            const char* usrname = usernames.item(i);
            if(usrname == NULL || usrname[0] == '\0')
                continue;

///////////////////////////////////////BUG#41536///////////////
            bool bFound = false;
            IUserArray usersInBaseDN;
            ldapsecmgr->searchUsers(usrname, usersInBaseDN);
            ForEachItemIn(x, usersInBaseDN)
            {
                ISecUser* usr = &usersInBaseDN.item(x);
                if(usr)
                {
                    const char* usrname = usr->getName();
                    if(usrname == NULL || usrname[0] == '\0')
                        continue;

                    bFound = true;
                    break;
                }
            }

            if (!bFound)
                continue;
//////////////////////////////////////////////////////////////

            Owned<IEspUserInfo> oneusr = createUserInfo();
            oneusr->setUsername(usrname);
            users.append(*oneusr.getLink());
        }
        resp.setUsers(users);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroupMemberQuery(IEspContext &context, IEspGroupMemberQueryRequest &req, IEspGroupMemberQueryResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }

        checkUser(context);

        __int64 pageStartFrom = 0;
        unsigned pageSize = 100;
        if (!req.getPageSize_isNull())
            pageSize = req.getPageSize();
        if (!req.getPageStartFrom_isNull())
            pageStartFrom = req.getPageStartFrom();

        UserField sortOrder[2] = {UFName, UFterm};
        CUserSortBy sortBy = req.getSortBy();
        switch (sortBy)
        {
        case CUserSortBy_FullName:
            sortOrder[0] = UFFullName;
            break;
        case CUserSortBy_PasswordExpiration:
            sortOrder[0] = UFPasswordExpiration;
            break;
        case CUserSortBy_EmployeeID:
            sortOrder[0] = UFEmployeeID;
            break;
        default:
            break;
        }
        sortOrder[0] = (UserField) (sortOrder[0] | UFnocase);
        bool descending = req.getDescending();
        if (descending)
            sortOrder[0] = (UserField) (sortOrder[0] | UFreverse);

        unsigned total;
        __int64 cacheHint;
        IArrayOf<IEspUserInfo> users;
        Owned<ISecItemIterator> it = secmgr->getGroupMembersSorted(req.getGroupName(), sortOrder, (const __int64) pageStartFrom, (const unsigned) pageSize, &total, &cacheHint);
        ForEach(*it)
        {
            IPropertyTree& usr = it->query();
            const char* userName = usr.queryProp(getUserFieldNames(UFName));
            if (!userName || !*userName)
                continue;

            Owned<IEspUserInfo> userInfo = createUserInfo();
            userInfo->setUsername(userName);
            const char* fullName = usr.queryProp(getUserFieldNames(UFFullName));
            if (fullName && *fullName)
                userInfo->setFullname(fullName);
            const char* passwordExpiration = usr.queryProp(getUserFieldNames(UFPasswordExpiration));
            if (passwordExpiration && *passwordExpiration)
                userInfo->setPasswordexpiration(passwordExpiration);

            const char* employeeID = usr.queryProp(getUserFieldNames(UFEmployeeID));
            if (employeeID && *employeeID)
                userInfo->setEmployeeID(employeeID);

            users.append(*userInfo.getLink());
        }

        resp.setUsers(users);
        resp.setTotalUsers(total);
        resp.setCacheHint(cacheHint);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroupMemberEditInput(IEspContext &context, IEspGroupMemberEditInputRequest &req, IEspGroupMemberEditInputResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
        resp.setGroupname(req.getGroupname());

        set<string> ousrs;
        StringArray ousernames;
        ldapsecmgr->getGroupMembers(req.getGroupname(), ousernames);
        unsigned i = 0;
        for(i = 0; i < ousernames.length(); i++)
        {
            const char* username = ousernames.item(i);
            if(username != NULL && *username != '\0')
            {
                ousrs.insert(username);
            }
        }

        const char* searchstr = req.getSearchinput();
        int numusers = secmgr->countUsers(searchstr, MAX_USERS_DISPLAY+ousernames.ordinality());

        if(numusers == -1)
        {
            resp.setToomany(true);
            return true;
        }

        resp.setToomany(false);

        IArrayOf<IEspUserInfo> espusers;
        IUserArray users;
        secmgr->searchUsers(searchstr, users);
        ForEachItemIn(x, users)
        {
            ISecUser* usr = &users.item(x);
            if(usr)
            {
                const char* usrname = usr->getName();
                if(usrname == NULL || usrname[0] == '\0')
                    continue;
                if(ousrs.find(usrname) == ousrs.end())
                {
                    Owned<IEspUserInfo> oneusr = createUserInfo();
                    oneusr->setUsername(usr->getName());
                    espusers.append(*oneusr.getLink());
                }
            }
        }
        resp.setUsers(espusers);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onGroupMemberEdit(IEspContext &context, IEspGroupMemberEditRequest &req, IEspGroupMemberEditResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)(context.querySecManager());

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* groupname = req.getGroupname();
        if(groupname == NULL || *groupname == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("group can't be empty");
            return false;
        }

///////////////////////////////////////BUG#41536///////////////
        StringArray existing_usernames;
        if (!stricmp(req.getAction(), "add"))
            secmgr->getGroupMembers(groupname, existing_usernames);
//////////////////////////////////////////////////////

        StringArray& usernames = req.getUsernames();
        try
        {
            for(unsigned i = 0; i < usernames.length(); i++)
            {
                const char* usrname = usernames.item(i);
                if(usrname == NULL || *usrname == '\0')
                    continue;

///////////////////////////////////////BUG#41536///////////////
                bool bFound = false;
                if (existing_usernames.length() > 0)
                {
                    for(unsigned i = 0; i < existing_usernames.length(); i++)
                    {
                        const char* existing_usrname = existing_usernames.item(i);
                        if(existing_usrname == NULL || existing_usrname[0] == '\0')
                            continue;

                        if (!strcmp(usrname, existing_usrname))
                        {
                            bFound = true;
                            break;
                        }
                    }
                }

                if (!bFound)
//////////////////////////////////////////////////////
                    secmgr->changeUserGroup(req.getAction(), usrname, groupname);
            }
        }
        catch(IException* e)
        {
            StringBuffer errmsg;
            e->errorMessage(errmsg);
            DBGLOG("error changing user's group membership: %s", errmsg.str());
            resp.setRetcode(e->errorCode());
            resp.setRetmsg(errmsg.str());
            return false;
        }

        resp.setRetcode(0);
        resp.setGroupname(groupname);
        resp.setAction(req.getAction());
        if(stricmp(req.getAction(), "add") == 0)
            resp.setRetmsg("members successfully added to group");
        else
            resp.setRetmsg("members successfully deleted from group");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onPermissions(IEspContext &context, IEspBasednsRequest &req, IEspBasednsResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);

        double version = context.getClientVersion();
        if (version > 1.03)
        {
            if(secmgr == NULL)
            {
                resp.setNoSecMngr(true);
                return true;
            }
        }
        else
        {
            if(secmgr == NULL)
                throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        }

        checkUser(context);

        if(m_basedns.length() == 0)
        {
            setBasedns(context);
        }

        resp.setBasedns(m_basedns);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

const char* Cws_accessEx::getBaseDN(IEspContext &context, const char* rtype, StringBuffer& baseDN)
{
    if(!m_basedns.length())
        setBasedns(context);
    ForEachItemIn(y, m_basedns)
    {
        IEspDnStruct* curbasedn = &(m_basedns.item(y));
        if(strieq(curbasedn->getRtype(), rtype))
        {
            baseDN.set(curbasedn->getBasedn());
            return baseDN.str();
        }
    }
    return NULL;
}

bool Cws_accessEx::onResources(IEspContext &context, IEspResourcesRequest &req, IEspResourcesResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Read);

        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        double version = context.getClientVersion();
        const char* filterInput = req.getSearchinput();
        const char* basedn = req.getBasedn();
        const char* rtypestr = req.getRtype();
        if (!rtypestr || !*rtypestr)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Rtype not specified");
        StringBuffer baseDN;
        if (!basedn || !*basedn)
        {
            basedn = getBaseDN(context, rtypestr, baseDN);
            if (!basedn || !*basedn)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "BaseDN not found");
        }

        const char* moduletemplate = NULL;
        ForEachItemIn(x, m_basedns)
        {
            IEspDnStruct* curbasedn = &(m_basedns.item(x));
            if(stricmp(curbasedn->getBasedn(), basedn) == 0)
            {
                moduletemplate = curbasedn->getTemplatename();
            }
        }

        resp.setBasedn(basedn);
        resp.setRtype(rtypestr);
        resp.setRtitle(req.getRtitle());
        SecResourceType rtype = str2type(rtypestr);
        if(rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE)
        {
            StringBuffer deft_basedn, deft_name;
            const char* comma = strchr(basedn, ',');
            const char* eqsign = strchr(basedn, '=');
            if(eqsign != NULL)
            {
                if(comma == NULL)
                    deft_name.append(eqsign+1);
                else
                {
                    deft_name.append(comma - eqsign - 1, eqsign+1);
                    deft_basedn.append(comma + 1);
                }

                resp.setDefault_basedn(deft_basedn.str());
                resp.setDefault_name(deft_name.str());
            }
        }
        IArrayOf<IEspResource> rarray;
        IArrayOf<ISecResource> resources;
        const char* prefix = req.getPrefix();
        int prefixlen = 0;
        if(prefix && *prefix)
        {
            prefixlen = strlen(prefix);
            resp.setPrefix(prefix);
        }

        if (version > 1.04)
        {
            int numResources = -1;
            if  (req.getRtitle() && !stricmp(req.getRtitle(), "CodeGenerator Permission"))
                numResources = secmgr->countResources(basedn, prefix, MAX_RESOURCES_DISPLAY);
            else
                numResources = secmgr->countResources(basedn, filterInput, MAX_RESOURCES_DISPLAY);
            if(numResources == -1)
            {
                resp.setToomany(true);
                return true;
            }
            else
            {
                resp.setToomany(false);
            }
        }

        if ((!filterInput || !*filterInput) && req.getRtitle() && !stricmp(req.getRtitle(), "CodeGenerator Permission"))
        {
            if(!secmgr->getResourcesEx(rtype, basedn, prefix, resources))
                return false;
        }
        else
        {
            if(!secmgr->getResourcesEx(rtype, basedn, filterInput, resources))
                return false;
        }

        ILdapConfig* cfg = secmgr->queryConfig();
        for(unsigned i = 0; i < resources.length(); i++)
        {
            ISecResource& r = resources.item(i);

            Owned<IEspResource> oneresource = createResource();
            oneresource->setIsSpecial(false);

            const char* rname = r.getName();
            if(rname == NULL || *rname == '\0')
                continue;

            if(prefix && *prefix)
            {
                if(strncmp(prefix, rname, prefixlen) != 0)
                    continue;
                else
                    rname += prefixlen;
            }

            if(rtype == RT_MODULE)
            {
                if(stricmp(rname, "repository") != 0)
                {
                    if(moduletemplate != NULL && stricmp(rname, moduletemplate) == 0)
                        oneresource->setIsSpecial(true);

                    if(Utils::strncasecmp(rname, "repository.", 11) == 0)
                        rname = rname + 11;
                    else
                        continue;
                }
                else
                {
                    oneresource->setIsSpecial(true);
                }
            }
            else if(rtype == RT_FILE_SCOPE && stricmp(rname, "file") == 0)
            {
                //oneresource->setIsSpecial(true); //33067
                continue;
            }

            oneresource->setName(rname);
            oneresource->setDescription(r.getDescription());

            rarray.append(*oneresource.getLink());
        }
        if (version >= 1.08)
        {
            Owned<IUserDescriptor> userdesc;
            userdesc.setown(createUserDescriptor());
            userdesc->set(context.queryUserId(), context.queryPassword(), context.querySessionToken(), context.querySignature());
            int retCode;
            StringBuffer retMsg;
            bool isEnabled = querySessionManager().queryScopeScansEnabled(userdesc, &retCode, retMsg);
            if (retCode != 0)
                DBGLOG("Error %d querying scope scan status : %s", retCode, retMsg.str());
            resp.updateScopeScansStatus().setIsEnabled(isEnabled);
            resp.updateScopeScansStatus().setRetcode(retCode);
            resp.updateScopeScansStatus().setRetmsg(retMsg.str());
        }
        resp.setResources(rarray);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onResourceQuery(IEspContext &context, IEspResourceQueryRequest &req, IEspResourceQueryResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }

        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Read);

        const char* rtypeStr = req.getRtype();
        if (!rtypeStr || !*rtypeStr)
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Rtype not specified");

        StringBuffer baseDN;
        const char* basednStr = req.getBasedn();
        if (!basednStr || !*basednStr)
        {
            basednStr = getBaseDN(context, rtypeStr, baseDN);
            if (!basednStr || !*basednStr)
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "BaseDN not found");
        }

        SecResourceType rtype = str2type(rtypeStr);
        const char* moduleTemplate = NULL;
        ForEachItemIn(x, m_basedns)
        {
            IEspDnStruct* curbasedn = &(m_basedns.item(x));
            if(strieq(curbasedn->getBasedn(), basednStr))
            {
                moduleTemplate = curbasedn->getTemplatename();
                break;
            }
        }

        StringBuffer nameReq = req.getName();
        const char* prefix = req.getPrefix();
        if (!nameReq.length() && req.getRtitle() && !stricmp(req.getRtitle(), "CodeGenerator Permission"))
            nameReq.set(prefix);

        __int64 pageStartFrom = 0;
        unsigned pageSize = 100;
        if (!req.getPageSize_isNull())
            pageSize = req.getPageSize();
        if (!req.getPageStartFrom_isNull())
            pageStartFrom = req.getPageStartFrom();

        ResourceField sortOrder[2] = {(ResourceField) (RFName | RFnocase), RFterm};
        bool descending = req.getDescending();
        if (descending)
            sortOrder[0] = (ResourceField) (sortOrder[0] | RFreverse);

        unsigned total;
        __int64 cacheHint;
        IArrayOf<IEspResource> rarray;
        Owned<ISecItemIterator> it = secmgr->getResourcesSorted(rtype, basednStr, nameReq.str(),
            RF_RT_FILE_SCOPE_FILE | RF_RT_MODULE_NO_REPOSITORY, sortOrder,
            (const __int64) pageStartFrom, (const unsigned) pageSize, &total, &cacheHint);
        ForEach(*it)
        {
            IPropertyTree& r = it->query();
            const char* rname = r.queryProp(getResourceFieldNames(RFName));
            if(!rname || !*rname)
                continue;

            if(prefix && *prefix)
                rname += strlen(prefix); //Remove the prefix from the name

            bool isSpecial = false;
            if(rtype == RT_MODULE)
            {
                if(strieq(rname, "repository"))
                    isSpecial = true;
                else
                {
                    if(moduleTemplate != NULL && stricmp(rname, moduleTemplate) == 0)
                        isSpecial = true;

                    rname = rname + 11; //Remove "repository." from the name
                }
            }

            Owned<IEspResource> oneresource = createResource();
            oneresource->setName(rname);
            oneresource->setIsSpecial(isSpecial);
            const char* desc = r.queryProp(getResourceFieldNames(RFDesc));
            if (desc && *desc)
                oneresource->setDescription(desc);

            rarray.append(*oneresource.getClear());
        }

        resp.setResources(rarray);
        resp.setTotalResources(total);
        resp.setCacheHint(cacheHint);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onResourceAddInput(IEspContext &context, IEspResourceAddInputRequest &req, IEspResourceAddInputResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

        resp.setBasedn(req.getBasedn());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

SecResourceType Cws_accessEx::str2type(const char* rtstr)
{
    if(rtstr == NULL || *rtstr == '\0')
        return RT_DEFAULT;
    else if(stricmp(rtstr, "module") == 0)
        return RT_MODULE;
    else if(stricmp(rtstr, "service") == 0)
        return RT_SERVICE;
    else if(stricmp(rtstr, "file") == 0)
        return RT_FILE_SCOPE;
    else if(stricmp(rtstr, "workunit") == 0)
        return RT_WORKUNIT_SCOPE;
    else
        return RT_DEFAULT;
}

bool Cws_accessEx::onResourceAdd(IEspContext &context, IEspResourceAddRequest &req, IEspResourceAddResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        resp.setBasedn(req.getBasedn());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());

        StringBuffer lastResource;
        StringArray newResources;
        if(str2type(req.getRtype()) == RT_FILE_SCOPE)
        {
            getNewFileScopePermissions(secmgr, req, lastResource, newResources);
        }

        SecResourceType rtype = str2type(req.getRtype());
        try
        {
            ISecUser* usr = NULL;
            Owned<ISecResourceList> rlist = secmgr->createResourceList("ws_access");
            const char* name = req.getName();
            if(name == NULL || *name == '\0')
            {
                resp.setRetcode(-1);
                StringBuffer errmsg;
                errmsg.append(req.getRtitle()).append(" name can't be empty");
                resp.setRetmsg(errmsg.str());
                return false;
            }

            if(strchr(name, '\\') != NULL || strchr(name, '/') != NULL)
            {
                resp.setRetcode(-1);
                StringBuffer errmsg;
                errmsg.append(" you can't have '\\' or '/' in the name");
                resp.setRetmsg(errmsg.str());
                return false;
            }

            const char* ptr = strchr(name, ':');
            while(ptr != NULL)
            {
                if(*(ptr+1) != ':')
                    throw MakeStringException(ECLWATCH_SINGLE_COLON_NOT_ALLOWED, "Single colon is not allowed in scope names. Please use double colon");
                ptr = strchr(ptr+2, ':');
            }

            StringBuffer namebuf(name);
            if(rtype == RT_MODULE && stricmp(name, "repository") != 0 && Utils::strncasecmp(name, "repository.", 11) != 0)
                namebuf.insert(0, "repository.");

            const char* prefix = req.getPrefix();
            if(prefix && *prefix)
                namebuf.insert(0, prefix);

            ISecResource* r = rlist->addResource(namebuf.str());
            r->setDescription(req.getDescription());
            secmgr->addResourcesEx(rtype, *usr, rlist, PT_ADMINISTRATORS_ONLY, req.getBasedn());

            if(str2type(req.getRtype()) == RT_FILE_SCOPE && newResources.ordinality())
            {
                setNewFileScopePermissions(secmgr, req, lastResource, newResources);

                StringBuffer retmsg;
                ForEachItemIn(y, newResources)
                {
                    StringBuffer namebuf = newResources.item(y);
                    if (retmsg.length() < 1)
                        retmsg.append(namebuf);
                    else
                        retmsg.appendf(", %s", namebuf.str());
                }
                resp.setRetmsg(retmsg.str());
            }
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            resp.setRetcode(e->errorCode());
            resp.setRetmsg(emsg.str());
            return false;
        }
        catch(...)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("unknown error");
            return false;
        }
        resp.setRetcode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onResourceDelete(IEspContext &context, IEspResourceDeleteRequest &req, IEspResourceDeleteResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

        CLdapSecManager* secmgr = (CLdapSecManager*)(context.querySecManager());

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        StringArray& names = req.getNames();

        int doUpdate = req.getDoUpdate();
        if (doUpdate)
        {
            const char* basedn = req.getBasedn();
            const char* rtype = req.getRtype();
            const char* rtitle = req.getRtitle();
            const char* prefix = req.getPrefix();

            StringBuffer url("/ws_access/PermissionsResetInput");
            url.appendf("?basedn=%s", basedn);
            url.appendf("&rtype=%s", rtype);
            url.appendf("&rtitle=%s", rtitle);
            url.appendf("&prefix=%s", prefix);

            if (names.length() < 1)
                throw MakeStringException(ECLWATCH_INVALID_RESOURCE_NAME, "Please select a resource name.");

            for(unsigned i = 0; i < names.length(); i++)
            {
                const char* name = names.item(i);

                if(name == NULL || *name == '\0')
                    continue;

                url.appendf("&names_i%d=%s", i, name);
            }

            resp.setRedirectUrl(url);
            return true;
        }

        resp.setBasedn(req.getBasedn());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());
        SecResourceType rtype = str2type(req.getRtype());
        try
        {
            for(unsigned i = 0; i < names.length(); i++)
            {
                const char* name = names.item(i);

                if(name == NULL || *name == '\0')
                    continue;

                StringBuffer namebuf(name);
                if(rtype == RT_MODULE && stricmp(name, "repository") != 0 && Utils::strncasecmp(name, "repository.", 11) != 0)
                    namebuf.insert(0, "repository.");

                const char* prefix = req.getPrefix();
                if(prefix && *prefix)
                    namebuf.insert(0, prefix);

                secmgr->deleteResource(rtype, namebuf.str(), req.getBasedn());
            }
        }
        catch(IException* e)
        {
            StringBuffer emsg;
            e->errorMessage(emsg);
            resp.setRetcode(e->errorCode());
            resp.setRetmsg(emsg.str());
            return false;
        }
        catch(...)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Unknown error");
            return false;
        }

        resp.setRetcode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onResourcePermissions(IEspContext &context, IEspResourcePermissionsRequest &req, IEspResourcePermissionsResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Read);

        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;

        const char* name = req.getName();
        StringBuffer namebuf(name);
        if(str2type(req.getRtype()) == RT_MODULE && stricmp(name, "repository") != 0 && Utils::strncasecmp(name, "repository.", 11) != 0)
            namebuf.insert(0, "repository.");

        const char* prefix = req.getPrefix();
        if(prefix && *prefix)
            namebuf.insert(0, prefix);

        IArrayOf<CPermission> permissions;
        ldapsecmgr->getPermissionsArray(req.getBasedn(), str2type(req.getRtype()), namebuf.str(), permissions);

        IArrayOf<IEspResourcePermission> parray;
        ForEachItemIn(x, permissions)
        {
            CPermission& perm = permissions.item(x);

            Owned<IEspResourcePermission> onepermission = createResourcePermission();
            const char* actname = perm.getAccount_name();
            if(actname != NULL && *actname != '\0')
            {
                StringBuffer escapedname;
                int i = 0;
                char c;
                while((c = actname[i++]) != '\0')
                {
                    if(c == '\'')
                        escapedname.append('\\').append('\'');
                    else
                        escapedname.append(c);
                }
                onepermission->setAccount_name(actname);
                onepermission->setEscaped_account_name(escapedname.str());
            }
            onepermission->setAccount_type(perm.getAccount_type());

            int allows = perm.getAllows();
            int denies = perm.getDenies();
            if((allows & NewSecAccess_Access) == NewSecAccess_Access)
                onepermission->setAllow_access(true);
            if((allows & NewSecAccess_Read) == NewSecAccess_Read)
                onepermission->setAllow_read(true);
            if((allows & NewSecAccess_Write) == NewSecAccess_Write)
                onepermission->setAllow_write(true);
            if((allows & NewSecAccess_Full) == NewSecAccess_Full)
                onepermission->setAllow_full(true);
            if((denies & NewSecAccess_Access) == NewSecAccess_Access)
                onepermission->setDeny_access(true);
            if((denies & NewSecAccess_Read) == NewSecAccess_Read)
                onepermission->setDeny_read(true);
            if((denies & NewSecAccess_Write) == NewSecAccess_Write)
                onepermission->setDeny_write(true);
            if((denies & NewSecAccess_Full) == NewSecAccess_Full)
                onepermission->setDeny_full(true);

            parray.append(*onepermission.getLink());
        }

        resp.setBasedn(req.getBasedn());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setName(req.getName());
        resp.setPrefix(req.getPrefix());
        resp.setPermissions(parray);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onQueryViews(IEspContext &context, IEspQueryViewsRequest &req, IEspQueryViewsResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        IArrayOf<IEspView> views;
        StringArray names, descriptions, viewManagedBy;

        secmgr->queryAllViews(names, descriptions, viewManagedBy);

        ForEachItemIn(i, names)
        {
            Owned<IEspView> oneView = createView();
            oneView->setViewname(names.item(i));
            oneView->setDescription(descriptions.item(i));
            views.append(*oneView.getLink());
        }

        resp.setViews(views);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onAddView(IEspContext &context, IEspAddViewRequest &req, IEspAddViewResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        const char* viewname = req.getViewname();
        const char* description = req.getDescription();

        secmgr->createView(viewname, description);
        resp.setViewname(viewname);
        resp.setDescription(description);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onDeleteView(IEspContext &context, IEspDeleteViewRequest &req, IEspDeleteViewResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        const char* viewname = req.getViewname();
        secmgr->deleteView(req.getViewname());

        resp.setViewname(viewname);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onQueryViewColumns(IEspContext &context, IEspQueryViewColumnsRequest &req, IEspQueryViewColumnsResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        IArrayOf<IEspViewColumn> viewColumns;
        StringArray files, columns;

        const char* viewname = req.getViewname();

        secmgr->queryViewColumns(viewname, files, columns);

        ForEachItemIn(i, files)
        {
            Owned<IEspViewColumn> oneViewColumn = createViewColumn();
            oneViewColumn->setViewname(req.getViewname());
            oneViewColumn->setFilename(files.item(i));
            oneViewColumn->setColumnname(columns.item(i));
            viewColumns.append(*oneViewColumn.getLink());
        }

        resp.setViewname(viewname);
        resp.setViewcolumns(viewColumns);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onAddViewColumn(IEspContext &context, IEspAddViewColumnRequest &req, IEspAddViewColumnResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        const char* filename = req.getFilename();
        const char* columnname = req.getColumnname();

        if (!filename || *filename == '\0')
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Filename cannot be empty.");

        if (!columnname || *columnname == '\0')
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Columnname cannot be empty.");

        // View column filename MUST be a full path including the scope, with a leading tilde (~)
        if (filename[0] != '~')
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Filename must include a scope name, with a leading tilde (~)");

        StringArray files, columns;
        const char* viewname = req.getViewname();

        files.append(filename);
        columns.append(columnname);

        secmgr->addViewColumns(viewname, files, columns);

        resp.setViewname(viewname);
        resp.setFilename(filename);
        resp.setColumnname(columnname);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onDeleteViewColumn(IEspContext &context, IEspDeleteViewColumnRequest &req, IEspDeleteViewColumnResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        StringArray files, columns;

        const char* viewname = req.getViewname();
        const char* filename = req.getFilename();
        const char* columnname = req.getColumnname();

        files.append(filename);
        columns.append(columnname);

        secmgr->removeViewColumns(req.getViewname(), files, columns);

        resp.setViewname(viewname);
        resp.setFilename(filename);
        resp.setColumnname(columnname);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onQueryViewMembers(IEspContext &context, IEspQueryViewMembersRequest &req, IEspQueryViewMembersResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        const char* reqViewname = req.getViewname();
        StringArray users, groups;
        IArrayOf<IEspViewMember> viewMembers;

        secmgr->queryViewMembers(reqViewname, users, groups);

        ForEachItemIn(i, users)
        {
            Owned<IEspViewMember> oneViewMember = createViewMember();
            oneViewMember->setViewname(reqViewname);
            oneViewMember->setName(users.item(i));
            oneViewMember->setMembertype(CViewMemberType_User);
            viewMembers.append(*oneViewMember.getLink());
        }

        ForEachItemIn(j, groups)
        {
            Owned<IEspViewMember> oneViewMember = createViewMember();
            oneViewMember->setViewname(reqViewname);
            oneViewMember->setName(groups.item(j));
            oneViewMember->setMembertype(CViewMemberType_Group);
            viewMembers.append(*oneViewMember.getLink());
        }

        resp.setViewname(reqViewname);
        resp.setViewmembers(viewMembers);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onAddViewMember(IEspContext &context, IEspAddViewMemberRequest &req, IEspAddViewMemberResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        StringArray users, groups;
        const char* viewname = req.getViewname();
        const char* membername = req.getMembername();
        CViewMemberType membertype = req.getMembertype();

        if (membertype == CViewMemberType_User)
        {
            users.append(membername);
        }
        else if (membertype == CViewMemberType_Group)
        {
            groups.append(membername);
        }
        else
        {
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Unknown view member type specified (Must be User or Group)");
        }

        secmgr->addViewMembers(viewname, users, groups);

        resp.setViewname(viewname);
        resp.setMembername(membername);
        resp.setMembertype(membertype);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onDeleteViewMember(IEspContext &context, IEspDeleteViewMemberRequest &req, IEspDeleteViewMemberResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        StringArray users, groups;
        const char* viewname = req.getViewname();
        const char* membername = req.getMembername();
        CViewMemberType membertype = req.getMembertype();

        if (membertype == CViewMemberType_User)
        {
            users.append(membername);
        }
        else if (membertype == CViewMemberType_Group)
        {
            groups.append(membername);
        }
        else
        {
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "Unknown view member type specified (Must be User' or Group)");
        }

        secmgr->removeViewMembers(req.getViewname(), users, groups);

        resp.setViewname(viewname);
        resp.setMembername(membername);
        resp.setMembertype(membertype);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onQueryUserViewColumns(IEspContext &context, IEspQueryUserViewColumnsRequest &req, IEspQueryUserViewColumnsResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        checkUser(context);

        const char* username = req.getUsername();

        IArrayOf<IEspViewColumn> viewColumns;

        StringArray viewnames, viewdescriptions, viewManagedBy;
        secmgr->queryAllViews(viewnames, viewdescriptions, viewManagedBy);

        ForEachItemIn(i, viewnames)
        {
            const char* viewname = viewnames.item(i);

            if (secmgr->userInView(username, viewname))
            {
                StringArray files, columns;
                secmgr->queryViewColumns(viewname, files, columns);

                ForEachItemIn(j, files)
                {
                    Owned<IEspViewColumn> oneViewColumn = createViewColumn();
                    oneViewColumn->setViewname(viewname);
                    oneViewColumn->setFilename(files.item(j));
                    oneViewColumn->setColumnname(columns.item(j));
                    viewColumns.append(*oneViewColumn.getLink());
                }
            }
        }

        resp.setUsername(username);
        resp.setViewcolumns(viewColumns);
    }
    catch (IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onPermissionAddInput(IEspContext &context, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

        resp.setBasedn(req.getBasedn());
        resp.setRname(req.getRname());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());

        double version = context.getClientVersion();
        if (version < 1.01)
        {
            return permissionAddInputOnResource(context, req, resp);
        }
        else
        {
            const char* accountName = req.getAccountName();
            if (!accountName || !*accountName)
            {
                return permissionAddInputOnResource(context, req, resp);
            }
            else
            {
                return permissionAddInputOnAccount(context, accountName, req, resp);
            }
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onPermissionsResetInput(IEspContext &context, IEspPermissionsResetInputRequest &req, IEspPermissionsResetInputResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

        resp.setBasedn(req.getBasedn());
        //resp.setRname(req.getRname());
        resp.setRname("Test");
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());

        StringArray& names = req.getNames();
        if (names.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_PERMISSION_NAME, "Please select a permission.");

        StringBuffer nameList; //For forwarding to Submit page
        StringArray names1;
        ForEachItemIn(k, names)
        {
            const char* name1 = names.item(k);
            nameList.appendf("%s,", name1);
            names1.append(name1);
        }

        resp.setResourceList(nameList.str());
        resp.setResources(names);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        int numusers = secmgr->countUsers("", MAX_USERS_DISPLAY);
        if(numusers == -1)
        {
            resp.setToomany(true);
        }
        else
        {
            resp.setToomany(false);
            IArrayOf<IEspUserInfo> espusers;
            IUserArray users;
            secmgr->getAllUsers(users);
            ForEachItemIn(x, users)
            {
                CLdapSecUser* usr = dynamic_cast<CLdapSecUser*>(&users.item(x));
                if(usr)
                {
                    Owned<IEspUserInfo> oneusr = createUserInfo();
                    oneusr->setUsername(usr->getName());
                    oneusr->setFullname(usr->getFullName());
                    espusers.append(*oneusr.getLink());
                }
            }
            resp.setUsers(espusers);
        }

        IArrayOf<IEspGroupInfo> groups;
        if(secmgr->getLdapServerType() != ACTIVE_DIRECTORY)
        {
            Owned<IEspGroupInfo> onegrp = createGroupInfo();
            onegrp->setName("anyone");
            groups.append(*onegrp.getLink());
        }
        StringArray grpnames;
        StringArray managedBy;
        StringArray descriptions;
        secmgr->getAllGroups(grpnames, managedBy, descriptions);
        for(unsigned i = 0; i < grpnames.length(); i++)
        {
            const char* grpname = grpnames.item(i);
            if(grpname == NULL || *grpname == '\0')
                continue;
            Owned<IEspGroupInfo> onegrp = createGroupInfo();
            onegrp->setName(grpname);
            onegrp->setGroupDesc(descriptions.item(i));
            onegrp->setGroupOwner(managedBy.item(i));
            groups.append(*onegrp.getLink());
        }

        resp.setGroups(groups);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onClearPermissionsCache(IEspContext &context, IEspClearPermissionsCacheRequest &req, IEspClearPermissionsCacheResponse &resp)
{
    checkUser(context);

    ISecManager* secmgr = context.querySecManager();
    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    //Clear local cache
    Owned<ISecUser> user = secmgr->createUser(context.queryUserId());
    ISecCredentials& cred = user->credentials();
    cred.setPassword(context.queryPassword());
    bool ok = secmgr->clearPermissionsCache(*user);

    //Request DALI to clear its cache
    if (ok)
    {
        Owned<IUserDescriptor> userdesc;
        userdesc.setown(createUserDescriptor());
        userdesc->set(context.queryUserId(), context.queryPassword(), context.querySessionToken(), context.querySignature());
        ok = querySessionManager().clearPermissionsCache(userdesc);
    }

    resp.setRetcode(ok ? 0 : -1);
    return true;
}

bool Cws_accessEx::onQueryScopeScansEnabled(IEspContext &context, IEspQueryScopeScansEnabledRequest &req, IEspQueryScopeScansEnabledResponse &resp)
{
    ISecManager* secmgr = context.querySecManager();
    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    Owned<IUserDescriptor> userdesc;
    userdesc.setown(createUserDescriptor());
    userdesc->set(context.queryUserId(), context.queryPassword(), context.querySessionToken(), context.querySignature());
    int retCode;
    StringBuffer retMsg;
    bool isEnabled = querySessionManager().queryScopeScansEnabled(userdesc, &retCode, retMsg);
    if (retCode != 0)
        throw MakeStringException(ECLWATCH_OLD_CLIENT_VERSION, "Error %d querying scope scan status : %s", retCode, retMsg.str());
    resp.updateScopeScansStatus().setIsEnabled(isEnabled);
    resp.updateScopeScansStatus().setRetcode(retCode);
    resp.updateScopeScansStatus().setRetmsg(retMsg.str());
    return true;

}

bool Cws_accessEx::onEnableScopeScans(IEspContext &context, IEspEnableScopeScansRequest &req, IEspEnableScopeScansResponse &resp)
{
    checkUser(context, FILE_SCOPE_RTYPE, FILE_SCOPE_RTITLE, SecAccess_Full);

    StringBuffer retMsg;
    int rc = enableDisableScopeScans(context, true, retMsg);
    resp.updateScopeScansStatus().setIsEnabled(rc == 0);
    resp.updateScopeScansStatus().setRetcode(rc);
    resp.updateScopeScansStatus().setRetmsg(retMsg.str());
    return true;
}

bool Cws_accessEx::onDisableScopeScans(IEspContext &context, IEspDisableScopeScansRequest &req, IEspDisableScopeScansResponse &resp)
{
    checkUser(context, FILE_SCOPE_RTYPE, FILE_SCOPE_RTITLE, SecAccess_Full);

    StringBuffer retMsg;
    int rc = enableDisableScopeScans(context, false, retMsg);
    resp.updateScopeScansStatus().setIsEnabled(rc != 0);
    resp.updateScopeScansStatus().setRetcode(rc);
    resp.updateScopeScansStatus().setRetmsg(retMsg.str());
    return true;
}

int Cws_accessEx::enableDisableScopeScans(IEspContext &context, bool doEnable, StringBuffer &retMsg)
{
    ISecManager* secmgr = context.querySecManager();
    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    Owned<IUserDescriptor> userdesc;
    userdesc.setown(createUserDescriptor());
    userdesc->set(context.queryUserId(), context.queryPassword(), context.querySessionToken(), context.querySignature());
    int retCode;
    bool rc = querySessionManager().enableScopeScans(userdesc, doEnable, &retCode, retMsg);
    if (!rc || retCode != 0)
        DBGLOG("Error %d enabling Scope Scans : %s", retCode, retMsg.str());
    return retCode;
}

bool Cws_accessEx::permissionsReset(CLdapSecManager* ldapsecmgr, const char* basedn, const char* rtype0, const char* prefix,
        const char* resourceName, ACT_TYPE accountType, const char* accountName,
        bool allow_access, bool allow_read, bool allow_write, bool allow_full,
        bool deny_access, bool deny_read, bool deny_write, bool deny_full)
{
    CPermissionAction paction;
    paction.m_basedn.append(basedn);

    //const char* name = req.getRname();
    StringBuffer namebuf(resourceName);
    SecResourceType rtype = str2type(rtype0);
    if(rtype == RT_MODULE && stricmp(resourceName, "repository") != 0 && Utils::strncasecmp(resourceName, "repository.", 11) != 0)
        namebuf.insert(0, "repository.");
    if(prefix && *prefix)
        namebuf.insert(0, prefix);

    paction.m_rname.append(namebuf.str());
    paction.m_rtype = str2type(rtype0);
    paction.m_allows = 0;
    paction.m_denies = 0;

    if(allow_full)
        paction.m_allows |= NewSecAccess_Full;
    if(allow_read)
        paction.m_allows |= NewSecAccess_Read;
    if(allow_write)
        paction.m_allows |= NewSecAccess_Write;
    if(allow_access)
        paction.m_allows |= NewSecAccess_Access;

    if(deny_full)
        paction.m_denies |= NewSecAccess_Full;
    if(deny_read)
        paction.m_denies |= NewSecAccess_Read;
    if(deny_write)
        paction.m_denies |= NewSecAccess_Write;
    if(deny_access)
        paction.m_denies |= NewSecAccess_Access;

    paction.m_action.append("update");
    paction.m_account_type = accountType;
    paction.m_account_name.append(accountName);

    bool ret = ldapsecmgr->changePermission(paction);
    return ret;
}

bool Cws_accessEx::onPermissionsReset(IEspContext &context, IEspPermissionsResetRequest &req, IEspPermissionsResetResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

        resp.setBasedn(req.getBasedn());
        resp.setRname(req.getRname());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());
        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
        const char* users = req.getUserarray();
        const char* groups = req.getGrouparray();
        if ((!users || !*users) && (!groups || !*groups))
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "A user or group must be specified.");

        StringArray& resources = req.getNames();
        if (resources.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_RESOURCE_NAME, "A resource name must be specified.");

        StringArray userAccounts, groupAccounts;
        if (users && *users)
        {
            char* pTr = (char*) users;
            while (pTr)
            {
                char* ppTr = strchr(pTr, ',');
                if (!ppTr)
                    break;

                if (ppTr - pTr > 1)
                {
                    char userName[255];
                    strncpy(userName, pTr, ppTr - pTr);
                    userName[ppTr - pTr] = 0;
                    userAccounts.append(userName);
                }
                pTr = ppTr+1;
            }
        }
        if (groups && *groups)
        {
            char* pTr = (char*) groups;
            while (pTr)
            {
                char* ppTr = strchr(pTr, ',');
                if (!ppTr)
                    break;

                if (ppTr - pTr > 1)
                {
                    char userName[255];
                    strncpy(userName, pTr, ppTr - pTr);
                    userName[ppTr - pTr] = 0;
                    groupAccounts.append(userName);
                }
                pTr = ppTr+1;
            }
        }

        if (userAccounts.length() < 1 && groupAccounts.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "A user or group must be specified.");

        for(unsigned i = 0; i < resources.length(); i++)
        {
            const char* name = resources.item(i);
            if (!name || !*name)
                continue;

            bool ret = true;
            StringBuffer retmsg;
            try
            {
                if (userAccounts.length() > 0)
                {
                    for(unsigned j = 0; j < userAccounts.length(); j++)
                    {
                        const char* name0 = userAccounts.item(j);
                        if (!name0 || !*name0)
                            continue;

                        ret = permissionsReset(ldapsecmgr, req.getBasedn(), req.getRtype(), req.getPrefix(), name, USER_ACT, name0,
                            req.getAllow_access(), req.getAllow_read(), req.getAllow_write(), req.getAllow_full(),
                            req.getDeny_access(), req.getDeny_read(), req.getDeny_write(), req.getDeny_full());

                        if(!ret)
                        {
                            resp.setRetcode(-1);
                            resp.setRetmsg("Unknown error");
                            return false;
                        }
                    }
                }
                if (groupAccounts.length() > 0)
                {
                    for(unsigned j = 0; j < groupAccounts.length(); j++)
                    {
                        const char* name0 = groupAccounts.item(j);
                        if (!name0 || !*name0)
                            continue;

                        ret = permissionsReset(ldapsecmgr, req.getBasedn(), req.getRtype(), req.getPrefix(), name, GROUP_ACT, name0,
                            req.getAllow_access(), req.getAllow_read(), req.getAllow_write(), req.getAllow_full(),
                            req.getDeny_access(), req.getDeny_read(), req.getDeny_write(), req.getDeny_full());

                        if(!ret)
                        {
                            resp.setRetcode(-1);
                            resp.setRetmsg("Unknown error");
                            return false;
                        }
                    }
                }
            }
            catch(IException* e)
            {
                resp.setRetcode(e->errorCode());
                e->errorMessage(retmsg);
                resp.setRetmsg(retmsg.str());
                return false;
            }
        }

        resp.setRetcode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

//For every resources inside a baseDN, if there is no permission for this account, add the baseDN name to the basednNames list
void Cws_accessEx::getBaseDNsForAddingPermssionToAccount(CLdapSecManager* secmgr, const char* prefix, const char* accountName,
                                           int accountType, StringArray& basednNames)
{
    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    ForEachItemIn(i, m_basedns)
    {
        IEspDnStruct* curbasedn = &(m_basedns.item(i));
        const char *basednName = curbasedn->getName();
        if (!basednName || !*basednName)
            continue;

        const char *basedn = curbasedn->getBasedn();
        const char *rtypestr = curbasedn->getRtype();
        if (!basedn || !*basedn || !rtypestr || !*rtypestr)
            continue;

        IArrayOf<ISecResource> resources;
        SecResourceType rtype = str2type(rtypestr);
        if(!secmgr->getResources(rtype, basedn, resources))
            continue;

        ForEachItemIn(j, resources)
        {
            ISecResource& r = resources.item(j);
            const char* rname = r.getName();
            if(!rname || !*rname)
                continue;

            if(prefix && *prefix)
            {
                int prefixlen = strlen(prefix);
                if(strncmp(prefix, rname, prefixlen) == 0)
                    rname += prefixlen;
            }

            StringBuffer namebuf(rname);
            if((rtype == RT_MODULE) && !strieq(rname, "repository") && Utils::strncasecmp(rname, "repository.", 11) != 0)
                namebuf.insert(0, "repository.");
            if(prefix && *prefix)
                namebuf.insert(0, prefix);

            try
            {
                IArrayOf<CPermission> permissions;
                secmgr->getPermissionsArray(basedn, rtype, namebuf.str(), permissions);

                bool foundPermissionInThisAccount = false;
                ForEachItemIn(k, permissions)
                {
                    CPermission& perm = permissions.item(k);
                    if ((accountType == perm.getAccount_type()) && perm.getAccount_name() && streq(perm.getAccount_name(), accountName))
                    {
                        foundPermissionInThisAccount = true;
                        break;
                    }
                }
                if (!foundPermissionInThisAccount)
                {
                    basednNames.append(basednName);
                    break;
                }
            }
            catch(IException* e) //exception may be thrown when no permission for the resource
            {
                e->Release();
                break;
            }
        }
    }

    return;
}

bool Cws_accessEx::permissionAddInputOnResource(IEspContext &context, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp)
{
    CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    int numusers = secmgr->countUsers("", MAX_USERS_DISPLAY);
    if(numusers == -1)
    {
        resp.setToomany(true);
    }
    else
    {
        resp.setToomany(false);
        IArrayOf<IEspUserInfo> espusers;
        IUserArray users;
        secmgr->getAllUsers(users);
        ForEachItemIn(x, users)
        {
            CLdapSecUser* usr = dynamic_cast<CLdapSecUser*>(&users.item(x));
            if(usr)
            {
                Owned<IEspUserInfo> oneusr = createUserInfo();
                oneusr->setUsername(usr->getName());
                oneusr->setFullname(usr->getFullName());
                espusers.append(*oneusr.getLink());
            }
        }
        resp.setUsers(espusers);
    }

    IArrayOf<IEspGroupInfo> groups;
    if(secmgr->getLdapServerType() != ACTIVE_DIRECTORY)
    {
        Owned<IEspGroupInfo> onegrp = createGroupInfo();
        onegrp->setName("anyone");
        groups.append(*onegrp.getLink());
    }
    StringArray grpnames;
    StringArray managedBy;
    StringArray descriptions;
    secmgr->getAllGroups(grpnames, managedBy, descriptions);
    for(unsigned i = 0; i < grpnames.length(); i++)
    {
        const char* grpname = grpnames.item(i);
        if(grpname == NULL || *grpname == '\0')
            continue;
        Owned<IEspGroupInfo> onegrp = createGroupInfo();
        onegrp->setName(grpname);
        onegrp->setGroupDesc(descriptions.item(i));
        onegrp->setGroupOwner(managedBy.item(i));
        groups.append(*onegrp.getLink());
    }

    resp.setGroups(groups);
    return true;
}

bool Cws_accessEx::permissionAddInputOnAccount(IEspContext &context, const char* accountName, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp)
{
    CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

    if(secmgr == NULL)
        throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    resp.setBasednName(req.getBasednName());
    resp.setAccountName(req.getAccountName());
    resp.setAccountType(req.getAccountType());

    const char* prefix = req.getPrefix();
    const char* basednName = req.getBasednName();
    int accountType = req.getAccountType();
    if (basednName && *basednName)
    {
        ForEachItemIn(y, m_basedns)
        {
            IEspDnStruct* curbasedn = &(m_basedns.item(y));
            const char *aName = curbasedn->getName();

            if (!aName || stricmp(basednName, aName))
                continue;

            const char *basedn = curbasedn->getBasedn();
            const char *rtypestr = curbasedn->getRtype();
            if (!basedn || !*basedn || !rtypestr || !*rtypestr)
                continue;

            IArrayOf<ISecResource> resources;
            SecResourceType rtype = str2type(rtypestr);
            if(secmgr->getResources(rtype, basedn, resources))
            {
                StringArray resourcenames;
                for(unsigned i = 0; i < resources.length(); i++)
                {
                    ISecResource& r = resources.item(i);
                    const char* rname = r.getName();
                    if(rname == NULL || *rname == '\0')
                        continue;

                    if(prefix && *prefix)
                    {
                        int prefixlen = strlen(prefix);
                        if(strncmp(prefix, rname, prefixlen) == 0)
                            rname += prefixlen;
                    }

                    if((rtype == RT_MODULE) && stricmp(rname, "repository"))
                    {
                        if(Utils::strncasecmp(rname, "repository.", 11) == 0)
                            rname = rname + 11;
                        else
                            continue;
                    }

                    StringBuffer namebuf(rname);
                    if((rtype == RT_MODULE) && stricmp(rname, "repository") != 0 && Utils::strncasecmp(rname, "repository.", 11) != 0)
                        namebuf.insert(0, "repository.");
                    if(prefix && *prefix)
                        namebuf.insert(0, prefix);

                    try
                    {
                        IArrayOf<CPermission> permissions;
                        secmgr->getPermissionsArray(basedn, rtype, namebuf.str(), permissions);

                        bool found = false;
                        ForEachItemIn(x, permissions)
                        {
                            CPermission& perm = permissions.item(x);
                            const char* actname = perm.getAccount_name();
                            int accType = perm.getAccount_type(); //0-individual, 1 - group
                            if ((accountType == accType) && actname && !strcmp(actname, accountName))
                            {
                                found = true;
                                break;
                            }
                        }

                        if (!found)
                            resourcenames.append(rname);
                    }
                    catch(IException* e) //exception may be thrown when no permission for the resource
                    {
                        e->Release();
                        break;
                    }
                }

                if (resourcenames.length() > 0)
                                  resp.setResources(resourcenames);
            }
        }
    }
    return true;
}

bool Cws_accessEx::onPermissionAction(IEspContext &context, IEspPermissionActionRequest &req, IEspPermissionActionResponse &resp)
{
    try
    {
        checkUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

        resp.setBasedn(req.getBasedn());
        resp.setRname(req.getRname());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());
        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManager(context);

        if(ldapsecmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        CPermissionAction paction;
        paction.m_basedn.append(req.getBasedn());

        const char* name = req.getRname();
        StringBuffer namebuf(name);
        SecResourceType rtype = str2type(req.getRtype());
        if(rtype == RT_MODULE && stricmp(name, "repository") != 0 && Utils::strncasecmp(name, "repository.", 11) != 0)
            namebuf.insert(0, "repository.");
        const char* prefix = req.getPrefix();
        if(prefix && *prefix)
            namebuf.insert(0, prefix);

        double version = context.getClientVersion();
        paction.m_rname.append(namebuf.str());
        paction.m_rtype = str2type(req.getRtype());
        paction.m_account_type = (ACT_TYPE)req.getAccount_type();
        if(stricmp(req.getAction(), "add") == 0)
        {
            StringBuffer basednNameStr, resourceNameStr;
            if (version >= 1.01)
            {
                const char* basedn_name = req.getBasednName();
                const char* resource_name = req.getResourceName();
                if (basedn_name && *basedn_name)
                    basednNameStr.append(basedn_name);
                if (resource_name && *resource_name)
                    resourceNameStr.append(resource_name);
            }
            const char* user = req.getUser();
            const char* grp = req.getGroup();
            if(user != NULL && *user != '\0')
            {
                paction.m_account_name.append(user);
                paction.m_account_type = USER_ACT;
            }
            else if(grp != NULL && *grp != '\0')
            {
                paction.m_account_name.append(grp);
                // anyone is actually treated as a virtual "user" by sun and open ldap.
                if((ldapsecmgr->getLdapServerType() != ACTIVE_DIRECTORY) && (stricmp(grp, "anyone") == 0))
                    paction.m_account_type = USER_ACT;
                else
                    paction.m_account_type = GROUP_ACT;
            }
            else if((basednNameStr.length() > 0) && (resourceNameStr.length() > 0))
            {
                const char* account_name = req.getAccount_name();
                if (!account_name || !*account_name)
                {
                    resp.setRetcode(-1);
                    resp.setRetmsg("Please input or select user/group");
                    return false;
                }

                paction.m_account_name.clear().append(account_name);

                ForEachItemIn(y, m_basedns)
                {
                    IEspDnStruct* curbasedn = &(m_basedns.item(y));
                    const char *aName = curbasedn->getName();
                    if (!aName || stricmp(basednNameStr.str(), aName))
                        continue;

                    const char *basedn = curbasedn->getBasedn();
                    const char *rtypestr = curbasedn->getRtype();
                    if (!basedn || !*basedn || !rtypestr || !*rtypestr)
                        continue;

                    StringBuffer namebuf(resourceNameStr);
                    SecResourceType rtype = str2type(rtypestr);
                    if(rtype == RT_MODULE && stricmp(namebuf.str(), "codegenerator.cpp") && stricmp(namebuf.str(), "repository") != 0 && Utils::strncasecmp(namebuf.str(), "repository.", 11) != 0)
                        namebuf.insert(0, "repository.");
                    if(prefix && *prefix)
                        namebuf.insert(0, prefix);
                    paction.m_basedn.clear().append(basedn);
                    paction.m_rname.clear().append(namebuf.str());
                    paction.m_rtype = rtype;
                    break;
                }
                resp.setAccountName(account_name);
                if (req.getAccount_type() < 1)
                    resp.setIsGroup(false);
                else
                    resp.setIsGroup(true);
            }
            else
            {
                resp.setRetcode(-1);
                resp.setRetmsg("Please input or select user/group");
                return false;
            }
        }
        else
        {
            paction.m_account_name.append(req.getAccount_name());
            if (version >= 1.01)
            {
                resp.setAccountName(req.getAccount_name());
                if (req.getAccount_type() < 1)
                    resp.setIsGroup(false);
                else
                    resp.setIsGroup(true);
            }
        }

        paction.m_allows = 0;
        paction.m_denies = 0;

        if(req.getAllow_full())
            paction.m_allows |= NewSecAccess_Full;
        if(req.getAllow_read())
            paction.m_allows |= NewSecAccess_Read;
        if(req.getAllow_write())
            paction.m_allows |= NewSecAccess_Write;
        if(req.getAllow_access())
            paction.m_allows |= NewSecAccess_Access;

        if(req.getDeny_full())
            paction.m_denies |= NewSecAccess_Full;
        if(req.getDeny_read())
            paction.m_denies |= NewSecAccess_Read;
        if(req.getDeny_write())
            paction.m_denies |= NewSecAccess_Write;
        if(req.getDeny_access())
            paction.m_denies |= NewSecAccess_Access;

        paction.m_action.append(req.getAction());

        bool ret = true;
        StringBuffer retmsg;
        try
        {
            ret = ldapsecmgr->changePermission(paction);
        }
        catch(IException* e)
        {
            resp.setRetcode(e->errorCode());
            e->errorMessage(retmsg);
            resp.setRetmsg(retmsg.str());
            return false;
        }

        if(!ret)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Unknown error");
            return false;
        }

        resp.setRetcode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserResetPassInput(IEspContext &context, IEspUserResetPassInputRequest &req, IEspUserResetPassInputResponse &resp)
{
    try
    {
        checkUser(context);

        resp.setUsername(req.getUsername());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}


bool Cws_accessEx::onUserResetPass(IEspContext &context, IEspUserResetPassRequest &req, IEspUserResetPassResponse &resp)
{
    try
    {
        checkUser(context);
        resp.setUsername(req.getUsername());

        ISecManager* secmgr = context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;

        const char* username = req.getUsername();
        if(username == NULL)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("username can't be empty");
            return false;
        }

        const char* newpass1 = req.getNewPassword();
        const char* newpass2 = req.getNewPasswordRetype();
        if(newpass1 == NULL || newpass2 == NULL || *newpass1 == '\0' || *newpass2 == '\0' || strcmp(newpass1, newpass2) != 0)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("new password and retype can't be empty and must match");
            return false;
        }

        bool ret = ldapsecmgr->updateUserPassword(username, req.getNewPassword());
        if(ret)
        {
            resp.setRetcode(0);
            resp.setRetmsg("");
            return false;
        }
        else
        {
            resp.setRetcode(-1);
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserPosix(IEspContext &context, IEspUserPosixRequest &req, IEspUserPosixResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("username can't be empty");
            return false;
        }

        bool enable = req.getPosixenabled();
        Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(username);
        if(enable)
        {
            const char* gidnumber = req.getGidnumber();
            const char* uidnumber = req.getUidnumber();
            const char* homedirectory = req.getHomedirectory();
            const char* loginshell = req.getLoginshell();

            if(!gidnumber || !*gidnumber || !uidnumber || !*uidnumber || !homedirectory || !*homedirectory)
            {
                resp.setRetcode(-1);
                resp.setRetmsg("gidnumber, uidnumber and homedirectory are required.");
                return false;
            }

            unsigned i;
            for(i = 0; i < strlen(gidnumber); i++)
            {
                if(!isdigit(gidnumber[i]))
                    throw MakeStringException(ECLWATCH_ID_MUST_BE_ALL_DIGITS, "Group ID Number should be all digits");
            }

            for(i = 0; i < strlen(uidnumber); i++)
            {
                if(!isdigit(uidnumber[i]))
                    throw MakeStringException(ECLWATCH_ID_MUST_BE_ALL_DIGITS, "User ID Number should be all digits");
            }

            user->setGidnumber(gidnumber);
            user->setUidnumber(uidnumber);
            user->setHomedirectory(homedirectory);
            user->setLoginshell(loginshell);
        }

        try
        {
            secmgr->updateUser(enable?"posixenable":"posixdisable", *user.get());
        }
        catch(IException* e)
        {
            resp.setRetcode(-1);
            StringBuffer errmsg;
            resp.setRetmsg(e->errorMessage(errmsg).str());
            return false;
        }

        resp.setUsername(username);
        resp.setRetcode(0);
        resp.setRetmsg("User's posix account info has been successfully updated");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserPosixInput(IEspContext &context, IEspUserPosixInputRequest &req, IEspUserPosixInputResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "Please specify a username.");
        }

        Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(username);
        secmgr->getUserInfo(*user.get());

        resp.setUsername(username);

        resp.setPosixenabled(user->getPosixenabled());
        if(user->getGidnumber())
            resp.setGidnumber(user->getGidnumber());
        if(user->getUidnumber())
            resp.setUidnumber(user->getUidnumber());
        if(user->getHomedirectory())
            resp.setHomedirectory(user->getHomedirectory());
        if(user->getLoginshell())
            resp.setLoginshell(user->getLoginshell());
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserInfoEdit(IEspContext &context, IEspUserInfoEditRequest &req, IEspUserInfoEditResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("username can't be empty");
            return false;
        }

        const char* firstname = req.getFirstname();
        const char* lastname = req.getLastname();
        if((!firstname || !*firstname) && (!lastname || !*lastname))
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Please specify both firstname and lastname");
            return false;
        }

        Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(username);

        user->setFirstName(firstname);
        user->setLastName(lastname);
        if (context.getClientVersion() >= 1.10)
        {
            user->setEmployeeID(req.getEmployeeID());
        }

        try
        {
            secmgr->updateUser("names", *user.get());
        }
        catch(IException* e)
        {
            resp.setRetcode(-1);
            StringBuffer errmsg;
            resp.setRetmsg(e->errorMessage(errmsg).str());
            return false;
        }

        resp.setUsername(username);
        resp.setRetcode(0);
        resp.setRetmsg("User's account info has been successfully updated");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserInfoEditInput(IEspContext &context, IEspUserInfoEditInputRequest &req, IEspUserInfoEditInputResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "Please specify a username.");
        }

        Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(username);
        secmgr->getUserInfo(*user.get());

        resp.setUsername(username);

        resp.setFirstname(user->getFirstName());
        resp.setLastname(user->getLastName());
        if (context.getClientVersion() >= 1.10)
        {
            resp.setEmployeeID(user->getEmployeeID());
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserSudoersInput(IEspContext &context, IEspUserSudoersInputRequest &req, IEspUserSudoersInputResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "Please specify a username.");
        }

        Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(username);
        secmgr->getUserInfo(*user.get(), "sudoers");
        resp.setUsername(username);
        resp.setInsudoers(user->getInSudoers());
        if(user->getInSudoers())
        {
            resp.setSudoHost(user->getSudoHost());
            resp.setSudoCommand(user->getSudoCommand());
            resp.setSudoOption(user->getSudoOption());
        }
        else
        {
            resp.setSudoHost("ALL");
            resp.setSudoCommand("ALL");
            resp.setSudoOption("!authenticate");
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onUserSudoers(IEspContext &context, IEspUserSudoersRequest &req, IEspUserSudoersResponse &resp)
{
    try
    {
        checkUser(context);

        CLdapSecManager* secmgr = (CLdapSecManager*)context.querySecManager();

        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("username can't be empty");
            return false;
        }

        resp.setUsername(username);

        Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(username);
        const char* action = req.getAction();
        if(!action || !*action)
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Action can't be empty");
            return false;
        }

        user->setSudoHost(req.getSudoHost());
        user->setSudoCommand(req.getSudoCommand());
        user->setSudoOption(req.getSudoOption());

        bool ok = false;
        StringBuffer retmsg;

        try
        {
            if(stricmp(action, "add") == 0)
                ok = secmgr->updateUser("sudoersadd", *user.get());
            else if(stricmp(action, "delete") == 0)
                ok = secmgr->updateUser("sudoersdelete", *user.get());
            else if(stricmp(action, "update") == 0)
                ok = secmgr->updateUser("sudoersupdate", *user.get());
        }
        catch(IException* e)
        {
            ok = false;
            e->errorMessage(retmsg);
            e->Release();
        }
        catch(...)
        {
            ok = false;
            retmsg.append("unknown exception");
        }

        if(!ok)
        {
            resp.setRetcode(-1);
            resp.setRetmsg(retmsg.str());
        }
        else
        {
            resp.setRetcode(0);
            resp.setRetmsg("succeeded.");
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onAccountPermissions(IEspContext &context, IEspAccountPermissionsRequest &req, IEspAccountPermissionsResponse &resp)
{
    try
    {
        StringBuffer userID;
        bool bGroupAccount = req.getIsGroup();
        const char* username = req.getAccountName();
        if(!username || !*username)
        {//send back the permissions for the current user.
            context.getUserID(userID);
            if (!userID.length())
                throw MakeStringException(ECLWATCH_INVALID_INPUT, "Could not get user ID.");
            username = userID.str();
            bGroupAccount = false;
        }
        else
            checkUser(context);

        double version = context.getClientVersion();

        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManager(context);

        if(ldapsecmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        bool bIncludeGroup = req.getIncludeGroup();

        if(m_basedns.length() == 0)
        {
            setBasedns(context);
        }

        StringArray groupnames;
        if (version > 1.02 && !bGroupAccount && bIncludeGroup)
        {
            ldapsecmgr->getGroups(username, groupnames);
        }

        groupnames.append("Authenticated Users");
        groupnames.append("everyone");

        IArrayOf<IEspAccountPermission> accountPermissions;

        bool bAuthUsersPerm = false;
        Owned<IEspGroupAccountPermission> grouppermission1 = createGroupAccountPermission();
        grouppermission1->setGroupName("Authenticated Users");
        if (version > 1.05)
        {
            StringArray basednNames;
            getBaseDNsForAddingPermssionToAccount(ldapsecmgr, NULL, "Authenticated Users", 1, basednNames);
            if (basednNames.length() > 0)
                grouppermission1->setBasednNames(basednNames);
        }
        IArrayOf<IConstAccountPermission>& authUsersPermissions = grouppermission1->getPermissions();

        bool bEveryonePerm = false;
        Owned<IEspGroupAccountPermission> grouppermission2 = createGroupAccountPermission();
        grouppermission2->setGroupName("Everyone");
        if (version > 1.05)
        {
            StringArray basednNames;
            getBaseDNsForAddingPermssionToAccount(ldapsecmgr, NULL, "Everyone", 1, basednNames);
            if (basednNames.length() > 0)
                grouppermission2->setBasednNames(basednNames);
        }
        IArrayOf<IConstAccountPermission>& everyonePermissions = grouppermission2->getPermissions();

        IArrayOf<IEspGroupAccountPermission> groupAccountPermissions;

        StringBuffer moduleBasedn; //To be used by the Permission: codegenerator.cpp
        ForEachItemIn(y1, m_basedns)
        {
            IEspDnStruct* curbasedn = &(m_basedns.item(y1));
            const char *aName = curbasedn->getName();
            const char *aBasedn = curbasedn->getBasedn();
            const char *aRtype = curbasedn->getRtype();
            const char *aRtitle = curbasedn->getRtitle();
            if (!aName || !*aName ||!aBasedn || !*aBasedn ||!aRtype || !*aRtype ||!aRtitle || !*aRtitle)
                continue;

            SecResourceType rtype = str2type(aRtype);
            if (rtype == RT_MODULE)
            {
                moduleBasedn.append(aBasedn);
                break;
            }
        }

        ForEachItemIn(y, m_basedns)
        {
            IEspDnStruct* curbasedn = &(m_basedns.item(y));
            const char *aName = curbasedn->getName();
            const char *aBasedn = curbasedn->getBasedn();
            const char *aRtype = curbasedn->getRtype();
            const char *aRtitle = curbasedn->getRtitle();
            if (!aName || !*aName ||!aBasedn || !*aBasedn ||!aRtype || !*aRtype ||!aRtitle || !*aRtitle)
                continue;

            SecResourceType rtype = str2type(aRtype);

            IArrayOf<IEspResource> ResourceArray;
            //if(rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE)
            if(rtype == RT_WORKUNIT_SCOPE)
            {
                StringBuffer deft_basedn, deft_name;
                const char* comma = strchr(aBasedn, ',');
                const char* eqsign = strchr(aBasedn, '=');
                if(eqsign != NULL)
                {
                    if(comma == NULL)
                        deft_name.append(eqsign+1);
                    else
                    {
                        deft_name.append(comma - eqsign - 1, eqsign+1);
                        deft_basedn.append(comma + 1);
                    }
                }

                if (deft_name.length() > 0)
                {
                    Owned<IEspResource> oneresource = createResource();
                    oneresource->setName(deft_name);
                    oneresource->setDescription(deft_basedn);
                    ResourceArray.append(*oneresource.getLink());
                }
            }

            IArrayOf<ISecResource> resources;
            if(ldapsecmgr->getResources(rtype, aBasedn, resources))
            {
                ForEachItemIn(y1, resources)
                {
                    ISecResource& r = resources.item(y1);
                    const char* rname = r.getName();
                    if(rname == NULL || *rname == '\0')
                        continue;

                    //permission codegenerator.cpp is saved as a service permission (not a module permission)
                    //when it is added for a user
                    if ((rtype == RT_MODULE) && (!stricmp(rname, "codegenerator.cpp")))
                        continue;

                    if((rtype == RT_MODULE) && Utils::strncasecmp(rname, "repository", 10))
                    {
                        continue;
                    }

                    Owned<IEspResource> oneresource = createResource();
                    oneresource->setName(rname);
                    oneresource->setDescription(aBasedn);
                    ResourceArray.append(*oneresource.getLink());
                }
            }

            if(rtype == RT_SERVICE && moduleBasedn.length() > 0)
            {  //permission codegenerator.cpp is saved as a service permission when it is added for a user
                Owned<IEspResource> oneresource = createResource();
                oneresource->setName("codegenerator.cpp");
                oneresource->setDescription(moduleBasedn.str());
                ResourceArray.append(*oneresource.getLink());

                moduleBasedn.clear();
            }

            ForEachItemIn(y2, ResourceArray)
            {
                IEspResource& r = ResourceArray.item(y2);
                const char* rname = r.getName();
                const char* dnname = r.getDescription();
                if(rname == NULL || *rname == '\0')
                    continue;

                StringBuffer namebuf(rname);
                //const char* prefix = req.getPrefix();
                //if(prefix && *prefix)
                //  namebuf.insert(0, prefix);

                try
                {
                    IArrayOf<CPermission> permissions;
                    ldapsecmgr->getPermissionsArray(dnname, rtype, namebuf.str(), permissions);
                    ForEachItemIn(x, permissions)
                    {
                        CPermission& perm = permissions.item(x);
                        int accountType = perm.getAccount_type(); //0-individual, 1 - group
                        if (bGroupAccount && accountType < 1)
                            continue;

                        if (!bGroupAccount && (accountType > 0) && (groupnames.length() < 1))
                            continue;

                        StringBuffer escapedname;
                        const char* actname = perm.getAccount_name();
                        if ((!bGroupAccount && accountType < 1) || (bGroupAccount && accountType > 0))
                        {
                            if(!actname || strcmp(actname, username))
                                continue;
                        }
                        else if (version > 1.02)
                        {
                            if(!actname || groupnames.length() < 1)
                                continue;

                            bool bFound = false;
                            for(unsigned i = 0; i < groupnames.length(); i++)
                            {
                                const char* group = groupnames.item(i);
                                if (!group || strcmp(actname, group))
                                    continue;
                                bFound = true;
                                break;
                            }

                            if (!bFound)
                                continue;
                        }

                        Owned<IEspAccountPermission> onepermission = createAccountPermission();
                        onepermission->setBasedn(dnname);
                        onepermission->setRType(aRtype);
                        onepermission->setResourceName(aRtitle);
                        onepermission->setPermissionName(namebuf.str());

                        int allows = perm.getAllows();
                        int denies = perm.getDenies();
                        if((allows & NewSecAccess_Access) == NewSecAccess_Access)
                            onepermission->setAllow_access(true);
                        if((allows & NewSecAccess_Read) == NewSecAccess_Read)
                            onepermission->setAllow_read(true);
                        if((allows & NewSecAccess_Write) == NewSecAccess_Write)
                            onepermission->setAllow_write(true);
                        if((allows & NewSecAccess_Full) == NewSecAccess_Full)
                            onepermission->setAllow_full(true);
                        if((denies & NewSecAccess_Access) == NewSecAccess_Access)
                            onepermission->setDeny_access(true);
                        if((denies & NewSecAccess_Read) == NewSecAccess_Read)
                            onepermission->setDeny_read(true);
                        if((denies & NewSecAccess_Write) == NewSecAccess_Write)
                            onepermission->setDeny_write(true);
                        if((denies & NewSecAccess_Full) == NewSecAccess_Full)
                            onepermission->setDeny_full(true);

                        if ((!bGroupAccount && accountType < 1) || (bGroupAccount && accountType > 0))
                            accountPermissions.append(*onepermission.getLink());
                        else if (version > 1.02)
                        {
                            if (!strcmp(actname, "Authenticated Users"))
                            {
                                authUsersPermissions.append(*onepermission.getLink());
                                bAuthUsersPerm = true;
                            }
                            else if (!strcmp(actname, "everyone"))
                            {
                                everyonePermissions.append(*onepermission.getLink());
                                bEveryonePerm = true;
                            }
                            else
                            {
                                bool bFound = false;
                                ForEachItemIn(k, groupAccountPermissions)
                                {
                                    IEspGroupAccountPermission& grouppermission0 = groupAccountPermissions.item(k);
                                    const char* g_name = grouppermission0.getGroupName();
                                    if (!g_name || strcmp(actname, g_name))
                                        continue;

                                    IArrayOf<IConstAccountPermission>& g_permission = grouppermission0.getPermissions();
                                    g_permission.append(*onepermission.getLink());
                                    bFound = true;
                                    break;
                                }

                                if (!bFound)
                                {
                                    Owned<IEspGroupAccountPermission> grouppermission = createGroupAccountPermission();
                                    grouppermission->setGroupName(actname);
                                    if (version > 1.05)
                                    {
                                        StringArray basednNames;
                                        getBaseDNsForAddingPermssionToAccount(ldapsecmgr, NULL, actname, 1, basednNames);
                                        if (basednNames.length() > 0)
                                            grouppermission->setBasednNames(basednNames);
                                    }

                                    IArrayOf<IConstAccountPermission>& g_permission = grouppermission->getPermissions();
                                    g_permission.append(*onepermission.getLink());
                                    groupAccountPermissions.append(*grouppermission.getLink());
                                }
                            }
                        }
                    }
                }
                catch(IException* e) //exception may be thrown when no permission for the resource
                {
                    e->Release();
                }
            }
        }

        StringArray basednNames;
        getBaseDNsForAddingPermssionToAccount(ldapsecmgr, NULL, username, bGroupAccount? 1:0, basednNames);
        if (basednNames.length() > 0)
        {
            resp.setBasednNames(basednNames);
        }

        if (accountPermissions.length() > 0)
        {
            resp.setPermissions(accountPermissions);
        }

        if (version > 1.02)
        {
            if (bAuthUsersPerm)
            {
                groupAccountPermissions.append(*grouppermission1.getLink());
            }

            if (bEveryonePerm)
            {
                groupAccountPermissions.append(*grouppermission2.getLink());
            }

            if (groupAccountPermissions.length() > 0)
            {
                resp.setGroupPermissions(groupAccountPermissions);
            }
        }

        resp.setAccountName(username);
        resp.setIsGroup(bGroupAccount);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool Cws_accessEx::onFilePermission(IEspContext &context, IEspFilePermissionRequest &req, IEspFilePermissionResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context);
        double version = context.getClientVersion();
        if (version > 1.03)
        {
            if(secmgr == NULL)
            {
                resp.setNoSecMngr(true);
                return true;
            }
        }
        else
        {
            if(secmgr == NULL)
                throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        }

        checkUser(context, FILE_SCOPE_RTYPE, FILE_SCOPE_RTITLE, SecAccess_Read);

        //Get all users for input form
        int numusers = secmgr->countUsers("", MAX_USERS_DISPLAY);
        if(numusers == -1)
        {
            resp.setToomany(true);
        }
        else
        {
            resp.setToomany(false);
            IArrayOf<IEspUserInfo> espusers;
            IUserArray users;
            secmgr->getAllUsers(users);
            ForEachItemIn(x, users)
            {
                CLdapSecUser* usr = dynamic_cast<CLdapSecUser*>(&users.item(x));
                if(usr)
                {
                    Owned<IEspUserInfo> oneusr = createUserInfo();
                    oneusr->setUsername(usr->getName());
                    oneusr->setFullname(usr->getFullName());
                    espusers.append(*oneusr.getLink());
                }
            }
            resp.setUsers(espusers);
        }

        //Get all groups for input form
        StringArray groupnames;
        StringArray managedBy;
        StringArray descriptions;
        secmgr->getAllGroups(groupnames, managedBy, descriptions);
        ///groupnames.append("Authenticated Users");
        ///groupnames.append("Administrators");
        if (groupnames.length() > 0)
        {
            IArrayOf<IEspGroupInfo> groups;
            for(unsigned i = 0; i < groupnames.length(); i++)
            {
                const char* grpname = groupnames.item(i);
                if(grpname == NULL || grpname[0] == '\0')
                    continue;
                Owned<IEspGroupInfo> onegrp = createGroupInfo();
                onegrp->setName(grpname);
                onegrp->setGroupDesc(descriptions.item(i));
                onegrp->setGroupOwner(managedBy.item(i));
                groups.append(*onegrp.getLink());
            }

            resp.setGroups(groups);
        }

        const char* fileName = req.getFileName();
        const char* userName = req.getUserName();
        const char* groupName = req.getGroupName();
        if (!fileName || !*fileName)
            return true; //no file name is set when the input form is launched first time

        if ((!groupName || !*groupName) && (!userName || !*userName))
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "Either user name or group name has to be specified.");

        SecAccessFlags access = SecAccess_Unavailable;
        if (userName && *userName) //for user
        {
            resp.setFileName(fileName);
            resp.setUserName(userName);

            Owned<ISecUser> sec_user = secmgr->findUser(userName);
            if (sec_user)
            {
                StringBuffer accessStr;
                access = secmgr->authorizeEx(RT_FILE_SCOPE, *sec_user, fileName, false);
                switch (access)
                {
                case SecAccess_Full:
                    resp.setUserPermission("Full Access Permission");
                    break;
                case SecAccess_Write:
                    resp.setUserPermission("Write Access Permission");
                    break;
                case SecAccess_Read:
                    resp.setUserPermission("Read Access Permission");
                    break;
                case SecAccess_Access:
                    resp.setUserPermission("Access Permission");
                    break;
                case SecAccess_None:
                    resp.setUserPermission("None Access Permission");
                    break;
                default:
                    resp.setUserPermission("Permission Unknown");
                    break;
                }
            }
        }
        else //for group
        {
            resp.setFileName(fileName);
            resp.setGroupName(groupName);

            if(m_basedns.length() == 0) //basedns may never be set
            {
                setBasedns(context);
            }

            //Find out the basedn for RT_FILE_SCOPE
            CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;
            StringBuffer basednStr;
            ForEachItemIn(y, m_basedns)
            {
                IEspDnStruct* curbasedn = &(m_basedns.item(y));
                const char *aBasedn = curbasedn->getBasedn();
                const char *aRtype = curbasedn->getRtype();
                if (!aBasedn || !*aBasedn || !aRtype || !*aRtype)
                    continue;

                SecResourceType rtype = str2type(aRtype);
                if (rtype != RT_FILE_SCOPE)
                    continue;

                basednStr.append(aBasedn);
            }

            char* pStr0 = (char*) fileName;
            while (pStr0[0] == ':') //in case of some ':' by mistake
                pStr0++;

            //Check the permissin for the file and the group
            if (*pStr0 && basednStr.length() > 0)
            {
                StringBuffer lastFileScope;
                StringArray scopes;

                char* pStr = strstr(pStr0, "::");
                while (pStr)
                {
                    char fileScope[10240];
                    strncpy(fileScope, pStr0, pStr-pStr0);
                    fileScope[pStr-pStr0] = 0;

                    if (lastFileScope.length() < 1)
                        lastFileScope.append(fileScope);
                    else
                        lastFileScope.appendf("::%s", fileScope);
                    scopes.add(lastFileScope.str(), 0);

                    pStr0 = pStr+2;
                    while (pStr0[0] == ':') //in case of more than two ':' by mistake
                        pStr0++;
                    if (pStr0[0] == 0)
                        break;

                    pStr = strstr(pStr0, "::");
                }

                if (pStr0[0] != 0)
                {
                    if (lastFileScope.length() < 1)
                        lastFileScope.append(pStr0);
                    else
                        lastFileScope.appendf("::%s", pStr0);
                    scopes.add(lastFileScope.str(), 0);
                }

                access = SecAccess_None;
                ForEachItemIn(y, scopes)
                {
                    StringBuffer namebuf = scopes.item(y);
                    try
                    {
                        IArrayOf<CPermission> permissions;
                        ldapsecmgr->getPermissionsArray(basednStr.str(), RT_FILE_SCOPE, namebuf.str(), permissions);
                        ForEachItemIn(x, permissions)
                        {
                            CPermission& perm = permissions.item(x);
                            int accountType = perm.getAccount_type(); //0-individual, 1 - group
                            if (accountType < 1)
                                continue;

                            const char* actname = perm.getAccount_name();
                            if(!actname || strcmp(actname, groupName))
                                continue;

                            int allows = perm.getAllows();
                            int denies = perm.getDenies();
                            access = (SecAccessFlags)(allows & (~denies));
                            break;
                        }
                    }
                    catch(IException* e) //exception may be thrown when no permission for the resource
                    {
                        e->Release();
                    }

                    if (access != SecAccess_None)
                        break;
                }
            }

            //Convert permission type to display string
            if((access & NewSecAccess_Full) == NewSecAccess_Full)
                resp.setUserPermission("Full Access Permission");
            else if((access & NewSecAccess_Write) == NewSecAccess_Write)
                resp.setUserPermission("Write Access Permission");
            else if((access & NewSecAccess_Read) == NewSecAccess_Read)
                resp.setUserPermission("Read Access Permission");
            else if((access & NewSecAccess_Access) == NewSecAccess_Access)
                resp.setUserPermission("Access Permission");
            else if (access == (SecAccessFlags)NewSecAccess_None)
                resp.setUserPermission("None Access Permission");
            else
                resp.setUserPermission("Permission Unknown");
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool Cws_accessEx::onUserAccountExport(IEspContext &context, IEspUserAccountExportRequest &req, IEspUserAccountExportResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr == NULL)
        {
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);
        }

        CLdapSecManager* ldapsecmgr = (CLdapSecManager*)secmgr;

        checkUser(context);

        StringBuffer xls;
        xls.append("<html xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">");
        xls.append("<head>");
        xls.append("<META http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\">");
        xls.append("<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\">");
        xls.append("<title>User Account Information</title>");
        xls.append("</head>");
        xls.append("<body>");
        xls.append("<table xmlns:msxsl=\"urn:schemas-microsoft-com:xslt\" cellspacing=\"0\" frame=\"box\" rules=\"all\">");
        xls.append("<thead>");
        xls.append("<tr valign=\"bottom\">");
        xls.append("<th>Login Name</th>");
        xls.append("<th>First Name</th>");
        xls.append("<th>Last Name</th>");
        xls.append("<th>Group Name</th>");
        xls.append("</tr>");

        StringArray& usernames = req.getUsernames();
        StringArray& groupnames = req.getGroupnames();
        if (usernames.length() > 0)
        {
            for(unsigned i = 0; i < usernames.length(); i++)
            {
                const char* username = usernames.item(i);
                if (!username || !*username)
                    continue;

                Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(username);
                secmgr->getUserInfo(*user.get());
                const char* firstname = user->getFirstName();
                const char* lastname = user->getLastName();

                StringArray groupnames1;
                ldapsecmgr->getGroups(username, groupnames1);
                ///groupnames1.append("TestGroup1");
                ///groupnames1.append("TestGroup2");
                if (groupnames1.length() < 1)
                {
                    xls.append("<tr>");
                    xls.appendf("<td>%s</td>", username);
                    if (!firstname || !*firstname)
                        xls.append("<td></td>");
                    else
                        xls.appendf("<td>%s</td>", firstname);
                    if (!lastname || !*lastname)
                        xls.append("<td></td>");
                    else
                        xls.appendf("<td>%s</td>", lastname);
                    xls.append("<td></td>");
                    xls.append("</tr>");
                }
                else
                {
                    for(unsigned i = 0; i < groupnames1.length(); i++)
                    {
                        const char* grpname = groupnames1.item(i);
                        if(grpname == NULL || grpname[0] == '\0')
                            continue;

                        xls.append("<tr>");
                        xls.appendf("<td>%s</td>", username);
                        if (!firstname || !*firstname)
                            xls.append("<td></td>");
                        else
                            xls.appendf("<td>%s</td>", firstname);
                        if (!lastname || !*lastname)
                            xls.append("<td></td>");
                        else
                            xls.appendf("<td>%s</td>", lastname);
                        xls.appendf("<td>%s</td>", grpname);
                        xls.append("</tr>");
                    }
                }
            }
        }
        else if (groupnames.length() > 0)
        {
            for(unsigned i = 0; i < groupnames.length(); i++)
            {
                const char* groupname = groupnames.item(i);
                if (!groupname || !*groupname)
                    continue;

                StringArray usernames1;
                ldapsecmgr->getGroupMembers(groupname, usernames1);
                ///usernames1.append("_clo");
                ///usernames1.append("_rkc");
                for(unsigned j = 0; j < usernames1.length(); j++)
                {
                    const char* usrname = usernames1.item(j);
                    if(usrname == NULL || usrname[0] == '\0')
                        continue;

                    Owned<CLdapSecUser> user = (CLdapSecUser*)secmgr->createUser(usrname);
                    secmgr->getUserInfo(*user.get());
                    const char* firstname = user->getFirstName();
                    const char* lastname = user->getLastName();

                    xls.append("<tr>");
                    xls.appendf("<td>%s</td>", usrname);
                    if (!firstname || !*firstname)
                        xls.append("<td></td>");
                    else
                        xls.appendf("<td>%s</td>", firstname);
                    if (!lastname || !*lastname)
                        xls.append("<td></td>");
                    else
                        xls.appendf("<td>%s</td>", lastname);
                    xls.appendf("<td>%s</td>", groupname);
                    xls.append("</tr>");
                }
            }
        }

        xls.append("</thead>");
        xls.append("</table>");
        xls.append("</body>");
        xls.append("</html>");

        MemoryBuffer buff;
        buff.setBuffer(xls.length(), (void*)xls.str());
        resp.setResult(buff);
        resp.setResult_mimetype("application/vnd.ms-excel");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

int Cws_accessSoapBindingEx::onGetForm(IEspContext &context, CHttpRequest* request, CHttpResponse* response, const char *service, const char *method)
{
    try
    {
        if(stricmp(method,"SecurityNotEnabled")==0)
        {
            StringBuffer page;
            page.append(
                "<html>"
                    "<head>"
                        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/default.css\"/>"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/yui/build/fonts/fonts-min.css\" />"
                        "<title>Security Not Enabled</title>"
                    "</head>"
                    "<body>"
                        "<p style=\"text-align:centre;\">In order to use this feature, authentication should be enabled.");
            page.append("</p></body>"
                "</html>");

            response->setContent(page.str());
            response->setContentType("text/html");
            response->send();
            return 0;
        }
        else if(stricmp(method,"FirefoxNotSupport")==0)
        {
            StringBuffer page;
            page.append(
                "<html>"
                    "<head>"
                        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/default.css\"/>"
                        "<link rel=\"stylesheet\" type=\"text/css\" href=\"/esp/files/yui/build/fonts/fonts-min.css\" />"
                        "<title>Firefox Not Support</title>"
                    "</head>"
                    "<body>"
                        "<p style=\"text-align:centre;\">This feature is not supported under Firefox.");
            page.append("</p></body>"
                "</html>");

            response->setContent(page.str());
            response->setContentType("text/html");
            response->send();
            return 0;
        }
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e,  ECLWATCH_INTERNAL_ERROR);
    }
    return onGetForm(context, request, response, service, method);
}
