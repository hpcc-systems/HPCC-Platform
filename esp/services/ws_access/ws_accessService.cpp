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
#include "dautils.hpp"

#include <set>

#define MSG_SEC_MANAGER_IS_NULL "Security manager is not found, or is not LDAP. Please check if the system authentication is set up correctly."
#define MSG_SEC_MANAGER_ISNT_LDAP "LDAP Security manager is required for this feature. Please enable LDAP in the system configuration"

#define FILE_SCOPE_URL "FileScopeAccess"
#define FILE_SCOPE_RTYPE "file"
#define FILE_SCOPE_RTITLE "FileScope"

#define MAX_USERS_DISPLAY 400
#define MAX_RESOURCES_DISPLAY 3000
static const long MAXXLSTRANSFER = 5000000;

SecResourceType str2RType(const char* str)
{
    if (isEmptyString(str))
        return RT_DEFAULT;
    else if (strieq(str, "module"))
        return RT_MODULE;
    else if (strieq(str, "service"))
        return RT_SERVICE;
    else if (strieq(str, "file"))
        return RT_FILE_SCOPE;
    else if (strieq(str, "workunit"))
        return RT_WORKUNIT_SCOPE;
    else
        return RT_DEFAULT;
}

void Cws_accessEx::checkUser(IEspContext& context, CLdapSecManager* secmgr, const char* rtype, const char* rtitle, unsigned int SecAccessFlags)
{
    if (secmgr == nullptr)
        secmgr = queryLDAPSecurityManager(context, true);

    if (rtype && rtitle && strieq(rtype, FILE_SCOPE_RTYPE) && strieq(rtitle, FILE_SCOPE_RTITLE))
    {
        if (!context.validateFeatureAccess(FILE_SCOPE_URL, SecAccessFlags, false))
        {
            context.setAuthStatus(AUTH_STATUS_NOACCESS);
            throw MakeStringException(ECLWATCH_DFU_WU_ACCESS_DENIED, "Access to File Scope is denied.");
        }
        return;
    }

    if(!secmgr->isSuperUser(context.queryUser()))
    {
        context.setAuthStatus(AUTH_STATUS_NOACCESS);
        throw MakeStringException(ECLWATCH_ADMIN_ACCESS_DENIED, "Access denied, administrators only.");
    }
}

CLdapSecManager* Cws_accessEx::queryLDAPSecurityManagerAndCheckUser(IEspContext& context, const char* rtype, const char* rtitle, unsigned int SecAccessFlags)
{
    CLdapSecManager* ldapSecMgr = queryLDAPSecurityManager(context, true);
    checkUser(context, ldapSecMgr, rtype, rtitle, SecAccessFlags);
    return ldapSecMgr;
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
        OWARNLOG("Config not found for service %s/%s",process, service);
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

    xpath.setf("Software/EspProcess[@name=\"%s\"]/@PageCacheTimeoutSeconds", process);
    if (cfg->hasProp(xpath.str()))
        setPageCacheTimeoutMilliSeconds(cfg->getPropInt(xpath.str()));
    xpath.setf("Software/EspProcess[@name=\"%s\"]/@MaxPageCacheItems", process);
    if (cfg->hasProp(xpath.str()))
        setMaxPageCacheItems(cfg->getPropInt(xpath.str()));
}

CLdapSecManager* Cws_accessEx::queryLDAPSecurityManager(IEspContext &context, bool excpt)
{
    ISecManager* secMgr = context.querySecManager();
    if(secMgr && secMgr->querySecMgrType() != SMT_LDAP)
        throw makeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_ISNT_LDAP);

    CLdapSecManager* ldapSecMgr = dynamic_cast<CLdapSecManager*>(secMgr);
    if (!ldapSecMgr && excpt)
        throw makeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

    return ldapSecMgr;
}

void Cws_accessEx::getBasednReq(IEspContext &context, const char* name, const char* basedn,
    const char* rType, const char* rTitle, IEspDnStruct* dn)
{
    double version = context.getClientVersion();
    if (version >= 1.14)
    {
        if (isEmptyString(name))
            throw MakeStringException(ECLWATCH_INVALID_INPUT, "BaseDN not specified");

        setBasedns(context);

        ForEachItemIn(i, m_basedns)
        {
            IEspDnStruct& cur = m_basedns.item(i);
            if(strieq(cur.getName(), name))
            {
                dn->setBasedn(cur.getBasedn());
                dn->setRtype(cur.getRtype());
                dn->setRtitle(cur.getRtitle());
                return;
            }
        }
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "BaseDN %s not found", name);
    }

    //before version 1.14
    if (isEmptyString(basedn))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Basedn not specified");
    if (isEmptyString(rType))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Rtype not specified");
    if (isEmptyString(rTitle))
        throw MakeStringException(ECLWATCH_INVALID_INPUT, "Rtitle not specified");

    dn->setBasedn(basedn);
    dn->setRtype(rType);
    dn->setRtitle(rTitle);
}

void Cws_accessEx::setBasedns(IEspContext &context)
{
    CLdapSecManager* secmgr = queryLDAPSecurityManager(context, true);

    CriticalBlock b(basednsCrit);
    if (m_basedns.length() > 0)
        return;

    std::set<std::string> alreadythere;
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

//Parse a filescope "name" spec (fs1::fs2::fs3) and populate the "newResources" array with each sub filespec (fs1, fs1::fs2, fs1::fs2::fs3).
//If any of the sub filespecs exist, return the deepest one as "existingResource", and remove it from the "newResources" array
bool Cws_accessEx::getNewFileScopeNames(CLdapSecManager* secmgr, const char* name, IEspDnStruct* basednReq, StringBuffer& existingResource, StringArray& newResources)
{
    if (!secmgr)
        return false;

    if (isEmptyString(name))
        return false;

    char* pStr0 = (char*) name;
    while (pStr0[0] == ':') //in case of some ':' by mistake
        pStr0++;

    if (pStr0[0] == 0)
        return false;

    readFileScopesFromString(pStr0, newResources, true);
    while (newResources.ordinality())
    {
        StringBuffer namebuf(newResources.item(0));
        try
        {
            //Check to see if filescope already exists
            IArrayOf<CPermission> permissions;
            secmgr->getPermissionsArray(basednReq->getBasedn(), str2type(basednReq->getRtype()), namebuf.str(), permissions);
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

        existingResource.clear().append(namebuf);//remember deepest scope that already exists
        newResources.remove(0);
    }

    return true;
}

//Parse a filescope spec (fs1::fs2::fs3) and populate the scopes array with each sub filespec (fs1, fs1::fs2, fs1::fs2::fs3).
void Cws_accessEx::readFileScopesFromString(const char* spec, StringArray& scopes, bool append)
{
    if (isEmptyString(spec))
        return;

    StringBuffer newFileScope;
    const char* scopeStart = spec;
    const char* scopeEnd = strstr(scopeStart, "::");
    while (scopeEnd)
    {
        StringBuffer scope;
        scope.append(scopeEnd - scopeStart, scopeStart);
        addAFileScope(scope, newFileScope, scopes, append);

        scopeStart = scopeEnd + 2;
        while (scopeStart[0] == ':') //in case of more than two ':' by mistake
            scopeStart++;
        if (!*scopeStart)
            break;

        scopeEnd = strstr(scopeStart, "::");
    }

    if (!isEmptyString(scopeStart))
        addAFileScope(scopeStart, newFileScope, scopes, append);
}

void Cws_accessEx::addAFileScope(const char* scope, StringBuffer& newFileScope, StringArray& fileScopes, bool append)
{
    if (!newFileScope.isEmpty())
        newFileScope.append("::");
    newFileScope.append(scope);

    if (append)
        fileScopes.append(newFileScope);
    else
        fileScopes.add(newFileScope, 0);
}

bool Cws_accessEx::setNewFileScopePermissions(CLdapSecManager* secmgr, IEspDnStruct* basednReq, StringBuffer& existingResource, StringArray& newResources)
{
    if (!secmgr || !newResources.ordinality())
    {
        return false;
    }

    const char* basedn = basednReq->getBasedn();
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
    secmgr->getPermissionsArray(basednBuf, str2type(basednReq->getRtype()), existingResource.str(), requiredPermissions);
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
        paction.m_basedn.append(basednReq->getBasedn());
        paction.m_rtype = str2type(basednReq->getRtype());
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
            StringBuffer namebuf(newResources.item(y));
            paction.m_rname.clear().append(namebuf.str());
            secmgr->changePermission(paction);
        }

    }

    return true;
}

bool Cws_accessEx::onUsers(IEspContext &context, IEspUserRequest &req, IEspUserResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context, false);

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

        checkUser(context, secmgr);

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
                    oneusr->setPasswordexpiration(getPasswordExpiration(usr, sb));
                }

                if (version >= 1.16)
                {
                    oneusr->setEmployeeNumber(usr->getEmployeeNumber());
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

const char *Cws_accessEx::getPasswordExpiration(ISecUser *usr, StringBuffer &passwordExpiration)
{
    switch (usr->getPasswordDaysRemaining())//-1 if expired, -2 if never expires
    {
    case scPasswordExpired:
        passwordExpiration.set("Expired");
        break;
    case scPasswordNeverExpires:
        passwordExpiration.set("Never");
        break;
    default:
        {
            CDateTime dt;
            usr->getPasswordExpiration(dt);
            dt.getDateString(passwordExpiration);
            break;
        }
    }
    return passwordExpiration.str();
}

bool Cws_accessEx::onUserQuery(IEspContext &context, IEspUserQueryRequest &req, IEspUserQueryResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context, false);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }
        checkUser(context, secmgr);

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
        case CUserSortBy_EmployeeNumber:
            sortOrder[0] = UFEmployeeNumber;
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

            const char* employeeNumber = usr.queryProp(getUserFieldNames(UFEmployeeNumber));
            if (employeeNumber && *employeeNumber)
                userInfo->setEmployeeNumber(employeeNumber);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

        resp.setUsername(req.getUsername());
        double version = context.getClientVersion();
        if (version >= 1.13)
            resp.setIsLDAPAdmin(secmgr->isSuperUser(context.queryUser()));

        StringArray groupnames;
        secmgr->getGroups(req.getUsername(), groupnames);
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
        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManagerAndCheckUser(context);
        resp.setUsername(req.getUsername());

        std::set<std::string> ogrps;
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
        ldapsecmgr->getAllGroups(groupnames, managedBy, descriptions, context.querySecureContext());
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
            OERRLOG("error changing user's group membership: %s", errmsg.str());
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
        CLdapSecManager* secmgr0 = queryLDAPSecurityManager(context, false);

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

        checkUser(context, secmgr0);

        StringArray groupnames;
        StringArray groupManagedBy;
        StringArray groupDescriptions;
        ISecManager* secmgr = context.querySecManager();
        if(secmgr == NULL)
            throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, MSG_SEC_MANAGER_IS_NULL);

        secmgr->getAllGroups(groupnames, groupManagedBy, groupDescriptions, context.querySecureContext());
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
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context, false);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }

        checkUser(context, secmgr);

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
        sortOrder[0] = (GroupField) (sortOrder[0] | GFnocase);
        bool descending = req.getDescending();
        if (descending)
            sortOrder[0] = (GroupField) (sortOrder[0] | GFreverse);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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

        if((secmgr->getLdapServerType() == ACTIVE_DIRECTORY) && (strlen(username) > 20))
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

        const char * employeeNumber = nullptr;
        if (context.getClientVersion() >= 1.16)
        {
            employeeNumber = req.getEmployeeNumber();
        }
        Owned<ISecUser> user = secmgr->createUser(username, context.querySecureContext());
        ISecCredentials& cred = user->credentials();
        const char* firstname = req.getFirstname();
        const char* lastname = req.getLastname();
        if(firstname != NULL)
            user->setFirstName(firstname);
        if(lastname != NULL)
            user->setLastName(lastname);
        if(employeeID != NULL)
            user->setEmployeeID(employeeID);
        if(employeeNumber != nullptr)
            user->setEmployeeNumber(employeeNumber);
        if(pass1 != NULL)
            cred.setPassword(pass1);
        try
        {
            if (user.get())
                secmgr->addUser(*user.get(), context.querySecureContext());
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

        const char* action = req.getActionType();
        if (!action || !*action)
            throw MakeStringException(ECLWATCH_INVALID_ACTION, "Action not specified.");

        if (!stricmp(action, "delete"))
        {
            StringArray& usernames = req.getUsernames();

            for(unsigned i = 0; i < usernames.length(); i++)
            {
                const char* username = usernames.item(i);
                Owned<ISecUser> user = secmgr->createUser(username, context.querySecureContext());
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
            StringArray& groupnames = req.getGroupnames();

            IArrayOf<IEspAccountPermission> accountPermissions;
            double version = context.getClientVersion();
            if (version > 1.01)
            {
                bool bDeletePermission = false;
                if(!req.getDeletePermission_isNull())
                    bDeletePermission = req.getDeletePermission();

                setBasedns(context);

                ForEachItemIn(y, m_basedns)
                {
                    IEspDnStruct* curbasedn = &(m_basedns.item(y));
                    const char *basednName = curbasedn->getName();
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
                    if(secmgr->getResources(rtype, aBasedn, resources, context.querySecureContext()))
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
                        const char* resourceName = r.getName();
                        const char* bnname = r.getDescription();
                        if (isEmptyString(resourceName))
                            continue; 

                        try
                        {
                            IArrayOf<CPermission> permissions;
                            secmgr->getPermissionsArray(bnname, rtype, resourceName, permissions);
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
                                            if (version < 1.15)
                                            {
                                                onepermission->setBasedn(bnname);
                                                onepermission->setRType(aRtype);
                                            }
                                            else
                                            {
                                                onepermission->setBasednName(basednName);
                                            }
                                            onepermission->setResourceName(resourceName);
                                            onepermission->setPermissionName(groupname);
                                            accountPermissions.append(*onepermission.getLink());
                                        }
                                        else
                                        {
                                            CPermissionAction paction;
                                            paction.m_basedn.append(bnname);
                                            paction.m_rtype = rtype;
                                            paction.m_rname.append(resourceName);
                                            paction.m_account_name.append(actname);
                                            paction.m_account_type = (ACT_TYPE) accountType;
                                            paction.m_allows = perm.getAllows();
                                            paction.m_denies = perm.getDenies();
                                            paction.m_action.append("delete");

                                            if (!secmgr->changePermission(paction))
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
        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context, false);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }

        checkUser(context, secmgr);

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
        case CUserSortBy_EmployeeNumber:
            sortOrder[0] = UFEmployeeNumber;
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

            const char* employeeNumber = usr.queryProp(getUserFieldNames(UFEmployeeNumber));
            if (employeeNumber && *employeeNumber)
                userInfo->setEmployeeNumber(employeeNumber);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

        resp.setGroupname(req.getGroupname());

        std::set<std::string> ousrs;
        StringArray ousernames;
        secmgr->getGroupMembers(req.getGroupname(), ousernames);
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
            OERRLOG("error changing user's group membership: %s", errmsg.str());
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
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context, false);

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

        checkUser(context, secmgr);
        setBasedns(context);
        resp.setBasedns(m_basedns);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onResources(IEspContext &context, IEspResourcesRequest &req, IEspResourcesResponse &resp)
{
    try
    {
        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Read);

        double version = context.getClientVersion();
        const char* filterInput = req.getSearchinput();
        const char* basedn = basednReq->getBasedn();

        const char* moduletemplate = NULL;
        ForEachItemIn(x, m_basedns)
        {
            IEspDnStruct* curbasedn = &(m_basedns.item(x));
            if(stricmp(curbasedn->getBasedn(), basedn) == 0)
            {
                moduletemplate = curbasedn->getTemplatename();
            }
        }

        if (version < 1.14)
        {
            resp.setBasedn(basedn);
            resp.setRtype(basednReq->getRtype());
            resp.setRtitle(basednReq->getRtitle());
        }
        SecResourceType rtype = str2type(basednReq->getRtype());
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
            if (version < 1.14)
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

            oneresource->setName(rname);
            oneresource->setDescription(r.getDescription());

            rarray.append(*oneresource.getLink());
        }
        if (version >= 1.08)
        {
            Owned<IUserDescriptor> userdesc;
            userdesc.setown(createUserDescriptor());
            userdesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
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
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context, false);
        if(!secmgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }
        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        checkUser(context, secmgr, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Read);

        SecResourceType rtype = str2type(basednReq->getRtype());
        const char* moduleTemplate = NULL;
        ForEachItemIn(x, m_basedns)
        {
            IEspDnStruct* curbasedn = &(m_basedns.item(x));
            if(strieq(curbasedn->getBasedn(), basednReq->getBasedn()))
            {
                moduleTemplate = curbasedn->getTemplatename();
                break;
            }
        }

        StringBuffer nameReq(req.getName());
        const char* prefix = req.getPrefix();
        if (!nameReq.length() && basednReq->getRtitle() && !stricmp(basednReq->getRtitle(), "CodeGenerator Permission"))
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
        Owned<ISecItemIterator> it = secmgr->getResourcesSorted(rtype, basednReq->getBasedn(), nameReq.str(),
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
        checkUser(context, nullptr, req.getRtype(), req.getRtitle(), SecAccess_Full);

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
        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        CLdapSecManager *secmgr = queryLDAPSecurityManagerAndCheckUser(context, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Full);

        double version = context.getClientVersion();
        if (version < 1.14)
        {
            resp.setBasedn(basednReq->getBasedn());
            resp.setRtype(basednReq->getRtype());
            resp.setRtitle(basednReq->getRtitle());
            resp.setPrefix(req.getPrefix());
        }

        StringBuffer lastResource;
        StringArray newResources;
        if(str2type(basednReq->getRtype()) == RT_FILE_SCOPE)
        {
            //Build newResources array of each filescope subscopes (fs1, fs1::fs2, fs1::fs2::fs3 ...),
            //and isolate deepest one (lastResource) that already exists
            getNewFileScopeNames(secmgr, req.getName(), basednReq, lastResource, newResources);
        }

        SecResourceType rtype = str2type(basednReq->getRtype());
        try
        {
            Owned<ISecUser> usr = secmgr->createUser(context.queryUserId(), context.querySecureContext());
            Owned<ISecResourceList> rlist = secmgr->createResourceList("ws_access", context.querySecureContext());
            const char* name = req.getName();
            if(name == NULL || *name == '\0')
            {
                resp.setRetcode(-1);
                StringBuffer errmsg;
                errmsg.append(basednReq->getRtitle()).append(" name can't be empty");
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
            secmgr->addResourcesEx(rtype, *usr, rlist, PT_ADMINISTRATORS_ONLY, basednReq->getBasedn());

            if(str2type(basednReq->getRtype()) == RT_FILE_SCOPE && newResources.ordinality())
            {
                setNewFileScopePermissions(secmgr, basednReq, lastResource, newResources);

                StringBuffer retmsg;
                ForEachItemIn(y, newResources)
                {
                    StringBuffer namebuf(newResources.item(y));
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
        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Full);

        StringArray& names = req.getNames();

        int doUpdate = req.getDoUpdate();
        if (doUpdate)
        {
            const char* basedn = basednReq->getBasedn();
            const char* rtype = basednReq->getRtype();
            const char* rtitle = basednReq->getRtitle();
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

        double version = context.getClientVersion();
        if (version < 1.14)
        {
            resp.setBasedn(basednReq->getBasedn());
            resp.setRtype(basednReq->getRtype());
            resp.setRtitle(basednReq->getRtitle());
            resp.setPrefix(req.getPrefix());
        }
        SecResourceType rtype = str2type(basednReq->getRtype());

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

            secmgr->deleteResource(rtype, namebuf.str(), basednReq->getBasedn(), context.querySecureContext());
        }

        resp.setRetcode(0);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

void Cws_accessEx::addResourcePermission(const char *name, int type, int allows, int denies, IArrayOf<IEspResourcePermission> &permissions)
{
    if (isEmptyString(name))
        return;

    StringBuffer nameIn(name);
    Owned<IEspResourcePermission> permission = createResourcePermission();
    permission->setAccount_name(name);
    permission->setEscaped_account_name(nameIn.replaceString("\'", "\\\'").str());
    permission->setAccount_type(type);
    if((allows & NewSecAccess_Access) == NewSecAccess_Access)
        permission->setAllow_access(true);
    if((allows & NewSecAccess_Read) == NewSecAccess_Read)
        permission->setAllow_read(true);
    if((allows & NewSecAccess_Write) == NewSecAccess_Write)
        permission->setAllow_write(true);
    if((allows & NewSecAccess_Full) == NewSecAccess_Full)
        permission->setAllow_full(true);
    if((denies & NewSecAccess_Access) == NewSecAccess_Access)
        permission->setDeny_access(true);
    if((denies & NewSecAccess_Read) == NewSecAccess_Read)
        permission->setDeny_read(true);
    if((denies & NewSecAccess_Write) == NewSecAccess_Write)
        permission->setDeny_write(true);
    if((denies & NewSecAccess_Full) == NewSecAccess_Full)
        permission->setDeny_full(true);

    permissions.append(*permission.getClear());
}

bool Cws_accessEx::onResourcePermissions(IEspContext &context, IEspResourcePermissionsRequest &req, IEspResourcePermissionsResponse &resp)
{
    try
    {
        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManagerAndCheckUser(context, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Read);

        double version = context.getClientVersion();
        SecResourceType rtype = str2type(basednReq->getRtype());
        const char* name = req.getName();
        StringBuffer namebuf(name);
        if (rtype == RT_MODULE && stricmp(name, "repository") != 0 && Utils::strncasecmp(name, "repository.", 11) != 0)
            namebuf.insert(0, "repository.");

        const char* basedn = basednReq->getBasedn();
        if (isEmptyString(name) && (rtype == RT_FILE_SCOPE || rtype == RT_WORKUNIT_SCOPE))
        {   //Since resource name is not specified, this is the request to check default resource permissions for file
            //scope or workunit scope. We need to parse file scope basedn (example: ou=files,ou=ecl,dc=dev,dc=local)
            //or workunit scope basedn (example: ou=workunits,ou=ecl,dc=dev,dc=local) to get the resource name
            //(example: files or workunits) and its basedn (example: ou=ecl,dc=dev,dc=local).
            const char* comma = strchr(basedn, ',');
            const char* eqsign = strchr(basedn, '=');
            if (!comma || !eqsign)
                throw MakeStringException(ECLWATCH_INVALID_SEC_MANAGER, "Invalid basedn: %s", basedn);

            namebuf.clear().append(comma - eqsign - 1, eqsign + 1);
            basedn = comma + 1;
        }

        const char* prefix = req.getPrefix();
        if(prefix && *prefix)
            namebuf.insert(0, prefix);

        IArrayOf<CPermission> permissions;
        ldapsecmgr->getPermissionsArray(basedn, rtype, namebuf, permissions);

        IArrayOf<IEspResourcePermission> parray;
        ForEachItemIn(x, permissions)
        {
            CPermission& perm = permissions.item(x);
            addResourcePermission(perm.getAccount_name(), perm.getAccount_type(), perm.getAllows(), perm.getDenies(), parray);
        }

        if (version < 1.14)
        {
            resp.setBasedn(basednReq->getBasedn());
            resp.setRtype(basednReq->getRtype());
            resp.setRtitle(basednReq->getRtitle());
            resp.setPrefix(req.getPrefix());
            resp.setName(req.getName());
        }
        resp.setPermissions(parray);
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }

    return true;
}

bool Cws_accessEx::onResourcePermissionQuery(IEspContext &context, IEspResourcePermissionQueryRequest &req, IEspResourcePermissionQueryResponse &resp)
{
    try
    {
        CLdapSecManager* ldapSecMgr = queryLDAPSecurityManager(context, false);
        if(!ldapSecMgr)
        {
            resp.setNoSecMngr(true);
            return true;
        }

        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        checkUser(context, ldapSecMgr, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Read);

        __int64 pageStartFrom = 0;
        unsigned pageSize = 100;
        if (!req.getPageSize_isNull())
            pageSize = req.getPageSize();
        if (!req.getPageStartFrom_isNull())
            pageStartFrom = req.getPageStartFrom();

        ResourcePermissionField sortOrder[2] = {RPFName, RPFterm};
        if (req.getSortBy() == CResourcePermissionSortBy_Type)
            sortOrder[0] = RPFType;

        sortOrder[0] = (ResourcePermissionField) (sortOrder[0] | RPFnocase);
        bool descending = req.getDescending();
        if (descending)
            sortOrder[0] = (ResourcePermissionField) (sortOrder[0] | RPFreverse);

        ACCOUNT_TYPE_REQ accountTypeReq = REQ_ANY_ACT;
        CAccountTypeReq accountType = req.getAccountType();
        switch(accountType)
        {
        case CAccountTypeReq_User:
            accountTypeReq = REQ_USER_ACT;
            break;
        case CAccountTypeReq_Group:
            accountTypeReq = REQ_GROUP_ACT;
            break;
        }
        unsigned total;
        __int64 cacheHint;
        IArrayOf<IEspResourcePermission> permissions;
        Owned<ISecItemIterator> it = ldapSecMgr->getResourcePermissionsSorted(req.getName(), accountTypeReq, basednReq->getBasedn(),
            basednReq->getRtype(), req.getPrefix(), sortOrder, (const __int64) pageStartFrom, (const unsigned) pageSize, &total, &cacheHint);
        ForEach(*it)
        {
            IPropertyTree& r = it->query();
            addResourcePermission(r.queryProp(getResourcePermissionFieldNames(RPFName)), r.getPropInt(getResourcePermissionFieldNames(RPFType)),
                r.getPropInt(getResourcePermissionFieldNames(RPFAllow)), r.getPropInt(getResourcePermissionFieldNames(RPFDeny)), permissions);
        }
        resp.setPermissions(permissions);
        resp.setTotalResourcePermissions(total);
        resp.setCacheHint(cacheHint);
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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
        CLdapSecManager *secmgr = queryLDAPSecurityManagerAndCheckUser(context);

        resp.setBasedn(req.getBasedn());
        resp.setRname(req.getRname());
        resp.setRtype(req.getRtype());
        resp.setRtitle(req.getRtitle());
        resp.setPrefix(req.getPrefix());

        double version = context.getClientVersion();
        if (version < 1.01)
        {
            return permissionAddInputOnResource(context, secmgr, req, resp);
        }
        else
        {
            const char* accountName = req.getAccountName();
            if (!accountName || !*accountName)
            {
                return permissionAddInputOnResource(context, secmgr, req, resp);
            }
            else
            {
                return permissionAddInputOnAccount(context, secmgr, accountName, req, resp);
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context, req.getRtype(), req.getRtitle(), SecAccess_Full);

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
        secmgr->getAllGroups(grpnames, managedBy, descriptions, context.querySecureContext());
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
    CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

    //Clear local cache
    Owned<ISecUser> user = secmgr->createUser(context.queryUserId(), context.querySecureContext());
    ISecCredentials& cred = user->credentials();
    cred.setPassword(context.queryPassword());
    cred.setSessionToken(context.querySessionToken());
    bool ok = secmgr->clearPermissionsCache(*user, context.querySecureContext());

    //Request DALI to clear its cache
    if (ok)
    {
        Owned<IUserDescriptor> userdesc;
        userdesc.setown(createUserDescriptor());
        userdesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
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
    userdesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
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
    CLdapSecManager *secmgr = queryLDAPSecurityManagerAndCheckUser(context, FILE_SCOPE_RTYPE, FILE_SCOPE_RTITLE, SecAccess_Full);

    StringBuffer retMsg;
    int rc = enableDisableScopeScans(context, secmgr, true, retMsg);
    resp.updateScopeScansStatus().setIsEnabled(rc == 0);
    resp.updateScopeScansStatus().setRetcode(rc);
    resp.updateScopeScansStatus().setRetmsg(retMsg.str());
    return true;
}

bool Cws_accessEx::onDisableScopeScans(IEspContext &context, IEspDisableScopeScansRequest &req, IEspDisableScopeScansResponse &resp)
{
    CLdapSecManager *secmgr = queryLDAPSecurityManagerAndCheckUser(context, FILE_SCOPE_RTYPE, FILE_SCOPE_RTITLE, SecAccess_Full);

    StringBuffer retMsg;
    int rc = enableDisableScopeScans(context, secmgr, false, retMsg);
    resp.updateScopeScansStatus().setIsEnabled(rc != 0);
    resp.updateScopeScansStatus().setRetcode(rc);
    resp.updateScopeScansStatus().setRetmsg(retMsg.str());
    return true;
}

int Cws_accessEx::enableDisableScopeScans(IEspContext &context, CLdapSecManager *secmgr, bool doEnable, StringBuffer &retMsg)
{
    Owned<IUserDescriptor> userdesc;
    userdesc.setown(createUserDescriptor());
    userdesc->set(context.queryUserId(), context.queryPassword(), context.querySignature());
    int retCode;
    bool rc = querySessionManager().enableScopeScans(userdesc, doEnable, &retCode, retMsg);
    if (!rc || retCode != 0)
        IERRLOG("Error %d enabling Scope Scans : %s", retCode, retMsg.str());
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
        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManagerAndCheckUser(context, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Full);

        double version = context.getClientVersion();
        if (version < 1.14)
        {
            resp.setBasedn(basednReq->getBasedn());
            resp.setRname(req.getRname());
            resp.setRtype(basednReq->getRtype());
            resp.setRtitle(basednReq->getRtitle());
            resp.setPrefix(req.getPrefix());
        }


        const char* users = req.getUserarray();
        const char* groups = req.getGrouparray();
        if ((!users || !*users) && (!groups || !*groups))
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "A user or group must be specified.");

        StringArray& resources = req.getNames();
        if (resources.length() < 1)
            throw MakeStringException(ECLWATCH_INVALID_RESOURCE_NAME, "A resource name must be specified.");

        StringArray userAccounts, groupAccounts;
        if (!isEmptyString(users))
            userAccounts.appendListUniq(users, ",");
        if (!isEmptyString(groups))
            groupAccounts.appendListUniq(groups, ",");
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

                        ret = permissionsReset(ldapsecmgr, basednReq->getBasedn(), basednReq->getRtype(), req.getPrefix(), name, USER_ACT, name0,
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

                        ret = permissionsReset(ldapsecmgr, basednReq->getBasedn(), basednReq->getRtype(), req.getPrefix(), name, GROUP_ACT, name0,
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

bool Cws_accessEx::permissionAddInputOnResource(IEspContext &context, CLdapSecManager *secmgr, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp)
{
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
    secmgr->getAllGroups(grpnames, managedBy, descriptions, context.querySecureContext());
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

bool Cws_accessEx::permissionAddInputOnAccount(IEspContext &context, CLdapSecManager *secmgr, const char* accountName, IEspPermissionAddRequest &req, IEspPermissionAddResponse &resp)
{
    double version = context.getClientVersion();
    if (version < 1.14)
    {
        resp.setBasednName(req.getBasednName());
        resp.setAccountName(req.getAccountName());
        resp.setAccountType(req.getAccountType());
    }

    const char* prefix = req.getPrefix();
    const char* basednName = req.getBasednName();
    int accountType = req.getAccountType();
    if (basednName && *basednName)
    {
        setBasedns(context);
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
            if(secmgr->getResources(rtype, basedn, resources, context.querySecureContext()))
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
        Owned<IEspDnStruct> basednReq = createDnStruct();
        getBasednReq(context, req.getBasednName(), req.getBasedn(), req.getRtype(), req.getRtitle(), basednReq);

        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManagerAndCheckUser(context, basednReq->getRtype(), basednReq->getRtitle(), SecAccess_Full);

        double version = context.getClientVersion();
        if (version < 1.14)
        {
            resp.setBasedn(req.getBasedn());
            resp.setRname(req.getRname());
            resp.setRtype(req.getRtype());
            resp.setRtitle(req.getRtitle());
            resp.setPrefix(req.getPrefix());
        }

        CPermissionAction paction;
        paction.m_basedn.append(basednReq->getBasedn());

        const char* name = req.getRname();
        StringBuffer namebuf(name);
        SecResourceType rtype = str2type(basednReq->getRtype());
        if(rtype == RT_MODULE && stricmp(name, "repository") != 0 && Utils::strncasecmp(name, "repository.", 11) != 0)
            namebuf.insert(0, "repository.");
        const char* prefix = req.getPrefix();
        if(prefix && *prefix)
            namebuf.insert(0, prefix);

        paction.m_rname.append(namebuf.str());
        paction.m_rtype = rtype;
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
        checkUser(context, nullptr);

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
        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManagerAndCheckUser(context);
        resp.setUsername(req.getUsername());

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

        SecFeatureSet sfs = ldapsecmgr->queryImplementedFeatures();
        if (!(sfs & SMF_UpdateUserPassword))
        {
            resp.setRetcode(-1);
            resp.setRetmsg("Changing password is not supported.");
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            resp.setRetcode(-1);
            resp.setRetmsg("username can't be empty");
            return false;
        }

        bool enable = req.getPosixenabled();
        Owned<CLdapSecUser> user = dynamic_cast<CLdapSecUser*>(secmgr->createUser(username, context.querySecureContext()));
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "Please specify a username.");
        }

        Owned<CLdapSecUser> user = dynamic_cast<CLdapSecUser*>(secmgr->createUser(username, context.querySecureContext()));
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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

        Owned<CLdapSecUser> user = dynamic_cast<CLdapSecUser*>(secmgr->createUser(username, context.querySecureContext()));

        user->setFirstName(firstname);
        user->setLastName(lastname);
        if (context.getClientVersion() >= 1.10)
        {
            user->setEmployeeID(req.getEmployeeID());
        }

        if (context.getClientVersion() >= 1.16)
        {
            user->setEmployeeNumber(req.getEmployeeNumber());
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

        const char* username = req.getUsername();
        if(username == NULL || *username == '\0')
        {
            throw MakeStringException(ECLWATCH_INVALID_ACCOUNT_NAME, "Please specify a username.");
        }

        Owned<CLdapSecUser> user = dynamic_cast<CLdapSecUser*>(secmgr->createUser(username, context.querySecureContext()));
        secmgr->getUserInfo(*user.get());

        resp.setUsername(username);

        resp.setFirstname(user->getFirstName());
        resp.setLastname(user->getLastName());
        double version = context.getClientVersion();
        if (version >= 1.10)
        {
            resp.setEmployeeID(user->getEmployeeID());
            if (version >= 1.12)
            {
                StringBuffer sb;
                resp.setPasswordExpiration(getPasswordExpiration(user, sb));
            }
            if (version >= 1.16)
            {
                resp.setEmployeeNumber(user->getEmployeeNumber());
            }
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
    throw MakeStringException(ECLWATCH_INVALID_ACTION, "UserSudoersInput no longer supported");
}

bool Cws_accessEx::onUserSudoers(IEspContext &context, IEspUserSudoersRequest &req, IEspUserSudoersResponse &resp)
{
    throw MakeStringException(ECLWATCH_INVALID_ACTION, "UserSudoers no longer supported");
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
            checkUser(context, nullptr);

        double version = context.getClientVersion();

        CLdapSecManager* ldapsecmgr = queryLDAPSecurityManager(context, true);

        bool bIncludeGroup = req.getIncludeGroup();
        setBasedns(context);

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
            if(ldapsecmgr->getResources(rtype, aBasedn, resources, context.querySecureContext()))
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
                const char* resourceName = r.getName();
                const char* dnname = r.getDescription();
                if (isEmptyString(resourceName))
                    continue;

                try
                {
                    IArrayOf<CPermission> permissions;
                    ldapsecmgr->getPermissionsArray(dnname, rtype, resourceName, permissions);
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
                        if (version < 1.15)
                        {
                            onepermission->setBasedn(dnname);
                            onepermission->setRType(aRtype);
                            onepermission->setResourceName(aRtitle);
                            onepermission->setPermissionName(resourceName);
                        }
                        else
                        {
                            onepermission->setBasednName(aName);
                            onepermission->setResourceName(resourceName);
                        }

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

//List permissions for a given account in a given BaseDN resource or all BaseDN resources.
//Revised based on onAccountPermissions() which lists permissions for a given account in all BaseDN resources.
bool Cws_accessEx::onAccountPermissionsV2(IEspContext &context, IEspAccountPermissionsV2Request &req,
    IEspAccountPermissionsV2Response &resp)
{
    class CAccountsInResource : public CInterface
    {
        StringAttr resourceName;
        StringArray accountNames;
    public:
        CAccountsInResource(const char *_resourceName) : resourceName(_resourceName) {}

        inline StringArray &getAccountNames() { return accountNames; };
        inline void addUniqueAccountName(const char *name) { accountNames.appendUniq(name); };
        inline bool findAccountName(const char *name) { return accountNames.find(name) != NotFound; }
    };

    class CAccountsInBaseDN : public CInterface
    {
        StringAttr baseDNName;
        CIArrayOf<CAccountsInResource> accountsInResources;
    public:
        CAccountsInBaseDN(const char *_baseDNName) : baseDNName(_baseDNName) {};

        inline const char *getBaseDNName() { return baseDNName.get(); };
        inline CIArrayOf<CAccountsInResource> &getAccountsInResources() { return accountsInResources; };
    };

    class CAccountPermissionsHelper : public CSimpleInterface
    {
        IEspContext *context = nullptr;
        CLdapSecManager *secMGR = nullptr;

        StringBuffer accountNameReq;
        StringAttr baseDNNameReq;
        bool isGroupAccountReq = false;
        bool includeGroup = false;

        StringArray groupsAccountBelongsTo;
        StringAttr moduleBaseDN; //Used by appendAccountPermissionsForCodeGenResource()
        CIArrayOf<CAccountsInBaseDN> accountsInBaseDNs; //Used by setBaseDNNamesForMissingPermissions().
        bool hasAuthUsersPerm = false; //May change in appendAccountPermission()
        bool hasEveryonePerm = false;  //May change in appendAccountPermission()
        Owned<IEspGroupAccountPermission> authUsersGroupPermission, everyOneGroupPermission;
        IArrayOf<ISecResource> resourcesInOneBaseDN;

        bool getResourcePermissions(const char *baseDN, SecResourceType rType,
            const char *resourceName, IArrayOf<CPermission> &permissions)
        {
            bool success = true;
            try
            {
                secMGR->getPermissionsArray(baseDN, rType, resourceName, permissions);
            }
            catch(IException *e) //exception may be thrown when no permission for the resource
            {
                e->Release();
                success = false;
            }
            return success;
        }
        void readAccountPermissionsInOneBaseDN(IArrayOf<IEspDnStruct> &allBaseDNs,
            IEspDnStruct &curBaseDN, IArrayOf<IEspAccountPermission> &accountPermissions, 
            IArrayOf<IEspGroupAccountPermission> &groupAccountPermissions)
        {
            const char *baseDNName = curBaseDN.getName();
            const char *baseDN = curBaseDN.getBasedn();
            const char *rTypeStr = curBaseDN.getRtype();
            SecResourceType rType = str2RType(rTypeStr);
            Owned<CAccountsInBaseDN> accountsInBaseDN = new CAccountsInBaseDN(baseDNName);

            //Read the resources for the BaseDN Resource.
            if (secMGR->getResources(rType, baseDN, resourcesInOneBaseDN))
            {
                ForEachItemIn(i, resourcesInOneBaseDN)
                {
                    ISecResource &r = resourcesInOneBaseDN.item(i);
                    const char *resourceName = r.getName();
                    if (isEmptyString(resourceName))
                        continue;
 
                    //Use the same code as in onAccountPermissions() to skip some RT_MODULE resources.
                    //The permission codegenerator.cpp is saved as a service permission (not a module permission)
                    //when it is added for a user.
                    if ((rType == RT_MODULE) && (strieq(resourceName, "codegenerator.cpp") || strnicmp(resourceName, "repository", 10)))
                        continue;

                    IArrayOf<CPermission> permissions;
                    if (getResourcePermissions(baseDN, rType, resourceName, permissions)) //get the permissions for this resource using secMGR->getPermissionsArray()
                    {
                        checkAndAppendAccountPermissions(baseDNName, resourceName, permissions, accountPermissions, groupAccountPermissions);
                        appendAccountsInResources(resourceName, permissions, accountsInBaseDN->getAccountsInResources());
                    }
                }
            }//If failed, log?

            if (rType == RT_WORKUNIT_SCOPE)
                appendAccountPermissionsForWUScopeResource(baseDNName, baseDN, accountPermissions, groupAccountPermissions);
            else if ((rType == RT_SERVICE) && !moduleBaseDN.isEmpty())
                appendAccountPermissionsForCodeGenResource(baseDNName, moduleBaseDN, accountPermissions, groupAccountPermissions);

            resourcesInOneBaseDN.kill(); //Clean it for possible next BaseDN.
            accountsInBaseDNs.append(*accountsInBaseDN.getClear());
        }
        void checkAndAppendAccountPermissions(const char *baseDNName, const char *resourceName,
            IArrayOf<CPermission> &permissions, IArrayOf<IEspAccountPermission> &accountPermissions,
            IArrayOf<IEspGroupAccountPermission> &groupAccountPermissions)
        {
            ForEachItemIn(i, permissions)
            {
                CPermission &perm = permissions.item(i);
                if (doesPermissionAccountMatchThisAccount(perm))
                {   //The account in the perm matches with this account. The match means: 1. both accounts
                    //have the same account name; or 2. this account belongs to a group and the name of the
                    //group account is the same as the account in the perm. Create an IEspAccountPermission
                    //using the resourceName and the perm and add it to the permission group where the 
                    //permission belongs to (accountPermissions, authUsersPermissions, etc).
                    Owned<IEspAccountPermission> newPermission = createNewAccountPermission(baseDNName, resourceName, perm);
                    appendAccountPermission(newPermission, perm, accountPermissions, groupAccountPermissions);
                }
            }
        }
        bool doesPermissionAccountMatchThisAccount(CPermission &perm)
        {
            int accountType = perm.getAccount_type();
            if (isGroupAccountReq && accountType == USER_ACT)
                return false; //The account in the perm is not a group account.

            const char *actName = perm.getAccount_name();
            if (isEmptyString(actName))
                return false;

            //If the accountType matches with isGroupAccountReq, validate the actName.
            if ((!isGroupAccountReq && (accountType == USER_ACT)) || (isGroupAccountReq && (accountType == GROUP_ACT)))
                return streq(actName, accountNameReq); //The actName must match with the accountNameReq.

            //Now, there is only one possibility left: isGroupAccountReq = false and accountType = GROUP_ACT.
            //isGroupAccountReq = false: the AccountPermissionsForResource call is for an individual account.
            //accountType = GROUP_ACT: the perm is for a group account; actName is the group name.
            //We need to check whether the individual is a member of this group. 
            return groupsAccountBelongsTo.find(actName) != NotFound;
        }
        IEspAccountPermission *createNewAccountPermission(const char *baseDNName,
            const char *resourceName, CPermission &perm)
        {
            //Use the same code as in onAccountPermissions().
            Owned<IEspAccountPermission> permission = createAccountPermission();
            permission->setBasednName(baseDNName);
            permission->setResourceName(resourceName);

            int allows = perm.getAllows();
            int denies = perm.getDenies();
            if((allows & NewSecAccess_Access) == NewSecAccess_Access)
                permission->setAllow_access(true);
            if((allows & NewSecAccess_Read) == NewSecAccess_Read)
                permission->setAllow_read(true);
            if((allows & NewSecAccess_Write) == NewSecAccess_Write)
                permission->setAllow_write(true);
            if((allows & NewSecAccess_Full) == NewSecAccess_Full)
                permission->setAllow_full(true);
            if((denies & NewSecAccess_Access) == NewSecAccess_Access)
                permission->setDeny_access(true);
            if((denies & NewSecAccess_Read) == NewSecAccess_Read)
                permission->setDeny_read(true);
            if((denies & NewSecAccess_Write) == NewSecAccess_Write)
                permission->setDeny_write(true);
            if((denies & NewSecAccess_Full) == NewSecAccess_Full)
                permission->setDeny_full(true);
            return permission.getClear();
        }
        void appendAccountPermission(IEspAccountPermission *permissionToBeAppended,
            CPermission &perm, IArrayOf<IEspAccountPermission> &accountPermissions, 
            IArrayOf<IEspGroupAccountPermission> &groupAccountPermissions)
        {
            //Use similar logic as in onAccountPermissions().
            //Append the Account Permission (permissionToBeAppended) to accountPermissions, groupAccountPermissions,
            //authUsersPermissions, or everyonePermissions.
            const char *actName = perm.getAccount_name();
            int accountType = perm.getAccount_type();
            if ((!isGroupAccountReq && accountType == USER_ACT) || (isGroupAccountReq && accountType == GROUP_ACT))
            {
                //Append the Account Permission to accountPermissions if: a. the requested account is not a group account
                //and this perm is not for a group account; or b. the requested account is a group account and this perm is
                //for a group account
                accountPermissions.append(*LINK(permissionToBeAppended));
                return;
            }

            if (streq(actName, "Authenticated Users"))
            {
                //Append the Account Permission to authUsersPermissions if this perm is for Authenticated Users.
                IArrayOf<IConstAccountPermission>& authUsersPermissions = authUsersGroupPermission->getPermissions();
                authUsersPermissions.append(*LINK(permissionToBeAppended));
                hasAuthUsersPerm = true;
                return;
            }

            if (streq(actName, "everyone"))
            {
                //Append the Account Permission to everyonePermissions if this perm is for everyone.
                IArrayOf<IConstAccountPermission>& everyonePermissions = everyOneGroupPermission->getPermissions();
                everyonePermissions.append(*LINK(permissionToBeAppended));
                hasEveryonePerm = true;
                return;
            }

            ForEachItemIn(i, groupAccountPermissions)
            {
                IEspGroupAccountPermission &groupPermission = groupAccountPermissions.item(i);
                if (!streq(actName, groupPermission.getGroupName()))
                    continue;

                //This perm is for a group account which is already in the groupPermission.
                //Append the Account Permission into the groupPermission.
                IArrayOf<IConstAccountPermission> &permissions = groupPermission.getPermissions();
                permissions.append(*LINK(permissionToBeAppended));
                return;
            }

            //This perm is for a group account which is not in the groupAccountPermissions yet.
            //Create a groupPermission. Append the Account Permission into the groupPermission.
            //Append the groupPermission to the groupAccountPermissions.
            Owned<IEspGroupAccountPermission> groupPermission = createGroupAccountPermissionEx(actName);
            IArrayOf<IConstAccountPermission> &permissions = groupPermission->getPermissions();
            permissions.append(*LINK(permissionToBeAppended));
            groupAccountPermissions.append(*groupPermission.getLink());
        }
        IEspGroupAccountPermission *createGroupAccountPermissionEx(const char *accountName)
        {
            Owned<IEspGroupAccountPermission> groupPermission = createGroupAccountPermission();
            groupPermission->setGroupName(accountName);
            return groupPermission.getClear();
        }
        void appendAccountPermissionsForWUScopeResource(const char *baseDNName, const char *baseDN, 
            IArrayOf<IEspAccountPermission> &accountPermissions, 
            IArrayOf<IEspGroupAccountPermission> &groupAccountPermissions)
        {
            //Use the same code as in onAccountPermissions() to find out the deftBaseDN and deftName.
            StringBuffer deftBaseDN, deftName;
            const char *comma = strchr(baseDN, ',');
            const char *eqsign = strchr(baseDN, '=');
            if (eqsign != nullptr)
            {
                if(comma == nullptr)
                    deftName.append(eqsign + 1);
                else
                {
                    deftName.append(comma - eqsign - 1, eqsign + 1);
                    deftBaseDN.append(comma + 1);
                }
            }

            //Based on the code in LdapUtils::normalizeDn(), the deftBaseDN can be empty.
            if (deftName.isEmpty())
                return;

            IArrayOf<CPermission> permissions;
            if (getResourcePermissions(deftBaseDN, RT_WORKUNIT_SCOPE, deftName, permissions))
                checkAndAppendAccountPermissions(baseDNName, deftName, permissions, accountPermissions, groupAccountPermissions);
        }
        void getModuleBaseDN(IArrayOf<IEspDnStruct> &allBaseDNs, StringAttr &moduleBaseDN)
        {
            //Use the same code as in onAccountPermissions() to find out the moduleBaseDN.
            ForEachItemIn(i, allBaseDNs)
            {
                IEspDnStruct &dn = allBaseDNs.item(i);
                const char *aName = dn.getName();
                const char *aBaseDN = dn.getBasedn();
                const char *aRType = dn.getRtype();
                const char *aRtitle = dn.getRtitle();
                if (!isEmptyString(aName) && !isEmptyString(aBaseDN) && !isEmptyString(aRtitle) &&
                    !isEmptyString(aRType) && strieq(aRType, "module"))
                {
                    moduleBaseDN.set(aBaseDN);
                    break;
                }
            }
        }
        void appendAccountPermissionsForCodeGenResource(const char *baseDNName, const char *moduleBaseDN,
            IArrayOf<IEspAccountPermission> &accountPermissions, IArrayOf<IEspGroupAccountPermission> &groupAccountPermissions)
        {
            IArrayOf<CPermission> permissions;
            if (getResourcePermissions(moduleBaseDN, RT_SERVICE, "codegenerator.cpp", permissions))
                checkAndAppendAccountPermissions(baseDNName, "codegenerator.cpp", permissions, accountPermissions, groupAccountPermissions);
        }
        //Collect the names of the accounts which have permissions in the resources of a BaseDN.
        void appendAccountsInResources(const char *resourceName, IArrayOf<CPermission> &permissions,
            CIArrayOf<CAccountsInResource> &accountsInResources)
        {
            Owned<CAccountsInResource> accountsInResource = new CAccountsInResource(resourceName);

            ForEachItemIn(i, permissions)
            {
                CPermission &perm = permissions.item(i);
                const char *accountName = perm.getAccount_name();
                int accountType = perm.getAccount_type();
                if (isEmptyString(accountName))
                    continue;

                StringBuffer accountNameEx;
                if (GROUP_ACT == accountType)
                    accountNameEx.append("G|");
                accountNameEx.append(accountName);
                accountsInResource->addUniqueAccountName(accountNameEx);
            }
            accountsInResources.append(*accountsInResource.getClear());
        }
        //Similar to onAccountPermissions():
        //For the account stored in the accountNameReq and related group accounts, loop 
        //through every resources in every BaseDNs. For each BaseDN, if the account is 
        //not set for one of its resources, add the BaseDN name to a BaseDN list of this 
        //account. A caller may use the list to enable the Add Permision functions for 
        //the BaseDN.
        void setBaseDNNamesForMissingPermissions(IEspAccountPermissionsV2Response &resp,
            IArrayOf<IEspGroupAccountPermission> &groupAccountPermissions)
        {
            StringArray missingPermissionBasednNames;
            getBaseDNNamesForAccountMissingPermissions(accountNameReq, isGroupAccountReq, missingPermissionBasednNames);
            if (missingPermissionBasednNames.length() > 0)
                resp.setBasednNames(missingPermissionBasednNames);

            ForEachItemIn(i, groupAccountPermissions)
            {
                IEspGroupAccountPermission &groupPermission = groupAccountPermissions.item(i);

                StringArray basednNames;
                getBaseDNNamesForAccountMissingPermissions(groupPermission.getGroupName(), 1, basednNames);
                if (basednNames.length() > 0)
                    groupPermission.setBasednNames(basednNames);
            }
        }
        //For the account stored in the accountName, loop through every resources in every BaseDNs.
        //For each BaseDN, if the account is not in one of its resources, add the BaseDN name to the basednNames.
        void getBaseDNNamesForAccountMissingPermissions(const char *accountName, bool isGroup,
            StringArray &basednNames)
        {
            StringBuffer accountNameEx;
            if (isGroup)
                accountNameEx.append("G|");
            accountNameEx.append(accountName);

            //There may be multiple accounts already in each BaseDN.
            ForEachItemIn(i, accountsInBaseDNs)
            { //for accounts in one BaseDN:
                CAccountsInBaseDN &accountsInBaseDN = accountsInBaseDNs.item(i);
                //One BaseDN may have multiple resources.
                CIArrayOf<CAccountsInResource> &accountsInResources = accountsInBaseDN.getAccountsInResources();
                ForEachItemIn(k, accountsInResources)
                { //for accounts in one resource winthin BaseDN:
                    CAccountsInResource &accountsInResource = accountsInResources.item(k);
                    if (!accountsInResource.findAccountName(accountNameEx))
                    {
                        //Not find the account in this resource. Add the BaseDN name to the basednNames.
                        basednNames.append(accountsInBaseDN.getBaseDNName());
                        break;
                    }
                }
            }
        }

    public:
        CAccountPermissionsHelper(IEspContext *ctx, CLdapSecManager *secmgr) : context(ctx), secMGR(secmgr) { }

        void readReq(IEspAccountPermissionsV2Request &req, const char *accountReq, const char *userID)
        {
            baseDNNameReq.set(req.getResourceName());

            isGroupAccountReq = req.getIsGroup();
            if (!isEmptyString(accountReq))
                accountNameReq.set(accountReq);
            else
            {//send back the permissions for the current user.
                accountNameReq.set(userID);
                isGroupAccountReq = false;
            }

            includeGroup = req.getIncludeGroup();
            if (!isGroupAccountReq && includeGroup)
                secMGR->getGroups(accountNameReq, groupsAccountBelongsTo);
            groupsAccountBelongsTo.append("Authenticated Users");
            groupsAccountBelongsTo.append("everyone");
        }

        void getAccountPermissions(IArrayOf<IEspDnStruct> &allBaseDNs, IEspAccountPermissionsV2Response &resp)
        {
            //accountPermissions: the permissions for the requested account (accountNameReq). The account
            //could be a group account or a personal account.
            //groupAccountPermissions: the permissions for group accounts which are not in the accountPermissions,
            //the authUsersPermissions and the everyonePermissions.
            IArrayOf<IEspAccountPermission> accountPermissions;
            IArrayOf<IEspGroupAccountPermission> groupAccountPermissions;

            //"Authenticated Users" and "Everyone" are default user groups. Create the permission containers for those default groups.
            //The permission containers for other groups are created in appendAccountPermission() when needed.
            authUsersGroupPermission.setown(createGroupAccountPermissionEx("Authenticated Users"));
            everyOneGroupPermission.setown(createGroupAccountPermissionEx("Everyone"));

            getModuleBaseDN(allBaseDNs, moduleBaseDN);
            ForEachItemIn(i, allBaseDNs)
            {
                IEspDnStruct& curBaseDN = allBaseDNs.item(i);
                if (baseDNNameReq.isEmpty()) //Get account permissions for all BaseDNs.
                    readAccountPermissionsInOneBaseDN(allBaseDNs, curBaseDN, accountPermissions, groupAccountPermissions);
                else if (strieq(curBaseDN.getName(), baseDNNameReq.get()))
                {
                    readAccountPermissionsInOneBaseDN(allBaseDNs, curBaseDN, accountPermissions, groupAccountPermissions);
                    break;
                }
            }

            if (hasAuthUsersPerm)
                groupAccountPermissions.append(*authUsersGroupPermission.getLink());

            if (hasEveryonePerm)
                groupAccountPermissions.append(*everyOneGroupPermission.getLink());

            setBaseDNNamesForMissingPermissions(resp, groupAccountPermissions);

            if (groupAccountPermissions.length() > 0)
                resp.setGroupPermissions(groupAccountPermissions);

            if (accountPermissions.length() > 0)
                resp.setPermissions(accountPermissions);
        }
    };

    try
    {
        CLdapSecManager *secMGR = queryLDAPSecurityManager(context, true);

        //Check user and access
        StringBuffer userID;
        context.getUserID(userID);
        if (userID.isEmpty())
            throw makeStringException(ECLWATCH_INVALID_INPUT, "Could not get user ID.");

        const char *accountName = req.getAccountName();
        if (!isEmptyString(accountName) && !streq(accountName, userID.str()))
            checkUser(context, secMGR);

        //Make sure BaseDN settings loaded
        setBasedns(context);

        CAccountPermissionsHelper helper(&context, secMGR);
        helper.readReq(req, accountName, userID);
        helper.getAccountPermissions(m_basedns, resp);
    }
    catch(IException *e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}

bool Cws_accessEx::onFilePermission(IEspContext &context, IEspFilePermissionRequest &req, IEspFilePermissionResponse &resp)
{
    try
    {
        CLdapSecManager* secmgr = queryLDAPSecurityManager(context, false);
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

        checkUser(context, secmgr, FILE_SCOPE_RTYPE, FILE_SCOPE_RTITLE, SecAccess_Read);

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
        secmgr->getAllGroups(groupnames, managedBy, descriptions, context.querySecureContext());
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

            Owned<ISecUser> sec_user = secmgr->findUser(userName, context.querySecureContext());
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
            setBasedns(context);

            //Find out the basedn for RT_FILE_SCOPE
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
                access = SecAccess_None;
                StringArray scopes;
                readFileScopesFromString(pStr0, scopes, false);
                ForEachItemIn(y, scopes)
                {
                    StringBuffer namebuf(scopes.item(y));
                    try
                    {
                        IArrayOf<CPermission> permissions;
                        secmgr->getPermissionsArray(basednStr.str(), RT_FILE_SCOPE, namebuf.str(), permissions);
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
        CLdapSecManager* secmgr = queryLDAPSecurityManagerAndCheckUser(context);

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

                Owned<CLdapSecUser> user = dynamic_cast<CLdapSecUser*>(secmgr->createUser(username, context.querySecureContext()));
                secmgr->getUserInfo(*user.get());
                const char* firstname = user->getFirstName();
                const char* lastname = user->getLastName();

                StringArray groupnames1;
                secmgr->getGroups(username, groupnames1);
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
                secmgr->getGroupMembers(groupname, usernames1);
                ///usernames1.append("_clo");
                ///usernames1.append("_rkc");
                for(unsigned j = 0; j < usernames1.length(); j++)
                {
                    const char* usrname = usernames1.item(j);
                    if(usrname == NULL || usrname[0] == '\0')
                        continue;

                    Owned<CLdapSecUser> user = dynamic_cast<CLdapSecUser*>(secmgr->createUser(usrname, context.querySecureContext()));
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
