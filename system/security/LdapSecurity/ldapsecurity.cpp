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

#define AXA_API DECL_EXPORT

#include "ldapsecurity.ipp"
#include "ldapsecurity.hpp"
#include "authmap.ipp"

/**********************************************************
 *     CLdapSecUser                                       *
 **********************************************************/
CLdapSecUser::CLdapSecUser(const char *name, const char *pw) : 
    m_pw(pw), m_authenticateStatus(AS_UNKNOWN)
{
    setName(name);
    setUserID(0);
    setPosixenabled(false);
    setSudoersEnabled(false);
    setInSudoers(false);
}

CLdapSecUser::~CLdapSecUser()
{
}

//non-interfaced functions
void CLdapSecUser::setUserID(unsigned userid)
{
    m_userid = userid;
}
void CLdapSecUser::setUserSid(int sidlen, const char* sid)
{
    m_usersid.clear();
    m_usersid.append(sidlen, sid);
}
MemoryBuffer& CLdapSecUser::getUserSid()
{
    return m_usersid;
}
//interface ISecUser
const char * CLdapSecUser::getName()
{
    return m_name.get();
}

bool CLdapSecUser::setName(const char * name)
{
    if(name != NULL)
    {
        const char* atsign = strchr(name, '@');
        if(atsign != NULL)
        {
            m_name.set(name, atsign - name);
            m_realm.set(atsign + 1);
        }
        else
        {
            m_name.set(name);
        }
    }           
    return TRUE;
}

const char * CLdapSecUser::getFullName()
{
    return m_fullname.get();
}

bool CLdapSecUser::setFullName(const char * name)
{
    if(name != NULL)
    {
        m_fullname.set(name);
    }
    return true;
}

const char * CLdapSecUser::getFirstName()
{
    return m_firstname.get();
}

bool CLdapSecUser::setFirstName(const char * fname)
{
    if(fname != NULL)
    {
        m_firstname.set(fname);
    }
    return true;
}

const char * CLdapSecUser::getLastName()
{
    return m_lastname.get();
}

bool CLdapSecUser::setLastName(const char * lname)
{
    if(lname != NULL)
    {
        m_lastname.set(lname);
    }
    return true;
}

const char * CLdapSecUser::getEmployeeID()
{
    return m_employeeID.get();
}

bool CLdapSecUser::setEmployeeID(const char * emplID)
{
    m_employeeID.set(emplID);
    return true;
}

const char * CLdapSecUser::getDistinguishedName()
{
    return m_distinguishedName.get();
}

bool CLdapSecUser::setDistinguishedName(const char * dn)
{
    m_distinguishedName.set(dn);
    return true;
}

const char * CLdapSecUser::getRealm()
{
    return m_realm.get();
}

bool CLdapSecUser::setRealm(const char * name)
{
    m_realm.set(name);
    return TRUE;
}

const char * CLdapSecUser::getFqdn()
{
    return m_Fqdn.get();
}
    
bool CLdapSecUser::setFqdn(const char * Fqdn)
{
    m_Fqdn.set(Fqdn);
    return true;
}

const char *CLdapSecUser::getPeer()
{
    return m_Peer.get();
}

bool CLdapSecUser::setPeer(const char *Peer)
{
    m_Peer.set(Peer);
    return true;
}


ISecCredentials & CLdapSecUser::credentials()
{
    return *this;
}

unsigned CLdapSecUser::getUserID()
{
    return m_userid;
}


//interface ISecCredentials
bool CLdapSecUser::setPassword(const char * pw)
{
    m_pw.set(pw);
    return TRUE;
}

const char* CLdapSecUser::getPassword()
{
    return m_pw;
}

bool CLdapSecUser::setEncodedPassword(SecPasswordEncoding enc, void * pw, unsigned length, void * salt, unsigned saltlen)
{
    return FALSE;  //not supported yet
}

void CLdapSecUser::setSessionToken(const MemoryBuffer * const token)
{
    m_sessionToken.clear().append(token);
}

const MemoryBuffer & CLdapSecUser::getSessionToken()
{
    return m_sessionToken;
}

void CLdapSecUser::setSignature(const MemoryBuffer * const signature)
{
    m_signature.clear().append(*signature);
}

const MemoryBuffer & CLdapSecUser::getSignature()
{
    return m_signature;
}

void CLdapSecUser::copyTo(ISecUser& destination)
{
    if (this == &destination)
        return;

    CLdapSecUser* dest = dynamic_cast<CLdapSecUser*>(&destination);
    if(!dest)
        return;

    dest->setAuthenticateStatus(getAuthenticateStatus());
    dest->setName(getName());
    dest->setFullName(getFullName());
    dest->setFirstName(getFirstName());
    dest->setLastName(getLastName());
    dest->setEmployeeID(getEmployeeID());
    dest->setRealm(getRealm());
    dest->credentials().setPassword(credentials().getPassword());
    dest->setUserSid(m_usersid.length(), m_usersid.toByteArray());
    dest->setUserID(m_userid);
    dest->setPasswordExpiration(m_passwordExpiration);
    dest->setDistinguishedName(m_distinguishedName);
    dest->credentials().setSessionToken(&m_sessionToken);
    dest->credentials().setSignature(&m_signature);
}

ISecUser * CLdapSecUser::clone()
{
    CLdapSecUser* newuser = new CLdapSecUser(m_name.get(), m_pw.get());
    if(newuser)
        copyTo(*newuser);
    return newuser;
}

/**********************************************************
 *     CLdapSecResource                                   *
 **********************************************************/

CLdapSecResource::CLdapSecResource(const char *name) : m_name(name), m_access(SecAccess_None), m_required_access(SecAccess_None)
{
    m_resourcetype = RT_DEFAULT;
}

void CLdapSecResource::addAccess(SecAccessFlags flags)
{
    m_access = (SecAccessFlags)((int)m_access | (int)flags);
}

void CLdapSecResource::setAccessFlags(SecAccessFlags flags)
{
    m_access = flags;
}

void CLdapSecResource::setRequiredAccessFlags(SecAccessFlags flags)
{
    m_required_access = flags;
}

SecAccessFlags CLdapSecResource::getRequiredAccessFlags()
{
    return m_required_access;
}

//interface ISecResource : extends IInterface
const char * CLdapSecResource::getName()
{
    return m_name.get();
}

SecAccessFlags CLdapSecResource::getAccessFlags()
{
    return m_access;
}

int CLdapSecResource::addParameter(const char* name, const char* value)
{
    if (!m_parameters)
        m_parameters.setown(createProperties(false));
    m_parameters->setProp(name, value);
    return 0;
}

const char * CLdapSecResource::getParameter(const char * name)
{
    if (m_parameters)
    {
        const char *value = m_parameters->queryProp(name);
        return value;
    }

    return NULL;

}

void CLdapSecResource::setDescription(const char* description)
{
    m_description.clear().append(description);
}

const char* CLdapSecResource::getDescription()
{
    return m_description.str();
}

void CLdapSecResource::setValue(const char* value)
{
    m_value.clear();
    m_value.append(value);
}

const char* CLdapSecResource::getValue()
{
    return m_value.str();
}


ISecResource * CLdapSecResource::clone()
{
    CLdapSecResource* _res = new CLdapSecResource(m_name.get());
    if(!_res)
        return NULL;
    
    _res->setResourceType(m_resourcetype);
    _res->setValue(m_value.str());
    _res->m_access = m_access;
    _res->m_required_access = m_required_access;
    _res->setDescription(m_description.str());

    if(!m_parameters)
        return _res;

    Owned<IPropertyIterator> Itr = m_parameters->getIterator();
    Itr->first();
    while(Itr->isValid())
    {
        _res->addParameter(Itr->getPropKey(),m_parameters->queryProp(Itr->getPropKey()));
        Itr->next();
    }
    return _res;
}

void CLdapSecResource::copy(ISecResource* from)
{
    if(!from)
        return;
    CLdapSecResource* ldapfrom = dynamic_cast<CLdapSecResource*>(from);
    if(!ldapfrom)
        return;
    m_access = ldapfrom->m_access;
    setDescription(ldapfrom->m_description.str());

    if(m_parameters.get())
    {
        m_parameters.clear();
    }

    if(!ldapfrom->m_parameters.get())
        return;

    Owned<IPropertyIterator> Itr = ldapfrom->m_parameters->getIterator();
    Itr->first();
    while(Itr->isValid())
    {
        addParameter(Itr->getPropKey(), ldapfrom->m_parameters->queryProp(Itr->getPropKey()));
        Itr->next();
    }
    return;
}

SecResourceType CLdapSecResource::getResourceType()
{
    return m_resourcetype;
}

void CLdapSecResource::setResourceType(SecResourceType resourcetype)
{
    m_resourcetype = resourcetype;
}

/**********************************************************
 *     CLdapSecResourceList                               *
 **********************************************************/

CLdapSecResourceList::CLdapSecResourceList(const char *name) : m_complete(0)
{
    m_name.set(name);
}

void CLdapSecResourceList::setAuthorizationComplete(bool value)
{
    m_complete=value;
}

IArrayOf<ISecResource>& CLdapSecResourceList::getResourceList()
{
    return m_rlist;
}

//interface ISecResourceList : extends IInterface
bool CLdapSecResourceList::isAuthorizationComplete()
{
    return m_complete;
}

ISecResourceList * CLdapSecResourceList::clone()
{
    CLdapSecResourceList* _newList = new CLdapSecResourceList(m_name.get());
    if(!_newList)
        return NULL;
    copyTo(*_newList);
    return _newList;
}

bool CLdapSecResourceList::copyTo(ISecResourceList& destination)
{
    ForEachItemIn(x, m_rlist)
    {
        CLdapSecResource* res = (CLdapSecResource*)(&(m_rlist.item(x)));
        if(res)
            destination.addResource(res->clone());
    }
    return false;
}

ISecResource* CLdapSecResourceList::addResource(const char * name)
{
    if(!name || !*name)
    {
        DBGLOG("CLdapSecResourceList::addResource resource name must be provided");
        return NULL;
    }

    ISecResource* resource = m_rmap[name];
    if(resource == NULL)
    {
        resource = new CLdapSecResource(name);
        m_rlist.append(*resource);
        m_rmap[name] = resource;
    }

    return resource;
}

void CLdapSecResourceList::addResource(ISecResource * resource)
{
    if(resource == NULL)
    {
        DBGLOG("CLdapSecResourceList::addResource2 ISecResource cannot be NULL");
        return;
    }
    const char* name = resource->getName();
    if(!name || !*name)
    {
        DBGLOG("CLdapSecResourceList::addResource2 resource name must be provided");
        return;
    }

    ISecResource* r = m_rmap[name];
    if(r == NULL)
    {
        m_rlist.append(*resource);
        m_rmap[name] = resource;
    }
}

bool CLdapSecResourceList::addCustomResource(const char * name, const char * config)
{
    return false;
}

ISecResource * CLdapSecResourceList::getResource(const char * Resource)
{
    if(!Resource || !*Resource)
    {
        DBGLOG("CLdapSecResourceList::getResource resource name must be provided");
        return NULL;
    }

    ISecResource* r = m_rmap[Resource];
    if(r)
        return LINK(r);
    else
        return NULL;
}

void CLdapSecResourceList::clear()
{
    m_rlist.kill();
}

int CLdapSecResourceList::count()
{
    return m_rlist.length();
}

const char* CLdapSecResourceList::getName()
{
    return m_name.get();
}

ISecResource * CLdapSecResourceList::queryResource(unsigned seq)
{
    if(seq < m_rlist.length())
        return &(m_rlist.item(seq));
    else
        return NULL;
}

ISecPropertyIterator * CLdapSecResourceList::getPropertyItr()
{
    return new ArrayIIteratorOf<IArrayOf<struct ISecResource>, ISecProperty, ISecPropertyIterator>(m_rlist);
}

ISecProperty* CLdapSecResourceList::findProperty(const char* name)
{
    if(!name || !*name)
    {
        DBGLOG("CLdapSecResourceList::findProperty property name must be provided");
        return NULL;
    }
    return m_rmap[name];
}


/**********************************************************
 *     CLdapSecManager                                    *
 **********************************************************/
CLdapSecManager::CLdapSecManager(const char *serviceName, const char *config)
{
    IPropertyTree* cfg = createPTreeFromXMLString(config, ipt_caseInsensitive);

    if(cfg == NULL)
    {
        throw MakeStringException(-1, "createPTreeFromXMLString() failed for %s", config);
    }

    init(serviceName, cfg);
}

void CLdapSecManager::init(const char *serviceName, IPropertyTree* cfg)
{
    for(int i = 0; i < RT_SCOPE_MAX; i++)
        m_cache_off[i] = false;
    m_cache_off[RT_VIEW_SCOPE] = true;
    
    m_usercache_off = false;

    m_cfg.setown(cfg);

    cfg->getProp(".//@ldapAddress", m_server);
    cfg->getProp(".//@description", m_description);

    ILdapClient* ldap_client = createLdapClient(cfg);
    
    IPermissionProcessor* pp;
    if(ldap_client->getServerType() == ACTIVE_DIRECTORY)
        pp = new PermissionProcessor(cfg);
    else if(ldap_client->getServerType() == IPLANET)
        pp = new CIPlanetAciProcessor(cfg);
    else if(ldap_client->getServerType() == OPEN_LDAP)
    {
        if (0 == stricmp(ldap_client->getLdapConfig()->getCfgServerType(), "389DirectoryServer"))//uses iPlanet style ACI
            pp = new CIPlanetAciProcessor(cfg);
        else
            pp = new COpenLdapAciProcessor(cfg);
    }
    else
        throwUnexpected();

    pp->setLdapClient(ldap_client);
    ldap_client->init(pp);

    m_ldap_client.setown(ldap_client);
    m_pp.setown(pp);
    int cachetimeout = cfg->getPropInt("@cacheTimeout", 5);

    if (cfg->getPropBool("@sharedCache", true))
        m_permissionsCache.setown(CPermissionsCache::getInstance(cfg->queryProp("@name")));
    else
        m_permissionsCache.setown(new CPermissionsCache());

    m_permissionsCache->setCacheTimeout( 60 * cachetimeout);
    m_permissionsCache->setTransactionalEnabled(true);
    m_permissionsCache->setSecManager(this);
    m_passwordExpirationWarningDays = cfg->getPropInt(".//@passwordExpirationWarningDays", 10); //Default to 10 days
    m_checkViewPermissions = cfg->getPropBool(".//@checkViewPermissions", false);
};


CLdapSecManager::CLdapSecManager(const char *serviceName, IPropertyTree &config)
{
    init(serviceName, &config);
}

CLdapSecManager::~CLdapSecManager()
{
}

//interface ISecManager : extends IInterface
ISecUser * CLdapSecManager::createUser(const char * user_name)
{
    return (new CLdapSecUser(user_name, NULL));
}

ISecResourceList * CLdapSecManager::createResourceList(const char * rlname)
{
    return (new CLdapSecResourceList(rlname));
}

bool CLdapSecManager::subscribe(ISecAuthenticEvents & events)
{
    m_subscriber.set(&events);
    return true;
}

bool CLdapSecManager::unsubscribe(ISecAuthenticEvents & events)
{
    if (&events == m_subscriber.get())
    {
        m_subscriber.set(NULL);
    }
    return true;
}

bool CLdapSecManager::authenticate(ISecUser* user)
{
    if(!user)
    {
        DBGLOG("CLdapSecManager::authenticate user cannot be NULL");
        return false;
    }

    if(user->getAuthenticateStatus() == AS_AUTHENTICATED)
        return true;

    if(m_permissionsCache->isCacheEnabled() && !m_usercache_off && m_permissionsCache->lookup(*user))
    {
        user->setAuthenticateStatus(AS_AUTHENTICATED);
        return true;
    }

    if (user->credentials().getSessionToken().length())//Already authenticated it token exists
    {
        user->setAuthenticateStatus(AS_AUTHENTICATED);
        if(m_permissionsCache->isCacheEnabled() && !m_usercache_off)
            m_permissionsCache->add(*user);
        return true;
    }

    bool ok = m_ldap_client->authenticate(*user);
    if(ok)
    {
        user->setAuthenticateStatus(AS_AUTHENTICATED);
        if(m_permissionsCache->isCacheEnabled() && !m_usercache_off)
            m_permissionsCache->add(*user);
    }

    return ok;
}

bool CLdapSecManager::authorizeEx(SecResourceType rtype, ISecUser& sec_user, ISecResourceList * Resources, IEspSecureContext* secureContext)
{
    if(!authenticate(&sec_user))
    {
        return false;
    }

    CLdapSecResourceList * reslist = (CLdapSecResourceList*)Resources;
    if(!reslist)
        return true;
    IArrayOf<ISecResource>& rlist = reslist->getResourceList();
    int nResources = rlist.length();
    int ri;
    for(ri = 0; ri < nResources; ri++)
    {
        ISecResource* res = &rlist.item(ri);
        if(res != NULL)
            res->setResourceType(rtype);
    }

    if (nResources <= 0)
        return true;

    bool rc;

    time_t tctime = getThreadCreateTime();
    if ((m_permissionsCache->isCacheEnabled() || (m_permissionsCache->isTransactionalEnabled() && tctime > 0)) && (!m_cache_off[rtype]))
    {
        bool* cached_found = (bool*)alloca(nResources*sizeof(bool));
        int nFound = m_permissionsCache->lookup(sec_user, rlist, cached_found);
        if (nFound < nResources)
        {
            IArrayOf<ISecResource> rlist2;
            int i;
            for (i=0; i < nResources; i++)
            {
                if (*(cached_found+i) == false)
                {
                    ISecResource& secRes = rlist.item(i);
                    secRes.Link();
                    rlist2.append(secRes);
                    //DBGLOG("CACHE: Fetching permissions for %s:%s", sec_user.getName(), secRes.getName());
                }
            }

            rc = m_ldap_client->authorize(rtype, sec_user, rlist2);
            if (rc)
                m_permissionsCache->add(sec_user, rlist2);
        }
        else
            rc = true;  
    }
    else
    {
        rc = m_ldap_client->authorize(rtype, sec_user, rlist, reslist->getName());
    }
    return rc;
}

SecAccessFlags CLdapSecManager::authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext)
{
    if(!resourcename || !*resourcename)
        return SecAccess_Full;

    Owned<ISecResourceList> rlist;
    rlist.setown(createResourceList("resources"));
    rlist->addResource(resourcename);
    
    bool ok = authorizeEx(rtype, user, rlist.get(), secureContext);
    if(ok)
        return rlist->queryResource(0)->getAccessFlags();
    else
        return SecAccess_Unavailable;
}

bool CLdapSecManager::authorizeEx(SecResourceType rtype, ISecUser& sec_user, ISecResourceList * Resources, bool doAuthentication)
{
    if(doAuthentication && !authenticate(&sec_user))
    {
        return false;
    }

    CLdapSecResourceList * reslist = (CLdapSecResourceList*)Resources;
    if(!reslist)
        return true;
    IArrayOf<ISecResource>& rlist = reslist->getResourceList();
    int nResources = rlist.length();
    int ri;
    for(ri = 0; ri < nResources; ri++)
    {
        ISecResource* res = &rlist.item(ri);
        if(res != NULL)
            res->setResourceType(rtype);
    }

    if (nResources <= 0)
        return true;

    bool rc;

    time_t tctime = getThreadCreateTime();
    if ((m_permissionsCache->isCacheEnabled() || (m_permissionsCache->isTransactionalEnabled() && tctime > 0)) && (!m_cache_off[rtype]))
    {
        bool* cached_found = (bool*)alloca(nResources*sizeof(bool));
        int nFound = m_permissionsCache->lookup(sec_user, rlist, cached_found);
        if (nFound < nResources)
        {
            IArrayOf<ISecResource> rlist2;
            int i;
            for (i=0; i < nResources; i++)
            {
                if (*(cached_found+i) == false)
                {
                    ISecResource& secRes = rlist.item(i);
                    secRes.Link();
                    rlist2.append(secRes);
                    //DBGLOG("CACHE: Fetching permissions for %s:%s", sec_user.getName(), secRes.getName());
                }
            }

            rc = m_ldap_client->authorize(rtype, sec_user, rlist2);
            if (rc)
                m_permissionsCache->add(sec_user, rlist2);
        }
        else
            rc = true;  
    }
    else
    {
        rc = m_ldap_client->authorize(rtype, sec_user, rlist);
    }
    return rc;
}

SecAccessFlags CLdapSecManager::authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, bool doAuthentication)
{
    if(!resourcename || !*resourcename)
        return SecAccess_Full;

    Owned<ISecResourceList> rlist;
    rlist.setown(createResourceList("resources"));
    rlist->addResource(resourcename);
    
    bool ok = authorizeEx(rtype, user, rlist.get(), doAuthentication);
    if(ok)
        return rlist->queryResource(0)->getAccessFlags();
    else
        return SecAccess_Unavailable;
}

SecAccessFlags CLdapSecManager::getAccessFlagsEx(SecResourceType rtype, ISecUser & user, const char * resourcename)
{
    if(!resourcename || !*resourcename)
        return SecAccess_Unavailable;

    Owned<ISecResourceList> rlist0;
    rlist0.setown(createResourceList("resources"));
    rlist0->addResource(resourcename);
    
    CLdapSecResourceList * reslist = (CLdapSecResourceList*)rlist0.get();
    if(!reslist)
        return SecAccess_Unavailable;
    IArrayOf<ISecResource>& rlist = reslist->getResourceList();
    int nResources = rlist.length();
    int ri;
    for(ri = 0; ri < nResources; ri++)
    {
        ISecResource* res = &rlist.item(ri);
        if(res != NULL)
            res->setResourceType(rtype);
    }

    if (nResources <= 0)
        return SecAccess_Unavailable;

    bool ok = false;

    time_t tctime = getThreadCreateTime();
    if ((m_permissionsCache->isCacheEnabled() || (m_permissionsCache->isTransactionalEnabled() && tctime > 0)) && (!m_cache_off[rtype]))
    {
        bool* cached_found = (bool*)alloca(nResources*sizeof(bool));
        int nFound = m_permissionsCache->lookup(user, rlist, cached_found);
        if (nFound < nResources)
        {
            IArrayOf<ISecResource> rlist2;
            int i;
            for (i=0; i < nResources; i++)
            {
                if (*(cached_found+i) == false)
                {
                    ISecResource& secRes = rlist.item(i);
                    secRes.Link();
                    rlist2.append(secRes);
                    //DBGLOG("CACHE: Fetching permissions for %s:%s", sec_user.getName(), secRes.getName());
                }
            }

            ok = m_ldap_client->authorize(rtype, user, rlist2);
            if (ok)
                m_permissionsCache->add(user, rlist2);
        }
        else
            ok = true;  
    }
    else
    {
        ok = m_ldap_client->authorize(rtype, user, rlist);
    }

    //bool ok = authorizeEx(rtype, user, rlist.get());
    if(ok)
        return rlist0->queryResource(0)->getAccessFlags();
    else
        return SecAccess_Unavailable;
}

bool CLdapSecManager::authorize(ISecUser& sec_user, ISecResourceList * Resources, IEspSecureContext* secureContext)
{
    return authorizeEx(RT_DEFAULT, sec_user, Resources, secureContext);
}


SecAccessFlags CLdapSecManager::authorizeFileScope(ISecUser & user, const char * filescope)
{
    if(filescope == 0 || filescope[0] == '\0')
        return SecAccess_Full;

    StringBuffer managedFilescope;
    if(m_permissionsCache->isCacheEnabled() && !m_usercache_off)
    {
        SecAccessFlags accessFlags;
        //See if file scope in question is managed by LDAP permissions.
        //  If not, return default file permission (dont call out to LDAP)
        //  If is, look in cache for permission of longest matching managed scope strings. If found return that permission (no call to LDAP),
        //  otherwise a call to LDAP "authorizeFileScope" is necessary, specifying the longest matching managed scope string
        bool gotPerms = m_permissionsCache->queryPermsManagedFileScope(user, filescope, managedFilescope, &accessFlags);
        if (gotPerms)
            return accessFlags;
    }

    Owned<ISecResourceList> rlist;
    rlist.setown(createResourceList("FileScope"));
    rlist->addResource(managedFilescope.length() ? managedFilescope.str() : filescope );
    
    bool ok = authorizeFileScope(user, rlist.get());
    if(ok)
        return rlist->queryResource(0)->getAccessFlags();
    else
        return SecAccess_Unavailable;
}

bool CLdapSecManager::authorizeFileScope(ISecUser & user, ISecResourceList * resources)
{
    return authorizeEx(RT_FILE_SCOPE, user, resources);
}

bool CLdapSecManager::authorizeViewScope(ISecUser & user, StringArray & filenames, StringArray & columnnames)
{
    if (filenames.length() != columnnames.length())
    {
        PROGLOG("Error authorizing view scope: number of filenames (%d) do not match number of columnnames (%d).", filenames.length(), columnnames.length());
        return false; 
    }

    const char* username = user.getName();
    StringArray viewnames, viewdescriptions, viewManagedBy;

    queryAllViews(viewnames, viewdescriptions, viewManagedBy);

    // All views where user belongs must pass
    ForEachItemIn(i, viewnames)
    {
        const char* viewname = viewnames.item(i);

        if (userInView(username, viewname))
        {
            Owned<ISecResourceList> resList;
            resList.setown(new CLdapSecResourceList(viewname));

            // Inefficient loop because we are adding same used columns all over again for each views.
            // we can improve the performance later if there is a way to rename and reuse same ISecResourceList for each view.
            ForEachItemIn(j, filenames)
            {
                StringBuffer resourceName;
                resourceName.append("QueryAccessedColumns");
                resourceName.append(j);
                ISecResource* res = resList->addResource(resourceName);
                res->addParameter("file", filenames.item(j));
                res->addParameter("column", columnnames.item(j));
            }

            if (!authorizeEx(RT_VIEW_SCOPE, user, resList.get()))
            {
                PROGLOG("View scope authorization denied by a view %s for a user %s", viewname, username);
                return false;
            }
        }
    }

    return true;
}

SecAccessFlags CLdapSecManager::authorizeWorkunitScope(ISecUser & user, const char * wuscope)
{
    if(wuscope == 0 || wuscope[0] == '\0')
        return SecAccess_Full;

    Owned<ISecResourceList> rlist;
    rlist.setown(createResourceList("WorkunitScope"));
    rlist->addResource(wuscope);
    
    bool ok = authorizeWorkunitScope(user, rlist.get());
    if(ok)
        return rlist->queryResource(0)->getAccessFlags();
    else
        return SecAccess_Unavailable;
}
    
bool CLdapSecManager::authorizeWorkunitScope(ISecUser & user, ISecResourceList * resources)
{
    return authorizeEx(RT_WORKUNIT_SCOPE, user, resources);
}


bool CLdapSecManager::addResourcesEx(SecResourceType rtype, ISecUser& sec_user, ISecResourceList * resources, SecPermissionType ptype, const char* basedn)
{
    CLdapSecResourceList * reslist = (CLdapSecResourceList*)resources;
    if(!reslist)
        return true;
    IArrayOf<ISecResource>& rlist = reslist->getResourceList();
    if(rlist.length() <= 0)
        return true;
    
    return m_ldap_client->addResources(rtype, sec_user, rlist, ptype, basedn);
}

bool CLdapSecManager::addResourceEx(SecResourceType rtype, ISecUser& user, const char* resourcename, SecPermissionType ptype, const char* basedn)
{
    Owned<ISecResourceList> rlist;
    rlist.setown(createResourceList("resources"));
    rlist->addResource(resourcename);
    
    return addResourcesEx(rtype, user, rlist.get(), ptype, basedn);
}


bool CLdapSecManager::addResources(ISecUser& sec_user, ISecResourceList * resources)
{
    return addResourcesEx(RT_DEFAULT, sec_user, resources);
}

bool CLdapSecManager::addUser(ISecUser & user)
{
    bool ok = m_ldap_client->addUser(user);
    if(!ok)
        return false;

    return m_pp->retrieveUserInfo(user);
}

ISecUser * CLdapSecManager::lookupUser(unsigned uid)
{
    return m_ldap_client->lookupUser(uid);
}

ISecUser * CLdapSecManager::findUser(const char * username)
{
    if(username == NULL || strlen(username) == 0)
    {
        DBGLOG("findUser - username is empty");
        return NULL;
    }

    Owned<ISecUser> user;
    user.setown(createUser(username));

    try
    {
        bool ok = m_pp->retrieveUserInfo(*user);
        if(ok)
        {
            return LINK(user.get());
        }
        else
        {
            return NULL;
        }
    }
    catch(IException*)
    {
        return NULL;
    }
    catch(...)
    {
        return NULL;
    }
}

ISecUserIterator * CLdapSecManager::getAllUsers()
{
    synchronized block(m_monitor);
    m_user_array.popAll(true);
    m_ldap_client->retrieveUsers(m_user_array);
    return new ArrayIIteratorOf<IUserArray, ISecUser, ISecUserIterator>(m_user_array);
}

void CLdapSecManager::searchUsers(const char* searchstr, IUserArray& users)
{
    m_ldap_client->retrieveUsers(searchstr, users);
}

ISecItemIterator* CLdapSecManager::getUsersSorted(const char* userName, UserField* sortOrder, const unsigned pageStartFrom,
    const unsigned pageSize, unsigned* total, __int64* cacheHint)
{
    return m_ldap_client->getUsersSorted(userName, sortOrder, pageStartFrom, pageSize, total, cacheHint);
}

void CLdapSecManager::getAllUsers(IUserArray& users)
{
    m_ldap_client->retrieveUsers(users);
}

bool CLdapSecManager::getResources(SecResourceType rtype, const char * basedn, IArrayOf<ISecResource> & resources)
{
    return m_ldap_client->getResources(rtype, basedn, "", resources);
}

bool CLdapSecManager::getResourcesEx(SecResourceType rtype, const char * basedn, const char* searchstr, IArrayOf<ISecResource> & resources)
{
    return m_ldap_client->getResourcesEx(rtype, basedn, "", searchstr, resources);
}

ISecItemIterator* CLdapSecManager::getResourcesSorted(SecResourceType rtype, const char * basedn, const char * resourceName, unsigned extraNameFilter,
    ResourceField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize, unsigned *total, __int64 *cachehint)
{
    return m_ldap_client->getResourcesSorted(rtype, basedn, resourceName, extraNameFilter, sortOrder, pageStartFrom, pageSize, total, cachehint);
}

void CLdapSecManager::setExtraParam(const char * name, const char * value)
{
    if(name == NULL || name[0] == '\0')
    {
        DBGLOG("CLdapSecManager::setExtraParam name must be provided");
        return;
    }

    if (!m_extraparams)
        m_extraparams.setown(createProperties(false));
    m_extraparams->setProp(name, value);

    if(value != NULL && value[0] != '\0')
    {
        if(stricmp(name, "resourcesBasedn") == 0)
            m_ldap_client->setResourceBasedn(value, RT_DEFAULT);
        else if(stricmp(name, "workunitsBasedn") == 0)
            m_ldap_client->setResourceBasedn(value, RT_WORKUNIT_SCOPE);
    }
}


IAuthMap * CLdapSecManager::createAuthMap(IPropertyTree * authconfig)
{
    CAuthMap* authmap = new CAuthMap(this);

    IPropertyTreeIterator *loc_iter = NULL;
    loc_iter = authconfig->getElements(".//Location");
    if (loc_iter != NULL)
    {
        IPropertyTree *location = NULL;
        loc_iter->first();
        while(loc_iter->isValid())
        {
            location = &loc_iter->query();
            if (location)
            {
                StringBuffer pathstr, rstr, required, description;
                location->getProp("@path", pathstr);
                location->getProp("@resource", rstr);
                location->getProp("@required", required);
                location->getProp("@description", description);
                
                if(pathstr.length() == 0)
                    throw MakeStringException(-1, "path empty in Authenticate/Location");
                if(rstr.length() == 0)
                    throw MakeStringException(-1, "resource empty in Authenticate/Location");

                ISecResourceList* rlist = authmap->queryResourceList(pathstr.str());
                if(rlist == NULL)
                {
                    rlist = createResourceList("ldapsecurity");                     
                    authmap->add(pathstr.str(), rlist);
                }
                ISecResource* rs = rlist->addResource(rstr.str());
                SecAccessFlags requiredaccess = str2perm(required.str());
                rs->setRequiredAccessFlags(requiredaccess);
                rs->setDescription(description.str());
            }
            loc_iter->next();
        }
        loc_iter->Release();
        loc_iter = NULL;
    }

    authmap->addToBackend();

    return authmap;
}


IAuthMap * CLdapSecManager::createFeatureMap(IPropertyTree * authconfig)
{
    CAuthMap* feature_authmap = new CAuthMap(this);

    IPropertyTreeIterator *feature_iter = NULL;
    feature_iter = authconfig->getElements(".//Feature");
    if (feature_iter != NULL)
    {
        IPropertyTree *feature = NULL;
        feature_iter->first();
        while(feature_iter->isValid())
        {
            feature = &feature_iter->query();
            if (feature)
            {
                StringBuffer pathstr, rstr, required, description;
                feature->getProp("@path", pathstr);
                feature->getProp("@resource", rstr);
                feature->getProp("@required", required);
                feature->getProp("@description", description);
                ISecResourceList* rlist = feature_authmap->queryResourceList(pathstr.str());
                if(rlist == NULL)
                {
                    rlist = createResourceList(pathstr.str());                      
                    feature_authmap->add(pathstr.str(), rlist);
                }
                ISecResource* rs = rlist->addResource(rstr.str());
                SecAccessFlags requiredaccess = str2perm(required.str());
                rs->setRequiredAccessFlags(requiredaccess);
                rs->setDescription(description.str());
            }
            feature_iter->next();
        }
        feature_iter->Release();
        feature_iter = NULL;
    }

    feature_authmap->addToBackend();
    
    return feature_authmap;
}

bool CLdapSecManager::updateUserPassword(ISecUser& user, const char* newPassword, const char* currPassword)
{
    // Authenticate User first
    if(!authenticate(&user) && user.getAuthenticateStatus() != AS_PASSWORD_VALID_BUT_EXPIRED)
    {
        return false;
    }

    //Update password if authenticated
    bool ok = m_ldap_client->updateUserPassword(user, newPassword, currPassword);
    if(ok && m_permissionsCache->isCacheEnabled() && !m_usercache_off)
    {
        m_permissionsCache->removeFromUserCache(user);
    }
    return ok;
}

bool CLdapSecManager::updateUser(const char* type, ISecUser& user)
{
    bool ok = m_ldap_client->updateUser(type, user);
    if(ok && m_permissionsCache->isCacheEnabled() && !m_usercache_off)
        m_permissionsCache->removeFromUserCache(user);

    return ok;
}

bool CLdapSecManager::updateUserPassword(const char* username, const char* newPassword)
{
    return m_ldap_client->updateUserPassword(username, newPassword);
}

void CLdapSecManager::getAllGroups(StringArray & groups, StringArray & managedBy, StringArray & descriptions)
{
    m_ldap_client->getAllGroups(groups, managedBy, descriptions);
}

ISecItemIterator* CLdapSecManager::getGroupsSorted(GroupField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize,
    unsigned *total, __int64 *cachehint)
{
    return m_ldap_client->getGroupsSorted(sortOrder, pageStartFrom, pageSize, total, cachehint);
}

ISecItemIterator* CLdapSecManager::getGroupMembersSorted(const char* groupName, UserField* sortOrder, const unsigned pageStartFrom, const unsigned pageSize,
    unsigned *total, __int64 *cachehint)
{
    return m_ldap_client->getGroupMembersSorted(groupName, sortOrder, pageStartFrom, pageSize, total, cachehint);
}

bool CLdapSecManager::getPermissionsArray(const char* basedn, SecResourceType rtype, const char* name, IArrayOf<CPermission>& permissions)
{
    return m_ldap_client->getPermissionsArray(basedn, rtype, name, permissions);
}

void CLdapSecManager::addGroup(const char* groupname, const char * groupOwner, const char * groupDesc)
{
    m_ldap_client->addGroup(groupname, groupOwner, groupDesc);
}

void CLdapSecManager::deleteGroup(const char* groupname)
{
    m_ldap_client->deleteGroup(groupname);
}

bool CLdapSecManager::changePermission(CPermissionAction& action)
{
    return m_ldap_client->changePermission(action);
}

void CLdapSecManager::getGroups(const char* username, StringArray & groups)
{
    m_ldap_client->getGroups(username, groups);
}

void CLdapSecManager::changeUserGroup(const char* action, const char* username, const char* groupname)
{
    m_ldap_client->changeUserGroup(action, username, groupname);
}

bool CLdapSecManager::deleteUser(ISecUser* user)
{
    return m_ldap_client->deleteUser(user);
}

void CLdapSecManager::getGroupMembers(const char* groupname, StringArray & users)
{
    m_ldap_client->getGroupMembers(groupname, users);
}

void CLdapSecManager::deleteResource(SecResourceType rtype, const char * name, const char * basedn)
{
    m_ldap_client->deleteResource(rtype, name, basedn);

    time_t tctime = getThreadCreateTime();
    if ((m_permissionsCache->isCacheEnabled() || (m_permissionsCache->isTransactionalEnabled() && tctime > 0)) && (!m_cache_off[rtype]))
        m_permissionsCache->remove(rtype, name);
}

void CLdapSecManager::renameResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
{
    m_ldap_client->renameResource(rtype, oldname, newname, basedn);

    time_t tctime = getThreadCreateTime();
    if ((m_permissionsCache->isCacheEnabled() || (m_permissionsCache->isTransactionalEnabled() && tctime > 0)) && (!m_cache_off[rtype]))
        m_permissionsCache->remove(rtype, oldname);
}

void CLdapSecManager::copyResource(SecResourceType rtype, const char * oldname, const char * newname, const char * basedn)
{
    m_ldap_client->copyResource(rtype, oldname, newname, basedn);
}

void CLdapSecManager::normalizeDn(const char* dn, StringBuffer& ndn)
{
    m_ldap_client->normalizeDn(dn, ndn);
}

bool CLdapSecManager::isSuperUser(ISecUser* user)
{
    return m_ldap_client->isSuperUser(user);
}

ILdapConfig* CLdapSecManager::queryConfig()
{
    return m_ldap_client->queryConfig();
}

void CLdapSecManager::cacheSwitch(SecResourceType rtype, bool on)
{
    m_cache_off[rtype] = !on;

    // To make things simple, turning off any resource type's permission cache turns off the userCache.
    if(!on)
        m_usercache_off = true;
}

int CLdapSecManager::countUsers(const char* searchstr, int limit)
{
    return m_ldap_client->countUsers(searchstr, limit);
}

int CLdapSecManager::countResources(const char* basedn, const char* searchstr, int limit)
{
    return m_ldap_client->countResources(basedn, searchstr, limit);
}

bool CLdapSecManager::getUserInfo(ISecUser& user, const char* infotype)
{
    return m_ldap_client->getUserInfo(user, infotype);
}

bool CLdapSecManager::createUserScopes()
{
    Owned<ISecUserIterator> it = getAllUsers();
    it->first();
    bool rc = true;
    while(it->isValid())
    {
        ISecUser &user = it->get();
        if (!m_ldap_client->createUserScope(user))
        {
            PROGLOG("CLdapSecManager::createUserScopes Error creating user scope for user '%s'", user.getName());
            rc = false;
        }
        it->next();
    }
    return rc;
}


aindex_t CLdapSecManager::getManagedFileScopes(IArrayOf<ISecResource>& scopes)
{
    return m_ldap_client->getManagedFileScopes(scopes);
}

SecAccessFlags CLdapSecManager::queryDefaultPermission(ISecUser& user)
{
    return m_ldap_client->queryDefaultPermission(user);
}

bool CLdapSecManager::clearPermissionsCache(ISecUser& user)
{
    if(m_permissionsCache->isCacheEnabled())
    {
        if (!authenticate(&user))
        {
            PROGLOG("User %s not authorized to clear permissions cache", user.getName());
            return false;
        }
        if (!isSuperUser(&user))
        {
            PROGLOG("User %s denied, only a superuser can clear permissions cache", user.getName());
            return false;
        }
        m_permissionsCache->flush();
    }
    return true;
}
bool CLdapSecManager::authenticateUser(ISecUser & user, bool *superUser)
{
    if (!authenticate(&user))
        return false;
    if (superUser)
        *superUser = isSuperUser(&user);
    return true;
}

//Data View related interfaces
void CLdapSecManager::createView(const char* viewName, const char * viewDescription)
{
    m_ldap_client->createView(viewName, viewDescription);
}

void CLdapSecManager::deleteView(const char* viewName)
{
    m_ldap_client->deleteView(viewName);
}

void CLdapSecManager::queryAllViews(StringArray & viewNames, StringArray & viewDescriptions, StringArray & viewManagedBy)
{
    m_ldap_client->queryAllViews(viewNames, viewDescriptions, viewManagedBy);
}

void CLdapSecManager::addViewColumns(const char* viewName, StringArray & files, StringArray & columns)
{
    m_ldap_client->addViewColumns(viewName, files, columns);
}

void CLdapSecManager::removeViewColumns(const char* viewName, StringArray & files, StringArray & columns)
{
    m_ldap_client->removeViewColumns(viewName, files, columns);
}

void CLdapSecManager::queryViewColumns(const char* viewName, StringArray & files, StringArray & columns)
{
    m_ldap_client->queryViewColumns(viewName, files, columns);
}

void CLdapSecManager::addViewMembers(const char* viewName, StringArray & viewUsers, StringArray & viewGroups)
{
    m_ldap_client->addViewMembers(viewName, viewUsers, viewGroups);
}

void CLdapSecManager::removeViewMembers(const char* viewName, StringArray & viewUsers, StringArray & viewGroups)
{
    m_ldap_client->removeViewMembers(viewName, viewUsers, viewGroups);
}

void CLdapSecManager::queryViewMembers(const char* viewName, StringArray & viewUsers, StringArray & viewGroups)
{
    m_ldap_client->queryViewMembers(viewName, viewUsers, viewGroups);
}

bool CLdapSecManager::userInView(const char * user, const char* viewName)
{
    return m_ldap_client->userInView(user, viewName);
}

extern "C"
{
LDAPSECURITY_API ISecManager * newLdapSecManager(const char *serviceName, IPropertyTree &config)
{
    return new CLdapSecManager(serviceName, config);
}

LDAPSECURITY_API IAuthMap *newDefaultAuthMap(IPropertyTree* config)
{
    CAuthMap* authmap = new CAuthMap(NULL);

    IPropertyTreeIterator *loc_iter = NULL;
    loc_iter = config->getElements(".//Location");

    if (loc_iter != NULL)
    {
        IPropertyTree *location = NULL;
        loc_iter->first();
        while(loc_iter->isValid())
        {
            location = &loc_iter->query();
            if (location)
            {
                StringBuffer pathstr, rstr;
                location->getProp("@path", pathstr);
                authmap->add(pathstr.str(), NULL);
            }
            loc_iter->next();
        }
        loc_iter->Release();
        loc_iter = NULL;
    }

    return authmap;
}

}
