/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#pragma warning (disable : 4786)
#pragma warning (disable : 4018)
#pragma warning (disable : 4146)
#pragma warning (disable : 4275)

#ifdef _WIN32
#define AXA_API __declspec(dllexport)
#endif
//#include "ctconnection.h"
#include "basesecurity.hpp"
#include "jmd5.hpp"

#define     cacheTimeout 30000
//#ifdef _WIN32

CBaseSecurityManager::CBaseSecurityManager(const char *serviceName, const char *config)
{
    Owned<IPropertyTree> cfg = createPTreeFromXMLString(config, ipt_caseInsensitive);
    if(cfg.get() == NULL)
        throw MakeStringException(-1, "createPTreeFromXMLString() failed for %s", config);
    init(serviceName,cfg);
    cfg->Release();
}

CBaseSecurityManager::CBaseSecurityManager(const char *serviceName, IPropertyTree *config)
{
    m_dbpasswordEncoding = SecPwEnc_unknown;
    init(serviceName,config);
}

void CBaseSecurityManager::init(const char *serviceName, IPropertyTree *config)
{
    if(config == NULL)
        return;
    
    m_config.set(config);

    m_permissionsCache.setCacheTimeout( 60 * config->getPropInt("@cacheTimeout", 5) );
    

    m_dbserver.appendf("%s",config->queryProp("@serverName"));
    m_dbuser.appendf("%s",config->queryProp("@systemUser"));
    if(config->hasProp("@ConnectionPoolSize"))
        m_poolsize = atoi(config->queryProp("@connectionPoolSize"));
    else
        m_poolsize = 2;

    StringBuffer encodedPass,encryptedPass;
    encodedPass.appendf("%s",config->queryProp("@systemPassword"));
    decrypt(m_dbpassword, encodedPass.str());

    m_dbpasswordEncoding = SecPwEnc_plain_text;

    StringBuffer strPasswordEncoding;
    const char* encodingType = config->queryProp("@encodePassword");
    if(encodingType && strcmp(encodingType,"MD5") == 0)
        m_dbpasswordEncoding=SecPwEnc_salt_md5;
    else if (encodingType && strcmp(encodingType,"Rijndael") == 0)
        m_dbpasswordEncoding=SecPwEnc_Rijndael;
    else if (encodingType && strcmp(encodingType,"Accurint MD5") == 0)
        m_dbpasswordEncoding = SecPwEnc_salt_accurint_md5;

    if(m_dbserver.length() == 0 || m_dbuser.length() == 0)
        throw MakeStringException(-1, "CBaseSecurityManager() - db server or user is missing");

    IPropertyTree* pNonRestrictedIPTree = config->queryBranch("SafeIPList");
    if(pNonRestrictedIPTree)
    {
        Owned<IPropertyTreeIterator> Itr = pNonRestrictedIPTree->getElements("ip");
        for(Itr->first();Itr->isValid();Itr->next())
        {
            IPropertyTree& tree = Itr->query();
            m_safeIPList[tree.queryProp("")]=true;
        }
    }

    m_enableIPRoaming = config->getPropBool("@enableIPRoaming");
    m_enableOTP = config->getPropBool("@enableOTP",false);
    m_passwordExpirationWarningDays = config->getPropInt(".//@passwordExpirationWarningDays", 10); //Default to 10 days
}

CBaseSecurityManager::~CBaseSecurityManager()
{
    MapStrToUsers::iterator pos;
    for(pos=m_userList.begin();pos!=m_userList.end();){
        pos->second->Release();
        pos++;
    }
    dbDisconnect();
}




//interface ISecManager : extends IInterface
ISecUser * CBaseSecurityManager::createUser(const char * user_name)
{
    return (new CSecureUser(user_name, NULL));
}

ISecResourceList * CBaseSecurityManager::createResourceList(const char * rlname)
{
    return (new CSecurityResourceList(rlname));
}

bool CBaseSecurityManager::subscribe(ISecAuthenticEvents & events)
{
    m_subscriber.set(&events);
    return true;
}

bool CBaseSecurityManager::unsubscribe(ISecAuthenticEvents & events)
{
    if (&events == m_subscriber.get())
    {
        m_subscriber.set(NULL);
    }
    return true;
}

bool CBaseSecurityManager::authorize(ISecUser & sec_user, ISecResourceList * Resources)
{   
    if(!sec_user.isAuthenticated())
    {
        bool bOk = ValidateUser(sec_user);
        if(bOk == false)
            return false;
    }
    return ValidateResources(sec_user,Resources);
}

bool CBaseSecurityManager::updateSettings(ISecUser &sec_user, ISecPropertyList* resources) 
{
    CSecurityResourceList * reslist = (CSecurityResourceList*)resources;
    if(!reslist)
        return true;
    IArrayOf<ISecResource>& rlist = reslist->getResourceList();
    int nResources = rlist.length();
    if (nResources <= 0)
        return true;

    bool rc = false;
    if (m_permissionsCache.isCacheEnabled()==false)
        return updateSettings(sec_user, rlist);

    bool* cached_found = (bool*)alloca(nResources*sizeof(bool));
    int nFound = m_permissionsCache.lookup(sec_user, rlist, cached_found);
    if (nFound >= nResources)
        return true;
    
    IArrayOf<ISecResource> rlist2;
    for (int i=0; i < nResources; i++)
    {
        if (*(cached_found+i) == false)
        {
            ISecResource& secRes = rlist.item(i);
            secRes.Link();
            rlist2.append(secRes);
        }
    }
    rc = updateSettings(sec_user, rlist2);
    if (rc)
        m_permissionsCache.add(sec_user, rlist2);
    return rc;
}

bool CBaseSecurityManager::updateSettings(ISecUser & sec_user,IArrayOf<ISecResource>& rlist) 
{
    CSecureUser* user = (CSecureUser*)&sec_user;
    if(user == NULL)
        return false;

    int usernum = findUser(user->getName(),user->getRealm());
    if(usernum < 0)
    {
        PrintLog("User number of %s can't be found", user->getName());
        return false;
    }
    bool sqchecked = false, sqverified = false, otpchecked = false;
    int otpok = -1;
    ForEachItemIn(x, rlist)
    {
        ISecResource* secRes = (ISecResource*)(&(rlist.item(x)));
        if(secRes == NULL)
            continue;
        //AccessFlags default value is -1. Set it to 0 so that the settings can be cached. AccessFlags is not being used for settings.
        secRes->setAccessFlags(0);
        if(secRes->getParameter("userprop") && *secRes->getParameter("userprop")!='\0')
        {
            //if we have a parameter in the user or company table it will have been added as a parameter to the ISecUser when 
            // the authentication query was run. We should keep this messiness here so that the the end user is insulated....
            dbValidateSetting(*secRes,sec_user);
            continue;
        }

        const char* resource_name = secRes->getParameter("resource");
        if(resource_name && *resource_name && 
            (stricmp(resource_name, "SSN Masking") == 0 || stricmp(resource_name, "Driver License Masking") == 0))
        {
            //If OTP Enabled and OTP2FACTOR cookie not valid, mask
            if(m_enableOTP)
            {
                if(!otpchecked)
                {
                    const char* otpcookie = sec_user.getProperty("OTP2FACTOR");
                    // -1 means OTP is not enabled for the user. 0: failed verfication, 1: passed verification.
                    otpok = validateOTP(&sec_user, otpcookie);
                    otpchecked = true;
                }
                if(otpok == 0)
                {
                    CSecurityResource* cres = dynamic_cast<CSecurityResource*>(secRes);
                    if(resource_name && *resource_name && cres)
                    {
                        if(stricmp(resource_name, "SSN Masking") == 0)
                        {
                            cres->setValue("All");
                            continue;
                        }
                        else if(stricmp(resource_name, "Driver License Masking") == 0)
                        {
                            cres->setValue("1");
                            continue;
                        }
                    }
                }
                else if(otpok == 1)
                {
                    CSecurityResource* cres = dynamic_cast<CSecurityResource*>(secRes);
                    if(resource_name && *resource_name && cres)
                    {
                        if(stricmp(resource_name, "SSN Masking") == 0)
                        {
                            cres->setValue("None");
                            continue;
                        }
                        else if(stricmp(resource_name, "Driver License Masking") == 0)
                        {
                            cres->setValue("0");
                            continue;
                        }
                    }
                }
            }

            if(m_enableIPRoaming && sec_user.getPropertyInt("IPRoaming") == 1)
            {
                if(!sqchecked)
                {
                    const char* sequest = sec_user.getProperty("SEQUEST");
                    if(sequest && *sequest)
                    {
                        sqverified = validateSecurityQuestion(&sec_user, sequest);
                    }
                    sqchecked = true;
                }
                if(!sqverified)
                {
                    CSecurityResource* cres = dynamic_cast<CSecurityResource*>(secRes);
                    if(resource_name && *resource_name && cres)
                    {
                        if(stricmp(resource_name, "SSN Masking") == 0)
                        {
                            cres->setValue("All");
                            continue;
                        }
                        else if(stricmp(resource_name, "Driver License Masking") == 0)
                        {
                            cres->setValue("1");
                            continue;
                        }
                    }
                }
            }

        }

        dbValidateSetting(*secRes,usernum,user->getRealm());
    }
    return true;
}


bool CBaseSecurityManager::ValidateResources(ISecUser & sec_user, ISecResourceList * resources)
{
    CSecurityResourceList * reslist = (CSecurityResourceList*)resources;
    if(!reslist)
        return true;
    IArrayOf<ISecResource>& rlist = reslist->getResourceList();
    int nResources = rlist.length();
    if (nResources <= 0)
        return true;

    bool rc = false;
    if (m_permissionsCache.isCacheEnabled()==false)
        return ValidateResources(sec_user, rlist);

    bool* cached_found = (bool*)alloca(nResources*sizeof(bool));
    int nFound = m_permissionsCache.lookup(sec_user, rlist, cached_found);
    if (nFound >= nResources)
    {
        return true;
    }

    IArrayOf<ISecResource> rlist2;
    for (int i=0; i < nResources; i++)
    {
        if (*(cached_found+i) == false)
        {
            ISecResource& secRes = rlist.item(i);
            secRes.Link();
            rlist2.append(secRes);
        }
    }
    rc = ValidateResources(sec_user, rlist2);
    if (rc)
    {
        IArrayOf<ISecResource> rlistValid;
        for (int i=0; i < rlist2.ordinality(); i++)
        {
            ISecResource& secRes = rlist2.item(i);
            if(secRes.getAccessFlags() >= secRes.getRequiredAccessFlags() || secRes.getAccessFlags() == SecAccess_Unknown)
            {
                secRes.Link();
                rlistValid.append(secRes);
            }
        }
        m_permissionsCache.add(sec_user, rlistValid);
    }
        
    return rc;
}

static bool stringDiff(const char* str1, const char* str2)
{
    if(!str1 || !*str1)
    {
        if(!str2 || !*str2)
            return false;
        else
            return true;
    }
    else
    {
        if(!str2 || !*str2)
            return true;
        else
            return (strcmp(str1, str2) != 0);
    }
}

bool CBaseSecurityManager::ValidateUser(ISecUser & sec_user)
{
    StringBuffer clientip(sec_user.getPeer());
    StringBuffer otpbuf, sqbuf;
    if(m_enableOTP)
    {
        otpbuf.append(sec_user.getProperty("OTP2FACTOR"));
    }
    if(m_enableIPRoaming)
    {
        sqbuf.append(sec_user.getProperty("SEQUEST"));
    }
    if(m_permissionsCache.isCacheEnabled() && m_permissionsCache.lookup(sec_user))
    {
        bool bReturn = true;
        if(IsIPRestricted(sec_user))
        {
            const char* cachedclientip = sec_user.getPeer();
            if(clientip.length() > 0 && cachedclientip && strncmp(clientip.str(), cachedclientip , clientip.length()) != 0)
            {
                //we seem to be coming from a different peer... this is not good
                WARNLOG("Found user %d in cache, but have to re-validate IP, because it was coming from %s but is now coming from %s",sec_user.getUserID(), cachedclientip, clientip.str());
                sec_user.setAuthenticated(false);
                sec_user.setPeer(clientip.str());
                m_permissionsCache.removeFromUserCache(sec_user);
                bReturn =  false;
            }
        }

        if(m_enableOTP)
        {
            const char* old_otp = sec_user.getProperty("OTP2FACTOR");
            if(stringDiff(old_otp, otpbuf.str()))
                bReturn = false;
        }
        if(m_enableIPRoaming)
        {
            const char* old_sq = sec_user.getProperty("SEQUEST");
            if(stringDiff(old_sq, sqbuf.str()))
                bReturn = false;
        }

        if(bReturn)
        {
            sec_user.setAuthenticated(true);
            return true;
        }
    }

    if(!IsPasswordValid(sec_user))
    {
        ERRLOG("Password validation failed for user: %s",sec_user.getName());
        return false;
    }
    else
    {
        if(IsIPRestricted(sec_user)==true)
        {
            if(ValidateSourceIP(sec_user,m_safeIPList)==false)
            {
                ERRLOG("IP check failed for user:%s coming from %s",sec_user.getName(),sec_user.getPeer());
                sec_user.setAuthenticated(false);
                return false;
            }
        }
        if(m_permissionsCache.isCacheEnabled())
            m_permissionsCache.add(sec_user);
        sec_user.setAuthenticated(true);
    }
    return true;
}

bool CBaseSecurityManager::IsPasswordValid(ISecUser& sec_user)
{
    StringBuffer password(sec_user.credentials().getPassword());
    EncodePassword(password);
    StringBuffer SQLQuery;
    buildAuthenticateQuery(sec_user.getName(),password.str(),sec_user.getRealm(),SQLQuery);
    return dbauthenticate(sec_user , SQLQuery);
}

bool CBaseSecurityManager::IsIPRestricted(ISecUser& sec_user)
{
    const char* iprestricted = sec_user.getProperty("iprestricted");
    if(iprestricted!=NULL && strncmp(iprestricted,"1",1)==0)
        return true;
    return false;
}


void CBaseSecurityManager::EncodePassword(StringBuffer& password)
{
    StringBuffer encodedPassword;
    switch (m_dbpasswordEncoding)
    {
        case SecPwEnc_salt_md5:
            md5_string(password,encodedPassword);
            password.clear().append(encodedPassword.str());
            break;
        case SecPwEnc_Rijndael:
            encrypt(encodedPassword,password.str());
            password.clear().append(encodedPassword.str());
            break;
        case SecPwEnc_salt_accurint_md5:
            password.toUpperCase();
            md5_string(password,encodedPassword);
            password.clear().append(encodedPassword.str());
            break;
    }
}



bool CBaseSecurityManager::addResources(ISecUser & sec_user, ISecResourceList * Resources)
{
    return false;
}
bool CBaseSecurityManager::addUser(ISecUser & user)
{
    return false;
}

int CBaseSecurityManager::getUserID(ISecUser& user)
{
    return findUser(user.getName(),user.getRealm());
}

bool CBaseSecurityManager::ValidateResources(ISecUser & sec_user,IArrayOf<ISecResource>& rlist)
{
    CSecureUser* user = (CSecureUser*)&sec_user;
    if(user == NULL)
        return false;

    int usernum = findUser(user->getName(),user->getRealm());
    if(usernum < 0)
    {
        PrintLog("User number of %s can't be found", user->getName());
        return false;
    }

    ForEachItemIn(x, rlist)
    {
        ISecResource* res = (ISecResource*)(&(rlist.item(x)));
        if(res == NULL)
            continue;
        dbValidateResource(*res,usernum,user->getRealm());
    }

    return true;
}

bool CBaseSecurityManager::updateResources(ISecUser & user, ISecResourceList * resources)
{   
    //("CBaseSecurityManager::updateResources");
    if(!resources)
        return false;

    const char* username = user.getName();
    //const char* realm = user.getRealm();
    const char* realm = NULL;

    int usernum = findUser(username,realm);
    if(usernum <= 0)
    {
        PrintLog("User number of %s can't be found", username);
        return false;
    }
    CSecurityResourceList * reslist = (CSecurityResourceList*)resources;
    if (reslist)
    {
        IArrayOf<ISecResource>& rlist = reslist->getResourceList();
        ForEachItemIn(x, rlist)
        {
            ISecResource* res = (ISecResource*)(&(rlist.item(x)));
            if(res == NULL)
                continue;
            dbUpdateResource(*res,usernum,realm);
        }
    }
    return true;
}

bool CBaseSecurityManager::updateUser(ISecUser& user, const char* newPassword)
{
    //("CBaseSecurityManager::updateUser");
    if(!newPassword)
        return false;
    StringBuffer password(newPassword);
    EncodePassword(password);
    const char* realm = NULL;
    bool bReturn =  dbUpdatePasswrd(user.getName(),realm,password.str());
    if(bReturn == true)
        user.credentials().setPassword(password.str());
    //need to flush the users info from the cache....
    if(m_permissionsCache.isCacheEnabled())
        m_permissionsCache.removeFromUserCache(user);
    return bReturn;
}


void CBaseSecurityManager::logon_failed(const char* user, const char* msg) 
{
    PrintLog("%s: %s", user, msg);
}

int CBaseSecurityManager::findUser(const char* user,const char* realm)
{
    if(user == NULL)
        return -1;
    synchronized block(m_usermap_mutex);
    int* uidptr = m_usermap.getValue(user);
    if(uidptr != NULL)
    {
        return *uidptr;
    }
    else
    {
        int uid = dbLookupUser(user,realm);
        if(uid >= 0)
        {
            m_usermap.setValue(user, uid);
        }
        return uid;
    }
}





//#endif //_WIN32
