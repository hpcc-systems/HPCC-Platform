/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "caching.hpp"
#include "jtime.hpp"

/**********************************************************
 *     CResPermissionsCache                               *
 *     (used by CPermissionsCache defined below)          *
 **********************************************************/

time_t getThreadCreateTime()
{
    time_t t;
    void* tslval = getThreadLocalVal();
    if(tslval == NULL)
        return 0;

    memcpy(&t, tslval, 4);
    return t;
}

CResPermissionsCache::~CResPermissionsCache()
{
    MapResAccess::const_iterator i;
    MapResAccess::const_iterator iEnd = m_resAccessMap.end(); 

    for (i = m_resAccessMap.begin(); i != iEnd; i++)
    {
        ISecResource* ptr = ((*i).second).second;
        if(ptr)
        {
            ptr->Release();
        }
    }   
}

int CResPermissionsCache::lookup( IArrayOf<ISecResource>& resources, bool* pFound )
{
    time_t tstamp;
    time(&tstamp);

    int timeout = m_pParentCache->getCacheTimeout();
    if(timeout == 0 && m_pParentCache->isTransactionalEnabled())
        timeout = 10; //Transactional timeout is set to 10 seconds for long transactions that might take over 10 seconds.
    tstamp -= timeout;
    if (m_tLastCleanup < tstamp)
        removeStaleEntries(tstamp);

    int nresources = resources.ordinality();
    int nFound = 0;

    for (int i = 0; i < nresources; i++)
    {
        ISecResource& secResource = resources.item(i);
        const char* resource = secResource.getName();
        if(resource == NULL)
        {
            *pFound++ = false;
            continue;
        }
        //DBGLOG("CACHE: Looking up %s:%s", m_user.c_str(), resource);

        MapResAccess::iterator it = m_resAccessMap.find(SecCacheKeyEntry(resource, secResource.getResourceType()));
        if (it != m_resAccessMap.end())//exists in cache
        {
            ResPermCacheEntry& resParamCacheEntry = (*it).second;
            const time_t timestamp = resParamCacheEntry.first;

            if (timestamp < tstamp)//entry was not stale during last cleanup but is stale now
                *pFound++ = false;
            else if(!m_pParentCache->isCacheEnabled() && m_pParentCache->isTransactionalEnabled())//m_pParentCache->getOriginalTimeout() == 0)
            {
                time_t tctime = getThreadCreateTime();
                if(tctime <= 0 || timestamp < tctime)
                {
                    *pFound++ = false;
                }
                else
                {
                    secResource.copy(resParamCacheEntry.second);
                    *pFound++ = true;
                    nFound++;
                }
            }
            else
            {
                secResource.copy(resParamCacheEntry.second);
                //DBGLOG("CACHE: Found %s:%s=>%d", m_user.c_str(), resource, resParamCacheEntry.second);
                *pFound++ = true;
                nFound++;
            }
        }
        else
            *pFound++ = false;
    }
    return nFound;
}

void CResPermissionsCache::add( IArrayOf<ISecResource>& resources )
{
    time_t tstamp;
    time(&tstamp);

    int nresources = resources.ordinality();
    for (int i = 0; i < nresources; i++)
    {
        ISecResource* secResource = &resources.item(i);
        if(!secResource)
            continue;
        const char* resource = secResource->getName();
        SecResourceType resourcetype = secResource->getResourceType();
        if(resource == NULL)
            continue;
        int permissions = secResource->getAccessFlags();
        if(permissions == -1)
            continue;

        MapResAccess::iterator it = m_resAccessMap.find(SecCacheKeyEntry(resource, resourcetype));
        if (it != m_resAccessMap.end())//already exists so overwrite it but first remove existing timestamp info
        {
            ResPermCacheEntry& resParamCacheEntry = (*it).second;
            time_t oldtstamp = resParamCacheEntry.first;


            //there may be multiple resources associated with the same timestamp 
            //in the multimap so find this entry
            //
            MapTimeStamp::iterator itL = m_timestampMap.lower_bound( oldtstamp );
            MapTimeStamp::iterator itU = m_timestampMap.upper_bound( oldtstamp );
            MapTimeStamp::iterator its;
            for ( its = itL; its != itU; its++)
            {
                SecCacheKeyEntry& cachekey = (*its).second;
                if (cachekey.first == resource && cachekey.second == resourcetype)
                {
                    m_timestampMap.erase(its);
                    break;
                }
            }
            m_resAccessMap.erase(SecCacheKeyEntry(resource, resourcetype));
        }
        //DBGLOG("CACHE: Adding %s:%s(%d)", m_user.c_str(), resource, permissions);
        m_resAccessMap.insert( pair<SecCacheKeyEntry, ResPermCacheEntry>(SecCacheKeyEntry(resource, resourcetype),  ResPermCacheEntry(tstamp, secResource->clone())));
        m_timestampMap.insert( pair<time_t, SecCacheKeyEntry>(tstamp, SecCacheKeyEntry(resource, resourcetype)));
    }
}

void CResPermissionsCache::removeStaleEntries(time_t tstamp)
{
    MapTimeStamp::iterator i; 
    MapTimeStamp::iterator itL    = m_timestampMap.lower_bound(tstamp);
    MapTimeStamp::iterator iBegin = m_timestampMap.begin();

    for (i = iBegin; i != itL; i++)
    {
        SecCacheKeyEntry& cachekey = (*i).second;
        MapResAccess::iterator it = m_resAccessMap.find(cachekey);
        if (it != m_resAccessMap.end())//exists in cache
        {
            ResPermCacheEntry& entry = (*it).second;
            if(entry.second)
                entry.second->Release();
        }
        m_resAccessMap.erase(cachekey);
    }

    m_timestampMap.erase(iBegin, itL);
    m_tLastCleanup = tstamp;
}

void CResPermissionsCache::remove(SecResourceType rtype, const char* resourcename)
{
    SecCacheKeyEntry key(resourcename, rtype);
    MapResAccess::iterator it = m_resAccessMap.find(key);
    if (it != m_resAccessMap.end())//exists in cache
    {
        ResPermCacheEntry& entry = (*it).second;
        if(entry.second)
            entry.second->Release();
    }
    m_resAccessMap.erase(key);
}

/**********************************************************
 *     CPermissionsCache                                  *
 **********************************************************/

CPermissionsCache::~CPermissionsCache()
{
    flush();
}

int CPermissionsCache::lookup( ISecUser& sec_user, IArrayOf<ISecResource>& resources, 
                            bool* pFound)
{
    synchronized block(m_cachemonitor);
    const char* userId = sec_user.getName();
    int nFound;
    MapResPermissionsCache::const_iterator i = m_resPermissionsMap.find( userId ); 

    if (i != m_resPermissionsMap.end())
    {
        CResPermissionsCache* pResPermissionsCache = (*i).second;
        nFound = pResPermissionsCache->lookup( resources, pFound );
    }
    else
    {
        nFound = 0;
        memset(pFound, 0, sizeof(bool)*resources.ordinality());
        //DBGLOG("CACHE: Looking up %s:*", userId);
    }

    return nFound;
}



void CPermissionsCache::add( ISecUser& sec_user, IArrayOf<ISecResource>& resources )
{
    synchronized block(m_cachemonitor);
    const char* user = sec_user.getName();
    MapResPermissionsCache::const_iterator i = m_resPermissionsMap.find( user ); 
    CResPermissionsCache* pResPermissionsCache;

    if (i == m_resPermissionsMap.end())
    {
        //DBGLOG("CACHE: Adding cache for %s", user);
        pResPermissionsCache = new CResPermissionsCache(this, user);
        m_resPermissionsMap.insert(pair<string, CResPermissionsCache*>(user, pResPermissionsCache));
    }
    else
        pResPermissionsCache = (*i).second;

    pResPermissionsCache->add( resources );
}

void CPermissionsCache::removePermissions( ISecUser& sec_user)
{
    synchronized block(m_cachemonitor);
    const char* user = sec_user.getName();
    if(user != NULL && *user != '\0')
    {
        m_resPermissionsMap.erase(user); 
    }
}

void CPermissionsCache::remove(SecResourceType rtype, const char* resourcename)
{
    synchronized block(m_cachemonitor);
    MapResPermissionsCache::const_iterator i;
    MapResPermissionsCache::const_iterator iEnd = m_resPermissionsMap.end(); 

    for (i = m_resPermissionsMap.begin(); i != iEnd; i++)
    {
        i->second->remove(rtype, resourcename);
    }
}


bool CPermissionsCache::lookup(ISecUser& sec_user)
{
    if(!isCacheEnabled())
        return false;

    const char* username = sec_user.getName();
    if(!username || !*username)
        return false;

    synchronized block(m_userCacheMonitor); 

    CachedUser* user = m_userCache[username];
    if(user == NULL)
        return false;       
    time_t now;
    time(&now);
    if(user->getTimestamp() < (now - m_cacheTimeout))
    {
        m_userCache.erase(username);
        delete user;
        return false;
    }

    const char* cachedpw = user->queryUser()->credentials().getPassword();
    StringBuffer pw(sec_user.credentials().getPassword());
    
    if(cachedpw && pw.length() > 0)
    {
        StringBuffer md5pbuf;
        md5_string(pw, md5pbuf);
        if(strcmp(cachedpw, md5pbuf.str()) == 0)
        {
            // Copy cached user to the sec_user structure, but still keep the original clear text password.
            user->queryUser()->copyTo(sec_user);
            sec_user.credentials().setPassword(pw.str());
            return true;
        }
        else
        {
            m_userCache.erase(username);
            delete user;
            return false;
        }
    }

    return false;
}
ISecUser* CPermissionsCache::getCachedUser( ISecUser& sec_user)
{
    if(!isCacheEnabled())
        return NULL;

    const char* username = sec_user.getName();
    if(!username || !*username)
        return NULL;

    synchronized block(m_userCacheMonitor); 
    CachedUser* user = m_userCache[username];
    if(user == NULL)
        return NULL;
    return LINK(user->queryUser());
}
void CPermissionsCache::add(ISecUser& sec_user)
{
    if(!isCacheEnabled() || &sec_user == NULL)
        return;
    
    const char* username = sec_user.getName();
    if(!username || !*username)
        return;
    
    synchronized block(m_userCacheMonitor);     
    CachedUser* user = m_userCache[username];
    if(user)
    {
        m_userCache.erase(username);
        delete user;
    }
    m_userCache[username] = new CachedUser(sec_user.clone());
}

void CPermissionsCache::removeFromUserCache(ISecUser& sec_user)
{
    const char* username = sec_user.getName();
    if(username && *username)
    {
        synchronized block(m_userCacheMonitor);
        CachedUser* user = m_userCache[username];
        if(user)
        {
            m_userCache.erase(username);
            delete user;
        }
    }
}

bool CPermissionsCache::addManagedFileScopes(IArrayOf<ISecResource>& scopes)
{
    synchronized block(m_managedFileScopesCacheMonitor);
    ForEachItemIn(x, scopes)
    {
        ISecResource* scope = &scopes.item(x);
        if(!scope)
            continue;
        const char* cachekey = scope->getName();
        if(cachekey == NULL)
            continue;
        map<string, ISecResource*>::iterator it = m_managedFileScopesMap.find(cachekey);
        if (it != m_managedFileScopesMap.end())
        {
            ISecResource *res = (*it).second;
            res->Release();
            m_managedFileScopesMap.erase(it);
        }
#ifdef _DEBUG
        DBGLOG("Caching Managed File Scope %s",cachekey);
#endif
        m_managedFileScopesMap.insert( pair<string, ISecResource*>(cachekey, LINK(scope)));
    }
    return true;
}

inline void CPermissionsCache::removeManagedFileScopes(IArrayOf<ISecResource>& scopes)
{
    synchronized block(m_managedFileScopesCacheMonitor);
    ForEachItemIn(x, scopes)
    {
        ISecResource* scope = &scopes.item(x);
        if(!scope)
            continue;
        const char* cachekey = scope->getName();
        if(cachekey == NULL)
            continue;
        map<string, ISecResource*>::iterator it = m_managedFileScopesMap.find(cachekey);
        if (it != m_managedFileScopesMap.end())
        {
            ISecResource *res = (*it).second;
            res->Release();
            m_managedFileScopesMap.erase(it);
        }
    }
}

inline void CPermissionsCache::removeAllManagedFileScopes()
{
    synchronized block(m_managedFileScopesCacheMonitor);
    map<string, ISecResource*>::const_iterator cit;
    map<string, ISecResource*>::const_iterator iEnd = m_managedFileScopesMap.end();

    for (cit = m_managedFileScopesMap.begin(); cit != iEnd; cit++)
    {
        ISecResource *res = (*cit).second;
        res->Release();
    }
    m_managedFileScopesMap.clear();
}

/*
    if perms set on 'scopeA::scopeB' only and lookup of 'scopeA::scopeB::scopeC::scopeD'
    need to lookup:
        'scopeA'
    no match=>continue
    match=>continue if read permissions (if no read, implies can't "see" child scopes)
        'scopeA::scopeB'
    no match=>continue
    match=>continue if read permissions (if no read, implies can't "see" child scopes)

    etc. Until full scope path checked, or no read permissions hit on ancestor scope.
*/
bool CPermissionsCache::queryPermsManagedFileScope(ISecUser& sec_user, const char * fullScope, StringBuffer& managedScope, int * accessFlags)
{
    if (!fullScope || !*fullScope)
    {
        *accessFlags = queryDefaultPermission(sec_user);
        return true;
    }

    time_t now;
    time(&now);
    if (m_secMgr && (0 == m_lastManagedFileScopesRefresh || m_lastManagedFileScopesRefresh < (now - m_cacheTimeout)))
    {
        removeAllManagedFileScopes();
        IArrayOf<ISecResource> scopes;
        aindex_t count = m_secMgr->getManagedFileScopes(scopes);
        if (count)
            addManagedFileScopes(scopes);
        m_defaultPermission = SecAccess_Unknown;//trigger refresh
        m_lastManagedFileScopesRefresh = now;
    }

    if (m_managedFileScopesMap.empty())
    {
        *accessFlags = queryDefaultPermission(sec_user);
        return true;
    }

    StringArray scopes;
    {
        StringBuffer scope;
        const char * p = fullScope;
        while (*p)
        {
            if (*p == ':')
            {
                if (*(p+1) != ':')
                    return false;//Malformed scope string, let LDAP figure it out
                scopes.append(scope.str());
                scope.append(*(p++));
            }
            scope.append(*(p++));
        }
        scopes.append(scope.str());
    }
    synchronized block(m_managedFileScopesCacheMonitor);
    ISecResource *matchedRes = NULL;
    ISecResource *res = NULL;
    bool isManaged = false;

    for(unsigned i = 0; i < scopes.length(); i++)
    {
        const char* scope = scopes.item(i);
        map<string, ISecResource*>::const_iterator it = m_managedFileScopesMap.find(scope);
        if (it != m_managedFileScopesMap.end())
        {
            isManaged = true;
            res = (*it).second;
            res->setResourceType(RT_FILE_SCOPE);
            LINK(res);
            IArrayOf<ISecResource> secResArr;
            secResArr.append(*res);
            bool found;
            int nFound = lookup(sec_user, secResArr, &found);
            if (nFound && found)
            {
                if (0 == (res->getAccessFlags() & SecAccess_Read))
                {
                    *accessFlags = res->getAccessFlags();
                    managedScope.append(const_cast<char *>(res->getName()));
#ifdef _DEBUG
                    DBGLOG("FileScope %s for %s(%s) access denied %d",fullScope, sec_user.getName(), res->getName(), *accessFlags);
#endif
                    return true;
                }
                else
                    matchedRes = res;//allowed at this scope, but must also look at child scopes
            }
        }
    }
    bool rc;
    if (isManaged)
    {
        if (matchedRes)
        {
            *accessFlags = matchedRes->getAccessFlags();
            managedScope.append(const_cast<char *>(matchedRes->getName()));
#ifdef _DEBUG
            DBGLOG("FileScope %s for %s(%s) access granted %d", fullScope, sec_user.getName(), matchedRes->getName(), *accessFlags);
#endif
            rc = true;
        }
        else
        {
            managedScope.append(const_cast<char *>(res->getName()));

#ifdef _DEBUG
            DBGLOG("FileScope %s for %s(%s) managed but not cached", fullScope, sec_user.getName(), res->getName());
#endif
            rc = false;//need to go to LDAP to check
        }
    }
    else
    {
        *accessFlags = queryDefaultPermission(sec_user);
#ifdef _DEBUG
        DBGLOG("FileScope %s for %s not managed, using default %d", fullScope, sec_user.getName(),*accessFlags);
#endif
        rc = true;
    }
    return rc;
}

int CPermissionsCache::queryDefaultPermission(ISecUser& user)
{
    if (m_defaultPermission == SecAccess_Unknown)
    {
        if (m_secMgr)
            m_defaultPermission = m_secMgr->queryDefaultPermission(user);
        else
            m_defaultPermission = SecAccess_None;
    }
    return m_defaultPermission;

}
void CPermissionsCache::flush()
{
    {
        synchronized block(m_cachemonitor);
        MapResPermissionsCache::const_iterator i;
        MapResPermissionsCache::const_iterator iEnd = m_resPermissionsMap.end();
        for (i = m_resPermissionsMap.begin(); i != iEnd; i++)
            delete (*i).second;
        m_resPermissionsMap.clear();
    }
    {
        synchronized block(m_userCacheMonitor);
        MapUserCache::const_iterator ui;
        MapUserCache::const_iterator uiEnd = m_userCache.end();
        for (ui = m_userCache.begin(); ui != uiEnd; ui++)
            delete (*ui).second;
        m_userCache.clear();
    }
    m_lastManagedFileScopesRefresh = 0;
    m_defaultPermission = SecAccess_Unknown;//trigger refresh
}
