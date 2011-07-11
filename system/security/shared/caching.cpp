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
    //delete all user-specific caches
    //
    MapResPermissionsCache::const_iterator i;
    MapResPermissionsCache::const_iterator iEnd = m_resPermissionsMap.end(); 

    for (i = m_resPermissionsMap.begin(); i != iEnd; i++)
        delete (*i).second;

    MapUserCache::const_iterator ui;
    MapUserCache::const_iterator uiEnd = m_userCache.end(); 
    for (ui = m_userCache.begin(); ui != uiEnd; ui++)
        delete (*ui).second;
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

