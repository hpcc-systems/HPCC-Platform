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

#ifndef _CACHING_HPP__
#define _CACHING_HPP__
#pragma warning(disable:4786)
 
#include "jliball.hpp"
#include "seclib.hpp"
#undef new
#include <map>
#include <string>
#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

using std::pair;
using std::map;
using std::multimap;
using std::string;

//Define type of cache entry stored for each resource (in each user specific cache).
//This is a pair of timestamp when this was fetched and the permission itself.
//

typedef pair<time_t, ISecResource*> ResPermCacheEntry;
typedef pair<string, SecResourceType> SecCacheKeyEntry;
//this a cache for a given user that stores permissions for individual resources 
//along with their timestamps when they were fetched.  The cache is periodically 
//cleaned up to remove stale (older than 5 minutes) entries - triggered by a lookup
//itself.  Note that each user has an instance of this cache, which is stored in 
//another map (CPermissionsCache) as defined below.
//
//
// CPermissionsCache(user) -> CResPermissionsCache|(resource) -> pair<timeout, permission>
//                                                |(timestamp)->resource

class CResPermissionsCache
{
public:
    CResPermissionsCache(class CPermissionsCache* parentCache, const char* user)
        : m_pParentCache(parentCache), m_user(user)
    {
        time( &m_tLastCleanup );
    }

    virtual ~CResPermissionsCache();

    //finds cached permissions for a number of resources and sets them in
    //and also returns status in the boolean array passed in
    //
    virtual int  lookup( IArrayOf<ISecResource>& resources, bool* found );

    //fetch permissions from resources passed in and store them in the cache
    //
    virtual void add( IArrayOf<ISecResource>& resources );
    virtual void remove(SecResourceType rtype, const char* resourcename);

private:
    //removes entries older than tstamp passed in
    //
    virtual void removeStaleEntries(time_t tstamp);

    //type definitions
    //define mapping from resource name to pair<timeout, permission>
    //
    //typedef map<string,  ResPermCacheEntry> MapResAccess;
    typedef map<SecCacheKeyEntry,  ResPermCacheEntry> MapResAccess;

    //define mapping from timeout to resource name (used for cleanup)
    //
    typedef multimap<time_t, SecCacheKeyEntry>        MapTimeStamp;

    //attributes
    time_t       m_tLastCleanup; //last time the cache was cleaned up
    MapResAccess m_resAccessMap; //map of resource to pair<timeout, permission>
    MapTimeStamp m_timestampMap; //map of timeout to resource name
    string       m_user;
    class CPermissionsCache* m_pParentCache;
};

class CachedUser
{
private:
    Owned<ISecUser> m_user;
    time_t          m_timestamp;
public:
    CachedUser(ISecUser* user)
    {
        if(!user)
            throw MakeStringException(-1, "can't create CachedUser, NULL user pointer");

        m_user.setown(user);
        const char* pw = user->credentials().getPassword();
        if(pw && *pw)
        {
            StringBuffer pbuf(pw), md5pbuf;
            md5_string(pbuf, md5pbuf);
            user->credentials().setPassword(md5pbuf.str());
        }

        time(&m_timestamp);
    }

    time_t getTimestamp()
    {
        return m_timestamp;
    }

    ISecUser* queryUser()
    {
        return m_user.get();
    }

    void setTimeStamp(time_t timestamp)
    {
        m_timestamp = timestamp;
    }
};

// main cache that stores all user-specific caches (defined by CResPermissionsCache above)
//
class CPermissionsCache
{
public:
    CPermissionsCache()
    { 
        m_cacheTimeout = 300;
        m_transactionalEnabled = false;
        m_secMgr = NULL;
        m_lastManagedFileScopesRefresh = 0;
        m_defaultPermission = SecAccess_Unknown;
    }
    virtual ~CPermissionsCache();

    //finds cached permissions for a number of resources and sets them in
    //and also returns status in the boolean array passed in
    //
    virtual int lookup( ISecUser& sec_user, IArrayOf<ISecResource>& resources, bool* found );

    //fetch permissions from resources passed in and store them in the cache
    //
    virtual void add   ( ISecUser& sec_user, IArrayOf<ISecResource>& resources );
    virtual void removePermissions( ISecUser& sec_user);
    virtual void remove   (SecResourceType rtype, const char* resourcename);

    virtual bool lookup( ISecUser& sec_user);
    virtual ISecUser* getCachedUser( ISecUser& sec_user);

    virtual void add (ISecUser& sec_user);
    virtual void removeFromUserCache(ISecUser& sec_user);

    void  setCacheTimeout(int timeout) { m_cacheTimeout = timeout; }
    const int getCacheTimeout() { return m_cacheTimeout; }
    bool  isCacheEnabled() { return m_cacheTimeout > 0; }
    void setTransactionalEnabled(bool enable) { m_transactionalEnabled = enable; }
    bool isTransactionalEnabled() { return m_transactionalEnabled;}
    void flush();
    bool addManagedFileScopes(IArrayOf<ISecResource>& scopes);
    void removeManagedFileScopes(IArrayOf<ISecResource>& scopes);
    void removeAllManagedFileScopes();
    bool queryPermsManagedFileScope(ISecUser& sec_user, const char * fullScope, StringBuffer& managedScope, int * accessFlags);
    void setSecManager(ISecManager * secMgr) { m_secMgr = secMgr; }
    int  queryDefaultPermission(ISecUser& user);
private:

    typedef std::map<string, CResPermissionsCache*> MapResPermissionsCache;
    typedef std::map<string, CachedUser*> MapUserCache;

    CPermissionsCache(const CPermissionsCache&);

    MapResPermissionsCache m_resPermissionsMap;  //user specific resource permissions cache
    Monitor m_cachemonitor;                               //for thread safety
    int m_cacheTimeout; //cleanup cycle period
    bool m_transactionalEnabled;

    MapUserCache m_userCache;
    Monitor m_userCacheMonitor;


    //Managed File Scope support
    int                         m_defaultPermission;
    map<string, ISecResource*>  m_managedFileScopesMap;
    Monitor                     m_managedFileScopesCacheMonitor;
    ISecManager *               m_secMgr;
    time_t                      m_lastManagedFileScopesRefresh;
};

time_t getThreadCreateTime();

#endif
