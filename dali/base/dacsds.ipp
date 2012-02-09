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

#ifndef DACSDS_IPP
#define DACSDS_IPP

#include <typeinfo>
#include "jobserve.hpp"
#include "jhash.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"

#include "jptree.ipp"

#include "dasds.ipp"
  

class CSubscriberContainerBase : public CInterface, implements IInterface
{
    DECL_NAMEDCOUNT;
public:
    IMPLEMENT_IINTERFACE;

    CSubscriberContainerBase(ISubscription *_subscriber, SubscriptionId _id) : 
      subscriber(_subscriber), id(_id)
    {
        INIT_NAMEDCOUNT;
        unsubscribed = false;
    }

    bool notify(MemoryBuffer &mb)
    {
        try { 
            subscriber->notify(mb); 
            return true;
        }
        catch (IException *e)
        {
            LOG(MCuserWarning, e, "SDS: Error notifying subscriber");
            e->Release();
            
        }
        return false; // unsubscribe 
    }

    const SubscriptionId &queryId() const { return id; }
    const void *queryFindParam() const
    {
        return (const void *) &id;
    }

    bool isUnsubscribed() { return unsubscribed || subscriber->aborted(); }
    void setUnsubscribed() { unsubscribed = true; }

protected:
    Owned<ISubscription> subscriber;
    SubscriptionId id;
    bool unsubscribed;
};

/////////////////

class CClientSDSManager;
class CClientRemoteTree;
class CRemoteConnection : public CConnectionBase, public CTrackChanges, implements IRemoteConnection
{
    DECL_NAMEDCOUNT;
public:
    IMPLEMENT_IINTERFACE;

    void beforeDispose()
    {
        if (connected)
        {
            bool deleteRoot = false;
            manager.commit(*this, &deleteRoot);
        }
        root.clear();
    }

    CRemoteConnection(ISDSConnectionManager &manager, ConnectionId connectionId, const char *xpath, SessionId sessionId, unsigned mode, unsigned timeout);

    inline void setStateChanges(bool _stateChanges) { stateChanges = _stateChanges; }
    inline bool queryStateChanges() const { return stateChanges; }
    inline bool queryLazyFetch() const { return lazyFetch; }
    inline bool setLazyFetch(bool fetch) { bool ret = lazyFetch; lazyFetch = fetch; return ret; }
    inline ISDSConnectionManager &queryManager() { return manager; }
    inline bool queryConnected() { return connected; }
    inline void setConnected(bool _connected) { connected = _connected; }
    inline void setOrphaned() { orphaned = true; }
    inline bool queryServerIter() const { return serverIter; }
    inline bool queryServerIterAvailable() const { return serverIterAvailable; }
    inline bool queryServerGetIdsAvailable() const { return serverGetIdsAvailable; }
    inline bool queryUseAppendOpt() const { return useAppendOpt; }

    void getDetails(MemoryBuffer &mb);
    void clearCommitChanges();
    IPropertyTreeIterator *doGetElements(CClientRemoteTree *tree, const char *xpath, IPTIteratorCodes flags);
    void _rollbackChildren(IPropertyTree *parent, bool force);

// IRemoteConnection
    virtual IPropertyTree *getRoot() { return CConnectionBase::getRoot(); }
    virtual IPropertyTree *queryRoot() { return CConnectionBase::queryRoot(); }
    virtual SessionId querySessionId() const { return CConnectionBase::querySessionId(); }
    virtual unsigned queryMode() const { return CConnectionBase::queryMode(); }
    virtual void changeMode(unsigned mode, unsigned timeout, bool suppressReloads);
    virtual void rollback();
    virtual void rollbackChildren(const char *xpath=NULL, bool force=false);
    virtual void rollbackChildren(IPropertyTree *parent, bool force=false);
    virtual void reload(const char *xpath=NULL);
    virtual void commit();
    virtual void close(bool deleteRoot=false); // invalidates root.
    virtual SubscriptionId subscribe(ISDSConnectionSubscription &notify);
    virtual void unsubscribe(SubscriptionId id);
    virtual IPropertyTreeIterator *getElements(const char *xpath, IPTIteratorCodes flags = iptiter_null);

private:
    CriticalSection lockCrit;
    unsigned lockCount;
    unsigned hash;
    bool lazyFetch;
    bool stateChanges;      // =false when client applying server received changes
    bool connected, orphaned, serverIterAvailable, serverIter, useAppendOpt, serverGetIdsAvailable;

friend class CConnectionLock;
friend class CSetServerIterBlock;
};

class CSetServerIterBlock
{
    CRemoteConnection &conn;
    bool serverIter;
public:
    CSetServerIterBlock(CRemoteConnection &_conn, bool state) : conn(_conn) { serverIter=conn.serverIter; conn.serverIter = state; }
    ~CSetServerIterBlock() { conn.serverIter = serverIter; }
};

//////////////////

class CConnectionLock
{
    CRemoteConnection &conn;
public:
    CConnectionLock(CRemoteConnection &_conn) : conn(_conn) { conn.lockCrit.enter(); }
    ~CConnectionLock() { conn.lockCrit.leave(); }
};
//////////////////
class CSDSConnectionSubscriberProxy : public CInterface, implements ISubscription
{
    DECL_NAMEDCOUNT;
public:
    IMPLEMENT_IINTERFACE;

    CSDSConnectionSubscriberProxy(ISDSConnectionSubscription &_sdsNotify, ConnectionId connId) : sdsNotify(&_sdsNotify)
    {
        INIT_NAMEDCOUNT;
        id = queryCoven().getUniqueId();
        MemoryBuffer mb; mb.append(connId);
        data.set(mb.length(), mb.toByteArray());
    }
    SubscriptionId getId() const { return id; }

// ISubscription impl.
    virtual const MemoryAttr &queryData()
    {
        return data;
    }
    virtual void notify(MemoryBuffer &returnData)
    {
        sdsNotify->notify();
    }

    virtual void abort() // called when server closes
    { 
        // JCS TBD?
    }

    virtual bool aborted() // called when server closes
    { 
        return false;
    }

private:
    SubscriptionId id;
    MemoryAttr data;
    Linked<ISDSConnectionSubscription> sdsNotify;
};

//////////////////
class CSDSSubscriberProxy : public CInterface, implements ISubscription
{
    DECL_NAMEDCOUNT;
public:
    IMPLEMENT_IINTERFACE;

    CSDSSubscriberProxy(const char *xpath, bool sub, bool sendValue, ISDSSubscription &_sdsNotify) : sdsNotify(&_sdsNotify)
    {
        INIT_NAMEDCOUNT;
        bool quote=false, sep=false;
        const char *_xpath = xpath;
        const char *end = _xpath+strlen(_xpath);
        while (_xpath != end)
        {
            if ('\"' == *_xpath)
            {
                sep = false;
                if (quote) quote = false;
                else quote = true;
            }
            else if ('/' == *_xpath && !quote)
            {
                if (sep)
                    throw MakeStringException(0, "UNSUPPORTED: '//' syntax unsupported in subscriber xpath (path=\"%s\")", xpath); // JCSMORE - TBD?
                sep = true;
            }
            else
                sep = false;
            ++_xpath;
        }
        MemoryBuffer _data;
        _data.append(xpath).append(sub).append(sendValue);
        data.set(_data.length(), _data.toByteArray());
        id = queryCoven().getUniqueId();
    }
    SubscriptionId getId() const { return id; }

// ISubscription impl.
    virtual const MemoryAttr &queryData()
    {
        return data;
    }
    virtual void notify(MemoryBuffer &returnData)
    {
        StringAttr xpath;
        SDSNotifyFlags flags;
        returnData.read(xpath);
        returnData.read((int &) flags);
        bool valueData;
        if (returnData.length()-returnData.getPos()) // remaining
        {
            returnData.read(valueData);
            if (valueData)
            {
                unsigned l;
                returnData.read(l);
                sdsNotify->notify(id, xpath, flags, l, returnData.readDirect(l));
            }
            else
                sdsNotify->notify(id, xpath, flags);
        }
        else
            sdsNotify->notify(id, xpath, flags);
    }

    virtual void abort() // called when server closes
    { 
        // JCS TBD?
    }

    virtual bool aborted() // called when server closes
    { 
        return false;
    }

private:
    SubscriptionId id;
    MemoryAttr data, valueData;
    Linked<ISDSSubscription> sdsNotify;
};

////////////////////
class CClientRemoteTree : public CRemoteTreeBase, implements ITrackChanges
{
    DECL_NAMEDCOUNT;
    IPropertyTree *_queryBranch(const char *xpath);
    ChildMap *_checkChildren();

public:
    CClientRemoteTree(CRemoteConnection &conn, CPState _state=CPS_Unchanged);
    CClientRemoteTree(const char *name, IPTArrayValue *value, ChildMap *children, CRemoteConnection &conn, CPState _state=CPS_Unchanged);
    void beforeDispose();
    inline bool queryStateChanges() const;

    virtual void Link() const;
    virtual bool Release() const;

    virtual bool renameTree(IPropertyTree *tree, const char *newName);

    virtual bool isEquivalent(IPropertyTree *tree) { return (NULL != QUERYINTERFACE(tree, CClientRemoteTree)); }
    
    virtual void deserializeSelfRT(MemoryBuffer &mb);
    virtual void deserializeChildrenRT(MemoryBuffer &src);

    inline void addServerTreeInfo(byte STIInfo) { serverTreeInfo += STIInfo; }
    inline bool queryLazyFetch() const { return connection.queryLazyFetch(); }
    inline CRemoteConnection &queryConnection() { return connection; }
    inline ISDSConnectionManager &queryManager() const { return connection.queryManager(); }
    inline SessionId querySessionId() { return connection.querySessionId(); }
    inline unsigned queryMode() { return connection.queryMode(); }
    inline unsigned queryTimeout() { return connection.queryTimeout(); }
    void checkExt() const;

    virtual bool setLazyFetch(bool fetch) { return connection.setLazyFetch(fetch); }
    virtual ChildMap *checkChildren() const;
    virtual IPropertyTree *create(const char *name, IPTArrayValue *value=NULL, ChildMap *children=NULL, bool existing=false);
    virtual IPropertyTree *create(MemoryBuffer &mb);
    virtual void createChildMap();
    virtual IPropertyTree *ownPTree(IPropertyTree *tree);
    virtual void setLocal(size32_t size, const void *data, bool _binary);
    virtual void appendLocal(size32_t size, const void *data, bool binary);
    virtual void addingNewElement(IPropertyTree &child, int pos);
    virtual void removingElement(IPropertyTree *tree, unsigned pos);
    virtual void setAttr(const char *attr, const char *val);
    virtual bool removeAttr(const char *attr);

// IPropertyTree
    virtual void addProp(const char *xpath, const char *val);
    virtual void setProp(const char *xpath, const char *val);
    virtual void addPropInt64(const char *xpath, __int64 val);
    virtual void setPropInt64(const char *xpath, __int64 val);
    virtual void setPropBin(const char *xpath, size32_t size, const void *data);
    virtual IPropertyTree *setPropTree(const char *xpath, IPropertyTree *val);
    virtual IPropertyTree *addPropTree(const char *xpath, IPropertyTree *val);
    virtual bool removeProp(const char *xpath);
    virtual bool removeTree(IPropertyTree *child);
    virtual IPropertyTreeIterator *getElements(const char *xpath, IPTIteratorCodes flags = iptiter_null) const;
    virtual bool isCompressed(const char *xpath=NULL) const;
    virtual bool getProp(const char *xpath, StringBuffer &ret) const;
    virtual const char *queryProp(const char * xpath) const;
    virtual bool getPropBool(const char *xpath, bool dft=false) const;
    virtual __int64 getPropInt64(const char *xpath, __int64 dft=0) const;
    virtual bool getPropBin(const char *xpath, MemoryBuffer &ret) const;
    virtual void localizeElements(const char *xpath, bool allTail=false);
    virtual IPropertyTree *queryBranch(const char *xpath) const;
    virtual bool hasChildren() const { return (children && children->count()) || (!children && 0 != (serverTreeInfo & STI_HaveChildren)); }

// ITrackChanges
    virtual ChangeInfo *queryChanges();
    virtual void registerRenamed(const char *newName, const char *oldName, unsigned pos, __int64 id);
    virtual void registerDeleted(const char *name, unsigned position, __int64 id);
    virtual void registerDeletedAttr(const char *attr);
    virtual void clearChanges();

    virtual void registerAttrChange(const char *attr);
    virtual void registerPropAppend(size32_t l);

private: // data
    CRemoteConnection &connection;
    mutable byte serverTreeInfo;
};

//////////

typedef ThreadSafeSimpleHashTableOf<CConnectionBase, ConnectionId> CCopyConnectionHashTable;
class CClientSDSManager : public CSDSManagerBase, implements ISDSManager
{
public:
    IMPLEMENT_IINTERFACE;

    CClientSDSManager();
    ~CClientSDSManager();
    StringBuffer &getInfo(SdsDiagCommand cmd, StringBuffer &out);
    bool sendRequest(CMessageBuffer &mb, bool throttle=false);

// ISDSConnectionManager
    virtual CRemoteTreeBase *get(CRemoteConnection &connection, __int64 serverId);
    virtual void getChildren(CRemoteTreeBase &parent, CRemoteConnection &connection, unsigned levels);
    virtual void getChildrenFor(CRTArray &childLessList, CRemoteConnection &connection, unsigned levels);
    virtual void ensureLocal(CRemoteConnection &connection, CRemoteTreeBase &_parent, IPropertyTree *serverMatchTree, IPTIteratorCodes flags=iptiter_null);
    virtual IPropertyTreeIterator *getElements(CRemoteConnection &connection, const char *xpath);
    virtual void commit(CRemoteConnection &connection, bool *disconnectDeleteRoot);
    virtual void changeMode(CRemoteConnection &connection, unsigned mode, unsigned timeout, bool suppressReloads);
    virtual IPropertyTree *getXPaths(__int64 serverId, const char *xpath, bool getServerIds=false);
    virtual IPropertyTreeIterator *getXPathsSortLimit(const char *baseXPath, const char *matchXPath, const char *sortby, bool caseinsensitive, bool ascending, unsigned from, unsigned limit);
    virtual void getExternalValueFromServerId(__int64 serverId, MemoryBuffer &mb);

// ISDSManager
    virtual IRemoteConnections *connect(IMultipleConnector *mConnect, SessionId id, unsigned timeout);
    virtual IRemoteConnection *connect(const char *xpath, SessionId id, unsigned mode, unsigned timeout);
    virtual SubscriptionId subscribe(const char *xpath, ISDSSubscription &notify, bool sub=true, bool sendValue=false);
    virtual void unsubscribe(SubscriptionId id);
    virtual StringBuffer &getLocks(StringBuffer &out);
    virtual StringBuffer &getUsageStats(StringBuffer &out);
    virtual StringBuffer &getConnections(StringBuffer &out);
    virtual StringBuffer &getSubscribers(StringBuffer &out);
    virtual StringBuffer &getExternalReport(StringBuffer &out);
    virtual IPropertyTree &queryProperties() const;
    virtual IPropertyTreeIterator *getElementsRaw(const char *xpath, INode *remotedali, unsigned timeout);
    virtual void setConfigOpt(const char *opt, const char *value);
    virtual unsigned queryCount(const char *xpath);
    virtual bool updateEnvironment(IPropertyTree *newEnv, bool forceGroupUpdate, StringBuffer &response);

private:
    CriticalSection crit;
    CCopyConnectionHashTable connections;
    Semaphore concurrentRequests;
    mutable IPropertyTree *properties;
    bool childrenCanBeMissing; // for backward compat servers <= 2.0
    unsigned lazyExtFlag; // for backward compat servers <= 3.3
};

extern da_decl void closeSDS(); // client only

#endif
