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

#define da_decl __declspec(dllexport)
#include "platform.h"
#include "jhash.hpp"
#include "jlib.hpp"
#include "jfile.hpp"
#include "jthread.hpp"
#include "javahash.hpp"
#include "javahash.tpp"
#include "jmisc.hpp"
#include "jlog.hpp"
#include "mplog.hpp"
#include "jptree.ipp"
#include "jqueue.tpp"
#include "dautils.hpp"
#include "dadfs.hpp"

#define DEBUG_DIR "debug"
#define DEFAULT_KEEP_LASTN_STORES 1
#define MAXDELAYS 5
static const char *deltaHeader = "<CRC>0000000000</CRC><SIZE>0000000000000000</SIZE>"; // fill in later
static unsigned deltaHeaderCrcOff = 5;
static unsigned deltaHeaderSizeStart = 21;
static unsigned deltaHeaderSizeOff = 27;

static unsigned readWriteSlowTracing = 10000; // 10s default
static bool readWriteStackTracing = false;
static unsigned fakeCritTimeout = 60000;
static unsigned readWriteTimeout = 60000;

// #define NODELETE
// #define DISABLE_COALESCE_QUIETTIME


#define LEGACY_CLIENT_RESPONSE

#define ENABLE_INSPOS

#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "dacoven.hpp"
#include "daserver.hpp"
#include "daclient.hpp"

#include "dacsds.ipp"
#include "dasds.ipp"

#define ALWAYSLAZY_NOTUSED
#define NoMoreChildrenMarker ((__int64)-1)
#define DEFAULT_MAXCLOSEDOWNRETRIES 20 // really do not want to give up.
#define DEFAULT_MAXRETRIES 3
#define DEFAULT_RETRYDELAY (2) // (seconds)
#define DELTANAME "daliinc"
#define DELTADETACHED "dalidet"
#define DELTAINPROGRESS "delta.progress"
#define DETACHINPROGRESS "detach.progress"
#define TMPSAVENAME "dali_store_tmp.xml"
#define INIT_NODETABLE_SIZE 0x380000
// #define SUBLOCKS

#define DEFAULT_LCIDLE_PERIOD (60*10)  // time has to be quiet for before blocking/saving (when using @lightweightCoalesce)
#define DEFAULT_LCMIN_TIME (24*60*60)  // don't save more than once a 'DEFAULT_LCMIN_TIME' period.
#define DEFAULT_LCIDLE_RATE 1          // 1 write transactions per idle period. <= this rate is deemed idle (suitable for save)
#define STORENOTSAVE_WARNING_PERIOD 72 // hours

static unsigned msgCount=(unsigned)-1;

// #define TEST_NOTIFY_HANDLER

#define TRACE_QWAITING

#define CRC_VALIDATION

#define SUBNTFY_POOL_SIZE 400
#define SUBSCAN_POOL_SIZE 100
#define RTM_INTERNAL        0x80000000 // marker for internal connection (performed within a transaction)
#define DEFAULT_EXTERNAL_SIZE_THRESHOLD (10*1024)

#define NOTIFY_ATTR "@sds:notify"
#define FETCH_ENTIRE      -1
#define FETCH_ENTIRE_COND -2

#define TIMEOUT_ON_CLOSEDOWN 120000 // On closedown, give up on trying to join a thread in CSDSTransactionServer after two minutes

#define _POOLED_SERVER_REMOTE_TREE  // use a pool for CServerRemoteTree allocations


#ifdef _POOLED_SERVER_REMOTE_TREE
static CFixedSizeAllocator *CServerRemoteTree_Allocator;
#endif

enum notifications { notify_delete=1 };
static const char *notificationStr(notifications n)
{
    switch (n)
    { 
        case notify_delete: return "Notify Delete";
        default: return "UNKNOWN NOTIFY TYPE";
    }
}

const char *queryNotifyHandlerName(IPropertyTree *tree)
{
    return tree->queryProp(NOTIFY_ATTR);
}

bool setNotifyHandlerName(const char *handlerName, IPropertyTree *tree)
{
#ifdef _DEBUG
    CClientRemoteTree *_tree = QUERYINTERFACE(tree, CClientRemoteTree);
    if (!_tree) return false; // has to a SDS tree!
#endif
    tree->setProp(NOTIFY_ATTR, handlerName);
    return true;
}

StringBuffer &getSdsCmdText(SdsCommand cmd, StringBuffer &ret)
{
    switch (cmd)
    {
        case DAMP_SDSCMD_CONNECT:
            return ret.append("DAMP_SDSCMD_CONNECT");
        case DAMP_SDSCMD_GET:
            return ret.append("DAMP_SDSCMD_GET");
        case DAMP_SDSCMD_GETCHILDREN:
            return ret.append("DAMP_SDSCMD_GETCHILDREN");
        case DAMP_SDSCMD_REVISIONS:
            return ret.append("DAMP_SDSCMD_REVISIONS");
        case DAMP_SDSCMD_DATA:
            return ret.append("DAMP_SDSCMD_DATA");
        case DAMP_SDSCMD_DISCONNECT:
            return ret.append("DAMP_SDSCMD_DISCONNECT");
        case DAMP_SDSCMD_CONNECTSERVER:
            return ret.append("DAMP_SDSCMD_CONNECTSERVER");
        case DAMP_SDSCMD_DATASERVER:
            return ret.append("DAMP_SDSCMD_DATASERVER");
        case DAMP_SDSCMD_DISCONNECTSERVER:
            return ret.append("DAMP_SDSCMD_DISCONNECTSERVER");
        case DAMP_SDSCMD_CHANGEMODE:
            return ret.append("DAMP_SDSCMD_CHANGEMODE");
        case DAMP_SDSCMD_CHANGEMODESERVER:
            return ret.append("DAMP_SDSCMD_CHANGEMODESERVER");
        case DAMP_SDSCMD_EDITION:
            return ret.append("DAMP_SDSCMD_EDITION");
        case DAMP_SDSCMD_GETSTORE:
            return ret.append("DAMP_SDSCMD_GETSTORE");
        case DAMP_SDSCMD_VERSION:
            return ret.append("DAMP_SDSCMD_VERSION");
        case DAMP_SDSCMD_DIAGNOSTIC:
            return ret.append("DAMP_SDSCMD_DIAGNOSTIC");
        case DAMP_SDSCMD_GETELEMENTS:
            return ret.append("DAMP_SDSCMD_GETELEMENTS");
        case DAMP_SDSCMD_MCONNECT:
            return ret.append("DAMP_SDSCMD_MCONNECT");
        case DAMP_SDSCMD_GETCHILDREN2:
            return ret.append("DAMP_SDSCMD_GETCHILDREN2");
        case DAMP_SDSCMD_GET2:
            return ret.append("DAMP_SDSCMD_GET2");
        case DAMP_SDSCMD_GETPROPS:
            return ret.append("DAMP_SDSCMD_GETPROPS");
        case DAMP_SDSCMD_GETXPATHS:
            return ret.append("DAMP_SDSCMD_GETXPATHS");
        case DAMP_SDSCMD_GETEXTVALUE:
            return ret.append("DAMP_SDSCMD_GETEXTVALUE");
        case DAMP_SDSCMD_GETXPATHSPLUSIDS:
            return ret.append("DAMP_SDSCMD_GETXPATHSPLUSIDS");
        case DAMP_SDSCMD_GETXPATHSCRITERIA:
            return ret.append("DAMP_SDSCMD_GETXPATHSCRITERIA");
        case DAMP_SDSCMD_GETELEMENTSRAW:
            return ret.append("DAMP_SDSCMD_GETELEMENTSRAW");
        case DAMP_SDSCMD_GETCOUNT:
            return ret.append("DAMP_SDSCMD_GETCOUNT");
        default:
            return ret.append("UNKNOWN");
    };
    return ret;
}

#ifdef USECHECKEDCRITICALSECTIONS
class LinkingCriticalBlock : public CheckedCriticalBlock, public CInterface
{
public:
    LinkingCriticalBlock(CheckedCriticalSection &crit, const char *file, unsigned line) : CheckedCriticalBlock(crit, fakeCritTimeout, file, line) { }
};
class CLCLockBlock : public CInterface
{
    ReadWriteLock &lock;
    bool readLocked; // false == writeLocked
    unsigned got, lnum;
public:
    CLCLockBlock(ReadWriteLock &_lock, bool readLock, unsigned timeout, const char *fname, unsigned _lnum) : lock(_lock), lnum(_lnum)
    {
        got = msTick();
        loop
        {
            if (readLock)
            {
                if (lock.lockRead(timeout))
                    break;
            }
            else
            {
                if (lock.lockWrite(timeout))
                    break;
            }
            PROGLOG("CLCLockBlock(write=%d) timeout %s(%d), took %d ms",!readLocked,fname,lnum,got-msTick());
            PrintStackReport();
        }
        got = msTick();
        readLocked = readLock; // false == writeLocked
    };
    ~CLCLockBlock()
    {
        if (readLocked)
            lock.unlockRead();
        else
            lock.unlockWrite();
        unsigned e=msTick()-got;
        if (e>readWriteSlowTracing)
        {
            StringBuffer s("TIME: CLCLockBlock(write=");
            s.append(!readLocked).append(",lnum=").append(lnum).append(") took ").append(e).append(" ms");
            DBGLOG("%s", s.str());
            if (readWriteStackTracing)
                PrintStackReport();
        }
    }
};
#else
class LinkingCriticalBlock : public CriticalBlock, public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    LinkingCriticalBlock(CriticalSection &crit, const char *file, unsigned line) : CriticalBlock(crit) { }
};
class CLCLockBlock : public CInterface
{
    ReadWriteLock &lock;
    bool readLocked; // false == writeLocked
public:
    CLCLockBlock(ReadWriteLock &_lock, bool readLock, unsigned timeout, const char *fname, unsigned lnum) : lock(_lock)
    {
        if (readLock)
            lock.lockRead();
        else
            lock.lockWrite();
        readLocked = readLock; // false == writeLocked
    };
    ~CLCLockBlock()
    {
        if (readLocked)
            lock.unlockRead();
        else
            lock.unlockWrite();
    }
};
#endif
class CLCReadLockBlock : public CLCLockBlock
{
public:
    CLCReadLockBlock(ReadWriteLock &lock, unsigned timeout, const char *fname, unsigned lnum) : CLCLockBlock(lock, true, timeout, fname, lnum) { }
};
class CLCWriteLockBlock : public CLCLockBlock
{
public:
    CLCWriteLockBlock(ReadWriteLock &lock, unsigned timeout, const char *fname, unsigned lnum) : CLCLockBlock(lock, false, timeout, fname, lnum) { }
};
#ifdef USECHECKEDCRITICALSECTIONS
#define CHECKEDDALIREADLOCKBLOCK(l,timeout)  Owned<CLCReadLockBlock> glue(block,__LINE__) = new CLCReadLockBlock(l,timeout,__FILE__,__LINE__)
#define CHECKEDDALIWRITELOCKBLOCK(l,timeout)  Owned<CLCWriteLockBlock> glue(block,__LINE__) = new CLCWriteLockBlock(l,timeout,__FILE__,__LINE__)
#else
#define CHECKEDDALIREADLOCKBLOCK(l,timeout)   ReadLockBlock glue(block,__LINE__)(l)
#define CHECKEDDALIWRITELOCKBLOCK(l,timeout)  WriteLockBlock glue(block,__LINE__)(l)
#endif

#define OVERFLOWSIZE 50000
class CFitArray
{
    unsigned size;
    CRemoteTreeBase **ptrs; // offset 0 not used
    CriticalSection crit;
    unsigned nextId, chk;
    unsigned freeChainHead;

    inline void _ensure(unsigned _size)
    {
        if (size<_size) {
            ptrs = (CRemoteTreeBase **)checked_realloc(ptrs, _size * sizeof(CRemoteTreeBase *), size * sizeof(CRemoteTreeBase *), -100);
            memset(&ptrs[size], 0, (_size-size)*sizeof(CRemoteTreeBase *));
            size = _size;
        }
    }
    unsigned getId()
    {
        if (freeChainHead)
        {
            unsigned nF = freeChainHead;
            freeChainHead = (CRemoteTreeBase **)ptrs[nF]-ptrs;
            return nF;
        }
        if (++nextId >= size)
            _ensure(nextId+OVERFLOWSIZE);
        return nextId;
    }
    CRemoteTreeBase *_queryElem(__int64 id)
    {
        unsigned i = (unsigned) id;
        if (i>=size)
            return NULL;
        CRemoteTreeBase *ret = ptrs[i];
        if (!ret)
            return NULL;
        if ((memsize_t)ret >= (memsize_t)&ptrs[0] && (memsize_t)ret < (memsize_t)&ptrs[size])
            return NULL; // a ptr within the table is part of the free chain
        if (id != ret->queryServerId()) // then obj at index is not the same object.
            return NULL;
        return ret;
    }
public:
    CFitArray()                  : ptrs(NULL), chk(0) { reset(); }
    CFitArray(unsigned initSize) : ptrs(NULL), chk(0) { reset(); ensure(initSize); }
    ~CFitArray()  { free(ptrs); }
    void reset()
    {
        if (ptrs) 
            free(ptrs);
        size = 0;
        ptrs = NULL;
        nextId = 1;
        freeChainHead = 0;
    }
    void ensure(unsigned size)
    {
        CriticalBlock b(crit);
        _ensure(size);
    }
    void addElem(CRemoteTreeBase *member)
    {
        CriticalBlock b(crit);
        __int64 id = getId();
        ptrs[id] = member;
        if (++chk==0x80000000)
            chk = 0;
        id |= ((__int64)chk << 32);
        member->setServerId(id);
    }
    void freeElem(__int64 id)
    {
        CriticalBlock b(crit);
        unsigned i = (unsigned) id;
        assertex(i<size);
        ptrs[i] = (CRemoteTreeBase *)&ptrs[freeChainHead];
        freeChainHead = i;
    }
    CRemoteTreeBase *queryElem(__int64 id)
    {
        CriticalBlock b(crit);
        return _queryElem(id);
    }
    CRemoteTreeBase *getElem(__int64 id)
    {
        CriticalBlock b(crit);
        return LINK(_queryElem(id));
    }
    unsigned maxElements() const { return nextId; } // actual is nextId - however many in free chain
};

////////////////

enum IncInfo { IINull=0x00, IncData=0x01, IncDetails=0x02, IncConnect=0x04, IncDisconnect=0x08, IncDisconnectDelete=0x24 };

StringBuffer &constructStoreName(const char *storeBase, unsigned e, StringBuffer &res)
{
    res.append(storeBase);
    if (e)
        res.append(e);
    res.append(".xml");
    return res;
}

////////////////
static CheckedCriticalSection loadStoreCrit, saveStoreCrit, saveIncCrit, nfyTableCrit, qntfyListCrit, extCrit, blockedSaveCrit;
class CCovenSDSManager;
static CCovenSDSManager *SDSManager;


static StringAttr remoteBackupLocation;

/////////////////

class TimingStats
{
public:
    TimingStats() : count(0), totalTime(0), maxTime(0), minTime((unsigned long)-1), totalSize(0) {}
    inline void record(unsigned long interval)
    {
        count++;
        totalTime += interval;
        if(interval>maxTime) maxTime = interval;
        if(interval<minTime) minTime = interval;
    }
    inline void recordSize(unsigned long size)
    {
        totalSize += size;
    }
    unsigned long queryCount() const { return count; }
    unsigned long queryMeanTime() const { return count ? (unsigned long)(((double)totalTime*0.1)/count + 0.5) : 0; }
    unsigned long queryMaxTime() const { return maxTime/10; }
    unsigned long queryMinTime() const { return count ? minTime/10 : 0; }
    unsigned long queryMeanSize() const { return count ? (unsigned long)(((double)totalSize)/count + 0.5) : 0; }
private:
    unsigned long count;
    unsigned long totalTime;
    unsigned long maxTime;
    unsigned long minTime;
    unsigned long totalSize;
};

class TimingBlock
{
public:
    TimingBlock(TimingStats & _stats) : stats(_stats) { start = msTick(); }
    ~TimingBlock() { stats.record(msTick()-start); }
protected:
    TimingStats & stats;
private:
    unsigned long start;
};

class TimingSizeBlock : public TimingBlock
{
public:
    TimingSizeBlock(TimingStats & _stats) : TimingBlock(_stats), size(0) {}
    ~TimingSizeBlock() { stats.recordSize(size); }
    inline void recordSize(unsigned long _size) { size = _size; }
private:
    unsigned long size;
};

class CSDSTransactionServer : public Thread, public CTransactionLogTracker
{
public:
    IMPLEMENT_IINTERFACE;

    CSDSTransactionServer(CCovenSDSManager &_manager);
    
    void stop();
    void processMessage(CMessageBuffer &mb);
    const bool &queryStopped() const { return stopped; }

    inline TimingStats const & queryXactTimingStats() const { return xactTimingStats; }
    inline TimingStats const & queryConnectTimingStats() const { return connectTimingStats; }
    inline TimingStats const & queryCommitTimingStats() const { return commitTimingStats; }

// Thread
    virtual int run();

// CTransactionLogTracker
    virtual StringBuffer &getCmdText(unsigned cmd, StringBuffer &ret) const
    {
        return getSdsCmdText((SdsCommand)cmd, ret);
    }

private:
    TimingStats xactTimingStats;
    TimingStats connectTimingStats;
    TimingStats commitTimingStats;
    bool stopped;
    CCovenSDSManager &manager;
};

//////////////
class CConnectionSubscriberContainer : public CSubscriberContainerBase
{
public:
    CConnectionSubscriberContainer(ISubscription *subscriber, SubscriptionId id) : CSubscriberContainerBase(subscriber, id) { }
    bool notify() { MemoryBuffer mb; return CSubscriberContainerBase::notify(mb); }
};
//////////////

enum ConnInfoFlags { ci_newParent = 0x01 };
//////////////
class CServerConnection : public CConnectionBase, implements ISessionNotify
{
    DECL_NAMEDCOUNT;
public:
    IMPLEMENT_IINTERFACE;

    CServerConnection(ISDSConnectionManager &manager, ConnectionId connectionId, const char *xpath, SessionId id, unsigned mode, unsigned timeout, IPropertyTree *_parent, ConnInfoFlags _connInfoFlags)
        : CConnectionBase(manager, connectionId, xpath, id, mode, timeout), parent(_parent), connInfoFlags(_connInfoFlags)
    {
        INIT_NAMEDCOUNT;
        subsid = 0;
        established = false;
    }
    
    ~CServerConnection();

    void initPTreePath(PTree &root, PTree &tail)
    {
        if (&root != &tail) 
        {
            StringBuffer head;
            const char *_tail = splitXPath(xpath, head);
            if (_tail == xpath)
                ptreePath.append(*LINK(&root));
            else
                ptreePath.fill(root, head.str(), tail);
        }
        ptreePath.append(*LINK(&tail));
    }
    CPTStack &queryPTreePath() { return ptreePath; }

    IPropertyTree *queryRootUnvalidated()
    {
        return root;
    }

    MemoryBuffer &getInfo(MemoryBuffer &out)
    {
        out.append(connectionId).append(xpath).append(sessionId).append(mode).append(timeout).append(established);
        return out;
    }

    void subscribe(SessionId id)
    {
        subsid = querySessionManager().subscribeSession(id, this);
    }

    void addConnInfoFlag(ConnInfoFlags flag) { connInfoFlags = (ConnInfoFlags) (((unsigned)connInfoFlags) | ((unsigned) flag)); }

    void closed(SessionId id)
    {
        LOG(MCwarning, unknownJob, "Connection (%"I64F"x) was leaked by exiting client (%"I64F"x) path=%s", connectionId, id, queryXPath());
        aborted(id);
        subsid=0;
    }

    void aborted(SessionId id);

    void unsubscribeSession()
    {
        if (subsid) {
            querySessionManager().unsubscribeSession(subsid);   
            subsid = 0;
        }
    }

    void notify()
    {
        ForEachItemInRev(s, subscriptions)
        {
            CConnectionSubscriberContainer &sub = subscriptions.item(s);
            if (!sub.notify())
                subscriptions.remove(s);
        }
    }

    void addSubscriber(CConnectionSubscriberContainer &sub)
    {
        subscriptions.append(sub);
    }

    void removeSubscriber(SubscriptionId id)
    {
        ForEachItemIn(s, subscriptions) // do not expect a lot of subscribers per connection - probably ~ 1.
        {
            if (id == subscriptions.item(s).queryId())
            {
                subscriptions.remove(s);
                break;
            }
        }
    }
    bool queryEstablished() { return established; }
    void setEstablished() { established = true; }
    IPropertyTree *queryParent() { return parent; }
    void removeRoot()
    {
        if (parent)
            parent->removeTree(root);
    }
    virtual IPropertyTree *queryRoot();

private:
    ConnInfoFlags connInfoFlags;
    IPropertyTree *parent;
    SubscriptionId subsid;
    CPTStack ptreePath;
    IArrayOf<CConnectionSubscriberContainer> subscriptions;
    bool established;
};

/////////////////

class CSubscriberContainer : public CSubscriberContainerBase
{
    StringAttr xpath, fullXpath;
    StringArray qualifierStack;
    bool sub, sendValue;
    unsigned depth;
public:
    CSubscriberContainer(ISubscription *subscriber, SubscriptionId id) : CSubscriberContainerBase(subscriber, id)
    {
        const MemoryAttr &ma = subscriber->queryData();
        MemoryBuffer mb(ma.length(), ma.get());
        mb.read(xpath);
        mb.read(sub);
        if (mb.length()-mb.getPos()) // remaining
            mb.read(sendValue);
        else
            sendValue = false;

        const char *path = xpath;
        const char *nextSep = path+1;
        StringBuffer head;
        depth = 1; // root
        loop
        {
            nextSep = queryHead(nextSep, head.clear());
            ++depth; // inc last
            if (!nextSep)
                break;
        }
        StringBuffer strippedXpath;
        loop
        {
            const char *startQ;
            if (NULL == (startQ = queryNextUnquoted(path, '['))) // escaped '[]' chars??
            {
                if (strippedXpath.length()) strippedXpath.append(path);
                break;
            }

            const char *nextSep = path+1;
            loop
            {
                nextSep = queryHead(nextSep, head.clear());
                if (!nextSep || startQ < nextSep)
                    break;

                qualifierStack.append(""); // no qualifier for this segment.
            }

            const char *endQ = queryNextUnquoted(startQ, ']');
            assertex(endQ);
            strippedXpath.append(startQ-path, path);

            StringAttr qualifier(startQ+1, endQ-startQ-1);
            qualifierStack.append(qualifier);
            path = endQ+1;
        }
        fullXpath.set(xpath);
        if (strippedXpath.length()) // some qualifications
            xpath.set(strippedXpath.str());

        // Notes on qualified (e.g. 'a/b[x="test"]/c' etc.)
        // 1) strip out all qualifies from xpath into 'qualifierStack' and translate xpath to simple absolute path.
        // 2) collate IPropertyTree stack in processData; to pass to querySubscribers.
        // 3) querySubscribers becomes getSubscribers, builds up a list of matching subscribers,
        //    initially obtained with findList, but pruned based on IPT stack and qualifierStack.
        //    by walking IPT stack performing either PTree->checkPattern or for index's parent->findChild etc.
    }

    const char *queryXPath() const { return xpath; }
    bool querySub() const { return sub; }
    bool querySendValue() const { return sendValue; }
    unsigned queryDepth() const { return depth; }
    bool qualify(CPTStack &stack)
    {
        ForEachItemIn(q, qualifierStack)
        {
            const char *qualifier = qualifierStack.item(q);
            if (stack.ordinality() <= q+1)
            {
                // No more stack available (e.g. because deleted below this point)
                return true;
            }
            PTree &item = stack.item(q+1); // stack +1, top is root unqualified.
            if (qualifier && '\0' != *qualifier)
            {
                const char *q = qualifier;
                bool numeric = true;
                loop
                {
                    if ('\0' == *q) break;
                    else if (!isdigit(*q)) { numeric = false; break; }
                    else q++;
                }
                if (numeric)
                {
                    unsigned qnum = atoi(qualifier);
                    if (!item.queryParent())
                    {
                        if (qnum != 1)
                            return false;
                    }
                    else if (((PTree *)item.queryParent())->findChild(&item) != qnum-1)
                        return false;
                }
                else if (!item.checkPattern(qualifier))
                    return false;
            }
        }
        return true;
    }

    MemoryBuffer &getInfo(MemoryBuffer &out)
    {
        out.append(id).append(sub).append(fullXpath);
        return out;
    }
};

typedef IArrayOf<CSubscriberContainer> CSubscriberArray;
class CSubscriberContainerList : public CInterface, public CSubscriberArray
{
public:
    CSubscriberContainerList() { }
    CSubscriberContainerList(const char *_xpath) : xpath(_xpath) { _xpath = xpath.get(); }
    const char *queryXPath() const { return xpath; }
    const char *queryFindString() const { return queryXPath(); }

private:
    StringAttr xpath;
};

typedef OwningStringSuperHashTableOf<CSubscriberContainerList> CSubscriberXPathTable;
class CSubscriberTable : public ThreadSafeSimpleHashTableOf<CSubscriberContainer, SubscriptionId>
{
public:
    ~CSubscriberTable() { kill(); }

    virtual void onAdd(void *et)
    {
        CSubscriberContainer *subscriber = (CSubscriberContainer *) et;
        CSubscriberContainerList *list = xpathTable.find(subscriber->queryXPath());
        if (!list)
        {
            list = new CSubscriberContainerList(subscriber->queryXPath());
            xpathTable.replace(*list);
        }
        list->append(*subscriber); // give over ownership.
    }
    virtual void onRemove(void *et)
    {
        CSubscriberContainer *subscriber = (CSubscriberContainer *) et;
        subscriber->setUnsubscribed();
        CSubscriberContainerList *list = xpathTable.find(subscriber->queryXPath());
        assertex(list);
        verifyex(list->zap(*subscriber));
        if (!list->ordinality())
            xpathTable.removeExact(list);
    }
    CSubscriberContainerList *getQualifiedList(const char *xpath, CPTStack &stack)
    {
        CriticalBlock b(crit);
        CSubscriberContainerList *list = xpathTable.find(xpath);
        if (!list) return NULL;

        CSubscriberContainerList *results = NULL;
        ForEachItemIn(s, *list)
        {
            CSubscriberContainer &subscriber = list->item(s);
            if (subscriber.qualify(stack))
            {
                if (!results) results = new CSubscriberContainerList(xpath);
                subscriber.Link();
                results->append(subscriber);
            }
        }
        return results;
    }
    void getSubscribers(CSubscriberArray &subs)
    {
        CriticalBlock b(crit);
        SuperHashIteratorOf<CSubscriberContainer> iter(queryBaseTable());
        ForEach(iter)
        {
            CSubscriberContainer &sub = iter.query();
            sub.Link();
            subs.append(sub);
        }
    }
private:
    CSubscriberXPathTable xpathTable;
};

#ifdef _DEBUG
struct DebugInfo
{
    DebugInfo() { clearExclusive(); }
    void clearExclusive() { ExclOwningThread =0; ExclOwningConnection=0; ExclOwningSession=0; }

    ThreadId ExclOwningThread;
    ConnectionId ExclOwningConnection;
    SessionId ExclOwningSession;
};
#endif

struct LockData
{
    unsigned mode;
    SessionId sessId;
    unsigned timeLockObtained;
};

class BoolSetBlock
{
public:
    BoolSetBlock(bool &_b, bool state=true) : b(_b) { o = b; b = state; }
    ~BoolSetBlock() { b = o; }
private:
    bool o, &b;
};

//////////////

template <class ET>
class LinkedStringHTMapping : public OwningStringHTMapping<ET>
{
public:
    LinkedStringHTMapping(const char *fp, ET &et) : OwningStringHTMapping<ET>(fp, et) { this->et.Link(); }
};

typedef LinkedStringHTMapping<IExternalHandler> CExternalHandlerMapping;
typedef OwningStringSuperHashTableOf<CExternalHandlerMapping> CExternalHandlerTable;

void serializeVisibleAttributes(IPropertyTree &tree, MemoryBuffer &mb)
{
    IAttributeIterator *aIter = tree.getAttributes();
    if (aIter->first())
    {
        loop
        {
            const char *attr = aIter->queryName();
            if (0 != strcmp(EXT_ATTR, attr))
            {
                mb.append(attr);
                mb.append(aIter->queryValue());
            }
            if (!aIter->next())
                break;
        }
    }
    aIter->Release();
    mb.append(""); // attribute terminator. i.e. blank attr name.
}

void writeDelta(StringBuffer &xml, IFile &iFile, const char *msg="", unsigned retrySecs=0, unsigned retryAttempts=10)
{
    Owned<IException> exception;
    OwnedIFileIO iFileIO;
    unsigned _retryAttempts = retryAttempts;
    Owned<IFileIOStream> stream;
    offset_t lastGood = 0;
    unsigned startCrc = ~0;
    MemoryBuffer header;
    char strNum[17];
    loop
    {
        header.append(deltaHeader);
        try
        {
            iFileIO.setown(iFile.open(IFOreadwrite));
            stream.setown(createIOStream(iFileIO));
            if (lastGood)
            {
                PROGLOG("Resetting delta file size");
                iFileIO->setSize(lastGood);
            }
            else
            {
                if (iFileIO->size())
                {
                    iFileIO->read(deltaHeaderCrcOff, 10, strNum);
                    startCrc = ~(unsigned)atoi64_l(strNum, 10);
                }
                else
                    stream->write(strlen(deltaHeader), deltaHeader);
                lastGood = iFileIO->size();
            }
            stream->seek(0, IFSend);
            stream->write(xml.length(), xml.toCharArray());
            stream->flush();
            stream.clear();
            offset_t fLen = lastGood + xml.length();
            unsigned crc = crc32(xml.toCharArray(), xml.length(), startCrc);
            char *headerPtr = (char *)header.bufferBase();
            sprintf(strNum, "%010u", ~crc);
            memcpy(headerPtr + deltaHeaderCrcOff, strNum, 10);
            sprintf(strNum, "%016"I64F"X", fLen);
            memcpy(headerPtr + deltaHeaderSizeOff, strNum, 16);
            iFileIO->write(0, strlen(deltaHeader), headerPtr);
        }
        catch (IException *e)
        {
            exception.setown(e);
            StringBuffer s(msg);
            LOG(MCoperatorError, unknownJob, e, s.append("writeDelta, failed").str());
        }
        if (!exception.get())
            break;
        if (0 == retrySecs)
            return;
        if (0 == --_retryAttempts)
        {
            WARNLOG("writeDelta, too many retry attemps [%d]", retryAttempts);
            return;
        }
        exception.clear();
        WARNLOG("writeDelta, retrying");
        MilliSleep(retrySecs*1000);
    }
}

struct BackupQueueItem
{
    static unsigned typeMask;
    enum { f_delta=0x1, f_addext=0x2, f_delext=0x3, f_first=0x10 } flagt;
    BackupQueueItem() : edition((unsigned)-1), flags(0) { text = new StringBuffer; dataLength = 0; data = NULL; }
    ~BackupQueueItem()
    {
        delete text;
        if (data) free(data);
    }
    StringBuffer *text;
    unsigned edition;
    unsigned dataLength;
    void *data;
    byte flags;
};
unsigned BackupQueueItem::typeMask = 0x0f;
class CBackupHandler : public CInterface, implements IThreaded
{
    typedef QueueOf<BackupQueueItem, false> BackupQueue;
    CThreaded threaded;
    BackupQueue itemQueue, freeQueue;
    Semaphore pending, softQueueLimitSem;
    bool aborted, waiting, addWaiting, async;
    unsigned currentEdition, throttleCounter;
    CriticalSection queueCrit, freeQueueCrit;
    StringAttr backupPath;
    unsigned freeQueueLimit;  // how many BackupQueueItems to cache for reuse
    unsigned largeWarningThreshold;    // point at which to start warning about large queue
    unsigned softQueueLimit; // threshold over which primary transactions will be delay by small delay, to allow backup catchup.
    unsigned softQueueLimitDelay; // delay for above
    CTimeMon warningTime;
    unsigned recentTimeThrottled;
    unsigned lastNumWarnItems;

    BackupQueueItem *getFreeItem()
    {
        BackupQueueItem *item;
        {
            CriticalBlock b(freeQueueCrit);
            item = freeQueue.dequeue();
        }
        if (!item)
            item = new BackupQueueItem;
        return item;
    }
    void clearQueue(BackupQueue &queue)
    {
        loop
        {
            BackupQueueItem *item = queue.dequeue();
            if (!item) break;
            delete item;
        }
    }
    void writeExt(const char *name, const unsigned length, const void *data, unsigned retrySecs=0, unsigned retryAttempts=10)
    {
        Owned<IException> exception;
        unsigned _retryAttempts = retryAttempts;
        StringBuffer rL(remoteBackupLocation);
        loop
        {
            try
            {
                rL.append(name);
                Owned<IFile> iFile = createIFile(rL.str());
                Owned<IFileIO> fileIO = iFile->open(IFOcreate);
                fileIO->write(0, length, data);
            }
            catch (IException *e)
            {
                exception.setown(e);
                StringBuffer err("Saving external (backup): ");
                LOG(MCoperatorError, unknownJob, e, err.append(rL).str());
            }
            if (!exception.get())
                break;
            if (0 == retrySecs)
                return;
            if (0 == --_retryAttempts)
            {
                WARNLOG("writeExt, too many retry attemps [%d]", retryAttempts);
                return;
            }
            exception.clear();
            WARNLOG("writeExt, retrying");
            MilliSleep(retrySecs*1000);
        }
    }
    void deleteExt(const char *name, unsigned retrySecs=0, unsigned retryAttempts=10)
    {
        Owned<IException> exception;
        unsigned _retryAttempts = retryAttempts;
        StringBuffer rL(remoteBackupLocation);
        loop
        {
            try
            {
                rL.append(name);
                Owned<IFile> iFile = createIFile(rL.str());
                iFile->remove();
            }
            catch (IException *e)
            {
                exception.setown(e);
                StringBuffer err("Removing external (backup): ");
                LOG(MCoperatorWarning, unknownJob, e, err.append(rL).str());
            }
            if (!exception.get())
                break;
            if (0 == retrySecs)
                return;
            if (0 == --_retryAttempts)
            {
                WARNLOG("deleteExt, too many retry attemps [%d]", retryAttempts);
                return;
            }
            exception.clear();
            WARNLOG("deleteExt, retrying");
            MilliSleep(retrySecs*1000);
        }
    }
    bool writeDelta(StringBuffer &xml, unsigned edition, bool first)
    {
        StringBuffer deltaFilename(backupPath);
        constructStoreName(DELTANAME, edition, deltaFilename);
        OwnedIFile iFile = createIFile(deltaFilename.str());
        if (!first && !iFile->exists())
            return false; // discard
        ::writeDelta(xml, *iFile, "CBackupHandler - ", 60, 30);
        return true;
    }
    void clearOld()
    {
        CriticalBlock b(queueCrit);
        loop
        {
            BackupQueueItem *item = itemQueue.dequeue();
            if (!item) break;
            if (BackupQueueItem::f_delta == (item->flags & BackupQueueItem::typeMask))
            {
                item->text->clear();
                if (freeQueue.ordinality() < freeQueueLimit)
                    freeQueue.enqueue(item);
                else
                    delete item;
            }
        }
        if (addWaiting && itemQueue.ordinality()<softQueueLimit)
            softQueueLimitSem.signal();
    }

public:
    CBackupHandler() : threaded("CBackupHandler")
    {
        currentEdition = (unsigned)-1;
        addWaiting = waiting = async = false;
        aborted = true;
        throttleCounter = 0;
        freeQueueLimit = 10;
        largeWarningThreshold = 50;
        softQueueLimit = 200;
        softQueueLimitDelay = 200;
        recentTimeThrottled = 0;
        lastNumWarnItems = 0;
    }
    ~CBackupHandler()
    {
        clearQueue(freeQueue);
        clearQueue(itemQueue);
    }
    void init(const char *_backupPath, bool _async)
    {
        backupPath.set(_backupPath);
        async = _async;
        aborted = false;
        PROGLOG("BackupHandler started, async=%s", async?"true":"false");
        threaded.init(this);
    }
    void stop()
    {
        if (!aborted)
        {
            aborted = true;
            pending.signal();
            threaded.join();
        }
    }
    void removeExt(const char *fname)
    {
        if (aborted) return;
        if (!async)
        {
            deleteExt(fname);
            return;
        }
        BackupQueueItem *item = getFreeItem();
        item->text->append(fname);
        item->flags = BackupQueueItem::f_delext;
        add(item);
    }
    void addExt(const char *fname, unsigned length, void *data)
    {
        if (aborted) return;
        if (!async)
        {
            writeExt(fname, length, data);
            return;
        }
        BackupQueueItem *item = getFreeItem();
        item->text->append(fname);
        item->dataLength = length;
        item->data = data; // take ownership
        item->flags = BackupQueueItem::f_addext;
        add(item);
    }
    void addDelta(StringBuffer &xml, unsigned edition, bool first)
    {
        if (aborted) return;
        if (!async)
        {
            writeDelta(xml, edition, first);
            if (xml.length() > 0x100000)
                xml.kill();
            else
                xml.clear();
            return;
        }
        if (edition != currentEdition)
        {
            clearOld();
            currentEdition = edition;
        }
        BackupQueueItem *item = getFreeItem();
        xml.swapWith(*item->text);
        item->edition = edition;
        item->flags = BackupQueueItem::f_delta | BackupQueueItem::f_first;
        add(item);
    }
    void add(BackupQueueItem *item)
    {
        CriticalBlock b(queueCrit);
        itemQueue.enqueue(item);
        unsigned items=itemQueue.ordinality();
        if (0==items%largeWarningThreshold)
        {
            if (items>lastNumWarnItems) // track as they go up
            {
                LOG(MCoperatorWarning, "Backup thread has a high # (%d) of pending transaction queued to write", items);
                lastNumWarnItems = items;
            }
            else if (warningTime.elapsed() >= 60000) // if falling, avoid logging too much
            {
                LOG(MCoperatorWarning, "Backup thread has a high # (%d) of pending transaction queued to write", items);
                lastNumWarnItems = 0;
                warningTime.reset(0);
            }
        }
        if (items>=softQueueLimit)
        {
            addWaiting = true;
            unsigned ms = msTick();
            {
                CriticalUnblock b(queueCrit);
                softQueueLimitSem.wait(softQueueLimitDelay);
            }
            addWaiting = false;
            recentTimeThrottled += (msTick()-ms); // reset when queue < largeWarningThreshold
            if (recentTimeThrottled >= softQueueLimitDelay && (0 == throttleCounter % 10)) // softQueueLimit exceeded - log every 10 transactions if recentTimeThrottled >= softQueueLimitDelay (1 unsignalled delay)
                LOG(MCoperatorWarning, "Primary transactions are being delayed by lagging backup, currently %d queued, recent total throttle delay=%d", items, recentTimeThrottled);
            ++throttleCounter; // also reset when queue < largeWarningThreshold
        }
        if (waiting)
            pending.signal();
    }
    bool doIt(BackupQueueItem &item)
    {
        try
        {
            switch (item.flags & BackupQueueItem::typeMask)
            {
                case BackupQueueItem::f_delta:
                    return writeDelta(*item.text, item.edition, 0 != (item.flags & BackupQueueItem::f_first));
                case BackupQueueItem::f_addext:
                    writeExt(item.text->str(), item.dataLength, item.data, 60, 30);
                    return true;
                case BackupQueueItem::f_delext:
                    deleteExt(item.text->str(), 60, 30);
                    return true;
            }
        }
        catch (IException *e)
        {
            LOG(MCoperatorWarning, e, "BackupHandler(async) write operation failed, possible backup data loss");
            e->Release();
        }
        return false;
    }
// IThreaded
    void main()
    {
        loop
        {
            BackupQueueItem *item=NULL;
            do
            {
                CriticalBlock b(queueCrit);
                if (itemQueue.ordinality())
                {
                    item = itemQueue.dequeue();
                    if (addWaiting && itemQueue.ordinality()<softQueueLimit)
                        softQueueLimitSem.signal();
                    if (itemQueue.ordinality() < largeWarningThreshold) // reset stats when falls below
                    {
                        recentTimeThrottled = 0;
                        throttleCounter = 0;
                    }
                }
                else
                {
                    waiting = true;
                    {
                        CriticalUnblock b(queueCrit);
                        if (!aborted)
                            pending.wait();
                    }
                    waiting = false;
                }
                if (aborted)
                {
                    if (item) delete item;
                    PROGLOG("BackupHandler stopped");
                    return;
                }
            }
            while (!item);
            if (!doIt(*item))
                clearOld();
            CriticalBlock b(freeQueueCrit);
            if (freeQueue.ordinality() < freeQueueLimit)
            {
                if (item->text->length() > 0x100000)
                    item->text->kill();
                else
                    item->text->clear();
                if (item->data)
                {
                    free(item->data);
                    item->data = NULL;
                    item->dataLength = 0;
                }
                freeQueue.enqueue(item);
            }
            else
                delete item;
        }
    }
};

class CExternalFile : public CInterface
{
    StringAttr ext, dataPath;
protected:
    CBackupHandler &backupHandler;
public:
    CExternalFile(const char *_ext, const char *_dataPath, CBackupHandler &_backupHandler) : ext(_ext), dataPath(_dataPath), backupHandler(_backupHandler) { }
    const char *queryExt() { return ext; }
    StringBuffer &getName(StringBuffer &fName, const char *base)
    {
        return fName.append(base).append(ext);
    }
    StringBuffer &getFilename(StringBuffer &fName, const char *base)
    {
        return fName.append(dataPath).append(base).append(ext);
    }
    bool isValid(const char *name)
    {
        StringBuffer filename;
        getFilename(filename, name);
        Owned<IFile> iFile = createIFile(filename.str());
        return iFile->exists();
    }
    void remove(const char *name)
    {
        StringBuffer filename;
        getFilename(filename, name);
        Owned<IFile> iFile = createIFile(filename.str());
        iFile->remove();
        if (remoteBackupLocation.length())
        {
            StringBuffer fname(name);
            backupHandler.removeExt(fname.append(queryExt()).str());
        }       
    }
};

class CLegacyBinaryFileExternal : public CExternalFile, implements IExternalHandler
{
public:
    IMPLEMENT_IINTERFACE;

    CLegacyBinaryFileExternal(const char *dataPath, CBackupHandler &backupHandler) : CExternalFile("."EF_LegacyBinaryValue, dataPath, backupHandler) { }
    virtual void resetAsExternal(IPropertyTree &tree)
    {
        tree.setProp(NULL, (char *)NULL);
    }
    virtual void readValue(const char *name, MemoryBuffer &mb)
    {
        StringBuffer filename;
        getFilename(filename, name);

        Owned<IFile> iFile = createIFile(filename.str());
        size32_t sz = (size32_t)iFile->size();
        if ((unsigned)-1 == sz)
        {
            StringBuffer s("Missing external file ");
            Owned<IException> e = MakeSDSException(SDSExcpt_MissingExternalFile, "%s", filename.str());
            LOG(MCoperatorWarning, unknownJob, e, s.str()); 
            StringBuffer str("EXTERNAL BINARY FILE: \"");
            str.append(filename.str()).append("\" MISSING");
            CPTValue v(str.length()+1, str.toCharArray(), false);
            v.serialize(mb);
        }
        else
        {
            Owned<IFileIO> fileIO = iFile->open(IFOread);
            MemoryBuffer vmb;
            verifyex(sz == ::read(fileIO, 0, sz, vmb));
            CPTValue v(sz, vmb.toByteArray(), true);
            v.serialize(mb);
        }
    }
    virtual void read(const char *name, IPropertyTree &owner, MemoryBuffer &mb, bool withValue)
    {
        StringBuffer filename;
        getFilename(filename, name);

        const char *_name = owner.queryName();
        mb.append(_name?_name:"");
        byte flags = ((PTree &)owner).queryFlags();
        mb.append(IptFlagSet(flags, ipt_binary));

        serializeVisibleAttributes(owner, mb);

        Owned<IFile> iFile = createIFile(filename.str());
        size32_t sz = (size32_t)iFile->size();
        if ((unsigned)-1 == sz)
        {
            StringBuffer s("Missing external file ");
            if (*_name)
                s.append("in property ").append(_name);
            Owned<IException> e = MakeSDSException(SDSExcpt_MissingExternalFile, "%s", filename.str());
            LOG(MCoperatorWarning, unknownJob, e, s.str()); 
            if (withValue)
            {
                StringBuffer str("EXTERNAL BINARY FILE: \"");
                str.append(filename.str()).append("\" MISSING");
                CPTValue v(str.length()+1, str.toCharArray(), false);
                v.serialize(mb);
            }
            else
                mb.append((size32_t)0);
        }
        else
        {
            if (withValue)
            {
                MemoryBuffer vmb;
                Owned<IFileIO> fileIO = iFile->open(IFOread);
                verifyex(sz == ::read(fileIO, 0, sz, vmb));
                CPTValue v(sz, vmb.toByteArray(), true);
                v.serialize(mb);
            }
            else
                mb.append((size32_t)0);
        }
    }
    virtual void write(const char *name, IPropertyTree &tree)
    {
        StringBuffer filename;
        getFilename(filename, name);
        Owned<IFile> iFile = createIFile(filename.str());
        Owned<IFileIO> fileIO = iFile->open(IFOcreate);

        MemoryBuffer out;
        ((PTree &)tree).queryValue()->serialize(out);
        const char *data = out.toByteArray();
        unsigned length = out.length();
        fileIO->write(0, length, data);
        if (remoteBackupLocation.length())
        {
            StringBuffer fname(name);
            backupHandler.addExt(fname.append(queryExt()).str(), length, out.detach());
        }       
    }
    virtual void remove(const char *name) { CExternalFile::remove(name); }
    virtual bool isValid(const char *name) { return CExternalFile::isValid(name); }
    virtual StringBuffer &getName(StringBuffer &fName, const char *base) { return CExternalFile::getName(fName, base); }
    virtual StringBuffer &getFilename(StringBuffer &fName, const char *base) { return CExternalFile::getFilename(fName, base); }
};

class CBinaryFileExternal : public CExternalFile, implements IExternalHandler
{
public:
    IMPLEMENT_IINTERFACE;

    CBinaryFileExternal(const char *dataPath, CBackupHandler &backupHandler) : CExternalFile("."EF_BinaryValue, dataPath, backupHandler) { }
    virtual void resetAsExternal(IPropertyTree &tree)
    {
        tree.setProp(NULL, (char *)NULL);
    }
    virtual void readValue(const char *name, MemoryBuffer &mb)
    {
        StringBuffer filename;
        getFilename(filename, name);

        Owned<IFile> iFile = createIFile(filename.str());
        size32_t sz = (size32_t)iFile->size();
        if ((unsigned)-1 == sz)
        {
            StringBuffer s("Missing external file ");
            Owned<IException> e = MakeSDSException(SDSExcpt_MissingExternalFile, "%s", filename.str());
            LOG(MCoperatorWarning, unknownJob, e, s.str());
            StringBuffer str("EXTERNAL BINARY FILE: \"");
            str.append(filename.str()).append("\" MISSING");
            CPTValue v(str.length()+1, str.toCharArray(), false);
            v.serialize(mb);
        }
        else
        {
            Owned<IFileIO> fileIO = iFile->open(IFOread);
            verifyex(sz == ::read(fileIO, 0, sz, mb));
        }
    }
    virtual void read(const char *name, IPropertyTree &owner, MemoryBuffer &mb, bool withValue)
    {
        StringBuffer filename;
        getFilename(filename, name);
        const char *_name = owner.queryName();
        mb.append(_name?_name:"");

        Owned<IFile> iFile = createIFile(filename.str());
        size32_t sz = (size32_t)iFile->size();
        if ((unsigned)-1 == sz)
        {
            byte flags = ((PTree &)owner).queryFlags();
            IptFlagClr(flags, ipt_binary);
            mb.append(flags);
            serializeVisibleAttributes(owner, mb);
            StringBuffer s("Missing external file ");
            if (*_name)
                s.append("in property ").append(_name);
            Owned<IException> e = MakeSDSException(SDSExcpt_MissingExternalFile, "%s", filename.str());
            LOG(MCoperatorWarning, unknownJob, e, s.str());
            if (withValue)
            {
                StringBuffer str("EXTERNAL BINARY FILE: \"");
                str.append(filename.str()).append("\" MISSING");
                CPTValue v(str.length()+1, str.toCharArray(), false);
                v.serialize(mb);
            }
            else
                mb.append((size32_t)0);
        }
        else
        {
            byte flags = ((PTree &)owner).queryFlags();
            mb.append(flags);
            serializeVisibleAttributes(owner, mb);
            if (withValue)
            {
                Owned<IFileIO> fileIO = iFile->open(IFOread);
                verifyex(sz == ::read(fileIO, 0, sz, mb));
            }
            else
                mb.append((size32_t)0);
        }
    }
    virtual void write(const char *name, IPropertyTree &tree)
    {
        StringBuffer filename;
        getFilename(filename, name);
        Owned<IFile> iFile = createIFile(filename.str());
        Owned<IFileIO> fileIO = iFile->open(IFOcreate);

        MemoryBuffer out;
        ((PTree &)tree).queryValue()->serialize(out);
        const char *data = out.toByteArray();
        unsigned length = out.length();
        fileIO->write(0, length, data);
        if (remoteBackupLocation.length())
        {
            StringBuffer fname(name);
            backupHandler.addExt(fname.append(queryExt()).str(), length, out.detach());
        }       
    }
    virtual void remove(const char *name) { CExternalFile::remove(name); }
    virtual bool isValid(const char *name) { return CExternalFile::isValid(name); }
    virtual StringBuffer &getName(StringBuffer &fName, const char *base) { return CExternalFile::getName(fName, base); }
    virtual StringBuffer &getFilename(StringBuffer &fName, const char *base) { return CExternalFile::getFilename(fName, base); }
};

class CXMLFileExternal : public CExternalFile, implements IExternalHandler
{
public:
    IMPLEMENT_IINTERFACE;

    CXMLFileExternal(const char *dataPath, CBackupHandler &backupHandler) : CExternalFile("."EF_XML, dataPath, backupHandler) { }
    virtual void resetAsExternal(IPropertyTree &_tree)
    {
        PTree &tree = *QUERYINTERFACE(&_tree, PTree);
        tree.clear();
    }
    virtual void readValue(const char *name, MemoryBuffer &mb)
    {
        StringBuffer filename;
        getFilename(filename, name);
        OwnedIFile ifile = createIFile(filename.str());
        if (!ifile->exists())
        {
            StringBuffer s("Missing external file ");
            Owned<IException> e = MakeSDSException(SDSExcpt_MissingExternalFile, "%s", filename.str());
            LOG(MCoperatorWarning, unknownJob, e, s.str());
            StringBuffer str("EXTERNAL XML FILE: \"");
            str.append(filename.str()).append("\" MISSING");
            CPTValue v(str.length()+1, str.toCharArray(), false);
            v.serialize(mb);
        }
        else
        {
            Owned<IPropertyTree> tree;
            tree.setown(createPTreeFromXMLFile(filename.str()));
            IPTArrayValue *v = ((PTree *)tree.get())->queryValue();
            if (v)
                v->serialize(mb);
            else
                mb.append((size32_t)0);
        }
    }
    virtual void read(const char *name, IPropertyTree &owner, MemoryBuffer &mb, bool withValue)
    {
        StringBuffer filename;
        getFilename(filename, name);
        Owned<IPropertyTree> tree;
        OwnedIFile ifile = createIFile(filename.str());
        if (!ifile->exists())
        {
            StringBuffer s("Missing external file ");
            const char *name = owner.queryName();
            if (name && *name)
                s.append("in property ").append(name);
            Owned<IException> e = MakeSDSException(SDSExcpt_MissingExternalFile, "%s", filename.str());
            LOG(MCoperatorWarning, unknownJob, e, s.str());
            StringBuffer str("EXTERNAL XML FILE: \"");
            str.append(filename.str()).append("\" MISSING");
            tree.setown(createPTree(owner.queryName()));
            if (withValue)
                tree->setProp(NULL, str.toCharArray());
        }
        else
        {
            tree.setown(createPTreeFromXMLFile(filename.str()));
            if (!withValue)
                tree->setProp(NULL, (char *)NULL);
        }
        ((PTree *)tree.get())->serializeSelf(mb);
    }
    virtual void write(const char *name, IPropertyTree &tree)
    {
        StringBuffer filename;
        getFilename(filename, name);
        Owned<IFile> iFile = createIFile(filename.str());
        Owned<IFileIO> fileIO = iFile->open(IFOcreate);
        Owned<IFileIOStream> fstream = createBufferedIOStream(fileIO);
        toXML(&tree, *fstream);
        if (remoteBackupLocation.length())
        {
            StringBuffer fname(name);
            StringBuffer str;
            toXML(&tree, str);
            unsigned l = str.length();
            backupHandler.addExt(fname.append(queryExt()).str(), l, str.detach());
        }       
    }
    virtual void remove(const char *name) { CExternalFile::remove(name); }
    virtual bool isValid(const char *name) { return CExternalFile::isValid(name); }
    virtual StringBuffer &getName(StringBuffer &fName, const char *base) { return CExternalFile::getName(fName, base); }
    virtual StringBuffer &getFilename(StringBuffer &fName, const char *base) { return CExternalFile::getFilename(fName, base); }
};

//////////////
class CBranchChange : public CInterface
{
    DECL_NAMEDCOUNT;
    typedef CIArrayOf<CBranchChange> CBranchChangeChildren;
public:
    CBranchChange(CRemoteTreeBase &_tree) : tree(&_tree), local(PDS_None), state(PDS_None) { INIT_NAMEDCOUNT; }

    void noteChange(PDState _local, PDState _state) { local = _local; state = _state; }

    void addChildBranch(CBranchChange &child) { children.append(child); }

    const void *queryFindParam() const { return (const void *) &tree; }

    CBranchChangeChildren children;
    Linked<CRemoteTreeBase> tree;
    PDState local, state; // change info
};


SDSNotifyFlags translatePDState(PDState state)
{
    return (SDSNotifyFlags) state; // mirrored for now.
}

void buildNotifyData(CPTStack &stack, MemoryBuffer &mb)
{
    mb.append('/');
    PTree *parent = &stack.item(0); // root

    unsigned n = stack.ordinality();
    if (n>1)
    {
        unsigned s = 1;
        loop
        {
            PTree &child = stack.item(s);
            const char *str = child.queryName();
            mb.append(strlen(str), str);
            if (child.queryParent())
            {
                char temp[12];
                unsigned written = numtostr(temp, parent->findChild(&child)+1);
                mb.append('[').append(written, temp).append(']');
            }
            else
                mb.append(3, "[1]");
            parent = &child;
            s++;
            if (s<n)
                mb.append('/');
            else
                break;
        }
    }
    mb.append('\0');
}

class CSubscriberNotifier;
typedef SimpleHashTableOf<CSubscriberNotifier, SubscriptionId> CSubscriberNotifierTable;

class CSubscriberNotifier : public CInterface
{
    DECL_NAMEDCOUNT;
    class CChange : public CInterface
    {
    public:
        CChange(MemoryBuffer &_notifyData) : notifyData(_notifyData.length(), _notifyData.toByteArray()) { }
        MemoryBuffer notifyData;
    };
public:
    CSubscriberNotifier(CSubscriberNotifierTable &_table, CSubscriberContainer &_subscriber, MemoryBuffer &notifyData)
        : table(_table), subscriber(_subscriber)
    {
        INIT_NAMEDCOUNT;
        change.setown(new CChange(notifyData));
        subscriber.Link();
    }
    ~CSubscriberNotifier() { subscriber.Release(); }

    void queueChange(MemoryBuffer &notifyData)
    {
        changeQueue.append(*new CChange(notifyData));
    }

    void notify()
    {
        loop
        {
            if (!subscriber.notify(change->notifyData))
            {
                subscriber.setUnsubscribed();
                break;
            }
            else if (subscriber.isUnsubscribed())
                break;

            CHECKEDCRITICALBLOCK(nfyTableCrit, fakeCritTimeout);
            if (changeQueue.ordinality())
            {
                change.set(&changeQueue.item(0));
                changeQueue.remove(0);
            }
            else
            {
                table.removeExact(this);
                break;
            }
        }
        if (subscriber.isUnsubscribed())
        {
            { CHECKEDCRITICALBLOCK(nfyTableCrit, fakeCritTimeout);
                table.removeExact(this);
            }
            querySubscriptionManager(SDS_PUBLISHER)->remove(subscriber.queryId());
        }
    }

    const void *queryFindParam() const
    {
        return &(subscriber.queryId());
    }

private:
    Linked<CChange> change;
    CIArrayOf<CChange> changeQueue;
    CSubscriberContainer &subscriber;
    MemoryAttr notifyData;
    CSubscriberNotifierTable &table;
};

////////////////

typedef MapBetween<SubscriptionId, SubscriptionId, ConnectionId, ConnectionId> SubConnMap;
class CConnectionSubscriptionManager : CInterface, implements ISubscriptionManager
{
public:
    IMPLEMENT_IINTERFACE;

    CConnectionSubscriptionManager()
    {
    }


    unsigned querySubscribers()
    {
        CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
        return subConnMap.count();
    }
    virtual void add(ISubscription *sub, SubscriptionId id);
    virtual void remove(SubscriptionId id);

private:
    SubConnMap subConnMap;
    CheckedCriticalSection crit;
};

//////////

class CLockInfo;
typedef ThreadSafeOwningSimpleHashTableOf<CLockInfo, __int64> CLockInfoTable;


//////////

interface IUnlockCallback
{
    virtual void block() = 0;
    virtual void unblock() = 0;
};

//////////

typedef LinkedStringHTMapping<ISDSNotifyHandler> CSDSNotifyHandlerMapping;
typedef OwningStringSuperHashTableOf<CSDSNotifyHandlerMapping> CNotifyHandlerTable;
class CServerRemoteTree;
typedef CServerRemoteTree *CServerRemoteTreePtr;
typedef MapBetween<__int64, __int64, CServerRemoteTreePtr, CServerRemoteTreePtr> UniqueTreeMapping;

struct CStoreInfo
{
    unsigned edition;
    unsigned crc;
    StringAttr cache;
};

interface ICoalesce : extends IInterface
{
    virtual void start() = 0;
    virtual void stop() = 0;
};

//////////////////////

class CCovenSDSManager : public CSDSManagerBase, implements ISDSManagerServer, implements ISubscriptionManager, implements IExceptionHandler
{
public:
    IMPLEMENT_IINTERFACE;

    CCovenSDSManager(ICoven &_coven, IPropertyTree &config, const char *dataPath);
    ~CCovenSDSManager();

    void start();
    void stop();
    void restart(IException * e);

    void loadStore(const char *store=NULL, const bool *abort=NULL);
    void saveStore(const char *store=NULL, bool currentEdition=false);
    bool unlock(__int64 treeId, ConnectionId connectionId);
    void unlockAll(__int64 treeId);
    void changeLockMode(CServerConnection &connection, unsigned newMode, unsigned timeout);
    void clearSDSLocks();
    void lock(CServerRemoteTree &tree, const char *xpath, ConnectionId connectionId, SessionId sessionId, unsigned mode, unsigned timeout, IUnlockCallback &callback);
    CLockInfo *queryLockInfo(__int64 id) { return lockTable.find(&id); }
    CSubscriberTable &querySubscriberTable() { return subscribers; }
    IExternalHandler *queryExternalHandler(const char *handler) { if (!handler) return NULL; CExternalHandlerMapping *mapping = externalHandlers.find(handler); return mapping ? &mapping->query() : NULL; }
    void handleNotify(CSubscriberContainer &subscriber, PDState state, CPTStack &stack, MemoryBuffer *data=NULL);
    void startNotification(IPropertyTree &changeTree, CPTStack &stack, CBranchChange &changes); // subscription notification
    MemoryBuffer &collectUsageStats(MemoryBuffer &out);
    MemoryBuffer &collectConnections(MemoryBuffer &out);
    MemoryBuffer &collectSubscribers(MemoryBuffer &out);
    void blockingSave(unsigned *writeTransactions=NULL);
    bool queryStopped() { return server.queryStopped(); }

    void handleNodeNotify(notifications n, CServerRemoteTree &tree); // server node notification

    void deleteExternal(__int64 index);
    void serializeExternal(__int64 index, IPropertyTree &owner, MemoryBuffer &mb, bool withValue);
    void writeExternal(CServerRemoteTree &tree, bool direct=false, __int64 existing=0);
    inline unsigned queryExternalThreshold() { return externalSizeThreshold; }
    CServerConnection *queryConnection(ConnectionId id);
    CServerConnection *getConnection(ConnectionId id);
    inline CFitArray &queryAllNodes() { return allNodes; }
    unsigned __int64 getNextExternal() { return nextExternal++; }
    CServerConnection *createConnectionInstance(CRemoteTreeBase *root, SessionId sessionId, unsigned mode, unsigned timeout, const char *xpath, CRemoteTreeBase *&tree, ConnectionId connectionId, StringAttr *deltaPath, Owned<IPropertyTree> &deltaChange, Owned<CBranchChange> &branchChange, unsigned &additions);
    void createConnection(SessionId sessionId, unsigned mode, unsigned timeout, const char *xpath, CServerRemoteTree *&tree, ConnectionId &connectionId, bool primary, Owned<LinkingCriticalBlock> &connectCritBlock);
    void disconnect(ConnectionId connectionId, bool deleteRoot=false, Owned<CLCLockBlock> *lockBlock=NULL);
    void registerTree(__int64 serverId, CServerRemoteTree &tree);
    void unregisterTree(__int64 uniqId);
    CServerRemoteTree *queryRegisteredTree(__int64 uniqId);
    CServerRemoteTree *getRegisteredTree(__int64 uniqId);
    CServerRemoteTree *queryRoot();
    void saveDelta(const char *path, IPropertyTree &changeTree);
    CSubscriberContainerList *getSubscribers(const char *xpath, CPTStack &stack);
    void getExternalValue(__int64 index, MemoryBuffer &mb);
    IPropertyTree *getXPathsSortLimitMatchTree(const char *baseXPath, const char *matchXPath, const char *sortby, bool caseinsensitive, bool ascending, unsigned from, unsigned limit);

// ISDSConnectionManager
    virtual CRemoteTreeBase *get(CRemoteConnection &connection, __int64 serverId);
    virtual void getChildren(CRemoteTreeBase &parent, CRemoteConnection &connection, unsigned levels);
    virtual void getChildrenFor(CRTArray &childLessList, CRemoteConnection &connection, unsigned levels);
    virtual void ensureLocal(CRemoteConnection &connection, CRemoteTreeBase &_parent, IPropertyTree *serverMatchTree, IPTIteratorCodes flags=iptiter_null);
    virtual IPropertyTreeIterator *getElements(CRemoteConnection &connection, const char *xpath);
    virtual void commit(CRemoteConnection &connection, bool *disconnectDeleteRoot);
    virtual void changeMode(CRemoteConnection &connection, unsigned mode, unsigned timeout, bool suppressReload);
    virtual IPropertyTree *getXPaths(__int64 serverId, const char *xpath, bool getServerIds=false);
    virtual IPropertyTreeIterator *getXPathsSortLimit(const char *baseXPath, const char *matchXPath, const char *sortby, bool caseinsensitive, bool ascending, unsigned from, unsigned limit);
    virtual void getExternalValueFromServerId(__int64 serverId, MemoryBuffer &mb);
    virtual bool unlock(__int64 connectionId, bool closeConn, StringBuffer &connectionInfo);

// ISDSManagerServer
    virtual IRemoteConnections *connect(IMultipleConnector *mConnect, SessionId id, unsigned timeout);
    virtual IRemoteConnection *connect(const char *xpath, SessionId id, unsigned mode, unsigned timeout);
    virtual SubscriptionId subscribe(const char *xpath, ISDSSubscription &notify, bool sub=true, bool sendValue=false);
    virtual void unsubscribe(SubscriptionId id);
    virtual StringBuffer &getLocks(StringBuffer &out);
    virtual StringBuffer &getUsageStats(StringBuffer &out);
    virtual StringBuffer &getConnections(StringBuffer &out);
    virtual StringBuffer &getSubscribers(StringBuffer &out);
    virtual StringBuffer &getExternalReport(StringBuffer &out);
    virtual void installNotifyHandler(const char *handlerKey, ISDSNotifyHandler *handler);
    virtual bool removeNotifyHandler(const char *handlerKey);
    virtual IPropertyTree *lockStoreRead() const;
    virtual void unlockStoreRead() const;
    virtual unsigned countConnections();
    virtual bool setSDSDebug(StringArray &params, StringBuffer &reply);
    virtual unsigned countActiveLocks();
    virtual unsigned countSubscribers() const;
    virtual unsigned queryExternalSizeThreshold() const { return externalSizeThreshold; }
    virtual void setExternalSizeThreshold(unsigned _size) { externalSizeThreshold = _size; }
    virtual bool queryRestartOnError() const { return restartOnError; }
    virtual void setRestartOnError(bool _restart) { restartOnError = _restart; }
    unsigned queryRequestsPending() const { return coven.probe(RANK_ALL,MPTAG_DALI_SDS_REQUEST,NULL); }
    unsigned queryXactCount() const { return server.queryXactTimingStats().queryCount(); }
    unsigned queryXactMeanTime() const { return server.queryXactTimingStats().queryMeanTime(); }
    unsigned queryXactMaxTime() const { return server.queryXactTimingStats().queryMaxTime(); }
    unsigned queryXactMinTime() const { return server.queryXactTimingStats().queryMinTime(); }
    unsigned queryConnectMeanTime() const { return server.queryConnectTimingStats().queryMeanTime(); }
    unsigned queryConnectMaxTime() const { return server.queryConnectTimingStats().queryMaxTime(); }
    unsigned queryCommitMeanTime() const { return server.queryCommitTimingStats().queryMeanTime(); }
    unsigned queryCommitMaxTime() const { return server.queryCommitTimingStats().queryMaxTime(); }
    unsigned queryCommitMeanSize() const { return server.queryCommitTimingStats().queryMeanSize(); }
    virtual void saveRequest();
    virtual IPropertyTree &queryProperties() const;
    virtual IPropertyTreeIterator *getElementsRaw(const char *xpath,INode *remotedali=NULL, unsigned timeout=MP_WAIT_FOREVER);
    virtual void setConfigOpt(const char *opt, const char *value);
    virtual unsigned queryCount(const char *xpath);
    virtual bool updateEnvironment(IPropertyTree *newEnv, bool forceGroupUpdate, StringBuffer &response);

// ISubscriptionManager impl.
    virtual void add(ISubscription *subs,SubscriptionId id);
    virtual void remove(SubscriptionId id);

// IExceptionHandler
    virtual bool fireException(IException *e);

public: // data
    mutable ReadWriteLock dataRWLock;
    CheckedCriticalSection connectCrit;
    CheckedCriticalSection connDestructCrit;
    CheckedCriticalSection cTableCrit;
    CheckedCriticalSection sTableCrit;
    CheckedCriticalSection lockCrit;
    CheckedCriticalSection treeRegCrit;
    Owned<Thread> unhandledThread;
    unsigned writeTransactions;
    bool ignoreExternals;
    StringAttr dataPath;
    Owned<IPropertyTree> properties;
private:
    void validateBackup();
    inline bool establishLock(CLockInfo &lockInfo, __int64 treeId, ConnectionId connectionId, SessionId sessionId, unsigned mode, unsigned timeout, IUnlockCallback &lockCallback);
    void _getChildren(CRemoteTreeBase &parent, CServerRemoteTree &serverParent, CRemoteConnection &connection, unsigned levels);
    void matchServerTree(CClientRemoteTree *local, IPropertyTree &matchTree, bool allTail);

    CSubscriberTable subscribers;
    CSDSTransactionServer server;
    ICoven &coven;
    CServerRemoteTree *root;
    CFitArray allNodes;
    IPropertyTree &config;
    MemoryBuffer incrementBuffer;
    Owned<ICoalesce> coalesce;
    unsigned __int64 nextExternal;
    unsigned externalSizeThreshold;
    CLockInfoTable lockTable;
    CNotifyHandlerTable nodeNotifyHandlers;
    Owned<IThreadPool> scanNotifyPool, notifyPool;
    CExternalHandlerTable externalHandlers;
    CSubscriberNotifierTable subscriberNotificationTable;
    Owned<CConnectionSubscriptionManager> connectionSubscriptionManager;
    bool restartOnError, externalEnvironment;
    IStoreHelper *iStoreHelper;
    bool doTimeComparison;
    StringBuffer blockedDelta;
    CBackupHandler backupHandler;
};

ISDSManagerServer &querySDSServer()
{
    assertex(queryCoven().inCoven());
    return *SDSManager;
}

/////////////////

void CConnectionSubscriptionManager::add(ISubscription *sub, SubscriptionId id)
{
    MemoryBuffer mb;
    mb.setBuffer(sub->queryData().length(), (void *)sub->queryData().get());
    ConnectionId connId;
    mb.read(connId);

    Owned<CServerConnection> conn = SDSManager->getConnection(connId);
    if (conn)
    {
        CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
        conn->addSubscriber(* new CConnectionSubscriberContainer(sub, id));
        subConnMap.setValue(id, connId);
    }
    // else assume connection has since been disconnected.
}

void CConnectionSubscriptionManager::remove(SubscriptionId id)
{
    CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
    ConnectionId *connId = subConnMap.getValue(id);
    assertex(connId);
    Owned<CServerConnection> conn = SDSManager->getConnection(*connId);
    if (conn)
        conn->removeSubscriber(id);
    subConnMap.remove(id);
}

/////////////////
CDisableFetchChangeBlock::CDisableFetchChangeBlock(CRemoteConnection &_connection) : connection(_connection)
{
    stateChanges = connection.queryStateChanges();
    connection.setStateChanges(false);
    lazyFetch = connection.setLazyFetch(false);
}
CDisableFetchChangeBlock::~CDisableFetchChangeBlock()
{
    connection.setLazyFetch(lazyFetch);
    connection.setStateChanges(stateChanges);
}

CDisableLazyFetchBlock::CDisableLazyFetchBlock(CRemoteConnection &_connection) : connection(_connection)
{
    lazyFetch = connection.setLazyFetch(false);
}
CDisableLazyFetchBlock::~CDisableLazyFetchBlock()
{
    connection.setLazyFetch(lazyFetch);
}

/////////////////
/// CPTStack impl.
bool CPTStack::_fill(IPropertyTree &current, const char *xpath, IPropertyTree &tail)
{
    const char *nextSep = '/' == *xpath ? xpath+1 : xpath;

    StringBuffer head;
    const char *_nextSep = queryHead(nextSep, head);
    if (!_nextSep)
    {
        Owned<IPropertyTreeIterator> iter = current.getElements(nextSep);
        ForEach (*iter)
        {
            if (NotFound != iter->query().queryChildIndex(&tail))
            {
                append(*LINK((PTree *)&iter->query()));
                append(*LINK((PTree *)&current));
                return true;
            }
        }
    }
    else
    {
        Owned<IPropertyTreeIterator> iter = current.getElements(head.str());
        ForEach (*iter)
        {
            if (&tail==&iter->query())
            {
                append(*LINK((PTree *)&iter->query()));
                append(*LINK((PTree *)&current));
                return true;
            }
            else if (_fill(iter->query(), _nextSep, tail))
            {
                append(*LINK((PTree *)&current));
                return true;
            }
        }
    }
    return false;
}

bool CPTStack::fill(IPropertyTree &root, const char *xpath, IPropertyTree &tail)
{
    assertex(&root != &tail);
    if (!xpath || !*xpath)
    {
        append(*LINK((PTree *)&root));
        return true;
    }
    kill();
    bool res = _fill(root, xpath, tail);
    unsigned elems = ordinality();
    if (elems<2) return res;
    elems--;
    unsigned to = 0;
    while (to < elems)
        swap(elems--, to++);
    return res;
}

/////////////////////

StringBuffer &CPTStack::getAbsolutePath(StringBuffer &str)
{
    str.append('/');
    IPropertyTree *parent = &item(0);
    if (ordinality()>1)
    {
        unsigned i = 1;
        loop
        {
            IPropertyTree *child = &item(i);
            str.append(child->queryName());
            str.append('[');
            unsigned pos = parent->queryChildIndex(child);
            str.append(pos+1);
            str.append(']');
            if (++i >= ordinality())
                break;
            str.append('/');
            parent = child;
        }
    }
    return str;
}

/////////////////////

CServerConnection::~CServerConnection()
{
    Owned<LinkingCriticalBlock> checkedCritBlock;
    if (!RTM_MODE(mode, RTM_INTERNAL))
    {
        ForEachItemIn(s, subscriptions)
            SDSManager->remove(subscriptions.item(s).queryId());
        checkedCritBlock.setown(new LinkingCriticalBlock(SDSManager->connDestructCrit, __FILE__, __LINE__));
    }
    ptreePath.kill();
    root.clear();
}

void CServerConnection::aborted(SessionId id)
{
    LOG(MCdebugInfo(100), unknownJob, "CServerConnection: connection aborted (%"I64F"x) sessId=%"I64F"x",connectionId, id);
#if 0 // JCSMORE - think this is ok, but concerned about deadlock, change later.
    Owned<CLCLockBlock> lockBlock = new CLCWriteLockBlock(((CCovenSDSManager &)manager).dataRWLock, readWriteTimeout, __FILE__, __LINE__);
    SDSManager->disconnect(connectionId, false);
#else
    Owned<CLCLockBlock> lockBlock = new CLCReadLockBlock(((CCovenSDSManager &)manager).dataRWLock, readWriteTimeout, __FILE__, __LINE__);
    SDSManager->disconnect(connectionId, false, &lockBlock);
#endif
}

///////////////////
enum IncCmd { None, PropDelete, AttrDelete, PropChange, PropNew, PropExisting, ChildEndMarker, PropRename, AttrChange };

CRemoteTreeBase::CRemoteTreeBase(const char *name, IPTArrayValue *value, ChildMap *children, CPState _state)
    : PTree(name, ipt_none, value, children), state(_state)
{
    serverId = 0;
}

CRemoteTreeBase::CRemoteTreeBase(MemoryBuffer &mb, CPState _state)
    : state(_state)
{
    serverId = 0;
}

void CRemoteTreeBase::reset(unsigned _state, bool sub)
{
    state = _state;
    serverId = 0;
    if (sub)
    {
        IPropertyTreeIterator *iter = getElements("*");
        ForEach(*iter)
        {
            CRemoteTreeBase &child = (CRemoteTreeBase &)iter->query();
            child.reset(state, sub);
        }
        iter->Release();
    }
}

void CRemoteTreeBase::deserializeRT(MemoryBuffer &src)
{
    deserializeSelfRT(src);
    deserializeChildrenRT(src);
}

void CRemoteTreeBase::deserializeChildrenRT(MemoryBuffer &src)
{
    StringAttr eName;
    loop
    {
        size32_t pos = src.getPos();
        src.read(eName);
        if (!eName.length())
            break;
        src.reset(pos); // reset to re-read tree name
        CRemoteTreeBase *child = (CRemoteTreeBase *) create(NULL);
        child->deserializeRT(src);
        addPropTree(eName, child);
    }
}

void CRemoteTreeBase::deserializeSelfRT(MemoryBuffer &mb)
{
    deserializeSelf(mb);
    assertex(!isnocase());
    __int64 _serverId;
    mb.read(_serverId);
    if (_serverId)
        setServerId(_serverId); // ignore deserializing 0 serverId (indicated new)
}

IPropertyTree *CRemoteTreeBase::collateData()
{
    ChangeInfo *changes = queryChanges();
    struct ChangeTree
    {
        ChangeTree(IPropertyTree *donor=NULL) { ptree = LINK(donor); }
        ~ChangeTree() { ::Release(ptree); }
        inline void createTree() { assertex(!ptree); ptree = createPTree(RESERVED_CHANGE_NODE); }
        inline IPropertyTree *queryTree() { return ptree; }
        inline IPropertyTree *getTree() { return LINK(ptree); }
        inline IPropertyTree *queryCreateTree()
        {
            if (!ptree)
                ptree = createPTree(RESERVED_CHANGE_NODE);
            return ptree;
        }
    private:
        StringAttr name;
        IPropertyTree *ptree;
    } ct(changes?changes->tree:NULL);
    if (changes) changes->tree.clear();

    if (0 == serverId)
    {
        if (ct.queryTree())
        {
            ct.queryTree()->removeProp(ATTRDELETE_TAG);
            ct.queryTree()->removeProp(ATTRCHANGE_TAG);
            ct.queryTree()->removeProp(DELETE_TAG);
        }
        else
            ct.createTree();
        Owned<IAttributeIterator> iter = getAttributes();
        if (iter->count())
        {
            IPropertyTree *t = createPTree();
            ForEach(*iter)
                t->setProp(iter->queryName(), queryProp(iter->queryName()));
            ct.queryTree()->addPropTree(ATTRCHANGE_TAG, t);
        }
        ct.queryTree()->setPropBool("@new", true);
    }
    else
    {
        if (ct.queryTree())
        {
            Linked<IPropertyTree> ac = ct.queryTree()->queryPropTree(ATTRCHANGE_TAG);
            if (ac)
            {
                ct.queryTree()->removeTree(ac);
                Owned<IAttributeIterator> iter = ac->getAttributes();
                IPropertyTree *t = createPTree();
                ForEach(*iter)
                    t->setProp(iter->queryName(), queryProp(iter->queryName()));
                ct.queryTree()->addPropTree(ATTRCHANGE_TAG, t);
            }
        }
    }
    if (CPS_Changed & state || (0 == serverId && queryValue()))
    {
        ct.queryCreateTree()->setPropBool("@localValue", true);
        if (queryValue())
        {
            bool binary=isBinary(NULL);
            ((PTree *)ct.queryTree())->setValue(new CPTValue(queryValue()->queryValueRawSize(), queryValue()->queryValueRaw(), binary, true, isCompressed(NULL)), binary);
        }
        else
            ((PTree *)ct.queryTree())->setValue(new CPTValue(0, NULL, false, true, false), false);
    }
    else if (CPS_PropAppend & state)
    {
        assertex(serverId);
        IPropertyTree *pa = ct.queryTree()->queryPropTree(APPEND_TAG);
        assertex(pa);
        unsigned from = pa->getPropInt(NULL);
        ct.queryTree()->removeTree(pa);
        ct.queryCreateTree()->setPropBool("@appendValue", true);
        MemoryBuffer mb;
        bool binary=isBinary(NULL);
        queryValue()->getValue(mb, true);
        ((PTree *)ct.queryTree())->setValue(new CPTValue(mb.length()-from, mb.toByteArray()+from, binary), binary);
    }

    Owned<IPropertyTree> childTree;
    Owned<IPropertyTreeIterator> _iter = getElements("*");
    IPropertyTreeIterator *iter = _iter;
    if (iter->first())
    {
        while (iter->isValid())
        {
            CRemoteTreeBase *child = (CRemoteTreeBase *) &iter->query();
            childTree.setown(child->collateData());
            if (childTree)
            {
                if (0 == child->queryServerId())
                {
                    if (CPS_InsPos & child->queryState())
                    {
                        int pos = findChild(child);
                        assertex(NotFound != pos);
                        childTree->setPropInt("@pos", pos+1);
                    }
                }
                else
                {
                    int pos = findChild(child);
                    assertex(NotFound != pos);
                    childTree->setPropInt("@pos", pos+1);
                    childTree->setPropInt64("@id", child->queryServerId());
                }
            }
            if (childTree)
                ct.queryCreateTree()->addPropTree(RESERVED_CHANGE_NODE, childTree.getClear());
            iter->next();
        }
    }
    if (ct.queryTree())
        ct.queryTree()->setProp("@name", queryName());
    return ct.getTree();
}

void CRemoteTreeBase::clearCommitChanges(MemoryBuffer *mb)
{
    class Cop : implements IIteratorOperator
    {
    public:
        Cop(MemoryBuffer *_mb=NULL) : mb(_mb) { }
        virtual bool applyTop(IPropertyTree &_tree)
        {
            CRemoteTreeBase &tree = (CRemoteTreeBase &) _tree;
            tree.clearChanges();
            if (tree.queryState())
                tree.setState(0);
            return true;
        }
        virtual bool applyChild(IPropertyTree &parent, IPropertyTree &child, bool &levelBreak)
        {
            CRemoteTreeBase &tree = (CRemoteTreeBase &) child;
            if (mb && 0==tree.queryServerId())
            {
                __int64 serverId;
                mb->read(serverId);
                tree.setServerId(serverId);
            }
            return true;
        }
    private:
        MemoryBuffer *mb;
    } op(mb);

    CIterationOperation iop(op);
    iop.iterate(*this);
}

bool CRemoteTreeBase::queryStateChanges() const
{
    return false;
}

void CRemoteTreeBase::setServerId(__int64 _serverId)
{
    serverId = _serverId;
}

void CRemoteTreeBase::clearChildren()
{
    if (children)
    {
        children->Release();
        children=NULL;
    }
}

CRemoteTreeBase *CRemoteTreeBase::createChild(int pos, const char *childName)
{
    CRemoteTreeBase *child = (CRemoteTreeBase *) create(NULL);
    if (-1 == pos)
        child = (CRemoteTreeBase *) addPropTree(childName, child);
    else
    {
        unsigned e = 0;
        if (children)
        {
            PTree *match = (PTree *) children->query(childName);
            if (match)
            {
                IPTArrayValue *value = match->queryValue();
                e = value && value->isArray()?value->elements() : 1;
            }
        }
        if ((unsigned)pos == e)
            child = (CRemoteTreeBase *) addPropTree(childName, child);
        else
        {
            StringBuffer childPos(childName);
            childPos.append("[").append(pos+1).append("]");
            child = (CRemoteTreeBase *) addPropTree(childPos.str(), child);
        }
    }
    return child;
}

///////////

static CheckedCriticalSection suppressedOrphanUnlockCrit; // to temporarily suppress unlockall
static bool suppressedOrphanUnlock=false;

#ifdef __64BIT__
#pragma pack(push,1)    // 64bit pack CServerRemoteTree's    (could do for 32bit also)
#endif

#if defined(new)
#define __old_new new
#undef new
#endif

class CServerRemoteTree : public CRemoteTreeBase
{
    DECL_NAMEDCOUNT;
    class COrphanHandler : public ChildMap
    {
    public:
        COrphanHandler() : ChildMap() { }
        ~COrphanHandler() { kill(); }
        static void setOrphans(CServerRemoteTree &tree, bool tf)
        {
            if (tf)
                IptFlagSet(tree.flags, ipt_ext5);
            else
                IptFlagClr(tree.flags, ipt_ext5);
            IPTArrayValue *v = tree.queryValue();
            if (v && v->isArray())
            {
                unsigned e;
                for(e=0; e<v->elements(); e++)
                    setOrphans(*(CServerRemoteTree *)v->queryElement(e), tf);
            }
            if (tree.queryChildren())
            {
                if (SDSManager->queryStopped()) return;
                SuperHashIteratorOf<IPropertyTree> iter(*tree.queryChildren());
                ForEach (iter)
                    setOrphans((CServerRemoteTree &)iter.query(), tf);
            }
        }
        virtual void onAdd(void *e) // ensure memory of constructed multi value elements are no longer orphaned.
        {
            ChildMap::onAdd(e);
            CServerRemoteTree &tree = *((CServerRemoteTree *)(IPropertyTree *)e);
            setOrphans(tree, false);
        }
        virtual void onRemove(void *e)
        {
            CServerRemoteTree &tree = *((CServerRemoteTree *)(IPropertyTree *)e);
            bool dounlock;
            {
                CHECKEDCRITICALBLOCK(suppressedOrphanUnlockCrit, fakeCritTimeout);
                dounlock = !suppressedOrphanUnlock;
            }
            if (dounlock) // element is moving, don't orphan or unlock
            {
                setOrphans(tree, true);
                SDSManager->unlockAll(tree.queryServerId());
            }
            ChildMap::onRemove(e);
        }
        virtual bool replace(const char *key, IPropertyTree *tree) // provides different semantics, used if element being replaced is not to be treated as deleted.
        {
            CHECKEDCRITICALBLOCK(suppressedOrphanUnlockCrit, fakeCritTimeout);
            BoolSetBlock bblock(suppressedOrphanUnlock);
            bool ret = ChildMap::replace(key, tree);
            return ret;
        }
        virtual bool set(const char *key, IPropertyTree *tree)
        {
            // NB: be careful if replacing, to remove element first, because may self-destruct if lastref in middle of SuperHashTable::replace, leaving tablecount wrong.
            unsigned vs = getHashFromElement(tree);
            const void *fp = getFindParam(tree);
            IPropertyTree *et = (IPropertyTree *)SuperHashTable::find(vs, fp);
            if (et)
                removeExact(et);        
            return ChildMap::set(key, tree);
        }
    };
public:

#ifdef _POOLED_SERVER_REMOTE_TREE

    void * operator new(memsize_t sz)
    {
#ifdef _DEBUG
        assertex(sz==sizeof(CServerRemoteTree));
#endif
        return CServerRemoteTree_Allocator->alloc();
    }

    void operator delete( void * p )
    {
        CServerRemoteTree_Allocator->dealloc(p);
    }
#endif  
    
    CServerRemoteTree(MemoryBuffer &mb) : CRemoteTreeBase(mb) { init(); }
    CServerRemoteTree(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL)
        : CRemoteTreeBase(name, value, children) { init(); }

    ~CServerRemoteTree()
    {
        if (hasProp(NOTIFY_ATTR))
            SDSManager->handleNodeNotify(notify_delete, *this);
        __int64 index = getPropInt64(EXT_ATTR);
        if (index)
        {
            try { SDSManager->deleteExternal(index); }
            catch (IException *e)
            {
                LOG(MCoperatorWarning, unknownJob, e, StringBuffer("Deleting external reference for ").append(queryName()).str());
                e->Release();
            }
        }
        if (SDSManager->queryStopped()) return; // don't bother building up free list that will never be used hence (could get v. big/slow)
        CHECKEDCRITICALBLOCK(SDSManager->treeRegCrit, fakeCritTimeout);

        SDSManager->queryAllNodes().freeElem(serverId);
    }
    virtual bool isEquivalent(IPropertyTree *tree) { return (NULL != QUERYINTERFACE(tree, CServerRemoteTree)); }

    PDState processData(CServerConnection &connection, IPropertyTree &changeTree, MemoryBuffer &newIds);

    void init()
    {
        INIT_NAMEDCOUNT;
        assertex(!isnocase());
        CHECKEDCRITICALBLOCK(SDSManager->treeRegCrit, fakeCritTimeout);
        SDSManager->queryAllNodes().addElem(this);
    }

    virtual bool isOrphaned() const { return IptFlagTst(flags, ipt_ext5); }

    virtual void setServerId(__int64 _serverId)
    {
        if (serverId && serverId != _serverId)
            WARNLOG("Unexpected - client server id mismatch in %s, id=%"I64F"x", queryName(), _serverId);
        CRemoteTreeBase::setServerId(_serverId);
    }

    virtual CSubscriberContainerList *getSubscribers(const char *xpath, CPTStack &stack)
    {
        return SDSManager->getSubscribers(xpath, stack);
    }

    IPropertyTree *create(const char *name=NULL, IPTArrayValue *value=NULL, ChildMap *children=NULL, bool existing=false)
    {
        return new CServerRemoteTree(name, value, children);
    }

    IPropertyTree *create(MemoryBuffer &mb)
    {
        return new CServerRemoteTree(mb);
    }

    virtual void createChildMap() { children = new COrphanHandler(); }

    inline bool testExternalCandidate()
    {
        // maybe be other cases.
        return (value && value->queryValueSize() >= SDSManager->queryExternalThreshold());
    }

    void serializeCutOffRT(MemoryBuffer &tgt, int cutoff=-1, int depth=0, bool extValues=true)
    {
        serializeSelfRT(tgt, extValues);
        serializeCutOffChildrenRT(tgt, cutoff, depth, extValues);
    }

    void serializeCutOffChildrenRT(MemoryBuffer &tgt, int cutoff=-1, int depth=0, bool extValues=true)
    {
#ifndef ALWAYSLAZY_NOTUSED
        if (cutoff < 0)
        {
            if (FETCH_ENTIRE_COND == cutoff && getPropBool("@alwaysLazy"))
            {
                tgt.append("");
                return; // i.e. truncate here, this already serialized, lazy fetch children
            }
        }
        else if (getPropBool("@fetchEntire"))
            cutoff = FETCH_ENTIRE_COND;
#else
        if (cutoff >= 0 && getPropBool("@fetchEntire"))
            cutoff = FETCH_ENTIRE_COND; // NB: can change all _COND references to FETCH_ENTIRE if not using alwaysFetch anymore
#endif

        if (cutoff < 0 || depth<cutoff)
        {
            IPropertyTreeIterator *iter = getElements("*");
            iter->first();
            while (iter->isValid())
            {
                IPropertyTree *_child = &iter->query();
                CServerRemoteTree *child = QUERYINTERFACE(_child, CServerRemoteTree); assertex(child);
                child->serializeCutOffRT(tgt, cutoff, depth+1, extValues);
                iter->next();
            }
            iter->Release();

        }
        tgt.append(""); // element terminator. i.e. blank child name.
    }

    void serializeSelfRT(MemoryBuffer &mb, bool extValues)
    {
        __int64 index = getPropInt64(EXT_ATTR);
        if (index)
        {
            SDSManager->serializeExternal(index, *this, mb, extValues);
            mb.append(serverId);
        }
        else
        {
            serializeSelf(mb);
            mb.append(serverId);
        }
        byte STIInfo = 0;
        if (children && children->count())
            STIInfo += STI_HaveChildren;
        if (index)
            STIInfo += STI_External;
        mb.append(STIInfo);
    }

    virtual void deserializeSelfRT(MemoryBuffer &src)
    {
        CRemoteTreeBase::deserializeSelfRT(src);
        assertex(!isnocase());
        byte STIInfo;
        src.read(STIInfo);
    }

    virtual void removingElement(IPropertyTree *tree, unsigned pos)
    {
        COrphanHandler::setOrphans(*(CServerRemoteTree *)tree, true);       
        CRemoteTreeBase::removingElement(tree, pos);
    }

    virtual bool isCompressed(const char *xpath=NULL) const
    {
        if (isAttribute(xpath)) return false;
        if (CRemoteTreeBase::isCompressed(xpath)) return true;
        if (SDSManager->ignoreExternals) return false;
        CServerRemoteTree *child = (CServerRemoteTree *)queryPropTree(xpath);
        return child->hasProp(EXT_ATTR);
    }

    bool getProp(const char *xpath, StringBuffer &ret) const
    {
        if (xpath)
            return CRemoteTreeBase::getProp(xpath, ret);
        if (SDSManager->ignoreExternals)
            return CRemoteTreeBase::getProp(xpath, ret);
        CHECKEDCRITICALBLOCK(extCrit, fakeCritTimeout);
        __int64 index = getPropInt64(EXT_ATTR);
        if (!index)
            return CRemoteTreeBase::getProp(xpath, ret);
        MemoryBuffer mbv, mb;
        SDSManager->getExternalValue(index, mbv);
        CPTValue v(mbv);
        v.getValue(mb, true);
        unsigned len = mb.length();
        char *mem = (char *)mb.detach();
        mem[len-1] = '\0';
        ret.setBuffer(len, mem, len-1);
        return true;
    }

private:
    PDState processData(IPropertyTree &changeTree, Owned<CBranchChange> &parentBranchChange, MemoryBuffer &newIds);
    PDState checkChange(IPropertyTree &tree, CBranchChange *parentBranchChange=NULL);
friend class COrphanHandler;
};

#if defined(_WIN32) && defined(__old_new)
#define new __old_new
#endif


#ifdef __64BIT__
#pragma pack(pop)   // 64bit pack CServerRemoteTree's    (could do for 32bit also)
#endif


void populateWithServerIds(IPropertyTree *matchParent, CRemoteTreeBase *parent)
{
    matchParent->setPropInt64("@serverId", parent->queryServerId());
    Owned<IPropertyTreeIterator> iter = matchParent->getElements("*");
    ForEach (*iter)
    {
        IPropertyTree *matchChild = &iter->query();
        StringBuffer path(matchChild->queryName());
        path.append("[").append(matchChild->queryProp("@pos")).append("]");
        CRemoteTreeBase *child = (CRemoteTreeBase *)parent->queryPropTree(path.str());
        assertex(child);
        populateWithServerIds(matchChild, child);
    }
}

IPropertyTree *createServerTree(const char *tag=NULL)
{
    return new CServerRemoteTree(tag);
}

// JCSMORE - these should be error conditions, for consistency with previous release not so for now.
#define consistencyCheck(TEXT, IDTREE, LOCALTREE, PATH, PARENTNAME, ID)                                                                     \
    if (!IDTREE)                                                                                                            \
    {                                                                                                                       \
        StringBuffer s(TEXT": Consistency check failure, id'd tree not found: ");                                           \
        s.append(PATH).append(", id=").append(ID);                                              \
        LOG(MCoperatorWarning, unknownJob, s.str());                                                                        \
    }                                                                                                                       \
    else if (!LOCALTREE)                                                                                                    \
    {                                                                                                                       \
        StringBuffer s(TEXT": Consistency check failure, positional property specification: ");                             \
        s.append(PATH).append(", in client update not found in parent tree: ").append(PARENTNAME);              \
        LOG(MCoperatorWarning, unknownJob, s.str());                                                                        \
    }                                                                                                                       \
    else if (IDTREE != LOCALTREE)                                                                                           \
    {                                                                                                                       \
        StringBuffer s(TEXT": Consistency check failure, positional property specification does not match id'd tree, prop=");\
        s.append(PATH);                                                                                         \
        LOG(MCoperatorWarning, unknownJob, s.str());                                                                        \
    }

PDState CServerRemoteTree::checkChange(IPropertyTree &changeTree, CBranchChange *parentBranchChange)
{
    PDState res = PDS_None;

    bool checkExternal = false;
    Owned<IPropertyTreeIterator> iter = changeTree.getElements("*");
    ICopyArrayOf<IPropertyTree> toremove;
    ForEach(*iter)
    {
        IPropertyTree &e = iter->query();
        const char *name = e.queryName();
        switch (name[0])
        {
            case 'R':
            {
                IPropertyTree *idTree = SDSManager->queryRegisteredTree(e.getPropInt64("@id"));
                if (!idTree)
                    throw MakeSDSException(SDSExcpt_OrphanedNode, "renaming %s to %s", e.queryProp("@from"), e.queryProp("@to"));
#ifdef SIBLING_MOVEMENT_CHECK
                StringBuffer localTreePath(e.queryProp("@from"));
                localTreePath.append('[').append(e.getPropInt("@pos")).append(']');
                IPropertyTree *localTree = queryPropTree(localTreePath.str());
                consistencyCheck("RENAME", idTree, localTree, localTreePath.str(), queryName(), e.getPropInt64("@id"));
#endif
                int pos = findChild(idTree);
                if (renameTree(idTree, e.queryProp("@to")))
                {
                    e.setPropInt("@pos", pos+1);
                    mergePDState(res, PDS_Structure);
                    if (parentBranchChange)
                    {
                        PDState _res = res;
                        mergePDState(_res, PDS_Renames);
                        Owned<CBranchChange> childChange = new CBranchChange(*(CRemoteTreeBase *)idTree);
                        childChange->noteChange(_res, _res);
                        childChange->Link();
                        parentBranchChange->addChildBranch(*childChange);
                    }
                }
                else
                {
                    toremove.append(e);
                    continue;
                }
                break;
            }
            case 'D':
            {
                IPropertyTree *idTree = SDSManager->queryRegisteredTree(e.getPropInt64("@id"));
                if (!idTree)
                {
                    toremove.append(e);
                    continue;
                }
#ifdef SIBLING_MOVEMENT_CHECK
                StringBuffer localTreePath(e.queryProp("@name"));
                localTreePath.append('[').append(e.getPropInt("@pos")).append(']');
                IPropertyTree *localTree = queryPropTree(localTreePath.str());
                consistencyCheck("DELETE", idTree, localTree, localTreePath.str(), queryName(), e.getPropInt64("@id"));
#endif
                int pos = findChild(idTree);
                if (NotFound == pos)
                {
                    toremove.append(e);
                    continue;
                }
                e.setPropInt("@pos", pos+1);
                Owned<CBranchChange> childChange = new CBranchChange(*(CRemoteTreeBase *)idTree);
                if (!removeTree(idTree))
                    throw MakeSDSException(-1, "::checkChange - Failed to remove child(%s) from parent(%s) at %s(%d)", idTree->queryName(), queryName(), __FILE__, __LINE__);
                mergePDState(res, PDS_Deleted);
                if (parentBranchChange)
                {
                    PDState _res = res;
                    mergePDState(_res, PDS_Deleted);
                    childChange->noteChange(_res, _res);
                    childChange->Link();
                    parentBranchChange->addChildBranch(*childChange);
                }
                break;
            }
            case 'A':
            {
                switch (name[1])
                {
                    case 'D':
                    {
                        Owned<IAttributeIterator> iter = e.getAttributes();
                        ForEach(*iter)
                        {
                            if (removeAttr(iter->queryName()))
                                mergePDState(res, PDS_Data);
                        }
                        break;
                    }
                    case 'C':
                    {
                        Owned<IAttributeIterator> iter = e.getAttributes();
                        ForEach(*iter)
                            setProp(iter->queryName(), iter->queryValue());
                        mergePDState(res, PDS_Data);
                        break;
                    }
                    default:
                        throwUnexpected();
                }
                break;
            }
            case 'T':
                break;
        }
    }
    ForEachItemIn(tr, toremove)
        changeTree.removeTree(&toremove.item(tr));
    if (changeTree.getPropBool("@localValue"))
    {
        bool binary=changeTree.isBinary(NULL);
        IPTArrayValue *v = ((PTree &)changeTree).queryValue();
        setValue(v?new CPTValue(v->queryValueRawSize(), v->queryValueRaw(), binary, true, v->isCompressed()):NULL, binary);
        if (changeTree.getPropBool("@new"))
            mergePDState(res, PDS_New);
        mergePDState(res, PDS_Data);
        checkExternal = true;
    }
    else if (changeTree.getPropBool("@appendValue"))
    {
        bool binary=changeTree.isBinary(NULL);
        if (binary != isBinary(NULL))
            throw MakeSDSException(-1, "Error attempting to append binary and non-binary data together, in node: %s", queryName());
        __int64  index = getPropInt64(EXT_ATTR);
        MemoryBuffer mb;
        if (index)
        {
            MemoryBuffer mbv;
            SDSManager->getExternalValue(index, mbv);
            CPTValue v(mbv);
            v.getValue(mb, binary);
        }
        else
            getPropBin(NULL, mb);
        changeTree.getPropBin(NULL, mb);
        if (binary)
            setPropBin(NULL, mb.length(), mb.toByteArray());
        else
        {
            mb.append('\0');
            setProp(NULL, (const char *)mb.toByteArray());
        }
        mergePDState(res, PDS_Data);
        checkExternal = true;
    }
    if (checkExternal)
    {
        __int64 index = getPropInt64(EXT_ATTR);
        if (index)
        {
            bool r = false;
            if (!testExternalCandidate())
            {
                SDSManager->deleteExternal(index); // i.e. no longer e.g. now less than threshold.
                r = removeProp(EXT_ATTR);
            }
            else
                SDSManager->writeExternal(*this, false, index);
            if (r)
            {
                IPropertyTree *t = changeTree.queryPropTree(ATTRDELETE_TAG);
                if (!t)
                    t = changeTree.addPropTree(ATTRDELETE_TAG, createPTree());
                t->addProp(EXT_ATTR, "");
            }
            else
                changeTree.setProp(NULL, (const char *)NULL);
        }
        else if (testExternalCandidate())
        {
            try
            {
                SDSManager->writeExternal(*this);
                IPropertyTree *t = changeTree.queryPropTree(ATTRCHANGE_TAG);
                if (!t)
                    t = changeTree.addPropTree(ATTRCHANGE_TAG, createPTree());
                changeTree.setProp(NULL, (const char *)NULL);
                t->setProp(EXT_ATTR, queryProp(EXT_ATTR));
            }
            catch (IException *)
            {
                setProp(NULL, NULL); // in the event of failure during externalization, lose the value
                throw;
            }
        }
    }
    return res;
}

PDState CServerRemoteTree::processData(CServerConnection &connection, IPropertyTree &changeTree, MemoryBuffer &newIds)
{
    Owned<CBranchChange> top;
    PDState res = processData(changeTree, top, newIds);
    changeTree.removeProp("@name");
    if (res)
        SDSManager->writeTransactions++;
    // return asap from here, don't even wait for pool threads to queue, can take time.

    if (res && !RTM_MODE(connection.queryMode(), RTM_INTERNAL))
    {
        CPTStack stack = connection.queryPTreePath();
        if (connection.queryRoot() == (IPropertyTree *)SDSManager->queryRoot())
            stack.pop();

        connection.notify();
        SDSManager->startNotification(changeTree, stack, *top);
    }

    return res;
}

PDState CServerRemoteTree::processData(IPropertyTree &changeTree, Owned<CBranchChange> &parentBranchChange, MemoryBuffer &newIds)
{
    Owned<CBranchChange> branchChange = new CBranchChange(*this);

    PDState localChange, res;
    localChange = res = checkChange(changeTree, branchChange);
    Owned<IPropertyTreeIterator> iter = changeTree.getElements(RESERVED_CHANGE_NODE);
    if (iter->first())
    {
        bool levelNotified = false;
        do
        {
            IPropertyTree &childChanges = iter->query();
            CServerRemoteTree *child;
            StringAttr childName;

            if (childChanges.getPropBool("@new"))
            {
                child = (CServerRemoteTree *)createChild(childChanges.getPropInt("@pos", -1), childChanges.queryProp("@name"));
                mergePDState(res, PDS_Structure);
                newIds.append(child->queryServerId());
                mergePDState(state, PDS_Added);
            }
            else
            {
                child = (CServerRemoteTree *) SDSManager->queryRegisteredTree(childChanges.getPropInt64("@id"));
#ifdef SIBLING_MOVEMENT_CHECK
                StringBuffer localTreePath(childChanges.queryProp("@name"));
                localTreePath.append('[').append(childChanges.getPropInt("@pos")).append(']');
                CRemoteTreeBase *localTree = (CRemoteTreeBase *) queryPropTree(localTreePath.str());
                consistencyCheck("PROP UPDATE", child, localTree, localTreePath.str(), queryName(), childChanges.getPropInt64("@id"))
#endif
                if (NULL == child)
                    throw MakeSDSException(SDSExcpt_ClientCacheDirty, "child(%s)  not found in parent(%s) at %s(%d)", childChanges.queryProp("@name"), queryName(), __FILE__, __LINE__);
                int pos = findChild(child);
                if (NotFound == pos)
                    throw MakeSDSException(SDSExcpt_ClientCacheDirty, "child(%s) not found in parent(%s) at %s(%d)", child->queryName(), queryName(), __FILE__, __LINE__);
                childChanges.setPropInt("@pos", pos+1);
            }
            if (!levelNotified)
            {
                levelNotified = true;
                branchChange->noteChange(localChange, res);
            }
            mergePDState(res, child->processData(childChanges, branchChange, newIds));
        }
        while (iter->next());
    }
    else
        branchChange->noteChange(localChange, res);

    if (!parentBranchChange.get())
        parentBranchChange.setown(branchChange.getClear());
    else
        parentBranchChange->addChildBranch(*branchChange.getClear());
    return res;
}

/////////////////

class CPendingLockBlock
{
    CLockInfo &lockInfo;
public:
    CPendingLockBlock(CLockInfo &_lockInfo);
    ~CPendingLockBlock();
};

typedef Int64Array IdPath;
typedef MapBetween<ConnectionId, ConnectionId, LockData, LockData> ConnectionInfoMap;
#define LOCKSESSCHECK (1000*60*5)
class CLockInfo : public CInterface, implements IInterface
{
    DECL_NAMEDCOUNT;

    CLockInfoTable &table;
    unsigned sub, readLocks, pending, waiting;
    IdPath idPath;
    ConnectionInfoMap connectionInfo;
    CheckedCriticalSection crit;
    Semaphore sem;
    StringAttr xpath;
    __int64 treeId;
    bool exclusive;
    Linked<CServerRemoteTree> parent, child;
    
#ifdef _DEBUG
    DebugInfo debugInfo;
#endif

    bool validateConnectionSessions()
    {
        PROGLOG("validateConnectionSessions");
        bool ret = false;
        try
        {
            IArrayOf<IMapping> entries;
            {
                CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);    
                HashIterator iter(connectionInfo);
                ForEach (iter)
                    entries.append(*LINK(&iter.query()));
            }
            ForEachItemIn(e, entries)
            {
                IMapping &imap = entries.item(e);
                LockData *lD = connectionInfo.mapToValue(&imap);            
                Owned<INode> node = querySessionManager().getProcessSessionNode(lD->sessId);
                if (node)
                {
                    SessionId nodeSessId = querySessionManager().lookupProcessSession(node);
                    if (nodeSessId)
                    {
                        if (lD->sessId != nodeSessId)
                        {
                            StringBuffer out("Removing stale connection session [");
                            out.appendf("%"I64F"x], connectionId [%"I64F"x]", lD->sessId, * ((ConnectionId *) imap.getKey()));
                            out.append(" xpath [").append(xpath).append("]");
                            PROGLOG("%s", out.str());
                            querySessionManager().stopSession(lD->sessId, true);
                            ret = true;
                        }
                        else
                        {
                            StringBuffer nodeStr;
                            node->endpoint().getUrlStr(nodeStr);
                            PROGLOG("Validating connection to %s", nodeStr.str());
                            if (!queryWorldCommunicator().verifyConnection(node, LOCKSESSCHECK))
                            {
                                StringBuffer out("Terminating connection session to ");
                                out.append(nodeStr);
                                out.append(" [");
                                out.appendf("%"I64F"x], connectionId [%"I64F"x]", lD->sessId, * ((ConnectionId *) imap.getKey()));
                                out.append(" xpath [").append(xpath).append("]");
                                PROGLOG("%s", out.str());
                                queryCoven().disconnect(node);
                                ret = true;
                            }
                        }
                    }
                }
            }
        }
        catch (IException *e)
        {
            EXCLOG(e, "validateConnectionSessions");
            e->Release();
        }
        PROGLOG("validateConnectionSessions done");
        return ret;
    }

public:
    IMPLEMENT_IINTERFACE;

    CLockInfo(CLockInfoTable &_table, __int64 _treeId, IdPath &_idPath, const char *_xpath, unsigned mode, ConnectionId id, SessionId sessId)
        : table(_table), treeId(_treeId), xpath(_xpath), exclusive(false), sub(0), readLocks(0), waiting(0), pending(0)
    {
        INIT_NAMEDCOUNT;
        verifyex(tryLock(mode, id, sessId));
        ForEachItemIn(i, _idPath)
            idPath.append(_idPath.item(i));
    }

    ~CLockInfo()
    {
        if (parent)
            clearLastRef();
    }

    inline void clearLastRef();

    bool querySub() { return (0 != sub); }
    const void *queryFindParam() const
    {
        return (const void *) &treeId;
    }

    bool matchHead(IdPath &_idPath)
    {
        unsigned o = idPath.ordinality();
        ForEachItemIn(i, _idPath)
        {
            if (i>=o) return false;
            else if (idPath.item(i) != _idPath.item(i))
                return false;
        }
        return true;
    }

    bool unlock(ConnectionId id)
    {
        bool ret = false;
        CPendingLockBlock b(*this); // carefully placed, removePending can destroy this, therefore must be destroyed last
        {
            CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);    
            LockData *ld = connectionInfo.getValue(id);
            if (ld)    // not necessarily any lock info for this connection
            {
                switch (ld->mode & RTM_LOCKBASIC_MASK)
                {
                    case RTM_LOCK_READ:
                        assertex(readLocks);
                        readLocks--;
                        break;

                    case (RTM_LOCK_WRITE+RTM_LOCK_READ):
                    case RTM_LOCK_WRITE:
                        assertex(exclusive && 0 == readLocks);
                        exclusive = false;
#ifdef _DEBUG
                        debugInfo.clearExclusive();
#endif
                        break;
                    case 0: // no locking
                        break;
                    default:
                        assertex(false);
                }
                if (RTM_LOCK_SUB & ld->mode)
                    sub--;
                connectionInfo.remove(id);
                if (parent && 0 == connectionInfo.count())
                {
                    clearLastRef();
                    ret = true;
                }
            }
            wakeWaiting();
        }
        return ret;
    }

    void unlockAll()
    {
        CPendingLockBlock b(*this); // carefully placed, removePending can destroy this.
        { CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
            HashIterator iter(connectionInfo);
            while (iter.first())
            {
                IMapping &map = iter.query();
                ConnectionId id = *(ConnectionId *)map.getKey();
                unlock(id);
            }
        }
    }

    inline void addPending() { CHECKEDCRITICALBLOCK(crit, fakeCritTimeout); pending++; }
    inline void removePending()
    { 
        Linked<CLockInfo> destroy;
        {
            CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
            pending--;
            if (0 == (lockCount()+pending))
            {
                destroy.set(this);
                table.removeExact(this);
            }
        }
    }

    bool _lock(unsigned mode, unsigned timeout, ConnectionId id, SessionId sessionId, IUnlockCallback &callback, bool change=false)
    {
        class CLockCallbackUnblock
        {
        public:
            CLockCallbackUnblock(IUnlockCallback &_callback) : callback(_callback) { callback.unblock(); }
            ~CLockCallbackUnblock() { callback.block(); }
        private:
            IUnlockCallback &callback;
        };

        if (INFINITE == timeout)
        {
            loop
            {
                if (!SDSManager->queryConnection(id)) return false;
                if (tryLock(mode, id, sessionId, change))
                    return true;
                else
                {
                    bool timedout = false;
                    waiting++;
                    {
                        CHECKEDCRITICALUNBLOCK(crit, fakeCritTimeout);
                        CLockCallbackUnblock cb(callback);
                        timedout = !sem.wait(LOCKSESSCHECK);
                    }
                    if (timedout)
                    {
                        if (!sem.wait(0))
                        {
                            waiting--;
                            StringBuffer s("Infinite timeout lock still waiting: ");
                            getLockInfo(s);
                            PROGLOG("%s", s.str());
                        }
                        {
                            CHECKEDCRITICALUNBLOCK(crit, fakeCritTimeout);
                            CLockCallbackUnblock cb(callback);
                            validateConnectionSessions();
                        }
                    }
                }
            }
        }
        else
        {
            CTimeMon tm(timeout);
            loop
            {
                if (!SDSManager->queryConnection(id)) return false;
                if (tryLock(mode, id, sessionId, change))
                    return true;
                else
                {
                    bool timedout = false;
                    waiting++;
                    {
                        CHECKEDCRITICALUNBLOCK(crit, fakeCritTimeout);
                        CLockCallbackUnblock cb(callback);
                        unsigned remaining;
                        if (tm.timedout(&remaining) || !sem.wait(remaining>LOCKSESSCHECK?LOCKSESSCHECK:remaining))
                            timedout = true;
                    }
                    if (timedout) { 
                        if (!sem.wait(0))
                            waiting--;  //// only dec waiting if waiting wasn't signalled.

                        bool disconnects;
                        {
                            CHECKEDCRITICALUNBLOCK(crit, fakeCritTimeout);
                            CLockCallbackUnblock cb(callback);
                            disconnects = validateConnectionSessions();
                        }
                        if (tm.timedout())
                        {
                            if (disconnects) // if some sessions disconnected, one final try
                            {
                                if (!SDSManager->queryConnection(id)) return false;
                                if (tryLock(mode, id, sessionId, change))
                                    return true;
                            }
                            break;
                        }
                    }
                    // have to very careful here, have regained crit locks but have since timed out
                    // therefore before gaining crits after signal (this lock was unlocked)
                    // other thread can grab lock at this time, but this thread can't cause others to increase 'waiting' at this time.
                    // and not after crit locks regained.
                    if (tm.timedout())
                        break;
                }
            }
        }
        return false;
    }

    bool lock(unsigned mode, unsigned timeout, ConnectionId id, SessionId sessionId, IUnlockCallback &callback)
    {
        bool ret = false;
        CPendingLockBlock b(*this); // carefully placed, removePending can destroy this, therefore must be destroyed last
        { CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
            return _lock(mode, timeout, id, sessionId, callback);
        }
        return false;
    }

    bool tryLock(unsigned mode, ConnectionId id, SessionId sessId, bool changingMode=false)
    {
        CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
        LockData *existingLd = NULL;
        bool hadReadLock = false;
        if (changingMode)
        {
            existingLd = connectionInfo.getValue(id);
            if (existingLd)
            {
                if ((existingLd->mode & RTM_LOCKBASIC_MASK) == (mode & RTM_LOCKBASIC_MASK))
                    return true; // nothing to do
                // record and unlock existing state
                switch (existingLd->mode & RTM_LOCKBASIC_MASK)
                {
                    case RTM_LOCK_READ:
                        readLocks--;
                        hadReadLock = true;
                        break;
                    case (RTM_LOCK_WRITE+RTM_LOCK_READ):
                    case RTM_LOCK_WRITE:
                        exclusive = false;
                        // change will succeed
                        break;
                    case 0: // no locking
                        break;
                    default:
                        assertex(false);
                }
            }
            else
                changingMode = false; // nothing to restore in event of no change
        }
        
        switch (mode & RTM_LOCKBASIC_MASK)
        {
            case 0:
            {
                if (changingMode)
                    break;
                return true;
            }
            case RTM_LOCK_READ: // cannot fail if changingMode=true (exclusive will have been unlocked)
                if (exclusive)
                    return false;
                readLocks++;
                break;
            case (RTM_LOCK_WRITE+RTM_LOCK_READ):
            case RTM_LOCK_WRITE:
                if (exclusive || readLocks)
                {
                    if (changingMode)
                    {
                        // only an unlocked read lock can fail and need restore here.
                        if (hadReadLock)
                            readLocks++;
                    }
                    return false;
                }
                exclusive = true;
#ifdef _DEBUG
                debugInfo.ExclOwningThread = GetCurrentThreadId();
                debugInfo.ExclOwningConnection = id;
                debugInfo.ExclOwningSession = sessId;
#endif
                break;
            default:
                assertex(false);
        }
        if (changingMode)
        {
            existingLd->mode = mode;
            wakeWaiting();
        }
        else
        {
            if (RTM_LOCK_SUB & mode)
                sub++;
            LockData ld;            
            ld.mode = mode;
            ld.sessId = sessId;
            ld.timeLockObtained = msTick();
            connectionInfo.setValue(id, ld);
        }
        return true;
    }

    inline void wakeWaiting()
    {
        if (waiting)
        {
            sem.signal(waiting); // get blocked locks to recheck.
            waiting=0;
        }
    }

    void changeMode(ConnectionId id, SessionId sessionId, unsigned newMode, unsigned timeout, IUnlockCallback &callback)
    {
        CPendingLockBlock b(*this); // carefully placed, removePending can destroy this.
        CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
        if (!_lock(newMode, timeout, id, sessionId, callback, true))
        {
            StringBuffer s;
            throw MakeSDSException(SDSExcpt_LockTimeout, "Lock timeout performing changeMode on connection to : %s, existing lock info: %s", xpath.get(), getLockInfo(s).str());
        }
    }

    unsigned lockCount()
    {
        CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
        return connectionInfo.count();
    }

    bool associated(ConnectionId connectionId)
    {
        CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
        return NULL!=connectionInfo.getValue(connectionId);
    }

    const char *queryXPath() const { return xpath; }
    StringBuffer &getLockInfo(StringBuffer &out)
    {
        unsigned nlocks=0;
        MemoryBuffer locks;
        UInt64Array keys;
        {
            CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
            HashIterator iter(connectionInfo);
            ForEach(iter)
            {
                IMapping &imap = iter.query();
                LockData *lD = connectionInfo.mapToValue(&imap);
                keys.append(* ((ConnectionId *) imap.getKey()));
                locks.append(sizeof(LockData), lD);
                ++nlocks;
            }
        }

        unsigned msNow = msTick();
        out.append("Locks on path: /").append(xpath).newline();
        out.append("Endpoint            |SessionId       |ConnectionId    |mode    |time(duration)]").newline().newline();
        unsigned l = 0;
        if (nlocks)
        {
            loop
            {
                LockData lD;
                memcpy(&lD, ((const byte *)locks.toByteArray())+l*sizeof(LockData), sizeof(LockData));
                ConnectionId connId = keys.item(l);

                StringBuffer sessEpStr;
                unsigned lockedFor = msNow-lD.timeLockObtained;
                CDateTime time;
                time.setNow();
                time_t tt = time.getSimple() - (lockedFor/1000);
                time.set(tt);
                StringBuffer timeStr;
                time.getString(timeStr);
                out.appendf("%-20s|%-16"I64F"x|%-16"I64F"x|%-8x|%s(%d ms)", querySessionManager().getClientProcessEndpoint(lD.sessId, sessEpStr).str(), lD.sessId, connId, lD.mode, timeStr.str(), lockedFor);
                ++l;
                if (l>=nlocks)
                    break;
                out.newline();
            }
        }
        out.newline();
        return out;
    }

    void setDROLR(CServerRemoteTree *_parent, CServerRemoteTree *_child)
    {
        CHECKEDCRITICALBLOCK(crit, fakeCritTimeout);
        if (parent)
        {
            assertex(parent.get() == _parent);
            assertex(child.get() == _child);
            return;
        }
        parent.set(_parent);
        child.set(_child);
    }
};

CPendingLockBlock::CPendingLockBlock(CLockInfo &_lockInfo) : lockInfo(_lockInfo)
{
    lockInfo.addPending();
}

CPendingLockBlock::~CPendingLockBlock()
{
    lockInfo.removePending();
}

typedef ICopyArrayOf<CLockInfo> CLockInfoArray;

///////////

template <> void CLockInfoTable::onRemove(void *et)
{
    ((CLockInfo*)et)->Release();
}


///////////////

CSDSTransactionServer::CSDSTransactionServer(CCovenSDSManager &_manager)
 : Thread("SDS Manager, CSDSTransactionServer"), manager(_manager), CTransactionLogTracker(DAMP_SDSCMD_MAX)
{
    stopped = true;
}

int CSDSTransactionServer::run()
{
    ICoven &coven=queryCoven();
    CMessageHandler<CSDSTransactionServer> handler("CSDSTransactionServer",this,&CSDSTransactionServer::processMessage, &manager, 100, TIMEOUT_ON_CLOSEDOWN);
    stopped = false;
    CMessageBuffer mb;
    while (!stopped)
    {
        try
        {
#ifdef TRACE_QWAITING
            unsigned waiting = coven.probe(RANK_ALL,MPTAG_DALI_SDS_REQUEST,NULL);
            static unsigned lastwaiting = 0;
            static unsigned lasttick = 0;
            if ((waiting>lastwaiting+25)||(waiting<lastwaiting/2)||
                ((waiting>100)&&(msTick()-lasttick>1000))) {
                DBGLOG("QPROBE: MPTAG_DALI_SDS_REQUEST has %d waiting",waiting);
                lastwaiting = waiting;
                lasttick = msTick();
            }
#endif
            mb.clear();
            if (coven.recv(mb, RANK_ALL, MPTAG_DALI_SDS_REQUEST, NULL))
            {
                msgCount++;
                try
                {
                    SdsCommand action;
                    mb.read((int &)action);
                    action = (SdsCommand) (((unsigned)action) & ~DAMP_SDSCMD_LAZYEXT);
                    switch (action)
                    {
                        case DAMP_SDSCMD_CONNECT:
                        case DAMP_SDSCMD_MCONNECT:
                        case DAMP_SDSCMD_GETCHILDREN:
                        case DAMP_SDSCMD_GETCHILDREN2:
                        case DAMP_SDSCMD_GET:
                        case DAMP_SDSCMD_GET2:
                        case DAMP_SDSCMD_GETELEMENTS:
                        case DAMP_SDSCMD_DATA:
                        case DAMP_SDSCMD_CHANGEMODE:
                        case DAMP_SDSCMD_GETXPATHS:
                        case DAMP_SDSCMD_GETXPATHSPLUSIDS:
                        case DAMP_SDSCMD_GETEXTVALUE:
                        case DAMP_SDSCMD_GETELEMENTSRAW:
                        case DAMP_SDSCMD_GETCOUNT:
                        {
                            mb.reset();
                            handler.handleMessage(mb);
                            mb.clear(); // ^ has copied mb
                            break;
                        }
                        case DAMP_SDSCMD_GETSTORE:
                        {
                            TimingBlock xactTimingBlock(xactTimingStats);
                            CServerRemoteTree *root = manager.queryRoot();
                            mb.clear();
                            if (root)
                            {
                                mb.append(DAMP_SDSREPLY_OK);
                                root->serializeCutOffRT(mb);
                            }
                            else
                                mb.append(DAMP_SDSREPLY_EMPTY);
                            break;
                        }
#ifdef LEGACY_CLIENT_RESPONSE
                        // give a re
                        case DAMP_SDSCMD_VERSION:
                        {
                            TimingBlock xactTimingBlock(xactTimingStats);
                            mb.clear().append(DAMP_SDSREPLY_ERROR);
                            throw MakeStringException(-1, "Client too old to communicate with this dali");
                        }
#endif
                        case DAMP_SDSCMD_DIAGNOSTIC:
                        {
                            TimingBlock xactTimingBlock(xactTimingStats);
                            SdsDiagCommand cmd;
                            mb.read((int &)cmd);
                            switch (cmd)
                            {
                                case DIAG_CMD_LOCKINFO:
                                {
                                    StringBuffer out;
                                    SDSManager->getLocks(out);
                                    mb.clear().append(DAMP_SDSREPLY_OK);
                                    mb.append(out.length());
                                    mb.append(out.length(), out.toCharArray());

                                    break;
                                }
                                case DIAG_CMD_STATS:
                                {
                                    mb.clear().append(DAMP_SDSREPLY_OK);
                                    SDSManager->collectUsageStats(mb);
                                    break;
                                }
                                case DIAG_CMD_CONNECTIONS:
                                {
                                    mb.clear().append(DAMP_SDSREPLY_OK);
                                    SDSManager->collectConnections(mb);
                                    break;
                                }
                                case DIAG_CMD_SUBSCRIBERS:
                                {
                                    mb.clear().append(DAMP_SDSREPLY_OK);
                                    SDSManager->collectSubscribers(mb);
                                    break;
                                }
                                default:
                                    assertex(false);
                            }
                            break;
                        }
                        case DAMP_SDSCMD_GETPROPS:
                        {
                            mb.clear().append(DAMP_SDSREPLY_OK);
                            manager.queryProperties().serialize(mb);
                            break;
                        }
                        case DAMP_SDSCMD_UPDTENV:
                        {
                            Owned<IPropertyTree> newEnv = createPTree(mb);
                            bool forceGroupUpdate;
                            mb.read(forceGroupUpdate);
                            StringBuffer response;
                            bool result = manager.updateEnvironment(newEnv, forceGroupUpdate, response);
                            mb.clear().append(DAMP_SDSREPLY_OK).append(result).append(response);
                            break;
                        }
                        default:
                            throw MakeSDSException(SDSExcpt_UnrecognisedCommand, "%d", action);
                    }
                }
                catch (IException *e)
                {
                    mb.clear();
                    mb.append((int) DAMP_SDSREPLY_ERROR);
                    mb.append(e->errorCode());
                    StringBuffer s;
                    mb.append(e->errorMessage(s).str());
                    StringBuffer clientUrl("EXCEPTION in reply to client ");
                    mb.getSender().getUrlStr(clientUrl);
                    EXCLOG(e, clientUrl.str(), MSGCLS_warning);
                    e->Release();
                }
                if (mb.length())
                {
                    try { coven.reply(mb); }
                    catch (IJSOCK_Exception *e)
                    {
                        LOG(MCwarning, unknownJob, e, "Failed to reply to client (CSDSTransactionServer thread)");
                        e->Release();
                    }
                    catch (IMP_Exception *e)
                    {
                        LOG(MCwarning, unknownJob, e, "Failed to reply to client (CSDSTransactionServer thread)");
                        e->Release();
                    }
                }
            }
            else
                stopped = true;
        }
        catch (IException *e)
        {
            StringBuffer s("Failure receiving message from client ");
            mb.getSender().getUrlStr(s);
            LOG(MCwarning, unknownJob, e, s.str());
            e->Release();
        }
    }
    return 0;
}

// backward compat.
bool checkOldFormat(CServerRemoteTree *parentServerTree, IPropertyTree *tree, MemoryBuffer &mb)
{
    CPState state;
    mb.read((int &)state);

    bool change = false;
    if (state)
    {
        if (CPS_Renames & state)
        {
            loop
            {
                __int64 id;
                mb.read(id);
                if (0 == id)
                    break;
                StringAttr newName;
                mb.read(newName);
                IPropertyTree *child = SDSManager->queryRegisteredTree(id);
                if (child)
                {
                    assertex(parentServerTree);
                    int pos = parentServerTree->findChild(child);
                    if (NotFound == pos)
                        throw MakeSDSException(SDSExcpt_ClientCacheDirty, "::checkChange - child(%s) not found in parent(%s) at %s(%d)", child->queryName(), parentServerTree->queryName(), __FILE__, __LINE__);

                    IPropertyTree *t = createPTree();
                    t->setProp("@from", child->queryName());
                    t->setProp("@to", newName);
                    t->setPropInt64("@id", id);
#ifdef SIBLING_MOVEMENT_CHECK
                    t->setProp("@pos", pos);
#endif
                    tree->addPropTree(RENAME_TAG, t);
                    change = true;
                }
            }
        }

        if (CPS_Deletions & state)
        {
            loop
            {
                __int64 id;
                mb.read(id);
                if (0 == id)
                    break;

                IPropertyTree *child = SDSManager->queryRegisteredTree(id);
                if (child)
                {
                    assertex(parentServerTree);
                    int pos = parentServerTree->findChild(child);
                    if (NotFound == pos)
                        continue;

                    IPropertyTree *t = createPTree();
                    t->setProp("@name", child->queryName());
                    t->setPropInt64("@id", id);
#ifdef SIBLING_MOVEMENT_CHECK
                    t->setPropInt("@pos", pos+1);
#endif
                    tree->addPropTree(DELETE_TAG, t);
                    change = true;
                }
            }
        }

        if (CPS_AttrDeletions & state)
        {
            unsigned count, c;
            mb.read(count);
            if (count)
            {
                IPropertyTree *ct = tree->queryPropTree(ATTRCHANGE_TAG);
                IPropertyTree *t = tree->addPropTree(ATTRDELETE_TAG, createPTree());
                for (c=0; c<count; c++)
                {
                    StringAttr attr;
                    mb.read(attr);
                    if (ct) ct->removeProp(attr);
                    t->addProp(attr, "");
                }
                change = true;
            }
        }

        if (CPS_Changed & state)
        {
            Owned<PTree> clientTree = new LocalPTree();
            clientTree->deserializeSelf(mb);
            __int64 serverId;
            mb.read(serverId);
            byte STIInfo;
            mb.read(STIInfo);

            tree->setPropBool("@localValue", true);
            if (clientTree->queryValue())
            {
                bool binary = clientTree->isBinary(NULL);
                IPTArrayValue *v = ((PTree *)clientTree)->detachValue();
                ((PTree *)tree)->setValue(v, binary);
            }
            else
                ((PTree *)tree)->setValue(new CPTValue(0, NULL, false, true, false), false);

            Owned<IAttributeIterator> attrs = clientTree->getAttributes();
            IPropertyTree *t = createPTree();
            if (attrs->first())
            {
                do
                {
                    t->setProp(attrs->queryName(), clientTree->queryProp(attrs->queryName()));
                }
                while (attrs->next());
                tree->addPropTree(ATTRCHANGE_TAG, t);
            }
            change = true;
        }
    }
    return change;
}

bool translateOldFormat(CServerRemoteTree *parentServerTree, IPropertyTree *parentTree, MemoryBuffer &mb)
{
    bool change = checkOldFormat(parentServerTree, parentTree, mb);

    bool hasChildren;
    mb.read(hasChildren);
    if (hasChildren)
    {
        loop
        {
            __int64 id;
            int pos = -1;
            mb.read(id);
            if (NoMoreChildrenMarker == id)
                break;
            mb.read(pos);
            CServerRemoteTree *serverTree = NULL;
            Owned<IPropertyTree> tree = createPTree(RESERVED_CHANGE_NODE);
            if (0 == id)
            {
                StringAttr childName;
                mb.read(childName);

                tree->setPropBool("@new", true);
                tree->setProp("@name", childName);
                if (-1 != pos)
                    tree->setPropInt("@pos", pos+1);
            }
            else
            {
                assertex(parentServerTree);
                serverTree = (CServerRemoteTree *) SDSManager->queryRegisteredTree(id);
                assertex(serverTree);
                pos = parentServerTree->findChild(serverTree);
                if (NotFound == pos)
                    throw MakeSDSException(SDSExcpt_ClientCacheDirty, "child(%s) not found in parent(%s) at %s(%d)", serverTree->queryName(), parentServerTree->queryName(), __FILE__, __LINE__);
                tree->setProp("@name", serverTree->queryName());
                tree->setPropInt64("@id", id);
                tree->setPropInt("@pos", pos+1);
            }

            if (translateOldFormat(serverTree, tree, mb))
            {
                parentTree->addPropTree(tree->queryName(), LINK(tree));
                change = true;
            }
        }
    }
    return change;
}

///


void CSDSTransactionServer::processMessage(CMessageBuffer &mb)
{
    TimingBlock xactTimingBlock(xactTimingStats);
    ICoven &coven = queryCoven();

    StringAttr xpath;
    ConnectionId connectionId;
    SessionId id;
    unsigned mode;
    unsigned timeout;

    SdsCommand action = (SdsCommand)-1;
    try
    {
        mb.read((int &)action);
        bool getExt = 0 == (action & DAMP_SDSCMD_LAZYEXT);
        action = (SdsCommand) (((unsigned)action) & ~DAMP_SDSCMD_LAZYEXT);

        TransactionLog transactionLog(*this, action, mb.getSender()); // only active if queryTransactionLogging()==true
        switch (action)
        {
            case DAMP_SDSCMD_CONNECT:
            {
                TimingBlock connectTimingBlock(connectTimingStats);
                Owned<CLCLockBlock> lockBlock;

                unsigned startPos = mb.getPos();
                mb.read(id);
                mb.read(mode);
                mb.read(timeout);
                mb.read(xpath);
                if (queryTransactionLogging())
                    transactionLog.log("xpath='%s' mode=%d", xpath.get(), (unsigned)mode);
                Owned<LinkingCriticalBlock> connectCritBlock = new LinkingCriticalBlock(manager.connectCrit, __FILE__, __LINE__);
                if (RTM_CREATE == (mode & RTM_CREATE_MASK) || RTM_CREATE_QUERY == (mode & RTM_CREATE_MASK))
                    lockBlock.setown(new CLCWriteLockBlock(manager.dataRWLock, readWriteTimeout, __FILE__, __LINE__));
                else
                    lockBlock.setown(new CLCReadLockBlock(manager.dataRWLock, readWriteTimeout, __FILE__, __LINE__));
                if (queryTransactionLogging())
                    transactionLog.markExtra();
                connectionId = 0;
                CServerRemoteTree *_tree;
                Owned<CServerRemoteTree> tree;
                manager.createConnection(id, mode, timeout, xpath, _tree, connectionId, true, connectCritBlock);
                if (connectionId)
                    tree.setown(_tree);
                connectCritBlock.clear();
                if (connectionId)
                {
                    if (0 == id)
                    {
                        StringBuffer str("Dali client passing sessionid=0 to connect (xpath=");
                        str.append(xpath).append(", mode=").append(mode).append(", connectionId=").appendf("%"I64F"x", connectionId).append(")");
                        WARNLOG("%s", str.str());
                    }
                    mb.clear();
                    mb.append((int)DAMP_SDSREPLY_OK);
                    mb.append(connectionId);
                    tree->serializeCutOffRT(mb, RTM_SUB & mode?FETCH_ENTIRE:tree->getPropBool("@fetchEntire")?FETCH_ENTIRE_COND : 0, 0, getExt);
                }
                else
                {
                    mb.clear();
                    mb.append((int)DAMP_SDSREPLY_EMPTY);
                }
                break;
            }
            case DAMP_SDSCMD_MCONNECT:
            {
                TimingBlock connectTimingBlock(connectTimingStats);
                Owned<CLCLockBlock> lockBlock;

                if (queryTransactionLogging())
                    transactionLog.log();
                unsigned startPos = mb.getPos();
                mb.read(id);
                mb.read(timeout);
                
                Owned<IMultipleConnector> mConnect = deserializeIMultipleConnector(mb);
                mb.clear();

                lockBlock.setown(new CLCReadLockBlock(manager.dataRWLock, readWriteTimeout, __FILE__, __LINE__));

                try
                {
                    Owned<CRemoteConnections> remoteConnections = new CRemoteConnections;
                    unsigned c;
                    for (c=0; c<mConnect->queryConnections(); c++)
                    {
                        StringAttr xpath;
                        unsigned mode;
                        mConnect->getConnectionDetails(c, xpath, mode);

                        if (queryTransactionLogging())
                            transactionLog.extra(", xpath='%s', mode=%d", xpath.get(), mode);
                        connectionId = 0;
                        CServerRemoteTree *_tree;
                        Owned<CServerRemoteTree> tree;
                        Owned<LinkingCriticalBlock> connectCritBlock = new LinkingCriticalBlock(manager.connectCrit, __FILE__, __LINE__);
                        manager.createConnection(id, mode, timeout, xpath, _tree, connectionId, true, connectCritBlock);
                        if (connectionId)
                            tree.setown(_tree);
                        connectCritBlock.clear();
                        if (connectionId)
                        {
                            if (0 == id)
                            {
                                StringBuffer str("Dali client passing sessionid=0 to multi connect (xpath=");
                                str.append(xpath).append(", mode=").append(mode).append(", connectionId=").appendf("%"I64F"x", connectionId).append(")");
                                WARNLOG("%s", str.str());
                            }
                            CRemoteConnection *conn = new CRemoteConnection(*SDSManager, connectionId, xpath, id, mode, timeout);
                            assertex(conn);
                            remoteConnections->add(conn);

                            mb.append((int)DAMP_SDSREPLY_OK);
                            mb.append(connectionId);
                            tree->serializeCutOffRT(mb, RTM_SUB & mode?FETCH_ENTIRE:tree->getPropBool("@fetchEntire")?FETCH_ENTIRE_COND : 0, 0, getExt);
                        }
                        else
                        {
                            mb.append((int)DAMP_SDSREPLY_EMPTY);
                        }
                    }
                    // success detach establish connections from holder (which would otherwise disconnect them)
                    remoteConnections->detachConnections();
                }
                catch (IException *e)
                {
                    StringBuffer s("Failed to establish locks to multiple paths: ");
                    getMConnectString(mConnect, s);
                    LOG(MCwarning, unknownJob, e, s.str());
                    throw;
                }
                catch (DALI_CATCHALL)
                {
                    StringBuffer s("(Unknown exception); Failed to establish locks to multiple paths: ");
                    getMConnectString(mConnect, s);
                    throw;
                }
                break;
            }
            case DAMP_SDSCMD_GET:
            case DAMP_SDSCMD_GET2:
            {
                mb.read(connectionId);
                if (queryTransactionLogging())
                    transactionLog.log();
                __int64 serverId;
                mb.read(serverId);
                CHECKEDDALIREADLOCKBLOCK(manager.dataRWLock, readWriteTimeout);
                CHECKEDCRITICALBLOCK(SDSManager->treeRegCrit, fakeCritTimeout);
                Owned<CServerRemoteTree> tree = manager.getRegisteredTree(serverId);
                if (queryTransactionLogging())
                {
                    CServerConnection *conn = manager.queryConnection(connectionId);
                    transactionLog.extra(", xpath='%s', node=%s", conn?conn->queryXPath():"???", tree?tree->queryName():"???");
                }
                mb.clear();
                if (!tree)
                {
                    if (DAMP_SDSCMD_GET2 == action)
                        mb.append((int)DAMP_SDSREPLY_EMPTY);
                    else
                    {
                        CServerConnection *connection = manager.queryConnection(connectionId);
                        StringBuffer s;
                        if (connection)
                        {
                            s.append("path=").append(connection->queryXPath());
                            s.append(", mode=").append(connection->queryMode());
                        }
                        else
                            s.append("Missing connection!");
                        throw MakeSDSException(SDSExcpt_UnknownTreeId, "get: treeId = (%d), connection={ %s }", (unsigned)serverId, s.str());
                    }
                }
                else
                {
                    mb.append((int)DAMP_SDSREPLY_OK);
                    tree->serializeCutOffRT(mb, tree->getPropBool("@fetchEntire")?-1 : 0, 0, getExt);
                }
                break;
            }
            case DAMP_SDSCMD_GETCHILDREN:
            case DAMP_SDSCMD_GETCHILDREN2:
            {
                mb.read(connectionId);
                if (queryTransactionLogging())
                {
                    CServerConnection *conn = manager.queryConnection(connectionId);
                    transactionLog.log("%s",conn?conn->queryXPath():"???");
                }
                __int64 serverId;
                CHECKEDDALIREADLOCKBLOCK(manager.dataRWLock, readWriteTimeout);
                CHECKEDCRITICALBLOCK(SDSManager->treeRegCrit, fakeCritTimeout);
                CMessageBuffer replyMb;
                replyMb.init(mb.getSender(), mb.getTag(), mb.getReplyTag());
                replyMb.append((int)DAMP_SDSREPLY_OK);
                bool first = true, empty = false;
                loop
                {
                    mb.read(serverId);
                    if (!serverId) break;
                    if (!first && empty) replyMb.clear();
                    unsigned levels;
                    mb.read(levels);
                    Owned<CServerRemoteTree> parent = manager.getRegisteredTree(serverId);
                    if (!parent)
                    {
                        if (DAMP_SDSCMD_GETCHILDREN2 == action)
                            replyMb.append(false);
                        else
                        {
                            if (first) // if only one, can acheive without serialization change.
                            {
                                empty = true;
                                replyMb.clear().append((int)DAMP_SDSREPLY_EMPTY);
                            }
                            else
                            {
                                CServerConnection *connection = manager.queryConnection(connectionId);
                                StringBuffer s;
                                if (connection)
                                {
                                    s.append("path=").append(connection->queryXPath());
                                    s.append(", mode=").append(connection->queryMode());
                                }
                                else
                                    s.append("Missing connection!");
                                throw MakeSDSException(SDSExcpt_UnknownTreeId, "GETCHILDREN: Failed to locate parent (%d), connection={ %s }", (unsigned)serverId, s.str());
                            }
                        }
                    }
                    else
                    {
                        if (DAMP_SDSCMD_GETCHILDREN2 == action)
                            replyMb.append(true);
                        parent->serializeCutOffChildrenRT(replyMb, 0==levels ? (unsigned)-1 : levels, 0, getExt);
                        if (queryTransactionLogging())
                            transactionLog.extra(", node=%s",parent->queryName());
                    }
                    first = false;
                }
                mb.clear();
                mb.transferFrom(replyMb);
                break;
            }
            case DAMP_SDSCMD_GETELEMENTS:
            {
                mb.read(connectionId);
                if (queryTransactionLogging())
                {
                    CServerConnection *conn = manager.queryConnection(connectionId);
                    transactionLog.log("%s",conn?conn->queryXPath():"???");
                }
                CHECKEDDALIREADLOCKBLOCK(manager.dataRWLock, readWriteTimeout);
                CHECKEDCRITICALBLOCK(SDSManager->treeRegCrit, fakeCritTimeout);

                CServerConnection *connection = manager.queryConnection(connectionId);
                if (!connection)
                    throw MakeSDSException(SDSExcpt_ConnectionAbsent, " [getElements]");
                StringAttr xpath;
                mb.read(xpath);
                if (queryTransactionLogging())
                    transactionLog.extra(", xpath='%s'", xpath.get());
                Owned<IPropertyTreeIterator> iter = connection->queryRoot()->getElements(xpath);
                ICopyArrayOf<CServerRemoteTree> arr;
                ForEach (*iter) arr.append((CServerRemoteTree &)iter->query());
                CMessageBuffer replyMb;
                replyMb.init(mb.getSender(), mb.getTag(), mb.getReplyTag());
                replyMb.append((int)DAMP_SDSREPLY_OK);
                replyMb.append(arr.ordinality());
                ForEachItemIn(i, arr)
                    arr.item(i).serializeSelfRT(replyMb, getExt);
                mb.clear();
                mb.transferFrom(replyMb);
                break;
            }
            case DAMP_SDSCMD_DATA:
            {
                TimingSizeBlock commitTimingBlock(commitTimingStats);
                CheckTime block0("DAMP_SDSCMD_DATA total");
                unsigned inputStart = mb.getPos();
                mb.read(connectionId);
                byte disconnect; // kludge, high bit to indicate new client format. (for backward compat.)
                bool deleteRoot;
                mb.read(disconnect);
                bool oldFormat = (0 == (0x80 & disconnect));
                disconnect &= ~0x80;
                if (1 == disconnect)
                    mb.read(deleteRoot);
                bool data = mb.length() != mb.getPos();
                if (queryTransactionLogging())
                {
                    CServerConnection *conn = manager.queryConnection(connectionId);
                    transactionLog.log("disconnect=%s, data=%s", disconnect?"true":"false", data?"true":"false");
                }
                Owned<CLCLockBlock> lockBlock;
                { 
                    CheckTime block1("DAMP_SDSCMD_DATA.1");
                    if (data || disconnect)
                        lockBlock.setown(new CLCWriteLockBlock(manager.dataRWLock, readWriteTimeout, __FILE__, __LINE__));
                    else
                        lockBlock.setown(new CLCReadLockBlock(manager.dataRWLock, readWriteTimeout, __FILE__, __LINE__));
                }
                unsigned dataStart = mb.getPos();
                commitTimingBlock.recordSize(mb.length() - dataStart);
                CServerConnection *connection = manager.queryConnection(connectionId);
                if (!connection)
                    throw MakeSDSException(SDSExcpt_ConnectionAbsent, " [commit]");
                try
                {
                    if (queryTransactionLogging())
                        transactionLog.extra(", xpath='%s'", connection->queryXPath());

                    CServerRemoteTree *tree = data ? (CServerRemoteTree *)connection->queryRoot() : (CServerRemoteTree *)connection->queryRootUnvalidated();
                    MemoryBuffer newIds;
                    Owned<IPropertyTree> changeTree;
                    if (data)
                    {
                        if (oldFormat)
                        {
                            Owned<IPropertyTree> t = createPTree(RESERVED_CHANGE_NODE);
                            t->setProp("@name", tree->queryName());
                            if (translateOldFormat(tree, t, mb))
                                changeTree.setown(LINK(t));
                        }
                        else
                            changeTree.setown(createPTree(mb));
                    }
                    if (changeTree && tree->processData(*connection, *changeTree, newIds))
                    { // something commited, if RTM_Create was used need to remember this.
                        CheckTime block6("DAMP_SDSCMD_DATA.6");
                        StringBuffer path;
                        connection->queryPTreePath().getAbsolutePath(path);
                        manager.saveDelta(path.str(), *changeTree);
                    }
                    mb.clear();
                    mb.append((int)DAMP_SDSREPLY_OK);
                    mb.append(newIds); // JCSMORE not particularly efficient change later
                    if (block0.slow())
                    {
                        block0.appendMsg(", xpath=").append(connection->queryXPath());
                        block0.appendMsg(", block size = ").append(mb.length());
                    }
                }
                catch (IException *)
                {
                    if (disconnect)
                        manager.disconnect(connectionId, deleteRoot, (data || disconnect)?NULL:&lockBlock);
                    throw;
                }
                if (disconnect)
                    manager.disconnect(connectionId, deleteRoot, (data || disconnect)?NULL:&lockBlock);

                break;
            }
            case DAMP_SDSCMD_CHANGEMODE:
            {
                mb.read(connectionId);
                if (queryTransactionLogging())
                    transactionLog.log();
                CHECKEDDALIWRITELOCKBLOCK(manager.dataRWLock, readWriteTimeout);
                Linked<CServerConnection> connection = manager.queryConnection(connectionId);
                if (!connection)
                    throw MakeSDSException(SDSExcpt_ConnectionAbsent, " [changeMode]");
                CServerRemoteTree *tree = (CServerRemoteTree *) connection->queryRoot();
                assertex(tree);
                if (queryTransactionLogging())
                    transactionLog.extra(", xpath='%s'", connection->queryXPath());

                unsigned newMode;
                unsigned timeout;
                mb.read(newMode);
                mb.read(timeout);
                mb.clear();

                manager.changeLockMode(*connection, newMode, timeout);

                if (!manager.queryConnection(connectionId))
                {
                    manager.unlock(tree->queryServerId(), connectionId);
                    throw MakeSDSException(SDSExcpt_AbortDuringConnection, " during changeMode");
                }
                mb.append((int) DAMP_SDSREPLY_OK);

                break;
            }
            case DAMP_SDSCMD_GETXPATHS:
            case DAMP_SDSCMD_GETXPATHSPLUSIDS:
            {
                __int64 serverId;
                mb.read(serverId);
                mb.read(xpath);
                if (queryTransactionLogging())
                    transactionLog.log("xpath='%s'", xpath.get());
                mb.clear();
                Owned<IPropertyTree> matchTree = SDSManager->getXPaths(serverId, xpath, DAMP_SDSCMD_GETXPATHSPLUSIDS==action);
                if (matchTree)
                {
                    mb.append((int) DAMP_SDSREPLY_OK);
                    matchTree->serialize(mb);
                }
                else
                    mb.append((int) DAMP_SDSREPLY_EMPTY);
                break;
            }
            case DAMP_SDSCMD_GETXPATHSCRITERIA:
            {
                StringAttr matchXPath, sortBy;
                bool caseinsensitive, ascending;
                unsigned from, limit;
                
                mb.read(xpath);
                mb.read(matchXPath);
                mb.read(sortBy);
                mb.read(caseinsensitive);
                mb.read(ascending);
                mb.read(from);
                mb.read(limit);
                if (queryTransactionLogging())
                {
                    transactionLog.log("xpath='%s',matchXPath='%s',sortBy='%s',acscending=%s,from=%d,limit=%d",
                        xpath.get(), matchXPath.get(), sortBy.get(),
                        ascending?"true":"false", from, limit);
                }
                mb.clear();
                Owned<IPropertyTree> matchTree = SDSManager->getXPathsSortLimitMatchTree(xpath, matchXPath, sortBy, caseinsensitive, ascending, from, limit);
                if (matchTree)
                {
                    mb.append((int) DAMP_SDSREPLY_OK);
                    matchTree->serialize(mb);
                }
                else
                    mb.append((int) DAMP_SDSREPLY_EMPTY);
                break;
            }
            case DAMP_SDSCMD_GETEXTVALUE:
            {
                __int64 serverId;
                mb.read(serverId);
                mb.clear().append((int) DAMP_SDSREPLY_OK);
                if (queryTransactionLogging())
                {
                    CServerRemoteTree *idTree = (CServerRemoteTree *) SDSManager->queryRegisteredTree(serverId);
                    transactionLog.log("%s", idTree?idTree->queryName():"???");
                }
                SDSManager->getExternalValueFromServerId(serverId, mb);
                break;
            }
            case DAMP_SDSCMD_GETELEMENTSRAW:
            {
                CHECKEDDALIREADLOCKBLOCK(manager.dataRWLock, readWriteTimeout);
                StringAttr _xpath;
                mb.read(_xpath);
                if (queryTransactionLogging())
                    transactionLog.log("%s", xpath.get());
                CMessageBuffer replyMb;
                replyMb.init(mb.getSender(), mb.getTag(), mb.getReplyTag());
                replyMb.append((int)DAMP_SDSREPLY_OK);
                unsigned pos = replyMb.length();
                unsigned count = 0;
                replyMb.append(count);
                const char *xpath = _xpath.get();
                if ('/' == *xpath) ++xpath;
                Owned<IPropertyTreeIterator> iter = manager.queryRoot()->getElements(xpath);
                ForEach (*iter)
                {
                    ++count;
                    IPropertyTree &e = iter->query();
                    e.serialize(replyMb);
                }
                replyMb.writeDirect(pos,sizeof(count),&count);
                mb.clear();
                mb.transferFrom(replyMb);
                break;
            }
            case DAMP_SDSCMD_GETCOUNT:
            {
                mb.read(xpath);
                if (queryTransactionLogging())
                    transactionLog.log("xpath='%s'", xpath.get());
                CHECKEDDALIREADLOCKBLOCK(manager.dataRWLock, readWriteTimeout);
                mb.clear();
                mb.append((int)DAMP_SDSREPLY_OK);
                mb.append(manager.queryCount(xpath));
                break;
            }
            default:
                assertex(false);
        }
    }
    catch (IException *e)                                       
    {                                                           
        mb.clear();
        mb.append((int) DAMP_SDSREPLY_ERROR);
        StringBuffer s;
        e->errorMessage(s);
        // NB: wanted to do this in a catch (IPTreeException *) block, but did catch,
        // in spite of being able to query with dynamic cast
        // something to do with rethrow, if I changed to catch early as IPT then it would catch here correctly.
        if (QUERYINTERFACE(e, IPTreeException))
        {
            s.append(" in xpath '").append(xpath).append("'");
            e->Release();
            e = MakeSDSException(SDSExcpt_IPTError, "%s", s.str());
        }
        mb.append(e->errorCode());
        mb.append(e->errorMessage(s.clear()));
        StringBuffer clientUrl("EXCEPTION in reply to client ");
        mb.getSender().getUrlStr(clientUrl);                    
        EXCLOG(e, clientUrl.str(), MSGCLS_warning);             
        e->Release();                                           
    }
    catch (DALI_CATCHALL)
    {
        Owned<IException> e = MakeSDSException(-1, "Unknown server exception processing client action: %d", action);
        mb.clear();
        mb.append((int) DAMP_SDSREPLY_ERROR);
        StringBuffer s;
        mb.append(e->errorCode());
        mb.append(e->errorMessage(s).str());
        StringBuffer clientUrl("EXCEPTION in reply to client ");
        mb.getSender().getUrlStr(clientUrl);
        LOG(MCoperatorError, unknownJob, e);
    }
    try { 
        CheckTime block10("DAMP_REQUEST reply");
        coven.reply(mb); 
    }
    catch (IMP_Exception *e)
    {
        LOG(MCwarning, unknownJob, e, "Failed to reply to client (processMessage)");
        e->Release();
    }
    catch (IException *e)
    {
        // attempt error reply, on failed initial reply, reply error *might* have been message sensitive, e.g. OOM.
        mb.clear();
        mb.append((int) DAMP_SDSREPLY_ERROR);
        mb.append(e->errorCode());
        StringBuffer s;
        mb.append(e->errorMessage(s).str());
        StringBuffer clientUrl("EXCEPTION in reply to client ");
        mb.getSender().getUrlStr(clientUrl);
        EXCLOG(e, clientUrl.str(), MSGCLS_warning);
        e->Release();
        try
        {
            coven.reply(mb);
            LOG(MCdebugInfo(100), unknownJob, "Failed to reply, but succeeded sending initial reply error to client");
        }
        catch (IException *e)
        {
            LOG(MCwarning, unknownJob, e, "Failed to reply and failed to send reply error to client");
            e->Release();
        }
    }
}

void CSDSTransactionServer::stop()
{
    if (!stopped) {
        stopped = true;
        queryCoven().cancel(RANK_ALL, MPTAG_DALI_SDS_REQUEST);
    }
    PROGLOG("clearing remaining sds locks");
    manager.clearSDSLocks();
    PROGLOG("waiting for transaction server to stop");
    join();
}

/////////////////
// CServerConnection impl.
IPropertyTree *CServerConnection::queryRoot()
{
    if (((CServerRemoteTree *)root.get())->isOrphaned())
        throw MakeSDSException(SDSExcpt_OrphanedNode, "%s", queryXPath());
    return root;
}
////////////////

void CLockInfo::clearLastRef()
{
    if (parent)
    {
        CPendingLockBlock b(*this); // carefully placed, removePending can destroy this.
        parent->removeTree(child);
        parent.clear();
        child.clear();
    }
}

////////////////

class ConnectionIdHashTable : public SuperHashTableOf<ConnectionId, ConnectionId>
{
public:
    ~ConnectionIdHashTable() { kill(); }
    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(ConnectionId, ConnectionId);
    virtual void onAdd(void *et) { }
    virtual void onRemove(void *et) { delete (ConnectionId *)et; }
    virtual unsigned getHashFromElement(const void *et) const
    {
        return hashc((const unsigned char *) et, sizeof(ConnectionId), 0);
    }
    virtual unsigned getHashFromFindParam(const void *fp) const
    {
        return hashc((const unsigned char *) fp, sizeof(ConnectionId), 0);
    }
    virtual const void *getFindParam(const void *et) const
    {
        return et;
    }
    virtual bool matchesFindParam(const void *et, const void *fp, unsigned) const
    {
        return *(ConnectionId *)et == *(ConnectionId *)fp;
    }
};

static bool retryRename(const char *from, const char *to, unsigned maxAttempts, unsigned delay)
{
    unsigned attempts=maxAttempts;
    loop
    {
        OwnedIFile iFile = createIFile(from);
        try
        {
            iFile->rename(to);
            break;
        }
        catch (IException *e)
        {
            StringBuffer errTxt("Failed to rename: ");
            EXCLOG(e, errTxt.append(from).append(" to ").append(to).append(", retrying...").str());
            e->Release();
        }
        if (attempts && 0 == --attempts)
            break;
        MilliSleep(delay);
    }
    return (attempts>0);
}

static bool retryCopy(const char *from, const char *to, unsigned maxAttempts, unsigned delay)
{
    unsigned attempts=maxAttempts;
    loop
    {
        StringBuffer _from;
        StringBuffer fname;
        splitFilename(from, &_from, &_from, &fname, &fname);
        _from.append('_').append(fname);
        OwnedIFile iFile = createIFile(from);
        try
        {
            iFile->rename(_from.str());
            copyFile(to, _from.str());
            break;
        }
        catch (IException *e)
        {
            EXCLOG(e, NULL);
            e->Release();
        }
        if (attempts && 0 == --attempts)
            break;
        DBGLOG("Failed to copy: %s to %s, retrying...", from, to);
        MilliSleep(delay);
    }
    return (attempts>0);
}

inline unsigned nextEditionN(unsigned e, unsigned i=1)
{
    return e+i;
}

inline unsigned prevEditionN(unsigned e, unsigned i=1)
{
    return e-i;
}

void removeDaliFile(const char *path, const char *base, unsigned e)
{
    StringBuffer filename(path);
    constructStoreName(base, e, filename);
    OwnedIFile iFile = createIFile(filename.str());
    try
    {
        iFile->remove();
    }
    catch (IException *e)
    {
        EXCLOG(e, NULL);
        e->Release();
    }
}

// Ensure internally used branches are present
void initializeInternals(IPropertyTree *root)
{
    ensurePTree(root, "Files");
    ensurePTree(root, "Queues");
    ensurePTree(root, "Groups");
    ensurePTree(root, "Status");
    ensurePTree(root, "WorkUnits");
    ensurePTree(root, "JobQueues");
    ensurePTree(root, "Environment");
    ensurePTree(root, "Locks");
    ensurePTree(root, "DFU");
    ensurePTree(root, "DFU/RECOVERY");
    ensurePTree(root, "DFU/WorkUnits");
    ensurePTree(root, "Files/Relationships");
    root->removeProp("Status/Servers");
    root->addPropTree("Status/Servers",createPTree());
}

IPropertyTree *loadStore(const char *storeFilename, IPTreeMaker *iMaker, unsigned crcValidation, bool logErrorsOnly=false, const bool *abort=NULL)
{
    CHECKEDCRITICALBLOCK(loadStoreCrit, fakeCritTimeout);
    CHECKEDCRITICALBLOCK(saveStoreCrit, fakeCritTimeout);
    Owned<IPropertyTree> root;
    try
    {
        OwnedIFile iFileStore = createIFile(storeFilename);
        OwnedIFileIO iFileIOStore = iFileStore->open(IFOread);
        if (!iFileIOStore)
            throw MakeSDSException(SDSExcpt_OpenStoreFailed, "%s", storeFilename);

        Owned<IFileIOStream> fstream = createIOStream(iFileIOStore);
        Owned<ICrcIOStream> crcPipeStream = createCrcPipeStream(fstream);
        Owned<IIOStream> ios = createBufferedIOStream(crcPipeStream);
        root.setown((CServerRemoteTree *) createPTree(*ios, ipt_none, ptr_ignoreWhiteSpace, iMaker));
        ios.clear();
        unsigned crc = crcPipeStream->queryCrc();

        if (crcValidation && crc != crcValidation)
            LOG(MCoperatorWarning, unknownJob, "Error processing store %s - CRC ERROR (file size=%"I64F"d, validation crc=%x, calculated crc=%x)", storeFilename, iFileIOStore->size(), crcValidation, crc); // not fatal yet (maybe later)
    }
    catch (IException *e)
    {
        if (!abort)
        {
            StringBuffer s("Exception - loading store file : ");
            s.appendf("%s", storeFilename);
            LOG(MCoperatorError, unknownJob, e, s.str());
        }
        if (SDSExcpt_OpenStoreFailed != e->errorCode())
            if (!logErrorsOnly)
                throw;
        e->Release();
    }
    catch (DALI_CATCHALL)
    {
        IException *e = MakeStringException(0, "Unknown exception - loading store file : %s", storeFilename);
        LOG(MCdisaster, unknownJob, e, "");
        if (!logErrorsOnly)
            throw;
        e->Release();
    }
    return LINK(root);
}


// Not really coalescing, blocking transations and saving store (which will delete pending transactions).
class CLightCoalesceThread : public CInterface, implements ICoalesce
{
    bool stopped, within24;
    Semaphore sem;
    unsigned writeTransactionsNow, lastSaveWriteTransactions, lastWarning;
    unsigned idlePeriod, minimumTimeBetweenSaves, idleRate;
    Linked<IPropertyTree> config;
    Owned<IJlibDateTime> quietStartTime, quietEndTime;
    CheckedCriticalSection crit;
    IStoreHelper *iStoreHelper;

    class CThreaded : public Thread
    {
        CLightCoalesceThread *coalesce;
    public:
        CThreaded() : Thread("CLightCoalesceThread") { }
        void init(CLightCoalesceThread *_coalesce) { coalesce = _coalesce; start(); }
        virtual int run() { coalesce->main(); return 1; }
    } threaded;
public:
    IMPLEMENT_IINTERFACE;
    CLightCoalesceThread(IPropertyTree &_config, IStoreHelper *_iStoreHelper) : config(&_config), iStoreHelper(_iStoreHelper)
    {
        stopped = false;
        idlePeriod = config->getPropInt("@lCIdlePeriod", DEFAULT_LCIDLE_PERIOD)*1000;
        minimumTimeBetweenSaves = config->getPropInt("@lCMinTime", DEFAULT_LCMIN_TIME)*1000;
        idleRate = config->getPropInt("@lCIdleRate", DEFAULT_LCIDLE_RATE);
        char const *quietStartTimeStr = config->queryProp("@lCQuietStartTime");
        if (quietStartTimeStr)
        {
            if (*quietStartTimeStr)
            {
                quietStartTime.setown(createDateTime());
                quietStartTime->setLocalTimeString(quietStartTimeStr);
                quietStartTime->setGmtDate(1970, 1, 1);
            }
            else
                quietStartTimeStr = NULL;
        }
        char const *quietEndTimeStr = config->queryProp("@lCQuietEndTime");
        if (quietStartTimeStr && !quietEndTimeStr)
        {
            WARNLOG("Start time for quiet period specified without end time, ignoring times");
            quietStartTime.clear();
        }
        else if (quietEndTimeStr && *quietEndTimeStr)
        {
            if (!quietStartTimeStr)
                WARNLOG("End time for quiet period specified without start time, ignoring times");
            else
            {
                quietEndTime.setown(createDateTime());
                quietEndTime->setLocalTimeString(quietEndTimeStr);
                quietEndTime->setGmtDate(1970, 1, 1);
                within24 = quietStartTime->compare(*quietEndTime) <= 0;
            }
        }
    }
    ~CLightCoalesceThread()
    {
        stop();
    }
    void main()
    {
        unsigned t = 0;
        lastSaveWriteTransactions = SDSManager->writeTransactions;
        lastWarning = 0;
    
        unsigned lastEdition = iStoreHelper->queryCurrentEdition();
        while (!stopped)
        {
            unsigned writeTransactionsNow = SDSManager->writeTransactions;
            if (!sem.wait(idlePeriod))
            {
                if (writeTransactionsNow != lastSaveWriteTransactions)
                {
                    if (quietStartTime)
                    {
                        Owned<IJlibDateTime> nowTime = createDateTimeNow();
                        nowTime->setGmtDate(1970, 1, 1);
                        if (within24)
                            if (!(nowTime->compare(*quietStartTime) >= 0 && nowTime->compare(*quietEndTime) <= 0))
                                continue; // if outside quiet period within 0-24
                        else if (nowTime->compare(*quietEndTime) > 0 && nowTime->compare(*quietStartTime) < 0)
                            continue; // if inside period excluded by quiet period
                    }

                    if (lastEdition == iStoreHelper->queryCurrentEdition()) // if not then something else has saved (e.g. probably sasha)
                    {
                        unsigned transactions = SDSManager->writeTransactions-writeTransactionsNow; // don't care about rollover.
                        if (0 == transactions ||
                            (0 != idleRate && idlePeriod>=60000 && (transactions/(idlePeriod/60000))<=idleRate))
                        {
                            StringBuffer filename;
                            iStoreHelper->getPrimaryLocation(filename);
                            iStoreHelper->getCurrentStoreFilename(filename);
                            OwnedIFile iFile = createIFile(filename);
                            CDateTime createTime, nowTime;
                            nowTime.setNow();
                            int diff = 0;
                            try
                            {
                                if (iFile->getTime(&createTime, NULL, NULL))
                                    diff = ((int)nowTime.getSimple()-(int)createTime.getSimple())*1000;
                            }
                            catch (IException *e)
                            {
                                StringBuffer errMsg("failed to get createtime for : ");
                                errMsg.append(filename);
                                EXCLOG(e, errMsg.str());
                                e->Release();
                            }
                            int period;
                            if (diff<=0 || diff > (int)minimumTimeBetweenSaves) // <0 - createTime>nowTime, assume time skew and allow save.
                            {
                                period = minimumTimeBetweenSaves-idlePeriod;
                                if (0 > period) period = 0;
                                {
                                    CHECKEDCRITICALBLOCK(saveStoreCrit, fakeCritTimeout);
                                    SDSManager->blockingSave(&lastSaveWriteTransactions);
                                    lastEdition = iStoreHelper->queryCurrentEdition();
                                }
                                t = lastWarning = 0;
                            }
                            else
                                period = minimumTimeBetweenSaves-diff;
                            sem.wait(period);
                        }
                        else
                        {
                            t += idlePeriod/1000;
                            if (t/3600 >= STORENOTSAVE_WARNING_PERIOD && ((t-lastWarning)/3600>(STORENOTSAVE_WARNING_PERIOD/2)))
                            {
                                WARNLOG("Store has not been saved for %d hours", t/3600);
                                lastWarning = t;
                            }
                        }
                    }
                    else
                    {
                        t = lastWarning = 0;
                        lastEdition = iStoreHelper->queryCurrentEdition();
                    }
                }
            }
        }
    }
// implements ICoalsce
    virtual void start()
    {
        threaded.init(this);
    }
    virtual void stop()
    {
        if (!stopped)
        {
            stopped = true;
            sem.signal();
            threaded.join();
        }
    }
};

/////////////////

class CUnlockCallback : implements IUnlockCallback
{ // NB: unblock() always called 1st, then block()
    StringAttr xpath;
    ConnectionId connectionId;
    CServerRemoteTree &tree;
    bool lockedForWrite, unlocked;
public:
    CUnlockCallback(const char *_xpath, ConnectionId _connectionId, CServerRemoteTree &_tree) : xpath(_xpath), connectionId(_connectionId), tree(_tree), lockedForWrite(false), unlocked(false) { }
    void block()
    {
        assertex(unlocked);
        unsigned got = msTick();
        if (lockedForWrite)
            CHECKEDWRITELOCKENTER(SDSManager->dataRWLock, readWriteTimeout);
        else
            CHECKEDREADLOCKENTER(SDSManager->dataRWLock, readWriteTimeout);
        CHECKEDCRITENTER(SDSManager->lockCrit, fakeCritTimeout);
        unlocked = false;
        unsigned e=msTick()-got;
        if (e>readWriteSlowTracing)
        {
            StringBuffer s("TIME: CUnlockCallback(write=");
            s.append(lockedForWrite).append(",xpath=").append(xpath).append(", connectionId=").appendf("%"I64F"x", connectionId).append(") took ").append(e);
            DBGLOG("%s", s.str());
            if (readWriteStackTracing)
                PrintStackReport();
        }
        if (tree.isOrphaned())
            throw MakeSDSException(SDSExcpt_OrphanedNode, "Whilst completing lock to %s", xpath.get());
    }
    void unblock()
    {
        unlocked = true;
        lockedForWrite = SDSManager->dataRWLock.queryWriteLocked();
        CHECKEDCRITLEAVE(SDSManager->lockCrit);
        if (lockedForWrite)
            SDSManager->dataRWLock.unlockWrite();
        else
            SDSManager->dataRWLock.unlockRead();
    }
};

class CStoreHelper : public CInterface, implements IStoreHelper
{
    StringAttr storeName, location, remoteBackupLocation;
    CStoreInfo storeInfo, deltaInfo;
    unsigned configFlags;
    const bool *abort;
    unsigned delay, keepStores;
    SessionId mySessId;

    void clearStoreInfo(const char *base, const char *location, unsigned edition, CStoreInfo *storeInfo=NULL)
    {
        StringBuffer wcard;
        wcard.append(base).append(".*");
        StringBuffer path, filename;
        filename.append(base).append('.').append(edition);
        if (location) path.append(location);
        path.append(filename);
        if (!storeInfo || !storeInfo->cache)
        {
            Owned<IDirectoryIterator> dIter = createDirectoryIterator(location, wcard.str());
            ForEach (*dIter)
            {
                loop
                {   
                    try { dIter->query().remove(); break; }
                    catch (IException *e)
                    {
                        e->Release();
                        if (abort && *abort)
                            return;
                        MilliSleep(delay);
                    }
                }
            }
        }
        else
        {
            if (0 != stricmp(filename.str(), storeInfo->cache))
            {
                StringBuffer path(location);
                path.append(storeInfo->cache);
                OwnedIFile iFile = createIFile(path.str());
                loop
                {   
                    try { iFile->remove(); break; }
                    catch (IException *e)
                    {
                        e->Release();
                        if (abort && *abort)
                            return;
                        MilliSleep(delay);
                    }
                }
            }
            storeInfo->cache.clear();
        }
    }

    void writeStoreInfo(const char *base, const char *location, unsigned edition, unsigned *crc, CStoreInfo *storeInfo=NULL)
    {
        StringBuffer path, filename;
        filename.append(base).append('.').append(edition);
        if (location) path.append(location);
        path.append(filename);

        OwnedIFile iFile = createIFile(path.str());
        OwnedIFileIO iFileIO = iFile->open(IFOcreate);
        if (crc)
            iFileIO->write(0, sizeof(unsigned), crc);
        if (storeInfo)
            storeInfo->cache.set(filename.str());
    }

    void updateStoreInfo(const char *base, const char *location, unsigned edition, unsigned *crc, CStoreInfo *storeInfo=NULL)
    {
        clearStoreInfo(base, location, edition, storeInfo);
        writeStoreInfo(base, location, edition, crc, storeInfo);
    }

    void refreshInfo(CStoreInfo &info, const char *base)
    {
        OwnedIFile found;
        OwnedIFileIO iFileIO;
        if (info.cache.length()) // avoid directory lookup if poss.
        {
            StringBuffer path(location);
            OwnedIFile iFile = createIFile(path.append(info.cache).str());
            if (iFile->exists())
            {
                found.set(iFile);
                try { iFileIO.setown(found->open(IFOread)); }
                catch (IException *e)
                {
                    e->Release();
                }
            }
        }
        if (!iFileIO)
        {
            StringBuffer wcard;
            wcard.append(base).append(".*");
            Owned<IDirectoryIterator> dIter = createDirectoryIterator(location, wcard.str());
            unsigned totalDelays = 0;
            loop
            {
                if (dIter->first())
                {
                    const char *name = dIter->query().queryFilename();
                    StringBuffer base, ext, fname;
                    splitFilename(name, NULL, NULL, &base, &ext);
                    fname.append(base).append(ext);
                    info.cache.set(fname.str());
                    found.set(&dIter->query());
                    try { iFileIO.setown(found->open(IFOread)); }
                    catch (IException *e)
                    {
                        e->Release();
                    }
                    if (iFileIO)
                        break;
                }
                totalDelays++;
                if (totalDelays >= MAXDELAYS)
                    throw MakeSDSException(SDSExcpt_StoreInfoMissing, "store.<edition> file appears to be missing");
                if (abort && *abort)
                    return;
                MilliSleep(delay);
            }
        }
        assertex(iFileIO);
        StringBuffer tail, ext, fname;
        splitFilename(found->queryFilename(), NULL, NULL, &tail, &ext);
        fname.append(tail).append(ext);
        const char *name = fname.str();
        const char *editionBegin = name+strlen(base)+1;
        info.edition = atoi(editionBegin);
        if (iFileIO->size())
            iFileIO->read(0, sizeof(unsigned), &info.crc);
        else
            info.crc = 0;
    }

    void refreshStoreInfo() { refreshInfo(storeInfo, "store"); }
    void refreshDeltaInfo() { refreshInfo(deltaInfo, "store"); }

    void checkInfo(const char *base, CStoreInfo &info)
    {
        StringBuffer wcard;
        wcard.append(base).append(".*");
        Owned<IDirectoryIterator> dIter = createDirectoryIterator(location, wcard.str());
        if (!dIter->first())
            updateStoreInfo(base, location, 0, NULL, &info);
        else if (dIter->next())
            throw MakeStringException(0, "Multiple store.X files - only one corresponding to latest dalisds<X>.xml should exist");
    }

    void renameDelta(unsigned oldEdition, unsigned newEdition, const char *path)
    {
        StringBuffer deltaName(path);
        constructStoreName(DELTANAME, oldEdition, deltaName);
        OwnedIFile oldDelta = createIFile(deltaName.str());
        if (oldDelta->exists())
        {
            deltaName.clear();
            constructStoreName(DELTANAME, newEdition, deltaName);
            oldDelta->rename(deltaName.str());
        }
    }

    struct CheckDeltaBlock
    {
        CheckDeltaBlock(CStoreHelper &_storeHelper) : storeHelper(_storeHelper)
        {
            bool created = false;
            StringBuffer deltaIPStr(storeHelper.location);
            OwnedIFile deltaIPIFile = createIFile(deltaIPStr.append(DELTAINPROGRESS).str());
            activeDetachIPStr.append(DETACHINPROGRESS);
            inactiveDetachIPStr.append(storeHelper.location).append('_').append(DETACHINPROGRESS);
            detachIPIFile.setown(createIFile(inactiveDetachIPStr.str()));

            OwnedIFileIO detachIPIO = detachIPIFile->open(IFOcreate);
            detachIPIO->write(0, sizeof(storeHelper.mySessId), &storeHelper.mySessId);
            detachIPIO.clear();
            detachIPIFile->rename(activeDetachIPStr.str());
            // check often do not wait any longer than necessary
            unsigned d=0;
            while (deltaIPIFile->exists())
            {
                if (0 == d++ % 50)
                    PROGLOG("Waiting for a saveDelta in progress");
                MilliSleep(100);
            }
        }
        ~CheckDeltaBlock()
        {
            if (detachIPIFile)
            {
                unsigned a=0;
                loop
                {
                    try { detachIPIFile->remove(); break; }
                    catch (IException *e)
                    {
                        EXCLOG(e, "removing detach file marker");
                        if (a++ > 10) throw;
                        e->Release();
                    }
                    MilliSleep(500);
                }
            }
        }
    private:
        CStoreHelper &storeHelper;
        OwnedIFile detachIPIFile;
        StringBuffer activeDetachIPStr, inactiveDetachIPStr;
    };
public:
    IMPLEMENT_IINTERFACE;

    CStoreHelper(const char *_storeName, const char *_location, const char *_remoteBackupLocation, unsigned _configFlags, unsigned _keepStores, unsigned _delay, const bool *_abort) : storeName(_storeName), location(_location), remoteBackupLocation(_remoteBackupLocation), configFlags(_configFlags), keepStores(_keepStores), delay(_delay), abort(_abort)
    {
        mySessId = daliClientActive()?myProcessSession():0;
        if (!keepStores) keepStores = DEFAULT_KEEP_LASTN_STORES;
        checkInfo("store", storeInfo);
        checkInfo("store", deltaInfo);
        if (0 == (SH_External & configFlags))
        {
            refreshStoreInfo();
            unsigned edition = storeInfo.edition;
            Owned<IDirectoryIterator> di = createDirectoryIterator(location, "dali*.xml");
            ForEach (*di)
            {
                StringBuffer fname;
                di->getName(fname);
                if ('_' != fname.charAt(7)) // Unhelpful naming convention to differentiate store files from externals!
                {
                    if (0 == memicmp("inc", fname.toCharArray()+4, 3) || 0 == memicmp("sds", fname.toCharArray()+4, 3))
                    {
                        const char *num = fname.toCharArray()+7;
                        const char *dot = (const char *)strchr(num, '.');
                        unsigned fileEdition = atoi_l(num, dot-num);
                        int d = (int)fileEdition-(int)edition;
                        if (edition != fileEdition && (d>=1 || d<(-(int)keepStores)))
                        {
                            IFile &file = di->query();
                            CDateTime dt;
                            dt.setNow();
                            StringBuffer newName(file.queryFilename());
                            newName.append('.');
                            unsigned i=newName.length();
                            dt.getString(newName); // base on date, incase any old copies.
                            for (;i<newName.length();i++)
                                if (newName.charAt(i)==':')
                                    newName.setCharAt(i,'_');
                            newName.append(".unused");
                            PROGLOG("Detected spurious data file : '%s' - renaming to %s", file.queryFilename(), newName.str());
                            try
                            {
                                file.rename(newName.str());
                            }
                            catch (IException *e) // safe to ignore these (should e.g. files be in use).
                            {
                                EXCLOG(e, NULL);
                                e->Release();
                            }
                        }
                    }
                }
            }
            StringBuffer dst(location);
            addPathSepChar(dst);
            dst.append(DEBUG_DIR);
            addPathSepChar(dst);
            OwnedIFile dFile = createIFile(dst.str());
            Owned<IDirectoryIterator> dIter = dFile->directoryFiles();
            ForEach(*dIter)
                dIter->query().remove();
        }
    }
    virtual StringBuffer &getDetachedDeltaName(StringBuffer &detachName)
    {
        refreshDeltaInfo();
        constructStoreName(DELTADETACHED, deltaInfo.edition, detachName);
        return detachName;
    }
    virtual bool loadDelta(const char *filename, IFile *iFile, IPropertyTree *root)
    {
        Owned<IFileIO> iFileIO = iFile->open(IFOread);
        if (!iFileIO) // no delta to load
            return true;
        MemoryBuffer tmp;
        char *ptr = (char *) tmp.reserveTruncate(strlen(deltaHeader));
        unsigned embeddedCrc = 0;
        offset_t pos = 0;
        bool hasCrcHeader = false; // check really only needed for deltas proceeding CRC header
        if (strlen(deltaHeader) == iFileIO->read(0, strlen(deltaHeader), ptr))
        {
            if (0 == memicmp(deltaHeader, ptr, 5))
            {
                pos = deltaHeaderSizeStart;
                hasCrcHeader = true;
                embeddedCrc = (unsigned)atoi64_l(ptr+deltaHeaderCrcOff, 10);
                if (0 == memicmp(deltaHeader+deltaHeaderSizeStart, ptr+deltaHeaderSizeStart, 6)) // has <SIZE> too
                {
                    pos = strlen(deltaHeader);
                    offset_t lastGood;
                    if (sscanf(ptr+deltaHeaderSizeOff, "%"I64F"X", &lastGood))
                    {
                        offset_t fSize = iFileIO->size();
                        if (fSize > lastGood)
                        {
                            size32_t diff = fSize - lastGood;
                            LOG(MCoperatorError, unknownJob, "Delta file '%s', has %d bytes of trailing data (possible power loss during save?), file size: %"I64F"d, last committed size: %"I64F"d", filename, diff, fSize, lastGood);
                            LOG(MCoperatorError, unknownJob, "Resetting delta file '%s' to size: %"I64F"d", filename, lastGood);
                            iFileIO->close();
                            backup(filename);
                            iFileIO.setown(iFile->open(IFOreadwrite));
                            iFileIO->setSize(lastGood);
                            iFileIO->close();
                            iFileIO.setown(iFile->open(IFOread));
                        }
                    }
                }
            }
        }
        OwnedIFileIOStream iFileIOStream = createIOStream(iFileIO);
        iFileIOStream->seek(pos, IFSbegin);
        Owned<ICrcIOStream> crcPipeStream = createCrcPipeStream(iFileIOStream); // crc *rest* of stream
        Owned<IIOStream> ios = createBufferedIOStream(crcPipeStream);
        bool noErrors;
        Owned<IException> deltaE;
        noErrors = applyXmlDeltas(*root, *ios, 0 == (SH_RecoverFromIncErrors & configFlags));
        if (noErrors && hasCrcHeader)
        {
            unsigned crc = crcPipeStream->queryCrc();
            if (embeddedCrc != crc)
            {
                noErrors = false;
                StringBuffer s;
                LOG(MCoperatorWarning, unknownJob, "%s", s.append("Delta '").append(filename).append("' crc mismatch").str());
            }
        }
        return noErrors;
    }
    virtual bool loadDeltas(IPropertyTree *root, bool *errors)
    {
        bool res = false;
        if (errors) *errors = false;
        StringBuffer deltaFilename(location);
        constructStoreName(DELTANAME, storeInfo.edition, deltaFilename);

        StringBuffer detachPath(location);
        OwnedIFile detachedDeltaIFile = createIFile(getDetachedDeltaName(detachPath).str());
        bool detached = detachedDeltaIFile->exists();
        OwnedIFile deltaIFile = createIFile(deltaFilename.str());
        loop
        {
            StringAttr filename;
            IFile *iFile;
            if (detached)
            {
                iFile = detachedDeltaIFile;
                filename.set(iFile->queryFilename());
            }
            else
            {
                iFile = deltaIFile;
                filename.set(iFile->queryFilename());
                if (!iFile->exists())
                    break;
            }
            PROGLOG("Loading delta: %s", filename.get());

            bool noError;
            Owned<IException> deltaE;
            try { noError = loadDelta(filename, iFile, root); }
            catch (IException *e) { deltaE.setown(e); noError = false; }
            if (!noError)
            {
                backup(filename);
                if (errors) *errors = true;
                if (deltaE)
                    throw LINK(deltaE);
            }

            res = true;

            if (detached)
                detached = false;
            else
                break;
        }
        return res;
    }
    virtual bool detachCurrentDelta()
    {
        StringBuffer deltaFilename(location);
        getCurrentDeltaFilename(deltaFilename);
        refreshStoreInfo();

        bool res = false;
        try
        {
            CheckDeltaBlock cD(*this);

            OwnedIFile deltaIFile = createIFile(deltaFilename.str());
            if (deltaIFile->exists())
            {
#ifdef NODELETE
                StringBuffer detachPath(location);
                getDetachedDeltaName(detachPath);
                if (retryCopy(deltaFilename.str(), detachPath.str(), 5, delay))
#else
                StringBuffer detachName;
                getDetachedDeltaName(detachName);
                if (retryRename(deltaFilename.str(), detachName.str(), 5, delay))
#endif
                    res = true;
            }
            if (remoteBackupLocation.length())
            {
                deltaFilename.clear().append(remoteBackupLocation);
                getCurrentDeltaFilename(deltaFilename);
                OwnedIFile iFile = createIFile(deltaFilename);
                if (iFile->exists())
                {
                    StringBuffer detachName;
                    getDetachedDeltaName(detachName);
                    iFile->rename(detachName.str());
                }
            }
        }
        catch (IException *e)
        {
            LOG(MCoperatorError, unknownJob, e, "detachCurrentDelta");
            e->Release();
        }
        return res;
    }
    virtual void saveStore(IPropertyTree *root, unsigned *_newEdition, bool currentEdition=false)
    {
        LOG(MCdebugInfo(100), unknownJob, "Saving store");

        refreshStoreInfo();

        unsigned edition = storeInfo.edition;
        unsigned newEdition = currentEdition?edition:nextEditionN(edition);
        bool done = false;
        try
        {
            unsigned crc = 0;
            StringBuffer tmpStoreName;
            OwnedIFileIO iFileIOTmpStore = createUniqueFile(location, TMPSAVENAME, NULL, tmpStoreName);
            OwnedIFile iFileTmpStore = createIFile(tmpStoreName);
            try
            {
                OwnedIFileIOStream fstream = createIOStream(iFileIOTmpStore);
                Owned<ICrcIOStream> crcPipeStream = createCrcPipeStream(fstream);
                Owned<IIOStream> ios = createBufferedIOStream(crcPipeStream);

#ifdef _DEBUG
                toXML(root, *ios);          // formatted (default)
#else
                toXML(root, *ios, 0, 0);
#endif
                ios.clear();
                fstream.clear();
                crc = crcPipeStream->queryCrc();
                crcPipeStream.clear();
                iFileIOTmpStore.clear();
            }
            catch (IException *e)
            {
                LOG(MCoperatorError, unknownJob, e, "Exception(1) - Error saving store file");
                iFileIOTmpStore.clear();
                iFileTmpStore->remove();
                throw;
            }

            StringBuffer newStoreName;
            constructStoreName(storeName, newEdition, newStoreName);
            StringBuffer newStoreNamePath(location);
            newStoreNamePath.append(newStoreName);
            refreshStoreInfo();
            if (storeInfo.edition != edition)
            {
                WARNLOG("Another process has updated the edition whilst saving the store: %s", newStoreNamePath.str());
                iFileTmpStore->remove();
                return;
            }
            try
            {
                OwnedIFile newStoreIFile = createIFile(newStoreNamePath.str());
                newStoreIFile->remove();
                iFileTmpStore->rename(newStoreName.str());
            }
            catch (IException *e)
            {
                StringBuffer errMsg;
                EXCLOG(e, errMsg.append("Failed to rename new store to : ").append(newStoreNamePath).append(". Has already been created by another process?").str());
                e->Release();
                iFileTmpStore->remove();
                return;
            }

            if (0 != (SH_CheckNewDelta & configFlags))
            {
                CheckDeltaBlock cD(*this);
                try { renameDelta(edition, newEdition, location); }
                catch (IException *e)
                {
                    StringBuffer s("Exception(2) - Error saving store file");
                    LOG(MCoperatorError, unknownJob, e, s.str());
                    e->Release();
                    return;
                }
                if (remoteBackupLocation.length())
                {
                    try { renameDelta(edition, newEdition, remoteBackupLocation); }
                    catch (IException *e)
                    {
                        LOG(MCoperatorError, unknownJob, e, "Failure handling backup");
                        e->Release();
                    }
                }
                clearStoreInfo("store", location, 0, NULL);
                writeStoreInfo("store", location, newEdition, &crc, &storeInfo);
            }
            else
            {
                clearStoreInfo("store", location, 0, NULL);
                writeStoreInfo("store", location, newEdition, &crc, &storeInfo);
            }

            try
            {
                if (remoteBackupLocation.length())
                {
                    PROGLOG("Copying store to backup location");
                    StringBuffer rL(remoteBackupLocation);
                    constructStoreName(storeName, newEdition, rL);
                    copyFile(rL.str(), newStoreNamePath.str());

                    clearStoreInfo("store", remoteBackupLocation, 0, NULL);
                    writeStoreInfo("store", remoteBackupLocation, newEdition, &crc, &storeInfo);
                    PROGLOG("Copy done");
                }
            }
            catch (IException *e)
            {
                StringBuffer s;
                LOG(MCoperatorError, unknownJob, e, s.append("Failure to backup dali to remote location: ").append(remoteBackupLocation));
                e->Release();
            }

            if (_newEdition)
                *_newEdition = newEdition;
            done = true;

            LOG(MCdebugInfo(100), unknownJob, "Store saved");
        }
        catch (IException *e)
        {
            StringBuffer s("Exception(3) - Error saving store file");
            LOG(MCoperatorError, unknownJob, e, s.str());
            e->Release();
        }
        if (done)
        {
#ifndef NODELETE
            unsigned toDeleteEdition = prevEditionN(edition, keepStores+(currentEdition?1:0));
            StringBuffer filename(location);
            constructStoreName(storeName, toDeleteEdition, filename);
            OwnedIFile iFile = createIFile(filename.str());
            if (iFile->exists())
                PROGLOG("Deleting old store: %s", filename.str());
            removeDaliFile(location, storeName, toDeleteEdition);
            removeDaliFile(location, DELTANAME, toDeleteEdition);
            removeDaliFile(location, DELTADETACHED, toDeleteEdition);
            if (remoteBackupLocation)
            {
                removeDaliFile(remoteBackupLocation, storeName, toDeleteEdition);
                removeDaliFile(remoteBackupLocation, DELTANAME, toDeleteEdition);
                removeDaliFile(remoteBackupLocation, DELTADETACHED, toDeleteEdition);
            }
#endif
        }
    }
    virtual unsigned queryCurrentEdition()
    {
        refreshStoreInfo();
        return storeInfo.edition;
    }
    virtual StringBuffer &getCurrentStoreFilename(StringBuffer &res, unsigned *crc=NULL)
    {
        refreshStoreInfo();
        constructStoreName(storeName, storeInfo.edition, res);
        if (crc)
            * crc = storeInfo.crc;
        return res;
    }
    virtual StringBuffer &getCurrentDeltaFilename(StringBuffer &res, unsigned *crc=NULL)
    {
        refreshDeltaInfo();
        constructStoreName(DELTANAME, deltaInfo.edition, res);
        if (crc)
            * crc = deltaInfo.crc; // TBD, combine into store.<edition>.<store_crc>.<delta_crc>
        return res;
    }
    virtual StringBuffer &getCurrentStoreInfoFilename(StringBuffer &res)
    {
        refreshStoreInfo();
        res.append(storeInfo.cache);
        return res;
    }
    virtual void backup(const char *filename)
    {
        try
        {
            unsigned crc = getFileCRC(filename);
            StringBuffer dst(location);
            if (dst.length())
                addPathSepChar(dst);
            dst.append(DEBUG_DIR);
            addPathSepChar(dst);
            recursiveCreateDirectoryForFile(dst.str());
            OwnedIFile dFile = createIFile(dst.str());
            Owned<IDirectoryIterator> dIter = dFile->directoryFiles();
            unsigned debugFiles = 0;
            ForEach (*dIter) debugFiles++;
            if (debugFiles >= 10) return;
            StringBuffer fname(filename);
            getFileNameOnly(fname);
            dst.append(fname.str()).append('.').append(crc);
            OwnedIFile backupIFile = createIFile(dst.str());
            if (!backupIFile->exists()) // a copy could already have been made
            {
                PROGLOG("Backing up: %s", filename);
                OwnedIFile iFile = createIFile(filename);
                copyFile(backupIFile, iFile);
                PROGLOG("Backup made: %s", dst.str());
            }
        }
        catch (IException *e)
        {
            StringBuffer tmp;
            EXCLOG(e, tmp.append("Failed to take backup of: ").append(filename).str());
            e->Release();
        }
    }
    virtual StringBuffer &getPrimaryLocation(StringBuffer &_location)
    {
        _location.append(location);
        return _location;
    }
    virtual StringBuffer &getBackupLocation(StringBuffer &backupLocation)
    {
        backupLocation.append(remoteBackupLocation);
        return backupLocation;
    }
friend struct CheckDeltaBlock;
};

IStoreHelper *createStoreHelper(const char *storeName, const char *location, const char *remoteBackupLocation, unsigned configFlags, unsigned keepStores, unsigned delay, const bool *abort)
{
    if (!storeName) storeName = "dalisds";
    return new CStoreHelper(storeName, location, remoteBackupLocation, configFlags, keepStores, delay, abort);
}

///////////////

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable : 4355)  // 'this' : used in base member initializer list
#endif

CCovenSDSManager::CCovenSDSManager(ICoven &_coven, IPropertyTree &_config, const char *_dataPath) 
    : coven(_coven), config(_config), server(*this), dataPath(_dataPath)
{
    config.Link();
    restartOnError = config.getPropBool("@restartOnUnhandled");
    root = NULL;
    writeTransactions=0;
    externalEnvironment = false;
    ignoreExternals=false;
    unsigned initNodeTableSize = queryCoven().getInitSDSNodes();
    allNodes.ensure(initNodeTableSize?initNodeTableSize:INIT_NODETABLE_SIZE);
    externalSizeThreshold = config.getPropInt("@externalSizeThreshold", DEFAULT_EXTERNAL_SIZE_THRESHOLD);
    remoteBackupLocation.set(config.queryProp("@remoteBackupLocation"));
    nextExternal = 1;
    if (0 == coven.getServerRank())
    {
        if (coven.size() > 1)
        {
            unsigned s;
            for (s=1; s<coven.size(); s++)
            {
                CMessageBuffer mb;
                __int64 dummy=0;
                mb.append(dummy);
                coven.sendRecv(mb, s, MPTAG_DALI_SDS_REQUEST);
                bool success;
                mb.read(success);
                assertex(success);
            }
        }
    }
    else
    {
        CMessageBuffer mb;
#ifdef TRACE_QWAITING
        unsigned waiting = coven.probe(0,MPTAG_DALI_SDS_REQUEST,NULL);
        if ((waiting!=0)&&(waiting%10==0))
            DBGLOG("QPROBE: MPTAG_DALI_SDS_REQUEST.2 has %d waiting",waiting);
#endif
        if (coven.recv(mb, 0, MPTAG_DALI_SDS_REQUEST, NULL))
        {
            __int64 dummy;
            mb.read(dummy);
            mb.clear().append(true); // denote success
            coven.reply(mb);
        }
        else
            assertex(false);
    }
    registerSubscriptionManager(SDS_PUBLISHER, this);
    connectionSubscriptionManager.setown(new CConnectionSubscriptionManager());
    registerSubscriptionManager(SDSCONN_PUBLISHER, connectionSubscriptionManager.get());

    // add external handlers
    Owned<CXMLFileExternal> xmlExternalHandler = new CXMLFileExternal(dataPath, backupHandler);
    externalHandlers.replace(* new CExternalHandlerMapping(EF_XML, *xmlExternalHandler));
    Owned<CLegacyBinaryFileExternal> legacyBinaryExternalHandler = new CLegacyBinaryFileExternal(dataPath, backupHandler);
    externalHandlers.replace(* new CExternalHandlerMapping(EF_LegacyBinaryValue, *legacyBinaryExternalHandler));
    Owned<CBinaryFileExternal> binaryExternalHandler = new CBinaryFileExternal(dataPath, backupHandler);
    externalHandlers.replace(* new CExternalHandlerMapping(EF_BinaryValue, *binaryExternalHandler));

    properties.setown(createPTree("Properties"));
    IPropertyTree *clientProps = properties->setPropTree("Client", config.hasProp("Client") ? config.getPropTree("Client") : createPTree());
    clientProps->setPropBool("@serverIterAvailable", true);
    clientProps->setPropBool("@useAppendOpt", true);
    clientProps->setPropBool("@serverGetIdsAvailable", true);
    IPropertyTree *throttle = clientProps->setPropTree("Throttle", createPTree());
    throttle->setPropInt("@limit", CLIENT_THROTTLE_LIMIT);
    throttle->setPropInt("@delay", CLIENT_THROTTLE_DELAY);
    // NB: dataPath is assumed to be local
    RemoteFilename rfn;
    if (dataPath.length())
        rfn.setLocalPath(dataPath);
    else
    {
        char cwd[1024];
        if (!GetCurrentDirectory(1024, cwd)) {
            ERRLOG("CCovenSDSManager: Current directory path too big, setting local path to null");
            cwd[0] = 0;
        }
        rfn.setLocalPath(cwd);
    }
    unsigned keepLastN = config.getPropInt("@keepStores", DEFAULT_KEEP_LASTN_STORES);
    StringBuffer path;
    rfn.getRemotePath(path);
    properties->setProp("@dataPathUrl", path.str());
    properties->setPropInt("@keepStores", keepLastN);
    if (remoteBackupLocation.length())
    {
        properties->setProp("@backupPathUrl", remoteBackupLocation.get());
        backupHandler.init(remoteBackupLocation, config.getPropBool("@asyncBackup", true));
    }

    const char *storeName = config.queryProp("@store");
    if (!storeName) storeName = "dalisds";
#if 1 // legacy
    StringBuffer tail, ext;
    splitFilename(storeName, NULL, NULL, &tail, &ext);
    if (0 == stricmp(".xml", ext.str()))
    {
        config.setProp("@store", tail.str());
        storeName = tail.str();
    }
#endif
    StringBuffer tmp(dataPath);
    OwnedIFile inProgressIFile = createIFile(tmp.append(DELTAINPROGRESS).str());
    inProgressIFile->remove();
    OwnedIFile detachIPIFile = createIFile(tmp.append(DETACHINPROGRESS).str());
    detachIPIFile->remove();

    unsigned configFlags = config.getPropBool("@recoverFromIncErrors", true) ? SH_RecoverFromIncErrors : 0;
    configFlags |= config.getPropBool("@backupErrorFiles", true) ? SH_BackupErrorFiles : 0;
    iStoreHelper = createStoreHelper(storeName, dataPath, remoteBackupLocation, configFlags, keepLastN, 100, &server.queryStopped());
    doTimeComparison = false;
    if (config.getPropBool("@lightweightCoalesce", true))
        coalesce.setown(new CLightCoalesceThread(config, iStoreHelper));
}

#ifdef _MSC_VER
#pragma warning (pop)
#endif

CCovenSDSManager::~CCovenSDSManager()
{
    backupHandler.stop();
    if (unhandledThread) unhandledThread->join();
    if (coalesce) coalesce->stop();
    scanNotifyPool.clear();
    notifyPool.clear();
    connections.kill();
    ::Release(iStoreHelper);
    if (!config.getPropBool("@leakStore", true)) // intentional default leak of time consuming deconstruction of tree
        ::Release(root);
    else
        enableMemLeakChecking(false);
    config.Release();
}

bool compareFiles(IFile *file1, IFile *file2, bool compareTimes=true)
{
    if (file1->exists())
    {
        if (file2->exists())
        {
            if (file1->size() == file2->size())
            {
                if (!compareTimes) return true;
                CDateTime modifiedTimeBackup;
                file1->getTime(NULL, &modifiedTimeBackup, NULL);
                CDateTime modifiedTime;
                file2->getTime(NULL, &modifiedTime, NULL);
                if (0 == modifiedTimeBackup.compare(modifiedTime, false))
                    return true;
            }
        }
    }
    else
        return !file2->exists();
    return false;
}

void CCovenSDSManager::validateBackup()
{
    // check consistency of store info file.
    StringBuffer storeInfoFilename(dataPath);
    iStoreHelper->getCurrentStoreInfoFilename(storeInfoFilename);
    OwnedIFile infoIFile = createIFile(storeInfoFilename.str());
    if (infoIFile->exists())
    {
        StringBuffer rL(remoteBackupLocation);
        iStoreHelper->getCurrentStoreInfoFilename(rL);
        copyFile(rL.str(), storeInfoFilename.str());
    }

    // check consistency of delta
    StringBuffer deltaFilename(dataPath);
    iStoreHelper->getCurrentDeltaFilename(deltaFilename);
    OwnedIFile iFileDelta = createIFile(deltaFilename.str());
    deltaFilename.clear().append(remoteBackupLocation);
    iStoreHelper->getCurrentDeltaFilename(deltaFilename);
    OwnedIFile iFileDeltaBackup = createIFile(deltaFilename.str());
    if (!compareFiles(iFileDeltaBackup, iFileDelta, false))
        WARNLOG("Delta file backup doesn't exist or differs, filename=%s", deltaFilename.str());

    // ensure there's a copy of the primary store present at startup.
    StringBuffer storeFilename(dataPath);
    iStoreHelper->getCurrentStoreFilename(storeFilename);
    OwnedIFile iFileStore = createIFile(storeFilename.str());

    storeFilename.clear().append(remoteBackupLocation);
    iStoreHelper->getCurrentStoreFilename(storeFilename);
    OwnedIFile iFileBackupStore = createIFile(storeFilename.str());
    if (!compareFiles(iFileBackupStore, iFileStore))
        WARNLOG("Store backup file doesn't exist or differs, filename=%s", storeFilename.str());
}

static int uint64compare(unsigned __int64 *i1, unsigned __int64 *i2)
{
    if (*i1==*i2) return 0;
    if (*i1<*i2) return -1;
    return 1;
}

class CLegacyFmtItem : public CInterface
{
public:
    CLegacyFmtItem(const char *_name, const char *_ext, unsigned __int64 _num) : name(_name), ext(_ext), num(_num) { }
    StringAttr name, ext;
    unsigned __int64 num;
};

static int extNcompareFunc(CInterface **_itm1, CInterface **_itm2)
{
    CLegacyFmtItem *itm1 = *(CLegacyFmtItem **)_itm1;
    CLegacyFmtItem *itm2 = *(CLegacyFmtItem **)_itm2;
    if (itm1->num==itm2->num) return 0;
    if (itm1->num<itm2->num) return -1;
    return 1;
}

void CCovenSDSManager::loadStore(const char *storeName, const bool *abort)
{
    if (root) root->Release();

    class CNodeCreate : public CInterface, implements IPTreeNodeCreator
    {
    public:
        IMPLEMENT_IINTERFACE;
        virtual IPropertyTree *create(const char *tag) { return createServerTree(tag); }
    } nodeCreator;
    class CSDSTreeMaker : public CPTreeMaker
    {
    public:
        CSDSTreeMaker(IPTreeNodeCreator *nodeCreator) : CPTreeMaker(ipt_none, nodeCreator) { }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            IPropertyTree *node = queryCurrentNode();
            CPTreeMaker::endNode(tag, length, value, binary, endOffset);
            if (((CServerRemoteTree *)node)->testExternalCandidate())
                convertQueue.append(*(CServerRemoteTree *)node);
        }
        ICopyArrayOf<CServerRemoteTree> convertQueue;
    } treeMaker(&nodeCreator);

    Owned<IPropertyTree> oldEnvironment;
    try
    {
        bool saveNeeded = false;
        if (!storeName)
            storeName = config.queryProp("@store");

        unsigned crc = 0;
        StringBuffer storeFilename(dataPath);
        iStoreHelper->getCurrentStoreFilename(storeFilename, &crc);

        LOG(MCdebugInfo(100), unknownJob, "loading store %d, storedCrc=%x", iStoreHelper->queryCurrentEdition(), crc);
        root = (CServerRemoteTree *)::loadStore(storeFilename.str(), &treeMaker, crc, false, abort);
        if (!root)
        {
            StringBuffer s(storeName);
            LOG(MCdebugInfo(100), unknownJob, "Store %d does not exist, creating new store", iStoreHelper->queryCurrentEdition());
            root = new CServerRemoteTree("SDS");
        }
        bool errors;
        Owned<IException> deltaE;
        try { iStoreHelper->loadDeltas(root, &errors); }
        catch (IException *e) { deltaE.setown(e); errors = true; }
        if (errors && config.getPropBool("@backupErrorFiles", true))
        {
            iStoreHelper->backup(storeFilename.str());
            if (deltaE.get())
                throw LINK(deltaE);
        }
        LOG(MCdebugInfo(100), unknownJob, "store loaded");
        const char *environment = config.queryProp("@environment");

        if (environment && *environment)
        {
            LOG(MCdebugInfo(100), unknownJob, "loading external Environment from: %s", environment);
            Owned<IFile> envFile = createIFile(environment);
            if (!envFile->exists())
                throw MakeStringException(0, "'%s' does not exist", environment);
            OwnedIFileIO iFileIO = envFile->open(IFOread);
            if (!iFileIO)
                throw MakeStringException(0, "Failed to open '%s'", environment);
            Owned<IPropertyTree> envTree = createPTreeFromXMLFile(environment);
            if (0 != stricmp("Environment", envTree->queryName()))
                throw MakeStringException(0, "External environment file '%s', has '%s' as root, expecting a 'Environment' xml node.", environment, envTree->queryName());

            oldEnvironment.setown(root->getPropTree("Environment"));
            root->removeTree(oldEnvironment);
            root->addPropTree("Environment", envTree.getClear());
            externalEnvironment = true;
        }

        UInt64Array refExts;
        PROGLOG("Scanning store for external references");
        Owned<IPropertyTreeIterator> rootIter = root->getElements("//*");
        ForEach (*rootIter)
        {
            IPropertyTree &tree = rootIter->query();
            __int64 index = tree.getPropInt64(EXT_ATTR);
            if (index)
                refExts.append(index);
        }
        PROGLOG("External reference count = %d", refExts.ordinality());
        refExts.sort(uint64compare);
        if (refExts.ordinality())
            nextExternal = refExts.tos()+1; // JCSMORE could keep array and fill gaps

// build list of primary, backup and legacy type external files
        CIArrayOf<CLegacyFmtItem> legacyFmts;
        UInt64Array primaryExts, backupExts;
        unsigned l = strlen(EXTERNAL_NAME_PREFIX);
        bool primary = true;
        Owned<IDirectoryIterator> di = createDirectoryIterator(dataPath);
        loop
        {
            try
            {
                ForEach(*di)
                {
                    StringBuffer fname;
                    di->getName(fname);
                    if (fname.length() > l && 0 == memicmp(EXTERNAL_NAME_PREFIX, fname.str(), l))
                    {
                        StringBuffer name, ext;
                        splitFilename(fname, NULL, NULL, &name, &ext);

                        if (ext.length())
                        {
                            ext.remove(0, 1); // delete '.'
                            if (0 != stricmp(EF_BinaryValue, ext.str()))
                            {
                                unsigned __int64 num = atoi64(name.str()+l);
                                legacyFmts.append(* new CLegacyFmtItem(name.str(), ext.str(), num));
                            }
#ifndef _WIN32 // win32 files are case insensitive, so no point in this step.
                            StringBuffer lfName(fname);
                            lfName.toLowerCase();
                            if (0 != strcmp(fname.str(), lfName.str()))
                                di->query().rename(lfName.str());
#endif
                            unsigned __int64 extN = atoi64(name.str()+l);
                            if (primary)
                                primaryExts.append(extN);
                            else
                                backupExts.append(extN);
                        }
                    }
                }
            }
            catch (IException *e)
            {
                if (primary)
                    throw;
                EXCLOG(e, NULL);
                e->Release();
            }
            if (!primary || 0 == remoteBackupLocation.length())
                break;
            di.setown(createDirectoryIterator(remoteBackupLocation));
            primary = false;
        }
        primaryExts.sort(uint64compare);
        backupExts.sort(uint64compare);

// Compare list of primary/backup externals against referenced externals and issue warnings
// Copy over any if reference and present either only in primary or backup
        IExternalHandler *extHandler = queryExternalHandler(EF_BinaryValue);
        primary = true;
        UInt64Array missingPrimarys;
        loop
        {
            UInt64Array &fileExts = primary ? primaryExts : backupExts;
            unsigned __int64 refN = refExts.ordinality() ? refExts.item(0) : (unsigned __int64)-1;
            unsigned __int64 fileN = fileExts.ordinality() ? fileExts.item(0) : (unsigned __int64)-1;
            unsigned fileP=0;
            unsigned refP=0;
            unsigned missP=0;
            while (fileN != ((unsigned __int64)-1) || refN != ((unsigned __int64)-1))
            {
                while (fileN == refN)
                {
                    if (!primary)
                    {
                        bool found = false;
                        while (missP < missingPrimarys.ordinality())
                        {
                            unsigned __int64 missN = missingPrimarys.item(missP);
                            if (refN == missN)
                            {
                                ++missP;
                                found = true;
                                break;
                            }
                            if (missN > refN)
                                break;
                            ++missP;
                        }   
                        // i.e. found in backup, but was listed missing in primary
                        if (found)
                        {
                            StringBuffer fname;
                            StringBuffer name(EXTERNAL_NAME_PREFIX);
                            name.append(refN);
                            extHandler->getFilename(fname, name.str());
                            PROGLOG("Copying missing primary external from backup: %s", fname.str());
                            StringBuffer backupFname(remoteBackupLocation);
                            extHandler->getName(backupFname, name.str());
                            try
                            {
                                copyFile(fname.str(), backupFname.str());
                            }
                            catch (IException *e)
                            {
                                EXCLOG(e, NULL);
                                e->Release();
                            }
                        }
                    }
                    ++refP;
                    ++fileP;
                    refN = (refP == refExts.ordinality()) ? (unsigned __int64)-1 : refExts.item(refP);
                    fileN = (fileP == fileExts.ordinality()) ? (unsigned __int64)-1 : fileExts.item(fileP);
                    if (fileN == ((unsigned __int64)-1) || refN == ((unsigned __int64)-1))
                        break;
                }
                while (fileN < refN)
                {
                    StringBuffer fname;
                    StringBuffer name(EXTERNAL_NAME_PREFIX);
                    name.append(fileN);
                    extHandler->getName(fname, name.str());
                    WARNLOG("Unreferenced %s external %s file", primary?"primary":"backup", fname.str());
                    ++fileP;
                    if (fileP == fileExts.ordinality())
                    {
                        fileN = (unsigned __int64)-1;
                        break;
                    }
                    fileN = fileExts.item(fileP);
                }
                while (refN < fileN)
                {
                    StringBuffer fname;
                    StringBuffer name(EXTERNAL_NAME_PREFIX);
                    name.append(refN);
                    extHandler->getName(fname, name.str());
                    WARNLOG("External %s file reference %s missing", primary?"primary":"backup", fname.str());
                    if (primary)
                        missingPrimarys.append(refN);
                    else
                    {
                        bool found = false;
                        while (missP < missingPrimarys.ordinality())
                        {
                            unsigned __int64 missN = missingPrimarys.item(missP);
                            if (refN == missN)
                            {
                                ++missP;
                                found = true;
                                break;
                            }
                            if (missN > refN)
                                break;
                            ++missP;
                        }
                        if (!found)
                        {
                            // i.e. not missing in primary, but missing in backup
                            StringBuffer fname;
                            StringBuffer name(EXTERNAL_NAME_PREFIX);
                            name.append(refN);
                            extHandler->getFilename(fname, name.str());
                            PROGLOG("Copying missing backup external from primary: %s", fname.str());
                            StringBuffer backupFname(remoteBackupLocation);
                            extHandler->getName(backupFname, name.str());
                            try
                            {
                                copyFile(backupFname.str(), fname.str());
                            }
                            catch (IException *e)
                            {
                                EXCLOG(e, NULL);
                                e->Release();
                            }
                        }
                    }
                    ++refP;
                    if (refP == refExts.ordinality())
                    {
                        refN = (unsigned __int64)-1;
                        break;
                    }
                    refN = refExts.item(refP);
                }
            }
            if (!primary || 0 == remoteBackupLocation.length())
                break;
            if (missingPrimarys.ordinality())
                missingPrimarys.sort(uint64compare);
            primary = false;
        }
// check any marked with legacy formats
        if (legacyFmts.ordinality() && refExts.ordinality())
        {
            legacyFmts.sort(extNcompareFunc);

            unsigned refP = 0;
            unsigned __int64 refN = refExts.item(refP++);
            bool done = false;
            ForEachItemIn(l, legacyFmts)
            {
                CLegacyFmtItem &itm = legacyFmts.item(l);
                while (refN<itm.num)
                {
                    if (refP == refExts.ordinality())
                    {
                        done = true;
                        break;
                    }
                    refN = refExts.item(refP++);
                }
                if (done)
                    break;
                if (refN == itm.num)
                {
                    IExternalHandler *extHandler = queryExternalHandler(itm.ext);
                    if (!extHandler)
                    {
                        WARNLOG("Unknown external extension, external=%s, extension=%s", itm.name.get(), itm.ext.get());
                        continue;
                    }
                    StringBuffer fname;
                    extHandler->getFilename(fname, itm.name);
                    PROGLOG("Converting legacy external type(%s) to new, file=%s", itm.ext.get(), fname.str());
                    MemoryBuffer mb;
                    Owned<IPropertyTree> tree = createPTree("tmp");
                    extHandler->read(itm.name, *tree, mb, true);
                    mb.append(""); // no children
                    tree.setown(createPTree(mb));
                    IExternalHandler *extHandlerNew = queryExternalHandler(EF_BinaryValue);
                    extHandlerNew->write(itm.name, *tree);
                    extHandler->remove(itm.name);
                }
            }
        }
        root->removeProp("External"); // remove legacy /External if present
        unsigned items = treeMaker.convertQueue.ordinality();
        if (items)
        {
            LOG(MCdebugInfo(100), unknownJob, "Converting %d items larger than threshold size %d, to external definitions", items, externalSizeThreshold);
            ForEachItemIn(i, treeMaker.convertQueue)
                SDSManager->writeExternal(treeMaker.convertQueue.item(i), true);
            saveNeeded = true;
        }
        if (saveNeeded)
        {
            LOG(MCdebugInfo(100), unknownJob, "Saving converted store");
            SDSManager->saveStore();
        }
    }
    catch (IException *e)
    {
        LOG(MCoperatorError, unknownJob, e, "Exception - Failed to load main store");
        throw;
    }
    catch (DALI_CATCHALL)
    {
        LOG(MCoperatorError, unknownJob, "Unknown exception - Failed to load main store");
        throw;
    }

    if (!root)
    {
        root = (CServerRemoteTree *) createServerTree();
        root->setName("root");
    }
    if (remoteBackupLocation.length())
    {
        try { validateBackup(); }
        catch (IException *e) { LOG(MCoperatorError, unknownJob, e, "Validating backup"); e->Release(); }

        StringBuffer deltaFilename(dataPath);
        iStoreHelper->getCurrentDeltaFilename(deltaFilename);
        OwnedIFile iFileDelta = createIFile(deltaFilename.str());
        deltaFilename.clear().append(remoteBackupLocation);
        iStoreHelper->getCurrentDeltaFilename(deltaFilename);
        OwnedIFile iFileDeltaBackup = createIFile(deltaFilename.str());
        CDateTime modifiedTime;
        if (!iFileDelta->exists() && !iFileDeltaBackup->exists())
            doTimeComparison = true;
        else if (iFileDelta->getTime(NULL, &modifiedTime, NULL))
        {
            if (iFileDeltaBackup->setTime(NULL, &modifiedTime, NULL))
                doTimeComparison = true;
        }
        if (!doTimeComparison)
            LOG(MCoperatorWarning, unknownJob, "Unable to use time comparison when comparing delta backup file");
    }
    Owned<IRemoteConnection> conn = connect("/", 0, RTM_INTERNAL, INFINITE);
    initializeInternals(conn->queryRoot());
    conn.clear();
    bool forceGroupUpdate = config.getPropBool("@forceGroupUpdate");
    StringBuffer response;
    initClusterGroups(forceGroupUpdate, response, oldEnvironment);
    if (response.length())
        PROGLOG("DFS group initialization : %s", response.str()); // should this be a syslog?
}

void CCovenSDSManager::saveStore(const char *storeName, bool currentEdition)
{
    struct CIgnore
    {
        CIgnore() { SDSManager->ignoreExternals=true; }
        ~CIgnore() { SDSManager->ignoreExternals=false; }
    } ignore;
    iStoreHelper->saveStore(root, NULL, currentEdition);
    unsigned initNodeTableSize = allNodes.maxElements()+OVERFLOWSIZE;
    queryCoven().setInitSDSNodes(initNodeTableSize>INIT_NODETABLE_SIZE?initNodeTableSize:INIT_NODETABLE_SIZE);
}

CServerRemoteTree *CCovenSDSManager::queryRegisteredTree(__int64 uniqId)
{
    CHECKEDCRITICALBLOCK(treeRegCrit, fakeCritTimeout);
    return (CServerRemoteTree *)allNodes.queryElem(uniqId);
}

CServerRemoteTree *CCovenSDSManager::getRegisteredTree(__int64 uniqId)
{
    CHECKEDCRITICALBLOCK(treeRegCrit, fakeCritTimeout);
    return (CServerRemoteTree *)allNodes.getElem(uniqId);
}

CServerRemoteTree *CCovenSDSManager::queryRoot()
{
    return root;
}

StringBuffer &transformToAbsolute(StringBuffer &result, const char *xpath, unsigned index)
{
    const char *end = xpath+strlen(xpath);
    const char *p = end;
    const char *q = NULL;
    loop
    {
        if (p == xpath)
        {
            p = end;
            break;
        }
        --p;
        if ('/' == *p)
        {
            p = end;
            break;
        }
        else if ('[' == *p)
        {
            q = p;
            break;
        }
    }
    if (q)
        result.append(p-xpath, xpath);
    else
        result.append(xpath);

    result.append('[');
    result.append(index);
    result.append(']');
    return result;
}

void cleanChangeTree(IPropertyTree &tree)
{
    tree.removeProp("@id");
    Owned<IPropertyTreeIterator> iter = tree.getElements(RENAME_TAG);
    ForEach (*iter)
        iter->query().removeProp("@id");
    iter.setown(tree.getElements(DELETE_TAG));
    ForEach (*iter)
        iter->query().removeProp("@id");
    iter.setown(tree.getElements(RESERVED_CHANGE_NODE));
    ForEach (*iter)
        cleanChangeTree(iter->query());
}

void CCovenSDSManager::saveDelta(const char *path, IPropertyTree &changeTree)
{
    CHECKEDCRITICALBLOCK(saveIncCrit, fakeCritTimeout);
    // translate changeTree to inc format (e.g. remove id's)
    if (externalEnvironment)
    {
        // don't save any changed to /Environment if external
        if (0 == strncmp("/Environment", path, strlen("/Environment")))
        {
            WARNLOG("Attempt to change read-only Dali environment, path = %s", path);
            return;
        }
        if (0 == strcmp("/", path) && changeTree.hasProp("*[@name=\"Environment\"]"))
        {
            WARNLOG("Attempt to change read-only Dali environment, path = %s", path);
            return;
        }
    }
    cleanChangeTree(changeTree);
    // write out with header details (e.g. path)
    Owned<IPropertyTree> header = createPTree("Header");
    header->setProp("@path", path);
    IPropertyTree *delta = header->addPropTree("Delta", createPTree());
    delta->addPropTree(changeTree.queryName(), LINK(&changeTree));

    StringBuffer fname(dataPath);
    OwnedIFile deltaIPIFile = createIFile(fname.append(DELTAINPROGRESS).str());
    OwnedIFileIO deltaIPIFileIO = deltaIPIFile->open(IFOcreate);
    deltaIPIFileIO.clear();
    struct RemoveDIPBlock
    {
        IFile &iFile;
        bool done;
        void doit() { done = true; iFile.remove(); }
        RemoveDIPBlock(IFile &_iFile) : iFile(_iFile), done(false) { }
        ~RemoveDIPBlock () { if (!done) doit(); }
    } removeDIP(*deltaIPIFile);
    StringBuffer detachIPStr(dataPath);
    OwnedIFile detachIPIFile = createIFile(detachIPStr.append(DETACHINPROGRESS).str());
    if (detachIPIFile->exists()) // very small window where this can happen.
    {
        // implies other operation about to access current delta
        // CHECK session is really alive, otherwise it has been orphaned, so remove it.
        try
        {
            SessionId sessId = 0;
            OwnedIFileIO detachIPIO = detachIPIFile->open(IFOread);
            if (detachIPIO)
            {
                size_t s = detachIPIO->read(0, sizeof(sessId), &sessId);
                detachIPIO.clear();
                if (sizeof(sessId) == s)
                {
                    // double check session is really alive
                    if (querySessionManager().sessionStopped(sessId, 0))
                        detachIPIFile->remove();
                    else
                    {
                        // *cannot block* because other op (sasha) accessing remote dali files, can access dali.
                        removeDIP.doit();
                        PROGLOG("blocked");
                        toXML(header, blockedDelta);
                        return;
                    }
                }
            }
        }
        catch (IException *e) { EXCLOG(e, NULL); e->Release(); }
    }
    try
    {
        StringBuffer deltaFilename(dataPath);
        iStoreHelper->getCurrentDeltaFilename(deltaFilename);
        toXML(header, blockedDelta);
        OwnedIFile iFile = createIFile(deltaFilename.str());
        bool first = !iFile->exists() || 0 == iFile->size();
        writeDelta(blockedDelta, *iFile);
        if (remoteBackupLocation.length())
            backupHandler.addDelta(blockedDelta, iStoreHelper->queryCurrentEdition(), first);
        else
        {
            if (blockedDelta.length() > 0x100000)
                blockedDelta.kill();
            else
                blockedDelta.clear();
        }
    }
    catch (IException *e)
    {
        LOG(MCoperatorError, unknownJob, e, "saveDelta");
        e->Release();
    }
}

CSubscriberContainerList *CCovenSDSManager::getSubscribers(const char *xpath, CPTStack &stack)
{
    return subscribers.getQualifiedList(xpath, stack);
}

inline void serverToClientTree(CServerRemoteTree &src, CClientRemoteTree &dst)
{       
    if (src.getPropInt64(EXT_ATTR))
    {
        MemoryBuffer mb;
        src.serializeSelfRT(mb, false);
        dst.deserializeSelfRT(mb);
    }
    else
        dst.clone(src, true, false);
    dst.setServerId(src.queryServerId());
    if (src.hasChildren()) dst.addServerTreeInfo(STI_HaveChildren);
}

class CMultipleConnector : public CInterface, implements IMultipleConnector
{
    StringArray xpaths;
    UnsignedArray modes;

public:
    IMPLEMENT_IINTERFACE;

    CMultipleConnector() { }
    CMultipleConnector(MemoryBuffer &src)
    {
        unsigned c;
        src.read(c);
        xpaths.ensure(c);
        modes.ensure(c);
        while (c--)
        {
            StringAttr xpath;
            unsigned mode;
            src.read(xpath);
            src.read(mode);
            xpaths.append(xpath);
            modes.append(mode);
        }
    }

// IMultipleConnector impl.
    virtual void addConnection(const char *xpath, unsigned mode)
    {
        if (RTM_CREATE == (mode & RTM_CREATE_MASK) || RTM_CREATE_QUERY == (mode & RTM_CREATE_MASK))
            throw MakeSDSException(SDSExcpt_BadMode, "multiple connections do not support creation modes");
        xpaths.append(xpath);
        modes.append(mode);
    }
    virtual unsigned queryConnections() { return xpaths.ordinality(); }
    virtual void getConnectionDetails(unsigned which, StringAttr &xpath, unsigned &mode)
    {
        xpath.set(xpaths.item(which));
        mode = modes.item(which);
    }
    virtual void serialize(MemoryBuffer &dst)
    {
        unsigned c=xpaths.ordinality();
        dst.append(c);
        while (c--)
            dst.append(xpaths.item(c)).append(modes.item(c));
    }
};

IMultipleConnector *deserializeIMultipleConnector(MemoryBuffer &src)
{
    return new CMultipleConnector(src);
}

IMultipleConnector *createIMultipleConnector()
{
    return new CMultipleConnector();
}

StringBuffer &getMConnectString(IMultipleConnector *mConnect, StringBuffer &s)
{
    unsigned c;
    for (c=0; c<mConnect->queryConnections(); c++)
    {
        StringAttr xpath;
        unsigned mode;
        mConnect->getConnectionDetails(c, xpath, mode);

        s.append("xpath=\"").append(xpath).append("\" [mode=").append(mode).append("]");
        if (c != mConnect->queryConnections()-1)
            s.append(", ");
    }
    return s;
}

// ISDSManager impl.
IRemoteConnections *CCovenSDSManager::connect(IMultipleConnector *mConnect, SessionId id, unsigned timeout)
{
    Owned<CLCLockBlock> lockBlock;
    lockBlock.setown(new CLCReadLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));

    Owned<CRemoteConnections> remoteConnections = new CRemoteConnections;
    unsigned c;
    for (c=0; c<mConnect->queryConnections(); c++)
    {
        StringAttr xpath;
        unsigned mode;
        mConnect->getConnectionDetails(c, xpath, mode);

        // connect can return NULL.
        remoteConnections->add(connect(xpath, id, mode, timeout));
    }
    return LINK(remoteConnections);
}

IRemoteConnection *CCovenSDSManager::connect(const char *xpath, SessionId id, unsigned mode, unsigned timeout)
{
    Owned<CLCLockBlock> lockBlock;
    Owned<LinkingCriticalBlock> connectCritBlock;
    if (!RTM_MODE(mode, RTM_INTERNAL))
    {
        connectCritBlock.setown(new LinkingCriticalBlock(connectCrit, __FILE__, __LINE__));
        if (RTM_CREATE == (mode & RTM_CREATE_MASK) || RTM_CREATE_QUERY == (mode & RTM_CREATE_MASK))
            lockBlock.setown(new CLCWriteLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));
        else
            lockBlock.setown(new CLCReadLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));
    }

    CServerRemoteTree *_tree;
    Owned<CServerRemoteTree> tree;
    ConnectionId connectionId = 0;
    createConnection(id, mode, timeout, xpath, _tree, connectionId, true, connectCritBlock);
    if (connectionId)
        tree.setown(_tree);
    connectCritBlock.clear();
    if (connectionId)
    {
        CRemoteConnection *conn = new CRemoteConnection(*this, connectionId, xpath, id, mode, timeout);
        assertex(conn);
        CDisableFetchChangeBlock block(*conn);
        CClientRemoteTree *clientTree = new CClientRemoteTree(*conn);
        assertex(clientTree);
        serverToClientTree(*tree, *clientTree);
        conn->setRoot(clientTree);
        return conn;
    }
    return NULL;
}

SubscriptionId CCovenSDSManager::subscribe(const char *xpath, ISDSSubscription &notify, bool sub, bool sendValue)
{
    assertex(xpath);
    if (sub && sendValue)
        throw MakeSDSException(SDSExcpt_Unsupported, "Subscription to sub elements, with sendValue option unsupported");
    StringBuffer s;
    if ('/' != *xpath)
    {
        s.append('/').append(xpath);
        xpath = s.str();
    }
    CSDSSubscriberProxy *subscriber = new CSDSSubscriberProxy(xpath, sub, sendValue, notify);
    querySubscriptionManager(SDS_PUBLISHER)->add(subscriber, subscriber->getId());
    return subscriber->getId();
}

void CCovenSDSManager::unsubscribe(SubscriptionId id)
{
    querySubscriptionManager(SDS_PUBLISHER)->remove(id);
}

bool CCovenSDSManager::removeNotifyHandler(const char *handlerKey)
{
    return nodeNotifyHandlers.remove(handlerKey);
}

IPropertyTree *CCovenSDSManager::lockStoreRead() const
{
    PROGLOG("lockStoreRead() called");
    CHECKEDREADLOCKENTER(dataRWLock, readWriteTimeout);
    return root;
}

void CCovenSDSManager::unlockStoreRead() const
{
    PROGLOG("unlockStoreRead() called");
    dataRWLock.unlockRead();
}

bool CCovenSDSManager::setSDSDebug(StringArray &params, StringBuffer &reply)
{
    if (0 == params.ordinality()) return false;
    else if (0 == stricmp("datalockHoldTiming", params.item(0)))
    {
        if (params.ordinality()<2)
        {
            reply.append("datalockHoldTiming currently = ").append(readWriteSlowTracing);
            return false;
        }
        unsigned ms = atoi(params.item(1));
        readWriteSlowTracing = ms;

        PROGLOG("datalock, readWriteSlowTracing timing set to %d", readWriteSlowTracing);
    }
    else if (0 == stricmp("datalockHoldStack", params.item(0)))
    {
        if (params.ordinality()<2)
        {
            reply.append("datalockHoldStack currently set to '").append(readWriteStackTracing?"on":"off").append("'");
            return false;
        }
        readWriteStackTracing = (0 == stricmp("on", params.item(1)));

        PROGLOG("datalock, held time stacks set to '%s'", readWriteStackTracing?"on":"off");
    }
    else if (0 == stricmp("datalockRetryStackTiming", params.item(0)))
    {
        if (params.ordinality()<2)
        {
            reply.append("datalockRetryStackTiming currently =").append(readWriteTimeout);
            return false;
        }
        unsigned ms = atoi(params.item(1));
        readWriteTimeout = ms;

        PROGLOG("datalock, readWriteTimeout timing set to %s", readWriteStackTracing?"on":"off");
    }
    else if (0 == stricmp("fakecritTiming", params.item(0)))
    {
        if (params.ordinality()<2)
        {
            reply.append("fakeCritTimeout currently =").append(fakeCritTimeout);
            return false;
        }
        unsigned ms = atoi(params.item(1));
        fakeCritTimeout = ms;

        PROGLOG("fakecrit, fakeCritTimeout timing set to %d", fakeCritTimeout);
    }
    else
    {
        reply.append("Unknown command");
        return false;
    }
    return true;
}

void CCovenSDSManager::saveRequest()
{
    blockingSave();
}

IPropertyTree &CCovenSDSManager::queryProperties() const
{
    return *properties;
}

void CCovenSDSManager::installNotifyHandler(const char *handlerKey, ISDSNotifyHandler *handler)
{
    nodeNotifyHandlers.replace(* new CSDSNotifyHandlerMapping(handlerKey, *handler));
}

// ISDSConnectionManager impl.
void CCovenSDSManager::commit(CRemoteConnection &connection, bool *disconnectDeleteRoot)
{
    Owned<CLCWriteLockBlock> lockBlock;
    if (!RTM_MODE(connection.queryMode(), RTM_INTERNAL))
        lockBlock.setown(new CLCWriteLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));

    CRemoteTreeBase *tree = (CRemoteTreeBase *) connection.queryRoot();

    bool lazyFetch = connection.setLazyFetch(false);
    Owned<IPropertyTree> changeTree = tree->collateData();
    connection.setLazyFetch(lazyFetch);
    if (NULL == disconnectDeleteRoot && !changeTree) return;

    ConnectionId connectionId = connection.queryConnectionId();
    CServerConnection *serverConnection = queryConnection(connectionId);
    if (!serverConnection)
        throw MakeSDSException(SDSExcpt_ConnectionAbsent, " [commit]");
    try
    {
        CServerRemoteTree *serverTree = (CServerRemoteTree *)serverConnection->queryRoot();
        assertex(serverTree);
        MemoryBuffer newIds, inc;
        if (changeTree && serverTree->processData(*serverConnection, *changeTree, newIds))
        { // something commited, if RTM_Create was used need to remember this.
            StringBuffer path;
            serverConnection->queryPTreePath().getAbsolutePath(path);
            saveDelta(path.str(), *changeTree);
            bool lazyFetch = connection.setLazyFetch(false);
            tree->clearCommitChanges(&newIds);
            assertex(newIds.getPos() == newIds.length()); // must have read it all
            connection.setLazyFetch(lazyFetch);
        }
    }
    catch (IException *)
    {
        if (disconnectDeleteRoot)
        {
            connection.setConnected(false);
            disconnect(connectionId, *disconnectDeleteRoot);
        }
        throw;
    }
    if (disconnectDeleteRoot)
    {
        connection.setConnected(false);
        disconnect(connectionId, *disconnectDeleteRoot);
    }
}

CRemoteTreeBase *CCovenSDSManager::get(CRemoteConnection &connection, __int64 serverId)
{
    Owned<CLCReadLockBlock> lockBlock;
    if (!RTM_MODE(connection.queryMode(), RTM_INTERNAL))
        lockBlock.setown(new CLCReadLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));
    CDisableFetchChangeBlock block(connection);
    CRemoteTreeBase *connectionRoot = (CRemoteTreeBase *) connection.queryRoot();
    Owned<CServerRemoteTree> tree = getRegisteredTree(connectionRoot->queryServerId());
    if (!tree)
        return NULL;
    CClientRemoteTree *newTree = new CClientRemoteTree(connection);
    serverToClientTree(*tree, *newTree);
    return newTree;
}

void CCovenSDSManager::getChildren(CRemoteTreeBase &parent, CRemoteConnection &connection, unsigned levels)
{
    Owned<CLCReadLockBlock> lockBlock;
    if (!RTM_MODE(connection.queryMode(), RTM_INTERNAL))
        lockBlock.setown(new CLCReadLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));
    CDisableFetchChangeBlock block(connection);
    Owned<CServerRemoteTree> serverParent = (CServerRemoteTree *)getRegisteredTree(parent.queryServerId());
    if (serverParent)
        _getChildren(parent, *serverParent, connection, levels);
}

void CCovenSDSManager::getChildrenFor(CRTArray &childLessList, CRemoteConnection &connection, unsigned levels)
{
    Owned<CLCReadLockBlock> lockBlock;
    if (!RTM_MODE(connection.queryMode(), RTM_INTERNAL))
        lockBlock.setown(new CLCReadLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));
    CDisableFetchChangeBlock block(connection);

    ForEachItemIn(f, childLessList)
    {
        CRemoteTreeBase &tree = childLessList.item(f);
        Owned<CServerRemoteTree> serverParent = (CServerRemoteTree *)getRegisteredTree(tree.queryServerId());
        if (serverParent)
            _getChildren(tree, *serverParent, connection, levels);
    }
}

static void addServerChildren(CClientRemoteTree &clientParent, CServerRemoteTree &serverParent, bool recurse)
{
    Owned<IPropertyTreeIterator> iter = serverParent.getElements("*");
    ForEach (*iter)
    {
        CServerRemoteTree &serverChild = (CServerRemoteTree &)iter->query();
        CClientRemoteTree *clientChild = (CClientRemoteTree *)clientParent.create(NULL);
        serverToClientTree(serverChild, *clientChild);
        clientChild = (CClientRemoteTree *)clientParent.addPropTree(clientChild->queryName(), clientChild);
        if (recurse)
            addServerChildren(*clientChild, serverChild, recurse);
    }
}

void CCovenSDSManager::matchServerTree(CClientRemoteTree *local, IPropertyTree &matchTree, bool allTail)
{
    Owned<IPropertyTreeIterator> matchIter = matchTree.getElements("*");
    if (matchIter->first())
    {
        if (local->hasChildren() && NULL == local->queryChildren())
        {
            local->createChildMap();
            Owned<CServerRemoteTree> tree = getRegisteredTree(matchTree.getPropInt64("@serverId"));
            addServerChildren(*local, *tree, false);
        }

        do
        {
            IPropertyTree &elem = matchIter->query();
            StringBuffer path(elem.queryName());
            path.append('[').append(elem.getPropInt("@pos")).append(']');
            CClientRemoteTree *child = (CClientRemoteTree *)local->queryPropTree(path.str());
            if (child) // if not would imply some other thread deleted I think.
                matchServerTree(child, elem, allTail);
        }
        while (matchIter->next());
    }
    else
    {
        if (local->hasChildren() && NULL == local->queryChildren())
        {
            local->createChildMap();
            Owned<CServerRemoteTree> tree = getRegisteredTree(matchTree.getPropInt64("@serverId"));
            addServerChildren(*local, *tree, allTail);
        }
    }
}

void CCovenSDSManager::ensureLocal(CRemoteConnection &connection, CRemoteTreeBase &_parent, IPropertyTree *serverMatchTree, IPTIteratorCodes flags)
{
    CClientRemoteTree &parent = (CClientRemoteTree &)_parent;
    bool getLeaves = iptiter_remotegetbranch == (flags & iptiter_remotegetbranch);

    CDisableFetchChangeBlock block(connection);
    matchServerTree(&parent, *serverMatchTree, getLeaves);
}

void CCovenSDSManager::_getChildren(CRemoteTreeBase &parent, CServerRemoteTree &serverParent, CRemoteConnection &connection, unsigned levels)
{
    Owned<IPropertyTreeIterator> iter = serverParent.getElements("*");
    assertex(iter);

    if (levels && serverParent.getPropBool("@fetchEntire"))
        levels = 0;

    ForEach(*iter)
    {
        CServerRemoteTree &child = (CServerRemoteTree &)iter->query();
        CClientRemoteTree *newChild = new CClientRemoteTree(connection); // clone create
        serverToClientTree(child, *newChild);
        parent.addPropTree(newChild->queryName(), newChild);
        if (0==levels || child.getPropBool("@fetchEntire"))
            _getChildren(*newChild, child, connection, 0);
    }
}

IPropertyTreeIterator *CCovenSDSManager::getElements(CRemoteConnection &connection, const char *xpath)
{
    Owned<CLCReadLockBlock> lockBlock = new CLCReadLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__);
    CDisableFetchChangeBlock block(connection);
    Owned<CServerRemoteTree> serverConnRoot = (CServerRemoteTree *)getRegisteredTree(((CClientRemoteTree *)connection.queryRoot())->queryServerId());
    Owned<CPTArrayIterator> elements = new CPTArrayIterator();
    Owned<IPropertyTreeIterator> iter = serverConnRoot->getElements(xpath);
    ForEach (*iter)
    {
        CServerRemoteTree &child = (CServerRemoteTree &)iter->query();
        Owned<CClientRemoteTree> newChild = new CClientRemoteTree(connection);
        serverToClientTree(child, *newChild);
        elements->array.append(*LINK(newChild));
    }
    return LINK(elements);
}

void CCovenSDSManager::changeMode(CRemoteConnection &connection, unsigned mode, unsigned timeout, bool suppressReloads)
{
    if (mode & RTM_CREATE_MASK)
        throw MakeSDSException(SDSExcpt_BadMode, "calling changeMode");
    ConnectionId connectionId = connection.queryConnectionId();
    Linked<CServerConnection> serverConnection = queryConnection(connectionId);
    if (!serverConnection)
        throw MakeSDSException(SDSExcpt_ConnectionAbsent, " [changeMode]");
    CServerRemoteTree *connectionRoot = (CServerRemoteTree *) serverConnection->queryRoot();

    unsigned prevMode = connection.queryMode();
    if (RTM_MODE(prevMode, RTM_LOCK_WRITE) && !RTM_MODE(mode, RTM_LOCK_WRITE))
        commit(connection, NULL);
    changeLockMode(*serverConnection, mode, timeout);
    if (!suppressReloads)
    {
        if (RTM_MODE(mode, RTM_LOCK_WRITE) && !RTM_MODE(prevMode, RTM_LOCK_WRITE) && !RTM_MODE(prevMode, RTM_LOCK_READ))
            connection.reload();
    }
}

IPropertyTree *CCovenSDSManager::getXPaths(__int64 serverId, const char *xpath, bool getServerIds)
{
    Owned<CServerRemoteTree> tree = getRegisteredTree(serverId);
    if (!tree)
        return NULL;
    IPropertyTree *matchTree = getXPathMatchTree(*tree, xpath);
    if (!matchTree)
        return NULL;
    if (getServerIds)
        populateWithServerIds(matchTree, tree);
    return matchTree;
}

IPropertyTreeIterator *CCovenSDSManager::getXPathsSortLimit(const char *baseXPath, const char *matchXPath, const char *sortBy, bool caseinsensitive, bool ascending, unsigned from, unsigned limit)
{
    Owned<IPropertyTree> matchTree = getXPathsSortLimitMatchTree(baseXPath, matchXPath, sortBy, caseinsensitive, ascending, from, limit);
    if (!matchTree)
        return createNullPTreeIterator();
    IPropertyTree *baseTree = SDSManager->queryRoot()->queryPropTree(baseXPath);
    return new CXPathIterator(baseTree, matchTree, iptiter_null);
}

IPropertyTree *CCovenSDSManager::getXPathsSortLimitMatchTree(const char *baseXPath, const char *matchXPath, const char *sortby, bool caseinsensitive, bool ascending, unsigned from, unsigned limit)
{
    UNIMPLEMENTED;
    return NULL;
}

void CCovenSDSManager::getExternalValue(__int64 index, MemoryBuffer &mb)
{
    IExternalHandler *handler = queryExternalHandler(EF_BinaryValue);
    assertex(handler);
    StringBuffer name(EXTERNAL_NAME_PREFIX);
    name.append(index);
    handler->readValue(name.str(), mb);
}

void CCovenSDSManager::getExternalValueFromServerId(__int64 serverId, MemoryBuffer &mb)
{
    CServerRemoteTree *idTree = (CServerRemoteTree *) SDSManager->queryRegisteredTree(serverId);
    if (idTree)
    {
        CHECKEDCRITICALBLOCK(extCrit, fakeCritTimeout);
        __int64 index = idTree->getPropInt64(EXT_ATTR);
        if (index)
            getExternalValue(index, mb);
        else
            WARNLOG("External file reference missing (node name='%s', id=%"I64F"d)", idTree->queryName(), serverId);
    }
}

IPropertyTreeIterator *CCovenSDSManager::getElementsRaw(const char *xpath,INode *remotedali, unsigned timeout)
{
    assertex(!remotedali); // only client side 
    CHECKEDDALIREADLOCKBLOCK(dataRWLock, readWriteTimeout);
    return root->getElements(xpath);
}

void CCovenSDSManager::setConfigOpt(const char *opt, const char *value)
{
    IPropertyTree &props = queryProperties();
    if (props.hasProp(opt) && (0 == strcmp(value, props.queryProp(opt))))
        return;
    ensurePTree(&queryProperties(), opt);
    queryProperties().setProp(opt, value);
}

unsigned CCovenSDSManager::queryCount(const char *xpath)
{
    unsigned count = 0;
    if (xpath && *xpath == '/')
        ++xpath;
    Owned<IPropertyTreeIterator> iter = root->getElements(xpath);
    ForEach(*iter)
        ++count;
    return count;
}

void CCovenSDSManager::start()
{
    server.start();
    if (coalesce) coalesce->start();
}

void CCovenSDSManager::stop() 
{
    server.stop();
    PROGLOG("waiting for coalescer to stop");
    if (coalesce) coalesce->stop();
}

void CCovenSDSManager::restart(IException * e)
{
    LOG(MCwarning, unknownJob, "-------: stopping SDS server");
    StringBuffer msg;
    msg.append("Unhandled exception, restarting: ").append(e->errorCode()).append(": ");
    e->errorMessage(msg);
    stop();
    connections.kill();
    LOG(MCwarning, unknownJob, "-------: stopped");
    LOG(MCwarning, unknownJob, "-------: saving current store . . . . . .");
    saveStore();
    LOG(MCwarning, unknownJob, "-------: store saved.");
    LOG(MCwarning, unknownJob, "-------: restarting SDS server restart");
    start();
    LOG(MCwarning, unknownJob, "-------: restarted");
}

CServerConnection *CCovenSDSManager::createConnectionInstance(CRemoteTreeBase *root, SessionId sessionId, unsigned mode, unsigned timeout, const char *xpath, CRemoteTreeBase *&tree, ConnectionId _connectionId, StringAttr *deltaPath, Owned<IPropertyTree> &deltaChange, Owned<CBranchChange> &branchChange, unsigned &additions)
{
    IPropertyTree *parent = NULL;
    ConnInfoFlags connInfoFlags = (ConnInfoFlags)0;
    const char *_xpath = ('/' == *xpath)?xpath+1:xpath;
    StringBuffer tXpath;
    if (!RTM_MODE(mode, RTM_CREATE_UNIQUE))
    {
        unsigned l = strlen(_xpath);
        if (l && '/' == _xpath[l-1])
        {
            tXpath.append(l-1, _xpath);
            _xpath = tXpath.toCharArray();
        }
    }
    bool newNode = false;
    bool replaceNode = false;
    IPropertyTree *created = NULL, *createdParent = NULL;
    StringBuffer head;
    if (mode & RTM_CREATE)
    {
        const char *prop = splitXPath(_xpath, head);
        if (head.length())
        {
            try
            {
                parent = createPropBranch(root, head.str(), true, &created, &createdParent); 
                if (created)
                    newNode = true;
            }
            catch (IException *) { if (created) createdParent->removeTree(created); throw; }
            catch (DALI_CATCHALL) { if (created) createdParent->removeTree(created); throw; }
        }
        else
            parent = root->splitBranchProp(_xpath, prop);
        if (parent)
        {
            connInfoFlags = ci_newParent;
            StringBuffer uProp;
            if (RTM_MODE(mode, RTM_CREATE_UNIQUE))
            {
                Owned<IPropertyTreeIterator> iter = parent->getElements(prop);
                if (iter->first())
                {           
                    uProp.append(prop).append('-');
                    unsigned l = uProp.length();
                    unsigned n=1;
                    loop
                    {
                        n += getRandom() % 5; // better chance of finding a mismatch soon.
                        uProp.append(n);
                        iter.setown(parent->getElements(uProp.str()));
                        if (!iter->first())
                            break;
                        uProp.setLength(l);
                    }
                    prop = uProp.str();
                    if (head.length())
                        tXpath.append(head).append('/');
                    _xpath = tXpath.append(prop).str();
                }
            }
            if (RTM_MODE(mode, RTM_CREATE_QUERY))
                tree = (CRemoteTreeBase *) root->queryCreateBranch(parent, prop, &newNode);
            else
            {
                IPropertyTree *newTree = ((CRemoteTreeBase *) parent)->create(NULL);
                if (RTM_MODE(mode, RTM_CREATE_ADD))
                    tree = (CRemoteTreeBase *) parent->addPropTree(prop, newTree);
                else // Default - RTM_CREATE - replace existing
                {
                    Owned<IPropertyTreeIterator> iter = parent->getElements(prop);
                    if (iter->first())
                        replaceNode = true;
                    tree = (CRemoteTreeBase *) parent->setPropTree(prop, newTree);
                }
                newNode = true;
            }
        }
        else
            tree = NULL;
    }
    else
    {
        if (NULL == _xpath || '\0' == *_xpath)
            tree = root;
        else
        {
            StringBuffer path;
            const char *prop = splitXPath(_xpath, path);
            if (!prop) prop = _xpath;

            // establish parent
            tree = NULL;
            Owned<IPropertyTreeIterator> iter = root->getElements(path.str());
            ForEach (*iter)
            {
                IPropertyTree *_parent = &iter->query();
                IPropertyTree *match = _parent->queryPropTree(prop);
                if (tree)
                {
                    if (match)
                        throw MakeSDSException(SDSExcpt_AmbiguousXpath, "Ambiguous: %s", _xpath);
                }
                else
                {
                    parent = _parent;
                    tree = (CRemoteTreeBase *)match;
                }
            }
        }
    }
    if (!tree)
        return NULL;
    assertex(tree->queryServerId());
    ConnectionId connectionId = _connectionId;
    if (!connectionId)
        connectionId = coven.getUniqueId();
    
    CServerConnection *connection = new CServerConnection(*this, connectionId, _xpath, sessionId, mode, timeout, parent, connInfoFlags);
    Owned<LinkingCriticalBlock> b;
    if (!RTM_MODE(mode, RTM_INTERNAL))
        b.setown(new LinkingCriticalBlock(lockCrit, __FILE__, __LINE__));
    connection->initPTreePath(*root, *tree);

    if (newNode)
    {
        writeTransactions++;
        // add tree into stack temporarily, or add manually at end.
        deltaChange.setown(createPTree(RESERVED_CHANGE_NODE));
        IPropertyTree *tail = deltaChange;
        if (created) // some elems in "head" created
        {
            // iterate stack until createdParent to build up new head path.
            StringBuffer _deltaPath;
            unsigned s = 0;
            StringBuffer headPath;
            connection->queryPTreePath().getAbsolutePath(headPath);
            if (headPath.length() && headPath.charAt(0) == '/')
                headPath.remove(0, 1);
            loop
            {
                _deltaPath.append('/');
                IPropertyTree &tree = connection->queryPTreePath().item(s);
                if (&tree == createdParent)
                {
                    ++s;
                    break;
                }
                unsigned l = _deltaPath.length();
                const char *t = queryHead(headPath.str(), _deltaPath);
                assertex(l != _deltaPath.length());
                headPath.clear();
                if (t)
                    headPath.append(t);
                if (++s>=connection->queryPTreePath().ordinality())
                    break;
            }
            additions = connection->queryPTreePath().ordinality()-s;
            deltaPath->set(_deltaPath.str());

            branchChange.setown(new CBranchChange(*(CRemoteTreeBase *)createdParent));
            CBranchChange *topChange = branchChange;
            // iterate remaining stack, iterate marking as 'new'
            for (;s<connection->queryPTreePath().ordinality();s++)
            {   
                
                IPropertyTree &tree = connection->queryPTreePath().item(s);
                IPropertyTree *n = tail->addPropTree(RESERVED_CHANGE_NODE, createPTree());
                n->setProp("@name", tree.queryName());
                n->setPropBool("@new", true);
                tail = n;

                Owned<CBranchChange> childChange = new CBranchChange((CRemoteTreeBase &)tree);
                childChange->noteChange(PDS_Added, PDS_Added);
                topChange->addChildBranch(*LINK(childChange));
                topChange = childChange;
            }
        }
        else
        {
            StringBuffer s, _deltaPath;
            connection->queryPTreePath().getAbsolutePath(s);
            const char *t = splitXPath(s.str(), _deltaPath);
            deltaPath->set(_deltaPath.str());
            IPropertyTree *n = tail->addPropTree(RESERVED_CHANGE_NODE, createPTree());
            n->setProp("@name", tree->queryName());
            if (replaceNode)
                n->setPropBool("@replace", true);
            else
                n->setPropBool("@new", true);

            branchChange.setown(new CBranchChange(*(CRemoteTreeBase *)tree));
            if (replaceNode)
                branchChange->noteChange(PDS_Data, PDS_Data);
            else
                branchChange->noteChange(PDS_Added, PDS_Added);
            additions=1;
        }
    }

    connection->setRoot(LINK(tree));
    return connection;
}

void CCovenSDSManager::clearSDSLocks()
{
    CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
    SuperHashIteratorOf<CLockInfo> iter(lockTable.queryBaseTable());
    ICopyArrayOf<CLockInfo> locks;
    ForEach(iter)
        locks.append(iter.query());
    ForEachItemIn(l, locks)
        locks.item(l).unlockAll();
}

void CCovenSDSManager::changeLockMode(CServerConnection &connection, unsigned newMode, unsigned timeout)
{
    CServerRemoteTree *tree = (CServerRemoteTree *) connection.queryRoot();
    ConnectionId connectionId = connection.queryConnectionId();
    __int64 treeId = tree->queryServerId();
    newMode = newMode & (RTM_LOCKBASIC_MASK|RTM_LOCK_SUB);
    newMode |= connection.queryMode() & ~(RTM_LOCKBASIC_MASK|RTM_LOCK_SUB);
    CUnlockCallback callback(connection.queryXPath(), connectionId, *tree);
    {
        CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
        CLockInfo *lockInfo = queryLockInfo(treeId);
        if (lockInfo)
        {
            lockInfo->changeMode(connectionId, connection.querySessionId(), newMode, timeout, callback);
            connection.setMode(newMode);
            return;
        }
    }
    // no existing lock for connection
    lock(*tree, connection.queryXPath(), connectionId, connection.querySessionId(), newMode, timeout, callback);
    connection.setMode(newMode);
}

bool CCovenSDSManager::unlock(__int64 connectionId, bool close, StringBuffer &connectionInfo)
{
    Owned<CServerConnection> connection = getConnection(connectionId);
    if (!connection) return false;
    StringBuffer str;
    MemoryBuffer connInfo;
    connection->getInfo(connInfo);
    formatConnectionInfo(connInfo, connectionInfo);

    if (close)
    {
        PROGLOG("forcing unlock & disconnection of connection : %s", connectionInfo.str());
        Owned<CLCLockBlock> lockBlock = new CLCWriteLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__);
        SDSManager->disconnect(connectionId, false);
    }
    else // leave connection open, just unlock
    {
        PROGLOG("forcing unlock for connection : %s", connectionInfo.str());
        __int64 nodeId = ((CRemoteTreeBase *)connection->queryRoot())->queryServerId();
        CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
        CLockInfo *lockInfo = queryLockInfo(nodeId);
        if (lockInfo)
            lockInfo->unlock(connectionId);
    }
    return true;
}

bool CCovenSDSManager::unlock(__int64 treeId, ConnectionId connectionId)
{
    CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
    CLockInfo *lockInfo = queryLockInfo(treeId);
    if (lockInfo)
        return lockInfo->unlock(connectionId);
    return false;
}

void CCovenSDSManager::unlockAll(__int64 treeId)
{
    CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
    CLockInfo *lockInfo = queryLockInfo(treeId);
    if (lockInfo)
        lockInfo->unlockAll();
}

bool CCovenSDSManager::establishLock(CLockInfo &lockInfo, __int64 treeId, ConnectionId connectionId, SessionId sessionId, unsigned mode, unsigned timeout, IUnlockCallback &lockCallback)
{
    bool res = lockInfo.lock(mode, timeout, connectionId, sessionId, lockCallback);
    if (res && server.queryStopped())
    {
        lockInfo.unlock(connectionId);
        throw MakeSDSException(SDSExcpt_ServerStoppedLockAborted);
    }
    return res;
}

void CCovenSDSManager::lock(CServerRemoteTree &tree, const char *__xpath, ConnectionId connectionId, SessionId sessionId, unsigned mode, unsigned timeout, IUnlockCallback &callback)
{
    if (0 == ((RTM_LOCK_READ | RTM_LOCK_WRITE) & mode)) // no point in creating lockInfo.
        return;
    CLockInfo *lockInfo = NULL;
    StringAttr sxpath;
    char *_xpath = (char *) (('/' == *__xpath) ? __xpath+1 : __xpath);
    char *xpath;
    if ('/' == _xpath[strlen(_xpath)-1])
        xpath = (char *)_xpath;
    else
    {
        unsigned l = strlen(_xpath);
        xpath = (char *)malloc(l+2);
        memcpy(xpath, _xpath, l);
        xpath[l] = '/';
        xpath[l+1] = '\0';
        sxpath.setown(xpath);
    }

    __int64 treeId = tree.queryServerId();
    CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
    lockInfo = lockTable.find(&treeId);
    
    IdPath idPath;
#ifdef SUBLOCKS
    class CLockInfoList : public CLockInfoArray
    {
    public:
        CLockInfoList(ConnectionId _connectionId) : connectionId(_connectionId) { }
        ~CLockInfoList() { clear(); }
        void clear()
        {
            ForEachItem(i)
            {
                CLockInfo &lockInfo = item(i);
                lockInfo.unlock(connectionId);
            }
        }
    private:
        ConnectionId connectionId;
    };
    CTimeMon tm(timeout);
    unsigned remaining = timeout;

    loop
    {
        if (!lockInfo) // establish if parent tree within this path has a lock. (e.g. want lock on 'a/b/c/' but already lock on 'a/')
        {
            CLockInfoList tmpExistingLocks(connectionId);
            StringAttr head("");
            const char *tail=xpath;
            do
            {
                Owned<IPropertyTreeIterator> headIter = root->getElements(head);
                ForEach(*headIter)
                {
                    CRemoteTreeBase &head = (CRemoteTreeBase &)headIter->query();
                    __int64 _treeId = head.queryServerId();
                    CLockInfo *_lockInfo = lockTable.find(&_treeId);
                    if (_lockInfo && _lockInfo->querySub())
                    {
                        if (tm.timedout(&remaining))
                            throw MakeSDSException(SDSExcpt_LockTimeout, "Failed to establish lock to %s", __xpath);
                        Owned<IPropertyTreeIterator> tailIter = head.getElements(tail);
                        ForEach(*tailIter)
                        {
                            CRemoteTreeBase &tail = (CRemoteTreeBase &)tailIter->query();
                            assertex(NULL == lockTable.find(treeId));
                            Linked<CLockInfo> tmp = _lockInfo;  // keep it alive could be destroyed whilst blocked in call below. (NB:1)
                            if (!establishLock(*_lockInfo, treeId, connectionId, sessionId, mode, remaining, callback))
                            {
                                StringBuffer s; // small window for debug info getLockInfo to be out of date. (NB:2)
                                throw MakeSDSException(SDSExcpt_LockTimeout, "Failed to establish lock to %s, timeout waiting for existing PARENT lock @ %s", xpath, _lockInfo->queryXPath());
                            }
                            if (lockTable.find(treeId))
                                break; // whilst waiting for intermediate, someone else established lock.
                            tmpExistingLocks.append(*_lockInfo);
                        }
                    }
                }
                if (lockInfo) break;

                const char *p = tail;
                const char *end = tail + strlen(tail) - 1;
                while (p < end)
                {
                    if (*p == '/')
                        break;
                    ++p;
                }
                if (p == end)
                    tail = NULL;
                else
                {
                    head.set(xpath, p-xpath);
                    tail = p+1;
                }
            }
            while (tail);

            CPTStack ptreePath;
            ptreePath.fill(*root, head, tree);
            ptreePath.append(*LINK(&tree));

            IdPath idPath;
            ForEachItemIn(p, ptreePath) idPath.append(((CRemoteTreeBase &)(ptreePath.item(p))).queryServerId());
            if (!lockInfo)
            {
                SuperHashIteratorOf<CLockInfo> iter(lockTable.queryBaseTable());
                ForEach (iter) // JCSMORE - think about other approach other than iterative.
                {
                    CLockInfo *_lockInfo = &iter.query();
                    if (_lockInfo->querySub() && _lockInfo->matchHead(idPath))
                    {
                        if (tm.timedout(&remaining))
                            throw MakeSDSException(SDSExcpt_LockTimeout, "Failed to establish lock to %s timeout", __xpath);
                        assertex(NULL == lockTable.find(treeId));
                        Linked<CLockInfo> tmp = _lockInfo; // NB 1
                        if (!establishLock(*_lockInfo, treeId, connectionId, sessionId, mode, remaining, callback))
                        {
                            StringBuffer s; // NB 2
                            throw MakeSDSException(SDSExcpt_LockTimeout, "Failed to establish lock to %s, timeout waiting for existing CHILD lock @ %s", xpath, _lockInfo->queryXPath());
                        }
                        if (lockTable.find(treeId))
                            break; // whilst waiting for intermediate, someone else established lock.
                        tmpExistingLocks.append(*_lockInfo);
                    }
                }
            }
            if (!lockInfo)
            {
                lockInfo = new CLockInfo(lockTable, treeId, idPath, xpath, mode, connectionId, sessionId);
                assertex(NULL == lockTable.find(treeId));
                lockTable.replace(*lockInfo);
                break;
            }
        }
        else
        {
            Linked<CLockInfo> tmp = lockInfo; // NB 1
            if (!establishLock(*lockInfo, treeId, connectionId, sessionId, mode, remaining, callback))
            {
                StringBuffer s; // NB 2
                throw MakeSDSException(SDSExcpt_LockTimeout, "Failed to establish lock to %s\nExisting lock status: %s", xpath, lockInfo->getLockInfo(s).str());
            }
            break;
        }
    }
#else
    if (!lockInfo)
    {
        lockInfo = new CLockInfo(lockTable, treeId, idPath, xpath, mode, connectionId, sessionId);
        lockTable.replace(*lockInfo);
    }
    else
    {
        Linked<CLockInfo> tmp = lockInfo; // keep it alive could be destroyed whilst blocked in call below.
        if (!establishLock(*lockInfo, treeId, connectionId, sessionId, mode, timeout, callback))
        {
            if (!queryConnection(connectionId)) return; // connection aborted.
            StringBuffer s;
            throw MakeSDSException(SDSExcpt_LockTimeout, "Failed to establish lock to %s\nExisting lock status: %s", xpath, lockInfo->getLockInfo(s).str());
        }
    }
#endif
}

void CCovenSDSManager::createConnection(SessionId sessionId, unsigned mode, unsigned timeout, const char *xpath, CServerRemoteTree *&tree, ConnectionId &connectionId, bool primary, Owned<LinkingCriticalBlock> &connectCritBlock)
{
    CRemoteTreeBase *_tree;
    Linked<CRemoteTreeBase> linkedTree;
    Owned<CServerConnection> connection;

    StringBuffer _xpath;
    if (!xpath || '/'!=*xpath)
        xpath = _xpath.append('/').append(xpath).str();
    struct CFreeExistingLocks
    {
        ~CFreeExistingLocks() { clear(); }
        void clear()
        {
            ForEachItemIn(l, existingLockTrees)
                SDSManager->unlock(existingLockTrees.item(l).queryServerId(), connId);
            existingLockTrees.kill();
        }
        void setConnectionId(ConnectionId _connId) { connId = _connId; }
        void add(CServerRemoteTree &tree)
        {
            tree.Link();
            existingLockTrees.append(tree);
        }
        void remove(CServerRemoteTree &tree)
        {
            existingLockTrees.zap(tree);
        }
        bool isExisting(CServerRemoteTree &tree) { return NotFound != existingLockTrees.find(tree); }
        ConnectionId connId;
        IArrayOf<CServerRemoteTree> existingLockTrees;
    } freeExistingLocks;

    StringAttr deltaPath;
    Owned<IPropertyTree> deltaChange;
    Owned<CBranchChange> branchChange;
    unsigned additions = 0;
    try
    {
        struct LockUnblock
        {
            LockUnblock(ReadWriteLock &_rWLock) : rWLock(_rWLock)
            {
                lockedForWrite = rWLock.queryWriteLocked();
                if (lockedForWrite) rWLock.unlockWrite();
                else rWLock.unlockRead();
            }
            ~LockUnblock() { if (lockedForWrite) rWLock.lockWrite(); else rWLock.lockRead(); }
            bool lockedForWrite;
            ReadWriteLock &rWLock;
        };
        bool locked = false;
        class CConnectExistingLockCallback : implements IUnlockCallback
        {
            CUnlockCallback lcb;
            CheckedCriticalSection *connectCrit;
        public:
            CConnectExistingLockCallback(const char *xpath, ConnectionId connectionId, CServerRemoteTree &tree, CheckedCriticalSection *_connectCrit) : lcb(xpath, connectionId, tree), connectCrit(_connectCrit) { }
            virtual void block()
            {
                if (connectCrit)
                    CHECKEDCRITENTER(*connectCrit, fakeCritTimeout);
                lcb.block();
            }
            virtual void unblock()
            {
                lcb.unblock();
                if (connectCrit)
                    CHECKEDCRITLEAVE(*connectCrit);
            }
        };

        if (!RTM_MODE(mode, RTM_CREATE_ADD) && !RTM_MODE(mode, RTM_CREATE_UNIQUE)) // cannot be pending on existing locked nodes in these cases
        {
            CTimeMon tm(timeout);
            connectionId = coven.getUniqueId();
            Owned<CServerConnection> tmpConn = new CServerConnection(*this, connectionId, xpath, sessionId, mode, timeout, NULL, (ConnInfoFlags)0);
            { CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
                connections.replace(*LINK(tmpConn));
            }
            if (sessionId)
            {
                { LockUnblock b(dataRWLock);
                    tmpConn->subscribe(sessionId);
                }
                if (!queryConnection(connectionId)) // aborted
                {
                    connectionId = 0;
                    return;
                }
            }
            freeExistingLocks.setConnectionId(connectionId);
            try
            {
                loop
                {                       
                    try
                    {
                        Owned<IPropertyTreeIterator> iter = root->getElements(xpath+1);
                        iter->first();
                        while (iter->isValid())
                        {
                            CServerRemoteTree &existing = (CServerRemoteTree &) iter->query();
                            if (freeExistingLocks.isExisting(existing))
                                iter->next();
                            else
                            {
                                freeExistingLocks.add(existing);
                                {
                                    unsigned remaining;
                                    if (tm.timedout(&remaining))
                                        throw MakeSDSException(SDSExcpt_LockTimeout, "Failed to establish lock to %s, timeout whilst retrying connection to orphaned connection path", xpath);
                                    CConnectExistingLockCallback connectLockCallback(xpath, connectionId, existing, &connectCrit);
                                    lock(existing, xpath, connectionId, sessionId, mode, remaining, connectLockCallback);
                                }
                                if (!queryConnection(connectionId)) // aborted
                                {
                                    connectionId = 0;
                                    return;
                                }
                                iter.setown(root->getElements(xpath+1));
                                iter->first();
                            }
                        }
                        break;
                    }
                    catch (ISDSException *e) // don't treat waiting on a now orpahned node an error, since trying to lock to create (retry)
                    {
                        if (SDSExcpt_OrphanedNode != e->errorCode())
                            throw;
                        else
                            e->Release();
                    }
                    freeExistingLocks.clear();
                }
            }
            catch (IException *)
            {
                CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
                tmpConn->unsubscribeSession();
                connections.removeExact(tmpConn);
                connectionId = 0;
                throw;
            }
            { CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
                tmpConn->unsubscribeSession();
                connections.removeExact(tmpConn);
            }
        }

        try
        {
            if (RTM_MODE(mode, RTM_CREATE_ADD) || RTM_MODE(mode, RTM_CREATE_UNIQUE))
            {
                CHECKEDCRITICALBLOCK(blockedSaveCrit, fakeCritTimeout);
                connection.setown(createConnectionInstance(root, sessionId, mode, timeout, xpath, _tree, connectionId, &deltaPath, deltaChange, branchChange, additions));
            }
            else
                connection.setown(createConnectionInstance(root, sessionId, mode, timeout, xpath, _tree, connectionId, &deltaPath, deltaChange, branchChange, additions));
        }
        catch (IException *) // do not want to miss recording change to inc, under any circumstances.
        {
            if (deltaChange.get())
            {
                PROGLOG("Exception on RTM_CREATE caused call to saveDelta, xpath=%s", xpath);
                saveDelta(deltaPath, *deltaChange);
            }
            throw;
        }
        linkedTree.set(_tree);
        if (!connection)
        {
            connectionId = 0;
            return;
        }
        assertex(_tree);
        if (deltaChange.get())
        {
            CPTStack stack = connection->queryPTreePath();
            if (connection->queryRoot() == SDSManager->queryRoot())
                stack.pop();
            stack.popn(additions);
            connection->notify();
            SDSManager->startNotification(*deltaChange, stack, *branchChange);
            
            saveDelta(deltaPath, *deltaChange);
        }

        connectionId = connection->queryConnectionId();
        if (freeExistingLocks.isExisting(*(CServerRemoteTree *)_tree))
        {
            locked = true;
            freeExistingLocks.remove(*(CServerRemoteTree *)_tree);
            connectCritBlock.clear();
        }               

        { CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
            connections.replace(*LINK(connection));
        }
        try
        {
            if (!locked)
            {
                CConnectExistingLockCallback connectLockCallback(xpath, connectionId, *(CServerRemoteTree *)_tree, connectCritBlock.get()?&connectCrit:NULL);
                lock(*(CServerRemoteTree *)_tree, xpath, connectionId, sessionId, mode, timeout, connectLockCallback);
            }
        }
        catch (IException *)
        {
            { CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
                connections.removeExact(connection);
            }
            throw;
        }
        catch (DALI_CATCHALL)
        {
            { CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
                connections.removeExact(connection);
            }
            throw;
        }
        if (sessionId)
        {
            // unlock global lock whilst subscription occurs, incase it calls me back inline (e.g. during an immediate abort)
            {
                LockUnblock b(dataRWLock);
                connection->subscribe(sessionId);
            }
            // subscription may have already disconnected by this stage.
            if (!queryConnection(connectionId))
            {
                connectionId = 0;
                return;
            }
        }
    }
    catch (IException *e)
    {
        if (SDSExcpt_OrphanedNode != e->errorCode())
            throw;
        e->Release();
        connectionId = 0;
        return;
    }
    // could have been blocked, now freed but in the meantime *this* connection has been aborted.
    if (!queryConnection(connectionId))
    {
        unlock(_tree->queryServerId(), connectionId);
        connectionId = 0;
        return;
    }
    connection->setEstablished();

    tree = (CServerRemoteTree *) LINK(_tree);
    connectionId = connection->queryConnectionId();
}

CServerConnection *CCovenSDSManager::queryConnection(ConnectionId id)
{
    CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
    return (CServerConnection *)connections.find(&id);
}

CServerConnection *CCovenSDSManager::getConnection(ConnectionId id)
{
    CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
    CServerConnection *conn = (CServerConnection *)connections.find(&id);
    if (conn) conn->Link();
    return conn;
}

void CCovenSDSManager::disconnect(ConnectionId id, bool deleteRoot, Owned<CLCLockBlock> *lockBlock)
{
    Linked<CServerConnection> connection;
    { CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
        connection.set(queryConnection(id));
        if (!connection)
            return;
        connections.removeExact(connection);
    }
    Linked<CServerRemoteTree> tree = (CServerRemoteTree *)connection->queryRootUnvalidated();
    if (!tree) return;

    unsigned index = (unsigned)-1;
    StringBuffer path;
    connection->queryPTreePath().getAbsolutePath(path);
    bool noLockDelete = false;
    if (connection->queryParent())
    {
        if (deleteRoot || RTM_MODE(connection->queryMode(), RTM_DELETE_ON_DISCONNECT))
        {
            CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
            CLockInfo *lockInfo = queryLockInfo(tree->queryServerId());
            if (lockInfo)
            {
                deleteRoot = false;
                lockInfo->setDROLR((CServerRemoteTree *)connection->queryParent(), tree);
            }
            else
                noLockDelete = deleteRoot = true;
        }
        if (lockBlock)
        {
            lockBlock->clear();
            lockBlock->setown(new CLCWriteLockBlock(dataRWLock, readWriteTimeout, __FILE__, __LINE__));
        }
        if ((unsigned)-1 == index)
            index = connection->queryParent()->queryChildIndex(connection->queryRootUnvalidated());
    }
    else
        deleteRoot = false;

    bool orphaned = ((CServerRemoteTree*)connection->queryRootUnvalidated())->isOrphaned();
    // Still want disconnection to be performed & recorded, if orphaned
    if (noLockDelete)
        connection->queryParent()->removeTree(tree);
    else
        deleteRoot |= unlock(tree->queryServerId(), id);
    if (deleteRoot)
        writeTransactions++;
    if (!orphaned && deleteRoot)
    {
        Owned<IPropertyTree> changeTree = createPTree(RESERVED_CHANGE_NODE);
        IPropertyTree *d = changeTree->setPropTree(DELETE_TAG, createPTree());
        d->setProp("@name", tree->queryName());
        d->setPropInt("@pos", index+1);

        Owned<CBranchChange> branchChange = new CBranchChange(*tree);
        branchChange->noteChange(PDS_Deleted, PDS_Deleted);
        CPTStack stack = connection->queryPTreePath();
        stack.pop();
        if (connection->queryRootUnvalidated() == SDSManager->queryRoot())
            stack.pop();

        if (!RTM_MODE(connection->queryMode(), RTM_INTERNAL))
        {
            connection->notify();
            SDSManager->startNotification(*changeTree, stack, *branchChange);
        }

        StringBuffer head;
        const char *tail = splitXPath(path.str(), head);
        CHECKEDCRITICALBLOCK(blockedSaveCrit, fakeCritTimeout);
        if (NotFound != index)
            saveDelta(head.str(), *changeTree);
        else
        { // NB: don't believe this can happen, but last thing want to do is save duff delete delta.
            WARNLOG("** CCovenSDSManager::disconnect - index position lost ** : noLockDelete=%d", noLockDelete);
            PrintStackReport();
        }
    }
    tree.clear();
    connection->unsubscribeSession();
}

StringBuffer &CCovenSDSManager::getLocks(StringBuffer &out)
{
    CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
    SuperHashIteratorOf<CLockInfo> iter(lockTable.queryBaseTable());    
    iter.first();
    while (iter.isValid())
    {
        CLockInfo &lockInfo = iter.query();
        if (lockInfo.lockCount())
            lockInfo.getLockInfo(out);
        if (!iter.next())
            break;
        if (out.length()) out.newline();
    }
    return out.length() ? out : out.append("No current locks");
}

StringBuffer &formatUsageStats(MemoryBuffer &src, StringBuffer &out)
{
    unsigned c;
    src.read(c);
    out.append("Connections              : ").append(c).newline();
    src.read(c);
    out.append("Locks                    : ").append(c).newline();
    src.read(c);
    out.append("Subscribers              : ").append(c).newline();
    src.read(c);
    out.append("Connection subscriptions : ").append(c).newline();
    return out;
}

StringBuffer &formatConnectionInfo(MemoryBuffer &src, StringBuffer &out)
{
    ConnectionId connectionId;
    StringAttr xpath;
    SessionId sessionId;
    unsigned mode;
    unsigned timeout;
    bool established;
    src.read(connectionId).read(xpath).read(sessionId).read(mode).read(timeout).read(established);
    out.append("ConnectionId=").appendf("%"I64F"x", connectionId).append(", xpath=").append(xpath).append(", sessionId=").appendf("%"I64F"x", sessionId).append(", mode=").append(mode).append(", timeout=");
    if (INFINITE == timeout)
        out.append("INFINITE");
    else
        out.append(timeout);
    out.append(established?"":" [BLOCKED]");
    return out;
}

StringBuffer &formatSubscriberInfo(MemoryBuffer &src, StringBuffer &out)
{
    SubscriptionId subscriptionId;
    bool sub;
    StringAttr xpath;
    src.read(subscriptionId).read(sub).read(xpath);
    out.append("SubscriptionId=").appendf("%"I64F"x", subscriptionId).append(", xpath=").append(xpath).append(", sub=").append(sub?"true":"false");
    return out;
}

StringBuffer &formatConnections(MemoryBuffer &src, StringBuffer &out)
{
    unsigned count;
    src.read(count);
    if (count)
    {
        while (count--)
        {
            formatConnectionInfo(src, out);
            if (count) out.newline();
        }
    }
    else
        out.append("No current connections");
    return out;
}

StringBuffer &formatSubscribers(MemoryBuffer &src, StringBuffer &out)
{
    unsigned count;
    src.read(count);
    if (count)
    {
        while (count--)
        {
            formatSubscriberInfo(src, out);
            if (count) out.newline();
        }
    }
    else
        out.append("No current subscriptions");
    return out;
}

unsigned CCovenSDSManager::countConnections()
{
    CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
    return connections.count();
}

unsigned CCovenSDSManager::countActiveLocks()
{
    unsigned activeLocks = 0;
    CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
    SuperHashIteratorOf<CLockInfo> iter(lockTable.queryBaseTable());    
    ForEach(iter) {
        CLockInfo &lockInfo = iter.query();
        if (lockInfo.lockCount()) activeLocks++;
    }
    return activeLocks;
}

unsigned CCovenSDSManager::countSubscribers() const
{
    return subscribers.count();
}

MemoryBuffer &CCovenSDSManager::collectUsageStats(MemoryBuffer &out)
{
    { CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
        out.append(connections.count());
    }
    unsigned activeLocks = 0;
    { CHECKEDCRITICALBLOCK(lockCrit, fakeCritTimeout);
        SuperHashIteratorOf<CLockInfo> iter(lockTable.queryBaseTable());    
        ForEach(iter)
        {
            CLockInfo &lockInfo = iter.query();
            if (lockInfo.lockCount()) activeLocks++;
        }
    }
    out.append(activeLocks);
    out.append(subscribers.count());
    out.append(connectionSubscriptionManager->querySubscribers());
    return out;
}

MemoryBuffer &CCovenSDSManager::collectConnections(MemoryBuffer &out)
{
    CHECKEDCRITICALBLOCK(cTableCrit, fakeCritTimeout);
    out.append(connections.count());
    SuperHashIteratorOf<CServerConnection> iter(connections.queryBaseTable());
    ForEach(iter)
        iter.query().getInfo(out);
    return out;
}

MemoryBuffer &CCovenSDSManager::collectSubscribers(MemoryBuffer &out)
{
    CHECKEDCRITICALBLOCK(sTableCrit, fakeCritTimeout);
    out.append(subscribers.count());
    SuperHashIteratorOf<CSubscriberContainer> iter(subscribers.queryBaseTable());
    ForEach(iter)
        iter.query().getInfo(out);
    return out;
}

void CCovenSDSManager::blockingSave(unsigned *writeTransactions)
{
    CHECKEDDALIREADLOCKBLOCK(SDSManager->dataRWLock, readWriteTimeout); // block all write actions whilst saving
    CHECKEDCRITICALBLOCK(blockedSaveCrit, fakeCritTimeout);
    if (writeTransactions)
        *writeTransactions = SDSManager->writeTransactions;
    // JCS - could in theory, not block, but abort save.
    SDSManager->saveStore();
}

StringBuffer &CCovenSDSManager::getUsageStats(StringBuffer &out)
{
    MemoryBuffer mb;
    formatUsageStats(collectUsageStats(mb), out);
    return out;
}

bool CCovenSDSManager::updateEnvironment(IPropertyTree *newEnv, bool forceGroupUpdate, StringBuffer &response)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/",myProcessSession(),0, INFINITE);
    if (conn)
    {
        Owned<IPropertyTree> root = conn->getRoot();
        Owned<IPropertyTree> oldEnvironment = root->getPropTree("Environment");
        if (oldEnvironment.get())
        {
            StringBuffer bakname;
            Owned<IFileIO> io = createUniqueFile(NULL, "environment", "bak", bakname);
            Owned<IFileIOStream> fstream = createBufferedIOStream(io);
            toXML(oldEnvironment, *fstream);         // formatted (default)
            root->removeTree(oldEnvironment);
        }
        root->addPropTree("Environment", LINK(newEnv));
        root.clear();
        conn->commit();
        conn->close();
        StringBuffer messages;
        initClusterGroups(forceGroupUpdate, messages, oldEnvironment);
        response.append(messages);
        PROGLOG("Environment and node groups updated");
    }
    return true;
}

// TODO
StringBuffer &CCovenSDSManager::getExternalReport(StringBuffer &out)
{
    return out;
}


StringBuffer &CCovenSDSManager::getConnections(StringBuffer &out)
{
    MemoryBuffer mb;
    formatConnections(collectConnections(mb), out);
    return out;
}

StringBuffer &CCovenSDSManager::getSubscribers(StringBuffer &out)
{
    MemoryBuffer mb;
    formatSubscribers(collectSubscribers(mb), out);
    return out;
}

void CCovenSDSManager::handleNodeNotify(notifications n, CServerRemoteTree &tree)
{
    const char *handlerKey = tree.queryProp(NOTIFY_ATTR);
    assertex(handlerKey);
    CSDSNotifyHandlerMapping *m = nodeNotifyHandlers.find(handlerKey);
    if (!m)
    {
        LOG(MCwarning, unknownJob, "Unknown notify handler name \"%s\", handing event %s", handlerKey, notificationStr(n));
        return;
    }
    switch (n)
    {
        case notify_delete:
            try { m->query().removed(tree); }
            catch (IException *e)
            {
                StringBuffer s("Exception calling ISDSNotifyHandler->removed(<tree>), for handler\"");
                s.append(notificationStr(n)).append("\"");
                EXCLOG(e, s.str());
                e->Release();
            }
            break;
        default:
            LOG(MCerror, unknownJob, "Unknown notification type (%d)", n);
    }
}

void CCovenSDSManager::handleNotify(CSubscriberContainer &subscriber, PDState state, CPTStack &stack, MemoryBuffer *data)
{
    class CNotifyPoolFactory : public CInterface, public IThreadFactory
    {
        class CNotifyHandler : public CInterface, implements IPooledThread
        {
            DECL_NAMEDCOUNT;
        public:
            IMPLEMENT_IINTERFACE;
            CNotifyHandler() { INIT_NAMEDCOUNT; }
            void init(void *startInfo) 
            {
                n.set((CSubscriberNotifier *)startInfo);
            }
            void main()
            {
                n->notify();
                n.clear();
            }
            bool canReuse()
            {
                return true;
            }
            bool stop()
            {
                return true;
            }
        private:
            Linked<CSubscriberNotifier> n;
        };
    public:
        IMPLEMENT_IINTERFACE;
        IPooledThread *createNew()
        {
            return new CNotifyHandler();
        }
    };
    if (!notifyPool)
    {
        CNotifyPoolFactory *factory = new CNotifyPoolFactory;
        notifyPool.setown(createThreadPool("SDS Notification Pool", factory, this, SUBNTFY_POOL_SIZE));
        factory->Release();
    }

    MemoryBuffer notifyData;
    buildNotifyData(stack, notifyData);
    notifyData.append(translatePDState(state));
    if (data)
    {
        notifyData.append(true);
        notifyData.append(data->length());
        notifyData.append(*data);
    }
    else
        notifyData.append(false);

    Owned<CSubscriberNotifier> _notifier;
    { CHECKEDCRITICALBLOCK(nfyTableCrit, fakeCritTimeout);
        SubscriptionId id = subscriber.queryId();
        CSubscriberNotifier *notifier = subscriberNotificationTable.find(id);
        if (notifier)
        {
            notifier->queueChange(notifyData);
            return;
        }
        else
        {
            _notifier.setown(new CSubscriberNotifier(subscriberNotificationTable, subscriber, notifyData));
            subscriberNotificationTable.replace(*_notifier);
        }
    }
    notifyPool->start(_notifier.get());
}

/////////////////
class CSubscriberNotifyScanner : public CInterface
{
    DECL_NAMEDCOUNT;
    CPTStack stack;
    Linked<IPropertyTree> changeTree;
    Linked<CBranchChange> rootChanges;
    StringBuffer xpath;
    CSubscriberArray subs;
public:
    struct PushPop
    {
        PushPop(CPTStack &_stack, CRemoteTreeBase &tree) : stack(_stack) { tree.Link(); stack.append(tree); }
        ~PushPop() { stack.pop(); }
        CPTStack &stack;
    };
    CSubscriberNotifyScanner(IPropertyTree &_changeTree, CPTStack &_stack, CBranchChange &_rootChanges) : changeTree(&_changeTree), rootChanges(&_rootChanges), stack(_stack)
    {
        INIT_NAMEDCOUNT;
        SDSManager->querySubscriberTable().getSubscribers(subs);
    }
    bool match(const char *head, const char *path, bool &sub)
    {
        bool wild = false;
        loop
        {
            if (wild)
            {
                if (*head == *path)
                {
                    path++;
                    wild = false;
                }
                else if ('/' == *head)
                    wild = false;
            }
            else if ('*' == *path)
            {
                path++;
                wild = true;
            }
            else if (*head != *path)
                return false;
            else
                path++;

            head++;
            if ('\0' == *path)
            {
                if (!wild && '/' != *(path-1) && '/' != *head) return false;
                sub = true;
                return true;
            }
            else 
            {
                if ('\0' == *head)
                {
                    sub = false;
                    return true;
                }
            }
        }
    }

    void scan() 
    {
        xpath.clear();
        stack.toString(xpath);
        if (stack.ordinality() && (rootChanges->tree == &(stack.tos()))) stack.pop();
        CSubscriberArray pruned; 
        scan(*rootChanges, stack, pruned);
    }

    bool prune(const char *xpath, bool &sub, CSubscriberArray &pruned)
    {
        sub = false;
        ForEachItemInRev(s, subs)
        {
            CSubscriberContainer &subscriber = subs.item(s);
            bool _sub; // false = (xpath NOT below subscriber), (true = xpath equals or is below subscriber)
            if (subscriber.isUnsubscribed() || !match(xpath, subscriber.queryXPath(), _sub))
            {
                pruned.append(*LINK(&subscriber));
                subs.remove(s);
            }
            else
                sub |= _sub;
        }
        return (subs.ordinality() > 0);
    }

    // recurse down all matching subscription stubs while qualified
    void scanAll(PDState state, CBranchChange &changes, CPTStack &stack, CSubscriberArray &pruned)
    {
        bool sub;
        if (prune(xpath.str(), sub, pruned))
        {
            if (sub)
            {
                ForEachItemInRev(s, subs)
                {
                    CSubscriberContainer &subscriber = subs.item(s);
                    if (!subscriber.isUnsubscribed())
                    {
                        if (subscriber.qualify(stack))
                            SDSManager->handleNotify(subscriber, state, stack);
                        else
                            pruned.append(*LINK(&subscriber));
                    }
                    subs.remove(s);
                }
            }
            else
            {
                if (0 == changes.children.ordinality())
                {
                    ForEachItemInRev(s, subs)
                    {
                        CSubscriberContainer &subscriber = subs.item(s);
                        if (!subscriber.isUnsubscribed())
                        {
                            if (subscriber.qualify(stack))
                                SDSManager->handleNotify(subscriber, state, stack);
                            else
                                pruned.append(*LINK(&subscriber));
                        }
                        subs.remove(s);
                    }
                }
                else
                {
                    ForEachItemIn (c, changes.children)
                    {
                        CBranchChange &childChange = changes.children.item(c);
                        PushPop pp(stack, *childChange.tree);
                        size32_t parentLength = xpath.length();
                        xpath.append(childChange.tree->queryName());
                        if ('/' != xpath.charAt(xpath.length()-1))
                            xpath.append('/');
                        CSubscriberArray _pruned;
                        scanAll(state, childChange, stack, _pruned);
                        ForEachItemIn(i, _pruned) subs.append(*LINK(&_pruned.item(i)));
                        if (0 == subs.ordinality())
                            break;
                        xpath.setLength(parentLength);
                    }
                }
            }
        }
    }

    void scan(CBranchChange &changes, CPTStack &stack, CSubscriberArray &pruned)
    {
        bool sub;
        if (!prune(xpath.str(), sub, pruned))
            return;
    
        PushPop pp(stack, *changes.tree);
        if (PDS_Deleted == (changes.local & PDS_Deleted))
        {
            scanAll(changes.local, changes, stack, pruned);
            return;
        }
        else if (sub) // xpath matched some subscribers, and/or below some, need to check for sub subscribers
        {
            bool ret = false;
            if (changes.state && changes.local)
            {
                ForEachItemInRev(s, subs)
                {
                    CSubscriberContainer &subscriber = subs.item(s);
                    if (!subscriber.isUnsubscribed())
                    {
                        if (subscriber.qualify(stack))
                        {
                            if (subscriber.querySendValue())
                            {
                                MemoryBuffer mb;
                                changes.tree->getPropBin(NULL, mb);
                                SDSManager->handleNotify(subscriber, changes.state, stack, &mb);
                            }
                            else
                                SDSManager->handleNotify(subscriber, changes.state, stack);
                        }
                        else
                            pruned.append(*LINK(&subscriber));
                    }
                    subs.remove(s);
                }
            }
            else
            {
                // remove non-sub subcribers at this level
                ForEachItemInRev(s, subs)
                {
                    CSubscriberContainer &subscriber = subs.item(s);
                    unsigned subDepth = subscriber.queryDepth();
                    unsigned stackDepth = stack.ordinality();
                    if ((!subscriber.querySub() && subDepth==stackDepth) || 0 == changes.children.ordinality())
                    {
                        pruned.append(*LINK(&subscriber));
                        subs.remove(s);
                    }
                }
            }
        }

        ForEachItemIn(c, changes.children)
        {
            CBranchChange &childChanges = changes.children.item(c);

            size32_t parentLength = xpath.length();
            xpath.append(childChanges.tree->queryName());
            if ('/' != xpath.charAt(xpath.length()-1))
                xpath.append('/');

            CSubscriberArray pruned;
            scan(childChanges, stack, pruned);
            ForEachItemIn(i, pruned) subs.append(*LINK(&pruned.item(i)));
            if (0 == subs.ordinality())
                break;

            xpath.setLength(parentLength);
        }
    }
};

void CCovenSDSManager::startNotification(IPropertyTree &changeTree, CPTStack &stack, CBranchChange &changes)
{
    class CScanNotifyPoolFactory : public CInterface, public IThreadFactory
    {
        class CScanNotifyHandler : public CInterface, implements IPooledThread
        {
        public:
            IMPLEMENT_IINTERFACE;
            void init(void *startInfo) 
            {
                n.set((CSubscriberNotifyScanner *)startInfo);
            }
            void main()
            {
                n->scan();
                n.clear();
            }
            bool canReuse()
            {
                return true;
            }
            bool stop()
            {
                return true;
            }
        private:
            Linked<CSubscriberNotifyScanner> n;
        };
    public:
        IMPLEMENT_IINTERFACE;
        IPooledThread *createNew()
        {
            return new CScanNotifyHandler();
        }
    };
    if (!scanNotifyPool)
    {
        CScanNotifyPoolFactory *factory = new CScanNotifyPoolFactory;
        scanNotifyPool.setown(createThreadPool("SDS Scan-Notification Pool", factory, this, SUBSCAN_POOL_SIZE));
        factory->Release();
    }

    Owned<CSubscriberNotifyScanner> scan = new CSubscriberNotifyScanner(changeTree, stack, changes);
    scanNotifyPool->start(scan.get());
}

void CCovenSDSManager::deleteExternal(__int64 index)
{
    if (server.queryStopped()) return;
    CHECKEDCRITICALBLOCK(extCrit, fakeCritTimeout);
    IExternalHandler *handler = queryExternalHandler(EF_BinaryValue);
    assertex(handler);
    StringBuffer name(EXTERNAL_NAME_PREFIX);
    name.append(index);
    handler->remove(name.str());
}

void CCovenSDSManager::serializeExternal(__int64 index, IPropertyTree &owner, MemoryBuffer &mb, bool withValue)
{
    CHECKEDCRITICALBLOCK(extCrit, fakeCritTimeout);
    IExternalHandler *extHandler = queryExternalHandler(EF_BinaryValue);
    assertex(extHandler);
    try
    {
        StringBuffer name(EXTERNAL_NAME_PREFIX);
        name.append(index);
        extHandler->read(name.str(), owner, mb, withValue);
    }
    catch (IException *)
    {
        extHandler->resetAsExternal(owner);
        throw;
    }
}

void CCovenSDSManager::writeExternal(CServerRemoteTree &tree, bool direct, __int64 existing)
{
    CHECKEDCRITICALBLOCK(extCrit, fakeCritTimeout);
    __int64 index;
    if (existing)
        index = existing;
    else
        index = getNextExternal();

    IExternalHandler *extHandler = queryExternalHandler(EF_BinaryValue);
    assertex(extHandler);
    tree.removeProp(EXT_ATTR);
    StringBuffer name(EXTERNAL_NAME_PREFIX);
    extHandler->write(name.append(index).str(), tree);
    tree.setPropInt64(EXT_ATTR, index); 
    extHandler->resetAsExternal(tree);
    // setPropInt64(EXT_ATTR, index); // JCSMORE not necessary
}

// ISubscriptionManager
void CCovenSDSManager::add(ISubscription *sub, SubscriptionId id)
{
    CHECKEDCRITICALBLOCK(sTableCrit, fakeCritTimeout);
    subscribers.replace(* new CSubscriberContainer(sub, id));
}

void CCovenSDSManager::remove(SubscriptionId id)
{
    CHECKEDCRITICALBLOCK(sTableCrit, fakeCritTimeout);
    subscribers.remove(&id);
}

// IExceptionHandler
static bool processingUnhandled = false;
static bool handled = false;
static CheckedCriticalSection unhandledCrit;
bool CCovenSDSManager::fireException(IException *e)
{
    //This code is rather dodgy (and causes more problems than it solves) 
    // so ignore unhandled exceptions for the moment! 
    LOG(MCoperatorError, unknownJob, e, "Caught unhandled exception!");
    return true;
    { CHECKEDCRITICALBLOCK(unhandledCrit, fakeCritTimeout);
        if (processingUnhandled)
        {
            if (handled)
            {
                LOG(MCdisaster, unknownJob, e, "FATAL, too many exceptions");
                return false; // did not successfully handle.
            }
            LOG(MCoperatorError, unknownJob, e, "Exception while restarting or shutting down");
            return true;
        }
        handled = false;
        processingUnhandled = true;
    }
    // Handle exception on a separate thread, to avoid complication with joining/restarting deadlocks.
    class CHandleException : public Thread
    {
        CCovenSDSManager &manager;
        Linked<IException> e;
        bool restart;
    public:
        CHandleException(CCovenSDSManager &_manager, IException *_e, bool _restart) : Thread("sds:CHandleException"), manager(_manager), e(_e), restart(_restart)
        {
            start();
        }
        virtual int run()
        {
            handled=true;
            try
            {
                if (restart)
                    manager.restart(e);
                else
                {
                    StringBuffer msg;
                    msg.append("Unhandled exception, shutting down: ").append(e->errorCode()).append(": ");
                    e->errorMessage(msg);
                    manager.stop();
                    manager.saveStore();
                }
                manager.unhandledThread.clear();
            }
            catch (IException *_e) { LOG(MCoperatorError, unknownJob, _e, "Exception while restarting or shutting down"); _e->Release(); }
            catch (DALI_CATCHALL) { LOG(MCoperatorError, unknownJob, "Unknown exception while restarting or shutting down"); }
            if (!restart)
            {
                e->Link();
                throw e.get();
            }
            processingUnhandled = false;
            handled = false;
            return 0;
        }
    };
    unhandledThread.setown(new CHandleException(*this, e, restartOnError));
    return true;
}

///////////////////////

class CDaliSDSServer: public CInterface, public IDaliServer
{
public:
    IMPLEMENT_IINTERFACE;

    CDaliSDSServer(IPropertyTree *_config) : config(_config)
    {
        manager = NULL;
        cancelLoad = false;
        storeLoaded = false;
    }

    ~CDaliSDSServer()
    {
        delete manager;
    }

    void start()
    {
        CriticalBlock b(crit);
        ICoven  &coven=queryCoven();
        assertex(coven.inCoven()); // must be member of coven
        if (config)
            sdsConfig.setown(config->getPropTree("SDS"));
        if (!sdsConfig)
            sdsConfig.setown(createPTree());
        manager = new CCovenSDSManager(coven, *sdsConfig, config?config->queryProp("@dataPath"):NULL);
        SDSManager = manager;
        addThreadExceptionHandler(manager);
        try { manager->loadStore(NULL, &cancelLoad); }
        catch (IException *)
        {
            LOG(MCdebugInfo(100), unknownJob, "Failed to load main store");
            throw;
        }
        storeLoaded = true;
        manager->start();
    }

    void ready()
    {
#ifdef TEST_NOTIFY_HANDLER
        class CTestHan : public CInterface, implements ISDSNotifyHandler
        {
        public:
            IMPLEMENT_IINTERFACE;
             virtual void removed(IPropertyTree &tree)
             {
                 PrintLog("Hello, tree(%s) handler(%s), being deleted", tree.queryName(), queryNotifyHandlerName(&tree));
             }
        };
        Owned<ISDSNotifyHandler> myHan = new CTestHan();

        ISDSManagerServer &sdsManager = querySDSServer();
        sdsManager.installNotifyHandler("testHandler", myHan);


        Owned<IRemoteConnection> conn = manager->connect("/", 0, 0, 0);
        IPropertyTree *root = conn->queryRoot();
        IPropertyTree *tree = root->setPropTree("test", createPTree());

        setNotifyHandlerName("testHandler", tree);

        conn->commit();

        root->removeProp("test");

        conn->commit();

        sdsManager.removeNotifyHandler("testHandler");
#endif
    }

    void suspend()
    {
    }

    void stop()
    {
        cancelLoad = true; // if in progress
        CriticalBlock b(crit);
        if (storeLoaded)
        {
            manager->stop();
            manager->saveStore();
        }
        removeThreadExceptionHandler(manager);
        ::Release(manager);
        manager = NULL;
    }

    void nodeDown(rank_t rank)
    {
        TBD;
    }

private:
    CCovenSDSManager *manager;
    Linked<IPropertyTree> config;
    Owned<IPropertyTree> sdsConfig;
    bool cancelLoad, storeLoaded;
    CriticalSection crit;
};

unsigned SDSLockTimeoutCount = 0;

unsigned querySDSLockTimeoutCount()
{
    return SDSLockTimeoutCount;
}

ISDSException *MakeSDSException(int errorCode, const char *errorMsg, ...)
{
    if(errorCode == SDSExcpt_LockTimeout)
        SDSLockTimeoutCount++;
    va_list args;
    va_start(args, errorMsg);
    ISDSException *ret = new CSDSException(errorCode, errorMsg, args);
    va_end(args);
    return ret;
}

IDaliServer *createDaliSDSServer(IPropertyTree *config)
{
    return new CDaliSDSServer(config);
}

//////////////////////

bool applyXmlDeltas(IPropertyTree &root, IIOStream &stream, bool stopOnError)
{
    class CDeltaProcessor : CInterface, implements IPTreeNotifyEvent
    {
        unsigned level;
        IPTreeMaker *maker;
        IPropertyTree &store;
        offset_t sectionEndOffset;
        StringAttr headerPath;
        bool stopOnError;
    public:
        IMPLEMENT_IINTERFACE;

        bool hadError;

        CDeltaProcessor(IPropertyTree &_store, bool _stopOnError) : store(_store), stopOnError(_stopOnError), level(0)
        {
            sectionEndOffset = 0;
            hadError = false;
            maker = createRootLessPTreeMaker();
        }
        ~CDeltaProcessor()
        {
            ::Release(maker);
        }

        void apply(IPropertyTree &change, IPropertyTree &currentBranch)
        {
            if (change.getPropBool("@localValue"))
            {
                bool binary = change.isBinary(NULL);
                if (binary)
                {
                    MemoryBuffer mb;
                    change.getPropBin(NULL, mb);
                    currentBranch.setPropBin(NULL, mb.length(), mb.toByteArray());
                }
                else
                    currentBranch.setProp(NULL, change.queryProp(NULL));
            }
            else if (change.getPropBool("@appendValue"))
            {
                if (change.queryProp(NULL))
                {
                    bool binary=change.isBinary(NULL);
                    __int64 index = currentBranch.getPropInt64(EXT_ATTR);
                    MemoryBuffer mb;
                    if (index && QUERYINTERFACE(&currentBranch, CServerRemoteTree))
                    {
                        MemoryBuffer mbv;
                        SDSManager->getExternalValue(index, mbv);
                        CPTValue v(mbv);
                        v.getValue(mb, binary);
                    }
                    else
                        currentBranch.getPropBin(NULL, mb);
                    change.getPropBin(NULL, mb);
                    if (binary)
                        currentBranch.setPropBin(NULL, mb.length(), mb.toByteArray());
                    else
                        currentBranch.setProp(NULL, (const char *)mb.toByteArray());
                }
            }
            Owned<IPropertyTreeIterator> iter = change.getElements(RENAME_TAG);
            ForEach (*iter)
            {
                IPropertyTree &d = iter->query();
                StringBuffer xpath(d.queryProp("@from"));
                xpath.append('[').append(d.queryProp("@pos")).append(']');
                verifyex(currentBranch.renameProp(xpath.str(), d.queryProp("@to")));
            }
            iter.setown(change.getElements(DELETE_TAG));
            ForEach (*iter)
            {
                IPropertyTree &d = iter->query();
                StringBuffer xpath(d.queryProp("@name"));
                xpath.append('[').append(d.queryProp("@pos")).append(']');
                if (!currentBranch.removeProp(xpath.str()))
                    LOG(MCoperatorWarning, unknownJob, "Property '%s' missing, but recorded as being present at time of delete, in section '%s'", xpath.str(), headerPath.get());
            }
            IPropertyTree *ac = change.queryPropTree(ATTRCHANGE_TAG);
            if (ac)
            {
                Owned<IAttributeIterator> aIter = ac->getAttributes();
                ForEach (*aIter)
                    currentBranch.setProp(aIter->queryName(), aIter->queryValue());
            }
            IPropertyTree *ad = change.queryPropTree(ATTRDELETE_TAG);
            if (ad)
            {
                Owned<IAttributeIterator> aIter = ad->getAttributes();
                ForEach (*aIter)
                {
                    if (!currentBranch.removeProp(aIter->queryName()))
                        LOG(MCoperatorWarning, unknownJob, "Property '%s' missing, but recorded as being present at time of delete, in section '%s'", aIter->queryName(), headerPath.get());
                }
            }

            processChildren(change, currentBranch);
        }

        void processChildren(IPropertyTree &change, IPropertyTree &currentBranch)
        {
            // process children
            Owned<IPropertyTreeIterator> iter = change.getElements("T");
            ForEach (*iter)
            {
                try
                {
                    IPropertyTree &child = iter->query();
                    const char *name = child.queryProp("@name");
                    if (child.getPropBool("@new"))
                    {
                        IPropertyTree *newBranch = currentBranch.addPropTree(name, createPTree());
                        apply(child, *newBranch);
                    }
                    else if (child.getPropBool("@replace"))
                    {
                        IPropertyTree *newBranch = currentBranch.setPropTree(name, createPTree());
                        apply(child, *newBranch);
                    }
                    else
                    {
                        const char *pos = child.queryProp("@pos");
                        if (!pos)
                            throw MakeStringException(0, "Missing position attribute in child reference, section end offset=%"I64F"d", sectionEndOffset);
                        StringBuffer xpath(name);
                        xpath.append('[').append(pos).append(']');
                        IPropertyTree *existingBranch = currentBranch.queryPropTree(xpath.str());
                        if (!existingBranch)
                            throw MakeStringException(0, "Failed to locate delta change in %s, section end offset=%"I64F"d", xpath.str(), sectionEndOffset);
                        apply(child, *existingBranch);
                    }
                }
                catch (IException *e)
                {
                    StringBuffer s("Error processing delta section: sectionEndOffset=");
                    LOG(MCoperatorError, unknownJob, e, s.append(sectionEndOffset).str());
                    if (stopOnError) throw;
                    hadError = true;
                    e->Release();
                }
            }
        }

        void process(IPropertyTree &match, offset_t endOffset)
        {
            sectionEndOffset = endOffset;
            const char *xpath = match.queryProp("@path");
            if (xpath && '/' == *xpath)
                xpath++;
            IPropertyTree *root = store.queryPropTree(xpath);
            if (!root)
                throw MakeStringException(0, "Failed to locate header xpath = %s", xpath);
            IPropertyTree *start = match.queryPropTree("Delta/T");
            if (!start)
                throw MakeStringException(0, "Badly constructed delta format (missing Delta/T) in header path=%s, section end offset=%"I64F"d", xpath, endOffset);
            headerPath.set(xpath);
            apply(*start, *root);
        }

        // IPTreeNotifyEvent
        virtual void beginNode(const char *tag, offset_t startOffset) { maker->beginNode(tag, startOffset); }
        virtual void newAttribute(const char *name, const char *value) { maker->newAttribute(name, value); }
        virtual void beginNodeContent(const char *tag) { level++; }
        virtual void endNode(const char *tag, unsigned length, const void *value, bool binary, offset_t endOffset)
        {
            try
            {
                --level;
                IPropertyTree *match = NULL;
                if (0 == level)
                {
                    if (0 == strcmp("Header", tag))
                        match = maker->queryCurrentNode();
                }
                maker->endNode(tag, length, value, binary, endOffset);
                if (match)
                {
                    process(*match, endOffset);
                    verifyex(maker->queryRoot()->removeTree(match)); // no longer needed.
                }
            }
            catch (IException *e)
            {
                StringBuffer s("Error processing delta section: sectionEndOffset=");
                LOG(MCoperatorError, unknownJob, e, s.append(endOffset).str());
                if (stopOnError) throw;
                hadError = true;
                e->Release();
            }
        }
    } deltaProcessor(root, stopOnError);

    Owned<IPullPTreeReader> xmlReader = createPullXMLStreamReader(stream, deltaProcessor, (PTreeReaderOptions)((unsigned)ptr_ignoreWhiteSpace+(unsigned)ptr_noRoot), false);
    try
    {
        xmlReader->load();
    }
    catch (IException *e)
    {
        if (stopOnError)
            throw;
        LOG(MCoperatorError, unknownJob, e, "XML parse error on delta load - load truncated");
        e->Release();
    }
    return !deltaProcessor.hadError;
}

void LogRemoteConn(IRemoteConnection *conn)
{
    CConnectionBase *conbase = QUERYINTERFACE(conn,CConnectionBase);
    if (!conn) {
        PROGLOG("Could not get base for %x",(unsigned)(memsize_t)conn);
        return;
    }
    IPropertyTree *root = conn->queryRoot();
    CRemoteTreeBase *remotetree = root?QUERYINTERFACE(root,CRemoteTreeBase):NULL;
    unsigned rcount = remotetree?remotetree->getLinkCount()-1:((unsigned)-1);
    PROGLOG("CONN(%x,%"I64F"x,%"I64F"x) path = '%s' mode = %x, link %d,%d", 
            (unsigned)(memsize_t)conn,
            (__int64)conbase->querySessionId(),
            (__int64)conbase->queryConnectionId(),
            conbase->queryXPath(),
            conbase->queryMode(),
            conbase->getLinkCount()-1,
            rcount);
}


#ifdef _POOLED_SERVER_REMOTE_TREE

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    CServerRemoteTree_Allocator = new CFixedSizeAllocator(sizeof(CServerRemoteTree));
    return true;
}
MODULE_EXIT()
{
    delete CServerRemoteTree_Allocator;
}

#endif
