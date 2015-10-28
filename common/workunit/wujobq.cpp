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


#include "platform.h"
#include <algorithm>
#include "limits.h"
#include "jlib.hpp"
#include "jbuff.hpp"
#include "dasess.hpp"
#include "dautils.hpp"
#include "portlist.h"

#include "dacoven.hpp"
#include "daclient.hpp"
#include "dasds.hpp"
#include "dasess.hpp"

#include "wujobq.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif


#if 0

JobQueues
  JobQueue @name= @count= @state=active|paused|stopped
    Edition <num>
    Client @session= @connected= @waiting=      -- connections and waiting can be > 1 (multiple threads)
    Item*  @wuid @owner @node @port @priority @session

#endif



class CJobQueueItem: public CInterface, implements IJobQueueItem
{
    StringAttr wu;
    StringAttr owner;
    int priority;
    SessionId sessid;
    SocketEndpoint ep;
    unsigned port;
    CDateTime enqueuedt;
public:
    IMPLEMENT_IINTERFACE;

    CJobQueueItem(MemoryBuffer &src)
    {
        deserialize(src);
    }

    CJobQueueItem(const char *_wu)
        : wu(_wu)
    {
        priority = 0;
        ep = queryMyNode()->endpoint();
        port = 0;
        sessid = myProcessSession();

    }

    CJobQueueItem(IPropertyTree *item)
    {
        const char * wuid = item->queryProp("@wuid");
        if (*wuid=='~')
            wuid++;
        wu.set(wuid);
        owner.set(item->queryProp("@owner"));
        sessid = (SessionId)item->getPropInt64("@session");
        priority = item->getPropInt("@priority");
        ep.set(item->queryProp("@node"));
        port = (unsigned)item->getPropInt("@port");
        StringBuffer dts;
        if (item->getProp("@enqueuedt",dts))
            enqueuedt.setString(dts.str());
    }

    static void assignBranch(IPropertyTree *item,IJobQueueItem *qi)
    {
        item->setPropInt64("@session",qi->getSessionId());
        item->setPropInt("@priority",qi->getPriority());
        item->setPropInt("@port",qi->getPort());
        item->setProp("@wuid",qi->queryWUID());
        item->setProp("@owner",qi->queryOwner());
        StringBuffer eps;
        qi->queryEndpoint().getUrlStr(eps);
        item->setProp("@node",eps.str());
        StringBuffer dts;
        qi->queryEnqueuedTime().getString(dts);
        if (dts.length()==0) {
            CDateTime dt;
            dt.setNow();
            dt.getString(dts);
            qi->setEnqueuedTime(dt);
        }
        item->setProp("@enqueuedt",dts.str());
    }

    IPropertyTree *createBranch(CJobQueueItem)
    {
        IPropertyTree *item = createPTree("Item");
        assignBranch(item,this);
        return item;
    }

    const char *queryWUID()
    {
        return wu.get();
    }
    int getPriority()
    {
        return priority;
    }
    unsigned getPort()
    {
        return port;
    }
    SessionId getSessionId()
    {
        return sessid;
    }
    SocketEndpoint &queryEndpoint()
    {
        return ep;
    }
    const char *queryOwner()
    {
        return owner.get();
    }
    bool equals(IJobQueueItem *other)
    {
        // work unit is primary key
        return strcmp(wu.get(),other->queryWUID())==0;
    }

    CDateTime &queryEnqueuedTime()
    {
        return enqueuedt;
    }

    void setEnqueuedTime(const CDateTime &dt)
    {
        enqueuedt.set(dt);
    }

    void serialize(MemoryBuffer &tgt)
    {
        tgt.append(priority).append(port).append(wu).append(sessid);
        ep.serialize(tgt);
        StringBuffer dts;
        enqueuedt.getString(dts);
        tgt.append(owner).append(dts);
    }
    void deserialize(MemoryBuffer &src)
    {
        src.read(priority).read(port).read(wu).read(sessid);
        ep.deserialize(src);
        StringBuffer dts;
        src.read(owner).read(dts);
        enqueuedt.setString(dts.str());
    }



    IJobQueueItem* clone()
    {
        IJobQueueItem* ret = new CJobQueueItem(wu);
        ret->setPriority(priority);
        ret->setPriority(port);
        ret->setEndpoint(ep);
        ret->setSessionId(sessid);
        return ret;
    }

    void setPriority(int _priority)
    {
        priority = _priority;
    }

    void setPort(unsigned _port)
    {
        port = _port;
    }

    void setEndpoint(const SocketEndpoint &_ep)
    {
        ep = _ep;
    }

    void setSessionId(SessionId _id)
    {
        if (_id)
            sessid = _id;
        else
            sessid = myProcessSession();
    }

    void setOwner(const char *_owner)
    {
        owner.set(_owner);
    }

    bool isValidSession()
    {
        Owned<INode> node = createINode(ep);
        return (querySessionManager().lookupProcessSession(node)==sessid);
    }

};



class CJobQueueIterator: public CInterface, implements IJobQueueIterator
{
public:
    CJobQueueContents &items;
    unsigned idx;
    IMPLEMENT_IINTERFACE;

    CJobQueueIterator(CJobQueueContents &_items)
        : items(_items)
    {
        idx = 0;
    }

    bool isValid()
    {
        return idx<items.ordinality();
    }

    bool first()
    {
        idx = 0;
        return isValid();
    }

    bool next()
    {
        idx++;
        return isValid();
    }

    IJobQueueItem & query()
    {
        return items.item(idx);
    }

};

IJobQueueIterator *CJobQueueContents::getIterator()
{
    return new CJobQueueIterator(*this);
}


IJobQueueItem *createJobQueueItem(const char *wuid)
{
    if (!wuid||!*wuid)
        throw MakeStringException(-1,"createJobQueueItem empty WUID");
    return new CJobQueueItem(wuid);;
}


IJobQueueItem *deserializeJobQueueItem(MemoryBuffer &mb)
{
    return new CJobQueueItem(mb);
}


#define ForEachQueue(qd) for (sQueueData *qd = qdata; qd!=NULL; qd=qd->next)
#define ForEachQueueIn(parent,qd) for (sQueueData *qd = parent.qdata; qd!=NULL; qd=qd->next)


struct sQueueData
{
    sQueueData *next;
    IRemoteConnection *conn;
    StringAttr qname;
    IPropertyTree *root;
    SubscriptionId subscriberid;
    unsigned lastWaitEdition;
};

class CJobQueueBase: public CInterface, implements IJobQueueConst
{
    class cOrderedIterator
    {
        CJobQueueBase &parent;
        unsigned numqueues;
        unsigned *queueidx;
        sQueueData **queues;
        IPropertyTree **queuet;
        MemoryAttr ma;
        unsigned current;

    public:
        cOrderedIterator(CJobQueueBase&_parent) : parent(_parent)
        {
            numqueues=0;
            ForEachQueueIn(parent,qd1)
                if (qd1->root)
                    numqueues++;
            queueidx = (unsigned *)ma.allocate(numqueues*(sizeof(unsigned)+sizeof(sQueueData *)+sizeof(IPropertyTree *)));
            queues = (sQueueData **)(queueidx+numqueues);
            queuet = (IPropertyTree **)(queues+numqueues);
            unsigned i = 0;
            ForEachQueueIn(parent,qd2)
            {
                if (qd2->root)
                    queues[i++] = qd2;
            }
            current = (unsigned)-1;
        }

        bool first()
        {
            StringBuffer path;
            parent.getItemPath(path,0U);
            current = (unsigned)-1;
            for (unsigned i = 0; i<numqueues;i++)
            {
                queueidx[i] = 0;
                queuet[i] = queues[i]->root->queryPropTree(path.str());
                if (queuet[i])
                    if ((current==(unsigned)-1)||parent.itemOlder(queuet[i],queuet[current]))
                        current = i;
            }
            return current!=(unsigned)-1;
        }

        bool next()
        {
            if (current==(unsigned)-1)
                return false;
            queueidx[current]++;
            StringBuffer path;
            parent.getItemPath(path,queueidx[current]);
            queuet[current] = queues[current]->root->queryPropTree(path.str());
            current = (unsigned)-1;
            for (unsigned i = 0; i<numqueues;i++)
            {
                if (queuet[i])
                    if ((current==(unsigned)-1)||parent.itemOlder(queuet[i],queuet[current]))
                        current = i;
            }
            return current!=(unsigned)-1;
        }

        bool isValid()
        {
            return current!=(unsigned)-1;
        }

        void item(sQueueData *&qd, IPropertyTree *&t,unsigned &idx)
        {
            assertex(current!=(unsigned)-1);
            qd = queues[current];
            t = queuet[current];
            idx = queueidx[current];
        }
        sQueueData &queryQueue()
        {
            assertex(current!=(unsigned)-1);
            return *queues[current];
        }

        IPropertyTree &queryTree()
        {
            assertex(current!=(unsigned)-1);
            return *queuet[current];
        }
    };
protected:
    bool doGetLastDequeuedInfo(sQueueData *qd, StringAttr &wuid, CDateTime &enqueuedt, int &priority)
    {
        priority = 0;
        if (!qd)
            return false;
        const char *w = qd->root->queryProp("@prevwuid");
        if (!w||!*w)
            return false;
        wuid.set(w);
        StringBuffer dts;
        if (qd->root->getProp("@prevenqueuedt",dts))
            enqueuedt.setString(dts.str());
        priority = qd->root->getPropInt("@prevpriority");
        return true;
    }
public:
    sQueueData *qdata;
    Semaphore notifysem;
    CriticalSection crit;

    IMPLEMENT_IINTERFACE;

    CJobQueueBase(const char *_qname)
    {
        StringArray qlist;
        qlist.appendListUniq(_qname, ",");
        sQueueData *last = NULL;
        ForEachItemIn(i,qlist)
        {
            sQueueData *qd = new sQueueData;
            qd->next = NULL;
            qd->qname.set(qlist.item(i));
            qd->conn = NULL;
            qd->root = NULL;
            qd->lastWaitEdition = 0;
            qd->subscriberid = 0;
            if (last)
                last->next = qd;
            else
                qdata = qd;
            last = qd;
        }
    };
    virtual ~CJobQueueBase()
    {
        while (qdata)
        {
            sQueueData * next = qdata->next;
            delete qdata;
            qdata = next;
        }
    }

    StringBuffer &getItemPath(StringBuffer &path,const char *wuid)
    {
        if (!wuid||!*wuid)
            return getItemPath(path,0U);
        return path.appendf("Item[@wuid=\"%s\"]",wuid);
    }

    StringBuffer &getItemPath(StringBuffer &path,unsigned idx)
    {
        path.appendf("Item[@num=\"%d\"]",idx+1);
        return path;
    }

    IPropertyTree *queryClientRootIndex(sQueueData &qd, unsigned idx)
    {
        VStringBuffer path("Client[%d]", idx+1);
        return qd.root->queryPropTree(path);
    }

    bool itemOlder(IPropertyTree *qt1, IPropertyTree *qt2)
    {
        // if this ever becomes time critical thne could cache enqueued values
        StringBuffer d1s;
        if (qt1)
            qt1->getProp("@enqueuedt",d1s);
        StringBuffer d2s;
        if (qt2)
            qt2->getProp("@enqueuedt",d2s);
        return (strcmp(d1s.str(),d2s.str())<0);
    }

    IJobQueueItem *doGetItem(sQueueData &qd,unsigned idx)
    {
        if (idx==(unsigned)-1)
        {
            idx = qd.root->getPropInt("@count");
            if (!idx)
                return NULL;
            idx--;
        }
        StringBuffer path;
        IPropertyTree *item = qd.root->queryPropTree(getItemPath(path,idx).str());
        if (!item)
            return NULL;
        return new CJobQueueItem(item);
    }

    IJobQueueItem *getItem(sQueueData &qd,unsigned idx)
    {
        return doGetItem(qd, idx);
    }

    IJobQueueItem *getHead(sQueueData &qd)
    {
        return getItem(qd,0);
    }

    unsigned doFindRank(sQueueData &qd,const char *wuid)
    {
        StringBuffer path;
        IPropertyTree *item = qd.root->queryPropTree(getItemPath(path,wuid).str());
        if (!item)
            return (unsigned)-1;
        return item->getPropInt("@num")-1;
    }

    unsigned findRank(sQueueData &qd,const char *wuid)
    {
        return doFindRank(qd,wuid);
    }

    IJobQueueItem *find(sQueueData &qd,const char *wuid)
    {
        StringBuffer path;
        IPropertyTree *item = qd.root->queryPropTree(getItemPath(path,wuid).str());
        if (!item)
            return NULL;
        bool cached = item->getPropInt("@num",0)<=0;
        if (wuid&&cached)
            return NULL;    // don't want cached value unless explicit
        return new CJobQueueItem(item);
    }

    unsigned copyItemsImpl(sQueueData &qd,CJobQueueContents &dest)
    {
        unsigned ret=0;
        StringBuffer path;
        for (unsigned i=0;;i++)
        {
            IPropertyTree *item = qd.root->queryPropTree(getItemPath(path.clear(),i).str());
            if (!item)
                break;
            ret++;
            dest.append(*new CJobQueueItem(item));
        }
        return ret;
    }

    virtual void copyItemsAndState(CJobQueueContents& contents, StringBuffer& state, StringBuffer& stateDetails)
    {
        assertex(qdata);
        assertex(qdata->root);

        copyItemsImpl(*qdata,contents);

        const char *st = qdata->root->queryProp("@state");
        if (st&&*st)
            state.set(st);
        if (st && (strieq(st, "paused") || strieq(st, "stopped")))
        {
            const char *stDetails = qdata->root->queryProp("@stateDetails");
            if (stDetails&&*stDetails)
                stateDetails.set(stDetails);
        }
    }

    sQueueData *findQD(const char *wuid)
    {
        if (wuid&&*wuid)
        {
            ForEachQueue(qd)
            {
                unsigned idx = doFindRank(*qd,wuid);
                if (idx!=(unsigned)-1)
                    return qd;
            }
        }
        return NULL;
    }

    virtual unsigned waiting()
    {
        unsigned ret = 0;
        ForEachQueue(qd)
        {
            for (unsigned i=0;;i++)
            {
                IPropertyTree *croot = queryClientRootIndex(*qd,i);
                if (!croot)
                    break;
                ret += croot->getPropInt("@waiting");
            }
        }
        return ret;
    }

    virtual unsigned findRank(const char *wuid)
    {
        assertex(qdata);
        if (!qdata->next)
            return findRank(*qdata,wuid);
        cOrderedIterator it(*this);
        unsigned i = 0;
        ForEach(it)
        {
            const char *twuid = it.queryTree().queryProp("@wuid");
            if (twuid&&(strcmp(twuid,wuid)==0))
                return i;
            i++;
        }
        return (unsigned)-1;
    }

    virtual unsigned copyItems(CJobQueueContents &dest)
    {
        assertex(qdata);
        if (!qdata->next)
            return copyItemsImpl(*qdata,dest);
        cOrderedIterator it(*this);
        unsigned ret = 0;
        ForEach(it)
        {
            dest.append(*new CJobQueueItem(&it.queryTree()));
            ret++;
        }
        return ret;
    }

    virtual IJobQueueItem *getItem(unsigned idx)
    {
        if (!qdata)
            return NULL;
        if (!qdata->next)
            return getItem(*qdata,idx);
        cOrderedIterator it(*this);
        unsigned i = 0;
        IPropertyTree *ret = NULL;
        ForEach(it)
        {
            if (i==idx)
            {
                ret = &it.queryTree();
                break;
            }
            else if (idx==(unsigned)-1)  // -1 means return last
                ret = &it.queryTree();
            i++;
        }
        if (ret)
            return new CJobQueueItem(ret);
        return NULL;
    }

    virtual IJobQueueItem *getHead()
    {
        if (!qdata)
            return NULL;
        if (!qdata->next)
            return getHead(*qdata);
        return getItem(0);
    }

    virtual IJobQueueItem *getTail()
    {
        if (!qdata)
            return NULL;
        if (!qdata->next)
            return getHead(*qdata);
        return getItem((unsigned)-1);
    }

    virtual IJobQueueItem *find(const char *wuid)
    {
        if (!qdata)
            return NULL;
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        if (!qd)
            return NULL;
        return find(*qd,wuid);
    }

    virtual bool paused()
    {
        // true if all paused
        ForEachQueue(qd)
        {
            if (qd->root)
            {
                const char *state = qd->root->queryProp("@state");
                if (state&&(strcmp(state,"paused")!=0))
                    return false;
            }
        }
        return true;
    }

    virtual bool paused(StringBuffer& info)
    {
        // true if all paused
        ForEachQueue(qd)
        {
            if (qd->root)
            {
                const char *state = qd->root->queryProp("@state");
                if (state&&(strcmp(state,"paused")!=0))
                    return false;
                if (state&&!info.length())
                {
                    const char *stateDetails = qd->root->queryProp("@stateDetails");
                    if (stateDetails && *stateDetails)
                        info.set(stateDetails);
                }
            }
        }
        return true;
    }

    virtual bool stopped()
    {
        // true if all stopped
        ForEachQueue(qd)
        {
            if (qd->root)
            {
                const char *state = qd->root->queryProp("@state");
                if (state&&(strcmp(state,"stopped")!=0))
                    return false;
            }
        }
        return true;
    }

    virtual bool stopped(StringBuffer& info)
    {
        // true if all stopped
        ForEachQueue(qd)
        {
            if (qd->root)
            {
                const char *state = qd->root->queryProp("@state");
                if (state&&(strcmp(state,"stopped")!=0))
                    return false;
                if (state&&!info.length())
                {
                    const char *stateDetails = qd->root->queryProp("@stateDetails");
                    if (stateDetails && *stateDetails)
                        info.set(stateDetails);
                }
            }
        }
        return true;
    }

    virtual unsigned ordinality()
    {
        unsigned ret = 0;
        ForEachQueue(qd)
        {
            if (qd->root)
                ret += qd->root->getPropInt("@count");
        }
        return ret;
    }

    virtual bool getLastDequeuedInfo(StringAttr &wuid, CDateTime &enqueuedt, int &priority)
    {
        return doGetLastDequeuedInfo(qdata, wuid, enqueuedt, priority);
    }

    //Similar to copyItemsAndState(), this method returns the state information for one queue.
    virtual void getState(StringBuffer& state, StringBuffer& stateDetails)
    {
        if (!qdata->root)
            return;
        const char *st = qdata->root->queryProp("@state");
        if (!st || !*st)
            return;

        state.set(st);
        if ((strieq(st, "paused") || strieq(st, "stopped")))
            stateDetails.set(qdata->root->queryProp("@stateDetails"));
    }
};

class CJobQueueConst: public CJobQueueBase
{
    Owned<IPropertyTree> jobQueueSnapshot;

public:
    IMPLEMENT_IINTERFACE;

    CJobQueueConst(const char *_qname, IPropertyTree* _jobQueueSnapshot) : CJobQueueBase(_qname)
    {
        if (!_jobQueueSnapshot)
            throw MakeStringException(-1, "No job queue snapshot");

        jobQueueSnapshot.setown(_jobQueueSnapshot);
        ForEachQueue(qd)
        {
            VStringBuffer path("Queue[@name=\"%s\"]", qd->qname.get());
            qd->root = jobQueueSnapshot->queryPropTree(path.str());
            if (!qd->root)
                throw MakeStringException(-1, "No job queue found for %s", qd->qname.get());
        }
    };
};

class CJobQueue: public CJobQueueBase, implements IJobQueue
{
public:
    sQueueData *activeq;
    SessionId sessionid;
    unsigned locknest;
    bool writemode;
    bool connected;
    Owned<IConversation> initiateconv;
    StringAttr initiatewu;
    bool dequeuestop;
    bool cancelwaiting;
    bool validateitemsessions;

    class csubs: public CInterface, implements ISDSSubscription
    {
        CJobQueue *parent;
    public:
        IMPLEMENT_IINTERFACE;
        csubs(CJobQueue *_parent)
        {
            parent = _parent;
        }
        void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
        {
            CriticalBlock block(parent->crit);
            parent->notifysem.signal();
        }
    } subs;

    IMPLEMENT_IINTERFACE;

    CJobQueue(const char *_qname) : CJobQueueBase(_qname), subs(this)
    {
        activeq = qdata;
        sessionid = myProcessSession();
        validateitemsessions = false;
        writemode = false;
        locknest = 0;
        connected = false;
        dequeuestop = false;
        cancelwaiting = false;
        Cconnlockblock block(this,false);   // this just checks queue exists
    }
    virtual ~CJobQueue()
    {
        try {
            while (locknest)
                connunlock(true); // auto rollback
            if (connected)
                disconnect();
        }
        catch (IException *e) {
            // server error
            EXCLOG(e, "~CJobQueue");
            e->Release();
        }
        try { // must attempt to remove subscription before object destroyed.
            dounsubscribe();
        }
        catch (IException *e) {
            EXCLOG(e, "~CJobQueue calling dounsubscribe");
            e->Release();
        }
    }
    void connlock(bool exclusive)
    {  // must be in sect
        if (locknest++==0) {
            unsigned wait = qdata&&qdata->next?5000:INFINITE;
            ForEachQueue(qd) {
                loop {
                    StringBuffer path;
                    path.appendf("/JobQueues/Queue[@name=\"%s\"]",qd->qname.get());
                    bool timeout;
                    loop {
                        timeout=false;
                        try {
                            qd->conn = querySDS().connect(path.str(),myProcessSession(),exclusive?RTM_LOCK_WRITE:RTM_LOCK_READ,wait);
                            if (qd->conn)
                                break;
                        }
                        catch (ISDSException *e) {
                            if (SDSExcpt_LockTimeout != e->errorCode())
                                throw;
                            e->Release();
                            timeout = true;
                        }
                        // create queue
                        Owned<IRemoteConnection> pconn;
                        try {
                            pconn.setown(querySDS().connect("/JobQueues",myProcessSession(),RTM_LOCK_WRITE|RTM_CREATE_QUERY,wait));
                            if (!pconn)
                                throw MakeStringException(-1,"CJobQueue could not create JobQueues");
                            IPropertyTree *proot = pconn->queryRoot();
                            StringBuffer cpath;
                            cpath.appendf("Queue[@name=\"%s\"]",qd->qname.get());
                            if (!proot->hasProp(cpath.str())) {
                                IPropertyTree *pt = proot->addPropTree("Queue",createPTree("Queue"));
                                pt->setProp("@name",qd->qname.get());
                                pt->setProp("@state","active");
                                pt->setPropInt("@count", 0);
                                pt->setPropInt("Edition", 1);
                            }
                        }
                        catch (ISDSException *e) {
                            if (SDSExcpt_LockTimeout != e->errorCode())
                                throw;
                            e->Release();
                            timeout = true;
                        }
                    }
                    if (!timeout)
                        break;
                    sQueueData *qd2 = qdata;
                    do {
                        ::Release(qd2->conn);
                        qd2->conn = NULL;
                        qd2->root = NULL;
                    }
                    while (qd2!=qd);
                    PROGLOG("Job Queue contention - delaying before retrying");
                    Sleep(getRandom()%5000);    // dining philosopher delay
                    wait = getRandom()%4000+3000; // try and prevent sync
                    qd  = qdata;
                }
                qd->root = qd->conn->queryRoot();
            }
            writemode = exclusive;
        }
        else {
            if (exclusive&&!writemode) {
                ForEachQueue(qd) {
                    assertex(qd->conn);
                    writemode = exclusive;
                    bool lockreleased;
                    safeChangeModeWrite(qd->conn,qd->qname.get(),lockreleased);
                    qd->root = qd->conn->queryRoot();
                }
            }
        }
    }

    void connunlock(bool rollback=false)
    {  // should be in sect
        if (--locknest==0) {
            ForEachQueue(qd) {
                if (qd->conn) { // can occur if connection to dali threw exception
                    if (writemode) {
                        if (rollback)
                            qd->conn->rollback();
                        else {
                            qd->root->setPropInt("Edition",qd->root->getPropInt("Edition")+1);
                            qd->conn->commit();
                        }
                    }
                    qd->conn->Release();
                    qd->conn = NULL;
                }
                qd->root = NULL;
            }
            writemode = false;
        }
    }

    void conncommit() // doesn't set edition
    {  // called within sect
        if (writemode) {
            ForEachQueue(qd) {
                if (qd->conn)
                    qd->conn->commit();
            }
        }
    }

    class Cconnlockblock: public CriticalBlock
    {
        CJobQueue *parent;
        bool rollback;
    public:
        Cconnlockblock(CJobQueue *_parent,bool exclusive)
            : CriticalBlock(_parent->crit)
        {
            parent = _parent;
            parent->connlock(exclusive);
            rollback = false;
        }

        ~Cconnlockblock()
        {
            parent->connunlock(rollback);
        }

        void setRollback(bool set=true)
        {
            rollback = set;
        }

        void commit()
        {
            parent->conncommit();
        }
    };

    void removeItem(sQueueData &qd,IPropertyTree *item, bool cache)
    {   // does not adjust or use @count
        unsigned n = item->getPropInt("@num");
        if (!n)
            return;
        if (cache) {
            StringBuffer s;
            item->getProp("@wuid",s.clear());
            qd.root->setProp("@prevwuid",s.str());
            item->getProp("@enqueuedt",s.clear());
            qd.root->setProp("@prevenqueuedt",s.str());
            qd.root->setPropInt("@prevpriority",item->getPropInt("@priority"));
        }
        item->setPropInt("@num",-1);
        StringBuffer path;
        loop {
            IPropertyTree *item2 = qd.root->queryPropTree(getItemPath(path.clear(),n).str());
            if (!item2)
                break;
            item2->setPropInt("@num",n);
            n++;
        }
        qd.root->removeTree(item);
    }

    IPropertyTree *addItem(sQueueData &qd,IPropertyTree *item,unsigned idx,unsigned count)
    {
        // does not set any values other than num
        StringBuffer path;
        // first move following up
        unsigned n=count;
        while (n>idx) {
            n--;
            qd.root->queryPropTree(getItemPath(path.clear(),n).str())->setPropInt("@num",n+2);
        }
        item->setPropInt("@num",idx+1);
        return qd.root->addPropTree("Item",item);
    }

    void dosubscribe()
    {   // called in crit section
        ForEachQueue(qd) {
            if (qd->subscriberid) {
                querySDS().unsubscribe(qd->subscriberid);
                qd->subscriberid = 0;
            }
            StringBuffer path;
            path.appendf("/JobQueues/Queue[@name=\"%s\"]/Edition",qd->qname.get());
            qd->subscriberid = querySDS().subscribe(path.str(), subs, false);
        }
    }

    bool haschanged() // returns if any changed
    {
        bool changed = false;
        ForEachQueue(qd) {
            if (!qd->subscriberid) {
                StringBuffer path;
                path.appendf("/JobQueues/Queue[@name=\"%s\"]/Edition",qd->qname.get());
                qd->subscriberid = querySDS().subscribe(path.str(), subs, false);
            }
            unsigned e = (unsigned)qd->root->getPropInt("Edition", 1);
            if (e!=qd->lastWaitEdition) {
                qd->lastWaitEdition = e;
                changed = true;
                break;
            }
        }
        return changed;
    }

    void dounsubscribe()
    {
        // called in crit section
        ForEachQueue(qd) {
            if (qd->subscriberid)  {
                querySDS().unsubscribe(qd->subscriberid);
                qd->subscriberid = 0;
            }
        }
    }

    IPropertyTree *queryClientRootSession(sQueueData &qd)
    {
        VStringBuffer path("Client[@session=\"%" I64F "d\"]", sessionid);
        IPropertyTree *ret = qd.root->queryPropTree(path.str());
        if (!ret)
        {
            ret = createPTree("Client");
            ret = qd.root->addPropTree("Client",ret);
            ret->setPropInt64("@session",sessionid);
            StringBuffer eps;
            ret->setProp("@node",queryMyNode()->endpoint().getUrlStr(eps).str());
        }
        return ret;
    }

    void connect(bool _validateitemsessions)
    {
        Cconnlockblock block(this,true);
        validateitemsessions = _validateitemsessions;
        if (connected)
            disconnect();
        dosubscribe();
        ForEachQueue(qd) {
            unsigned connected;
            unsigned waiting;
            unsigned count;
            getStats(*qd,connected,waiting,count); // clear any duff clients
            IPropertyTree *croot = queryClientRootSession(*qd);
            croot->setPropInt64("@connected",croot->getPropInt64("@connected",0)+1);
        }
        connected = true;
    }

    void disconnect()   // signal no longer wil be dequeing (optional - done automatically on release)
    {
        Cconnlockblock block(this,true);
        if (connected) {
            dounsubscribe();
            ForEachQueue(qd) {
                IPropertyTree *croot = queryClientRootSession(*qd);
                croot->setPropInt64("@connected",croot->getPropInt64("@connected",0)-1);
            }
            connected = false;
        }
    }

    sQueueData *findbestqueue(bool useprev,int minprio,unsigned numqueues,sQueueData **queues)
    {
        if (numqueues==0)
            return NULL;
        if (numqueues==1)
            return *queues;
        sQueueData *best = NULL;
        IPropertyTree *bestt = NULL;
        for (unsigned i=0;i<numqueues;i++) {
            sQueueData *qd = queues[i];
            unsigned count = qd->root->getPropInt("@count");
            if (count) {
                int mpr = useprev?std::max(qd->root->getPropInt("@prevpriority"),minprio):minprio;
                if (count&&((minprio==INT_MIN)||checkprio(*qd,mpr))) {
                    StringBuffer path;
                    IPropertyTree *item = qd->root->queryPropTree(getItemPath(path,0U).str());
                    if (!item)
                        continue;
                    if (item->getPropInt("@num",0)<=0)
                        continue;
                    CDateTime dt;
                    StringBuffer enqueued;
                    if (!best||itemOlder(item,bestt)) {
                        best = qd;
                        bestt = item;
                    }
                }
            }
        }
        return best;
    }

    void setWaiting(unsigned numqueues,sQueueData **queues, bool set)
    {
        for (unsigned i=0; i<numqueues; i++) {
            IPropertyTree *croot = queryClientRootSession(*queues[i]);
            croot->setPropInt64("@waiting",croot->getPropInt64("@waiting",0)+(set?1:-1));
        }
    }

// 'simple' queuing
    IJobQueueItem *dodequeue(int minprio,unsigned timeout=INFINITE, bool useprev=false, bool *timedout=NULL)
    {
        bool hasminprio=(minprio!=INT_MIN);
        if (timedout)
            *timedout = false;
        IJobQueueItem *ret=NULL;
        bool waitingset = false;
        while (!dequeuestop) {
            unsigned t = 0;
            if (timeout!=(unsigned)INFINITE)
                t = msTick();
            {
                Cconnlockblock block(this,true);
                block.setRollback(true);    // assume not going to update
                // now cycle through queues looking at state
                unsigned total = 0;
                unsigned stopped = 0;
                PointerArray active;
                ForEachQueue(qd) {
                    total++;
                    const char *state = qd->root->queryProp("@state");
                    if (state) {
                        if (strcmp(state,"stopped")==0)
                            stopped++;
                        else if (strcmp(state,"paused")!=0)
                            active.append(qd);
                    }
                    else
                        active.append(qd);
                }
                if (stopped==total)
                    return NULL; // all stopped
                sQueueData **activeqds = (sQueueData **)active.getArray();
                unsigned activenum = active.ordinality();
                if (activenum) {
                    sQueueData *bestqd = findbestqueue(useprev,minprio,activenum,activeqds);
                    unsigned count = bestqd?bestqd->root->getPropInt("@count"):0;
                    // load minp from cache
                    if (count) {
                        int mpr = useprev?std::max(bestqd->root->getPropInt("@prevpriority"),minprio):minprio;
                        if (!hasminprio||checkprio(*bestqd,mpr)) {
                            block.setRollback(false);
                            ret = dotake(*bestqd,NULL,true,hasminprio,mpr);
                            if (ret)            // think it must be!
                                timeout = 0;    // so mark that done
                            else if (!hasminprio) {
                                WARNLOG("Resetting queue %s",bestqd->qname.get());
                                clear(*bestqd); // reset queue as seems to have become out of sync
                            }
                        }
                    }
                    if (timeout!=0) { // more to do
                        if (!connected) {   // if connect already done non-zero
                            connect(validateitemsessions);
                            block.setRollback(false);
                        }
                        if (!waitingset) {
                            setWaiting(activenum,activeqds,true);
                            block.commit();
                            waitingset = true;
                        }
                    }
                }
                if (timeout==0) {
                    if (waitingset) {
                        setWaiting(activenum,activeqds,false);
                        block.commit();
                    }
                    if (timedout)
                        *timedout = (ret==NULL);
                    break;
                }
            }
            unsigned to = 5*60*1000;
            // check every 5 mins independant of notify (in case subscription lost for some reason)
            if (to>timeout)
                to = timeout;
            notifysem.wait(to);
            if (timeout!=(unsigned)INFINITE) {
                t = msTick()-t;
                if (t<timeout)
                    timeout -= t;
                else
                    timeout = 0;
            }
        }
        return ret;
    }

    IJobQueueItem *dequeue(unsigned timeout=INFINITE)
    {
        return dodequeue(INT_MIN,timeout);
    }


    IJobQueueItem *prioDequeue(int minprio,unsigned timeout=INFINITE) // minprio == MAX_INT - used cache priority
    {
        return dodequeue(minprio,timeout);
    }



    void placeonqueue(sQueueData &qd, IJobQueueItem *qitem,unsigned idx) // takes ownership of qitem
    {
        Owned<IJobQueueItem> qi = qitem;
        remove(qi->queryWUID());                                // just in case trying to put on twice!
        int priority = qi->getPriority();
        unsigned count = qd.root->getPropInt("@count");
        StringBuffer path;
        if (count&&(idx!=(unsigned)-1)) { // need to check before and after
            if (idx) {
                IPropertyTree *pt = qd.root->queryPropTree(getItemPath(path.clear(),idx-1).str());
                if (pt) {
                    int pp = pt->getPropInt("@priority");
                    if (priority>pp) {
                        qi->setPriority(pp);
                        priority = pp;
                    }
                }
                else  // what happened here?
                    idx = (unsigned)-1;
            }
            if (idx<count) {
                IPropertyTree *pt = qd.root->queryPropTree(getItemPath(path.clear(),idx).str());
                if (pt) {
                    int pp = pt->getPropInt("@priority");
                    if (priority<pp) {
                        qi->setPriority(pp);
                        priority = pp;
                    }
                }
                else  // what happened here?
                    idx = (unsigned)-1;
            }
        }
        if (idx==(unsigned)-1) {
            idx = count;
            while (idx) {
                IPropertyTree *previtem = qd.root->queryPropTree(getItemPath(path.clear(),idx-1).str());
                if (previtem) {
                    if (previtem->getPropInt("@priority")>=priority) {
                        break;
                    }
                }
                else
                    count--; // how did that happen?
                idx--;
            }
        }
        CJobQueueItem::assignBranch(addItem(qd,createPTree("Item"),idx,count),qi);
        qd.root->setPropInt("@count",count+1);
    }


    void enqueue(sQueueData &qd,IJobQueueItem *qitem) // takes ownership of qitem
    {
        Cconnlockblock block(this,true);
        placeonqueue(qd,qitem,(unsigned)-1);
    }

    void enqueueBefore(sQueueData &qd,IJobQueueItem *qitem,const char *wuid)
    {
        Cconnlockblock block(this,true);
        placeonqueue(qd,qitem,doFindRank(qd,wuid));
    }


    void enqueueAfter(sQueueData &qd,IJobQueueItem *qitem,const char *wuid)
    {
        Cconnlockblock block(this,true);
        unsigned idx = doFindRank(qd,wuid);
        if (idx!=(unsigned)-1)
            idx++;
        placeonqueue(qd,qitem,idx);
    }

    void enqueueTail(sQueueData &qd,IJobQueueItem *qitem)
    {
        Cconnlockblock block(this,true);
        Owned<IJobQueueItem> qi = getTail(qd);
        if (qi)
            enqueueAfter(qd,qitem,qi->queryWUID());
        else
            enqueue(qd,qitem);
    }

    void enqueueHead(sQueueData &qd,IJobQueueItem *qitem)
    {
        Cconnlockblock block(this,true);
        Owned<IJobQueueItem> qi = doGetItem(qd, 0);
        if (qi)
            enqueueBefore(qd,qitem,qi->queryWUID());
        else
            enqueue(qd,qitem);
    }

    unsigned ordinality(sQueueData &qd)
    {
        Cconnlockblock block(this,false);
        return qd.root->getPropInt("@count");
    }

    IJobQueueItem *getTail(sQueueData &qd)
    {
        return doGetItem(qd,(unsigned)-1);
    }

    IJobQueueItem *loadItem(sQueueData &qd,IJobQueueItem *qi)
    {
        Cconnlockblock block(this,false);
        StringBuffer path;
        IPropertyTree *item = qd.root->queryPropTree(getItemPath(path,qi->queryWUID()).str());
        if (!item)
            return NULL;
        bool cached = item->getPropInt("@num",0)<=0;
        if (cached)
            return NULL;    // don't want cached value
        return new CJobQueueItem(item);
    }

    bool checkprio(sQueueData &qd,int minprio=0)
    {
        StringBuffer path;
        IPropertyTree *item = qd.root->queryPropTree(getItemPath(path,0U).str());
        if (!item)
            return false;
        return (item->getPropInt("@priority")>=minprio);
    }

    IJobQueueItem *dotake(sQueueData &qd,const char *wuid,bool saveitem,bool hasminprio=false,int minprio=0)
    {
        StringBuffer path;
        IPropertyTree *item = qd.root->queryPropTree(getItemPath(path,wuid).str());
        if (!item)
            return NULL;
        if (item->getPropInt("@num",0)<=0)
            return NULL;    // don't want (old) cached value
        if (hasminprio&&(item->getPropInt("@priority")<minprio))
            return NULL;
        IJobQueueItem *ret = new CJobQueueItem(item);
        removeItem(qd,item,saveitem);
        unsigned count = qd.root->getPropInt("@count");
        assertex(count);
        qd.root->setPropInt("@count",count-1);
        return ret;
    }

    IJobQueueItem *take(sQueueData &qd,const char *wuid)
    {
        Cconnlockblock block(this,true);
        return dotake(qd,wuid,false);
    }

    unsigned takeItems(sQueueData &qd,CJobQueueContents &dest)
    {
        Cconnlockblock block(this,true);
        unsigned ret = copyItemsImpl(qd,dest);
        clear(qd);
        return ret;
    }

    void enqueueItems(sQueueData &qd,CJobQueueContents &items)
    {
        unsigned n=items.ordinality();
        if (n) {
            Cconnlockblock block(this,true);
            for (unsigned i=0;i<n;i++)
                enqueue(qd,items.item(i).clone());
        }
    }

    void enqueueBefore(IJobQueueItem *qitem,const char *wuid)
    {
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        enqueueBefore(*qd,qitem,wuid);
    }


    void enqueueAfter(IJobQueueItem *qitem,const char *wuid)
    {
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        enqueueAfter(*qd,qitem,wuid);
    }


    bool moveBefore(const char *wuid,const char *nextwuid)
    {
        if (!qdata)
            return false;
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        if (!qd)
            return false;
        IJobQueueItem *qi=take(*qd,wuid);
        if (!qi)
            return false;
        sQueueData *qdd = NULL;
        if (qdata->next)
            qdd = findQD(nextwuid);
        if (!qdd)
            qdd = qd;
        enqueueBefore(*qdd,qi,nextwuid);
        return true;
    }

    bool moveAfter(const char *wuid,const char *prevwuid)
    {
        if (!qdata)
            return false;
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        if (!qd)
            return false;
        IJobQueueItem *qi=take(*qd,wuid);
        if (!qi)
            return false;
        sQueueData *qdd = NULL;
        if (qdata->next)
            qdd = findQD(prevwuid);
        if (!qdd)
            qdd = qd;
        enqueueAfter(*qdd,qi,prevwuid);
        return true;
    }

    bool moveToHead(const char *wuid)
    {
        if (!qdata)
            return false;
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        if (!qd)
            return false;
        IJobQueueItem *qi=take(*qd,wuid);
        if (!qi)
            return false;
        enqueueHead(*qd,qi);
        return true;
    }

    bool moveToTail(const char *wuid)
    {
        if (!qdata)
            return false;
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        if (!qd)
            return false;
        IJobQueueItem *qi=take(*qd,wuid);
        if (!qi)
            return false;
        enqueueTail(*qd,qi);
        return true;
    }


    bool remove(const char *wuid)
    {
        if (!qdata)
            return false;
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        if (!qd)
            return false;
        StringBuffer path;
        IPropertyTree *item = qd->root->queryPropTree(getItemPath(path,wuid).str());
        if (!item)
            return false;
        bool cached = item->getPropInt("@num",0)<=0; // old cached (bwd compat)
        removeItem(*qd,item,false);
        if (!cached) {
            unsigned count = qd->root->getPropInt("@count");
            assertex(count);
            qd->root->setPropInt("@count",count-1);
        }
        return true;

    }

    bool changePriority(const char *wuid,int value)
    {
        if (!qdata)
            return false;
        Cconnlockblock block(this,true);
        sQueueData *qd = qdata->next?findQD(wuid):qdata;
        if (!qd)
            return false;
        IJobQueueItem *qi=take(*qd,wuid);
        if (!qi) {
            StringBuffer ws("~");   // change cached item
            ws.append(wuid);
            StringBuffer path;
            IPropertyTree *item = qd->root->queryPropTree(getItemPath(path,ws.str()).str());
            if (item) {
                item->setPropInt("@priority",value);
                return true;
            }
            return false;
        }
        qi->setPriority(value);
        enqueue(*qd,qi);
        return true;
    }

    void clear(sQueueData &qd)
    {
        Cconnlockblock block(this,true);
        qd.root->setPropInt("@count",0);
        loop {
            IPropertyTree *item = qd.root->queryPropTree("Item[1]");
            if (!item)
                break;
            qd.root->removeTree(item);
        }
    }

    void lock()
    {
        connlock(false);        // sub functions will change to exclusive if needed
    }

    void unlock(bool rollback=false)
    {
        connunlock(rollback);
    }

    void pause(sQueueData &qd)
    {
        Cconnlockblock block(this,true);
        qd.root->setProp("@state","paused");
    }

    void resume(sQueueData &qd)
    {
        Cconnlockblock block(this,true);
        qd.root->setProp("@state","active");
    }

    bool paused(sQueueData &qd)
    {
        Cconnlockblock block(this,false);
        const char *state = qd.root->queryProp("@state");
        return (state&&(strcmp(state,"paused")==0));
    }

    void stop(sQueueData &qd)
    {
        Cconnlockblock block(this,true);
        qd.root->setProp("@state","stopped");
    }

    bool stopped(sQueueData &qd)
    {
        Cconnlockblock block(this,false);
        const char *state = qd.root->queryProp("@state");
        return (state&&(strcmp(state,"stopped")==0));
    }

    void doGetStats(sQueueData &qd,unsigned &connected,unsigned &waiting,unsigned &enqueued)
    {
        Cconnlockblock block(this,false);
        connected = 0;
        waiting = 0;
        unsigned i=0;
        loop {
            IPropertyTree *croot = queryClientRootIndex(qd,i);
            if (!croot)
                break;
            if (!validSession(croot)) {
                Cconnlockblock block(this,true);
                qd.root->removeTree(croot);
            }
            else {
                waiting += croot->getPropInt("@waiting");
                connected += croot->getPropInt("@connected");
                i++;
            }
        }
        // now remove any duff queue items
        unsigned count = qd.root->getPropInt("@count");
        if (!validateitemsessions) {
            enqueued = count;
            return;
        }
        i=0;
        StringBuffer path;
        loop {
            IPropertyTree *item = qd.root->queryPropTree(getItemPath(path.clear(),i).str());
            if (!item)
                break;
            if (!validSession(item)) {
                Cconnlockblock block(this,true);
                item = qd.root->queryPropTree(path.str());
                if (!item)
                    break;
//              PROGLOG("WUJOBQ: Removing %s as session %" I64F "x not active",item->queryProp("@wuid"),item->getPropInt64("@session"));
                removeItem(qd,item,false);
            }
            else
                i++;
        }
        if (count!=i) {
            Cconnlockblock block(this,true);
            qd.root->setPropInt("@count",i);
        }
        enqueued = i;
    }

    void getStats(sQueueData &qd,unsigned &connected,unsigned &waiting,unsigned &enqueued)
    {
        Cconnlockblock block(this,false);
        doGetStats(qd,connected,waiting,enqueued);
    }

    void getStats(unsigned &connected,unsigned &waiting,unsigned &enqueued)
    {
        // multi queue
        Cconnlockblock block(this,false);
        connected=0;
        waiting=0;
        enqueued=0;
        ForEachQueue(qd) {
            unsigned c;
            unsigned w;
            unsigned e;
            doGetStats(*qd,c,w,e);
            connected+=c;
            waiting+=w;
            enqueued+=e;
        }
    }

    IJobQueueItem *take(const char *wuid)
    {
        assertex(qdata);
        if (!qdata->next)
            return take(*qdata,wuid);
        Cconnlockblock block(this,true);
        ForEachQueue(qd) {
            IJobQueueItem *ret = dotake(*qd,wuid,false);
            if (ret)
                return ret;
        }
        return NULL;
    }
    unsigned takeItems(CJobQueueContents &dest)
    {
        assertex(qdata);
        if (!qdata->next)
            return takeItems(*qdata,dest);
        Cconnlockblock block(this,true);
        unsigned ret = 0;
        ForEachQueue(qd) {
            ret += copyItemsImpl(*qd,dest);
            clear(*qd);
        }
        return ret;
    }
    void enqueueItems(CJobQueueContents &items)
    {  // enqueues to firs sub-queue (not sure that useful)
        assertex(qdata);
        return enqueueItems(*qdata,items);
    }

    void clear()
    {
        ForEachQueue(qd) {
            clear(*qd);
        }
    }


    bool validSession(IPropertyTree *item)
    {
        Owned<INode> node = createINode(item->queryProp("@node"),DALI_SERVER_PORT); // port should always be present
        return (querySessionManager().lookupProcessSession(node)==(SessionId)item->getPropInt64("@session"));
    }

    IConversation *initiateConversation(sQueueData &qd,IJobQueueItem *item)
    {
        CriticalBlock block(crit);
        assertex(!initiateconv.get());
        SocketEndpoint ep = item->queryEndpoint();
        unsigned short port = (unsigned short)item->getPort();
        initiateconv.setown(createSingletonSocketConnection(port));
        if (!port)
            item->setPort(initiateconv->setRandomPort(WUJOBQ_BASE_PORT,WUJOBQ_PORT_NUM));
        initiatewu.set(item->queryWUID());
        enqueue(qd,item);
        bool ok;
        {
            CriticalUnblock unblock(crit);
            ok = initiateconv->accept(INFINITE);
        }
        if (!ok)
            initiateconv.clear();
        return initiateconv.getClear();
    }

    IConversation *acceptConversation(IJobQueueItem *&retitem, unsigned prioritytransitiondelay,IDynamicPriority *maxp)
    {
        CriticalBlock block(crit);
        retitem = NULL;
        assertex(connected); // must be connected
        int curmp = maxp?maxp->get():0;
        int nextmp = curmp;
        loop {
            bool timedout = false;
            Owned<IJobQueueItem> item;
            {
                CriticalUnblock unblock(crit);
                // this is a bit complicated with multi-thor
                if (prioritytransitiondelay||maxp) {
                    item.setown(dodequeue((std::max(curmp,nextmp)/10)*10, // round down to multiple of 10
                        prioritytransitiondelay?prioritytransitiondelay:60000,prioritytransitiondelay>0,&timedout));
                                                            // if dynamic priority check every minute
                    if (!prioritytransitiondelay) {
                        curmp = nextmp; // using max above is a bit devious to allow transition
                        nextmp = maxp->get();
                    }
                }
                else
                    item.setown(dequeue(INFINITE));
            }
            if (item.get()) {
                if (item->isValidSession()) {
                    SocketEndpoint ep = item->queryEndpoint();
                    ep.port = item->getPort();
                    Owned<IConversation> acceptconv = createSingletonSocketConnection(ep.port,&ep);
                    if (acceptconv->connect(3*60*1000)) { // shouldn't need that long
                        retitem = item.getClear();
                        return acceptconv.getClear();
                    }
                }
            }
            else if (prioritytransitiondelay)
                prioritytransitiondelay = 0;
            else if (!timedout)
                break;
        }
        return NULL;
    }

    void cancelInitiateConversation(sQueueData &qd)
    {
        CriticalBlock block(crit);
        if (initiatewu.get())
            remove(initiatewu);
        if (initiateconv.get())
            initiateconv->cancel();
    }

    void cancelAcceptConversation()
    {
        CriticalBlock block(crit);
        dequeuestop = true;
        notifysem.signal();
    }

    bool cancelInitiateConversation(sQueueData &qd,const char *wuid)
    {
        Cconnlockblock block(this,true);
        loop {
            Owned<IJobQueueItem> item = dotake(qd,wuid,false);
            if (!item.get())
                break;
            if (item->isValidSession()) {
                SocketEndpoint ep = item->queryEndpoint();
                ep.port = item->getPort();
                Owned<IConversation> acceptconv = createSingletonSocketConnection(ep.port,&ep);
                acceptconv->connect(3*60*1000); // connect then close should close other end
                return true;
            }
        }
        return false;
    }

    bool waitStatsChange(unsigned timeout)
    {
        assertex(!connected); // not allowed to call this while connected
        cancelwaiting = false;
        while(!cancelwaiting) {
            {
                Cconnlockblock block(this,false);
                if (haschanged())
                    return true;
            }
            if (!notifysem.wait(timeout))
                break;
        }
        return false;
    }
    void cancelWaitStatsChange()
    {
        CriticalBlock block(crit);
        cancelwaiting = true;
        notifysem.signal();
    }

    virtual void enqueue(IJobQueueItem *qitem)
    {
        enqueue(*activeq,qitem);
    }

    void enqueueHead(IJobQueueItem *qitem)
    {
        enqueueHead(*activeq,qitem);
    }

    void enqueueTail(IJobQueueItem *qitem)
    {
        enqueueTail(*activeq,qitem);
    }

    void pause()
    {
        Cconnlockblock block(this,true);
        ForEachQueue(qd) {
            if (qd->root)
                qd->root->setProp("@state","paused");
        }
    }
    void pause(const char* info)
    {
        Cconnlockblock block(this,true);
        ForEachQueue(qd) {
            if (qd->root) {
                qd->root->setProp("@state","paused");
                if (info && *info)
                    qd->root->setProp("@stateDetails",info);
            }
        }
    }
    void stop()
    {
        Cconnlockblock block(this,true);
        ForEachQueue(qd) {
            if (qd->root)
                qd->root->setProp("@state","stopped");
        }
    }
    void stop(const char* info)
    {
        Cconnlockblock block(this,true);
        ForEachQueue(qd) {
            if (qd->root) {
                qd->root->setProp("@state","stopped");
                if (info && *info)
                    qd->root->setProp("@stateDetails",info);
            }
        }
    }

    void resume()
    {
        Cconnlockblock block(this,true);
        ForEachQueue(qd) {
            if (qd->root)
                qd->root->setProp("@state","active");
        }
    }
    void resume(const char* info)
    {
        Cconnlockblock block(this,true);
        ForEachQueue(qd) {
            if (qd->root) {
                qd->root->setProp("@state","active");
                if (info && *info)
                    qd->root->setProp("@stateDetails",info);
            }
        }
    }

    IConversation *initiateConversation(IJobQueueItem *item)
    {
        return initiateConversation(*activeq,item);
    }
    void cancelInitiateConversation()
    {
        return cancelInitiateConversation(*activeq);
    }
    bool cancelInitiateConversation(const char *wuid)
    {
        return cancelInitiateConversation(*activeq,wuid);
    }

    const char * queryActiveQueueName()
    {
        return activeq->qname;
    }

    void setActiveQueue(const char *name)
    {
        ForEachQueue(qd) {
            if (!name||(strcmp(qd->qname.get(),name)==0)) {
                activeq = qd;
                return;
            }
        }
        if (name)
            throw MakeStringException (-1,"queue %s not found",name);
    }

    const char *nextQueueName(const char *last)
    {
        ForEachQueue(qd) {
            if (!last||(strcmp(qd->qname.get(),last)==0)) {
                if (qd->next)
                    return qd->next->qname.get();
                break;
            }
        }
        return NULL;
    }

    virtual bool paused()
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::paused();
    }
    virtual bool paused(StringBuffer& info)
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::paused(info);
    }
    virtual bool stopped()
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::stopped();
    }
    virtual bool stopped(StringBuffer& info)
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::stopped(info);
    }
    virtual unsigned ordinality()
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::ordinality();
    }
    virtual unsigned waiting()
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::waiting();
    }
    virtual IJobQueueItem *getItem(unsigned idx)
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::getItem(idx);
    }
    virtual IJobQueueItem *getHead()
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::getHead();
    }
    virtual IJobQueueItem *getTail()
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::getTail();
    }
    virtual IJobQueueItem *find(const char *wuid)
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::find(wuid);
    }
    virtual unsigned findRank(const char *wuid)
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::findRank(wuid);
    }
    virtual unsigned copyItems(CJobQueueContents &dest)
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::copyItems(dest);
    }
    virtual bool getLastDequeuedInfo(StringAttr &wuid, CDateTime &enqueuedt, int &priority)
    {
        Cconnlockblock block(this,false);
        return CJobQueueBase::doGetLastDequeuedInfo(activeq, wuid, enqueuedt, priority);
    }
    virtual void copyItemsAndState(CJobQueueContents& contents, StringBuffer& state, StringBuffer& stateDetails)
    {
        Cconnlockblock block(this,false);
        CJobQueueBase::copyItemsAndState(contents, state, stateDetails);
    }
    virtual void getState(StringBuffer& state, StringBuffer& stateDetails)
    {
        Cconnlockblock block(this,false);
        CJobQueueBase::getState(state, stateDetails);
    }
};

class CJQSnapshot : public CInterface, implements IJQSnapshot
{
    Owned<IPropertyTree> jobQueueInfo;

public:
    IMPLEMENT_IINTERFACE;

    CJQSnapshot()
    {
        Owned<IRemoteConnection> connJobQueues = querySDS().connect("/JobQueues", myProcessSession(), RTM_LOCK_READ, 30000);
        if (!connJobQueues)
            throw MakeStringException(-1, "CJQSnapshot::CJQSnapshot: /JobQueues not found");

        jobQueueInfo.setown(createPTreeFromIPT(connJobQueues->queryRoot()));
    }

    IJobQueueConst* getJobQueue(const char *name)
    {
        if (!jobQueueInfo)
            return NULL;
        return new CJobQueueConst(name, jobQueueInfo.getLink());
    }
};

IJQSnapshot *createJQSnapshot()
{
    return new CJQSnapshot();
}

IJobQueue *createJobQueue(const char *name)
{
    if (!name||!*name)
        throw MakeStringException(-1,"createJobQueue empty name");
    return new CJobQueue(name);
}

extern bool WORKUNIT_API runWorkUnit(const char *wuid, const char *cluster)
{
    Owned<IConstWUClusterInfo> clusterInfo = getTargetClusterInfo(cluster);
    if (!clusterInfo.get())
        return false;
    SCMStringBuffer agentQueue;
    clusterInfo->getAgentQueue(agentQueue);
    if (!agentQueue.length())
        return false;

    Owned<IJobQueue> queue = createJobQueue(agentQueue.str());
    if (!queue.get()) 
        throw MakeStringException(-1, "Could not create workunit queue");

    IJobQueueItem *item = createJobQueueItem(wuid);
    queue->enqueue(item);
    PROGLOG("Agent request '%s' enqueued on '%s'", wuid,  agentQueue.str());
    return true;
}

extern bool WORKUNIT_API runWorkUnit(const char *wuid)
{
    Owned<IWorkUnitFactory> factory = getWorkUnitFactory();
    Owned<IConstWorkUnit> w = factory->openWorkUnit(wuid);
    if (w)
    {
        StringAttr clusterName = (w->queryClusterName());
        w.clear();
        return runWorkUnit(wuid, clusterName.str());
    }
    else
        return false;
}

extern WORKUNIT_API StringBuffer &getQueuesContainingWorkUnit(const char *wuid, StringBuffer &queueList)
{
    Owned<IRemoteConnection> conn = querySDS().connect("/JobQueues", myProcessSession(), RTM_LOCK_READ, 5000);
    if (!conn)
        return queueList;

    VStringBuffer xpath("Queue[Item/@wuid='%s']", wuid);
    Owned<IPropertyTreeIterator> it = conn->getElements(xpath.str());
    ForEach(*it)
    {
        if (queueList.length())
            queueList.append(',');
        queueList.append(it->query().queryProp("@name"));
    }
    return queueList;
}

extern void WORKUNIT_API removeWorkUnitFromAllQueues(const char *wuid)
{
    StringBuffer queueList;
    if (!getQueuesContainingWorkUnit(wuid, queueList).length())
        return;
    Owned<IJobQueue> q = createJobQueue(queueList.str());
    if (q)
        while(q->remove(wuid));
}

extern bool WORKUNIT_API switchWorkUnitQueue(IWorkUnit* wu, const char *cluster)
{
    if (!wu)
        return false;

    class cQswitcher: public CInterface, implements IQueueSwitcher
    {
    public:
        IMPLEMENT_IINTERFACE;
        void * getQ(const char * qname, const char * wuid)
        {
            Owned<IJobQueue> q = createJobQueue(qname);
            return q->take(wuid);
        }
        void putQ(const char * qname, const char * wuid, void * qitem)
        {
            Owned<IJobQueue> q = createJobQueue(qname);
            q->enqueue((IJobQueueItem *)qitem);
        }
        bool isAuto()
        {
            return false;
        }
    } switcher;

    return wu->switchThorQueue(cluster, &switcher);
}
