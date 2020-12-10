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

#define da_decl DECL_EXPORT
#include "platform.h"
#include "jlib.hpp"
#include "jsuperhash.hpp"
#include "dacoven.hpp"
#include "daclient.hpp"
#include "dasds.hpp"
#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "mputil.hpp"
#include "daserver.hpp"
#include "danqs.hpp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

// # HELP nq_requests The total number of Dali NQ requests handled
// # TYPE nq_requests counter
// Probably requires this counter to be held in a __int64 scalar.

// # HELP nq_active_requests Current number of active NQ requests being handled.
// # TYPE nq_active_requests gauge
// A unsigned scalar should suffice, i.e. never going to have that many active transactions


enum MQueueRequestKind { 
    MQR_ADD_QUEUE,
    MQR_GET_QUEUE, 
    MQR_PROBE,
    MQR_CANCEL_SUB,     // prior to cancel subscription
    MQR_COMMIT_TRANSACTION,
    MQR_ROLLBACK_TRANSACTION
};


class CQueueChannel;

class CNamedQueueConnection: implements INamedQueueConnection, public CInterface
{
    SecurityToken tok;
    CheckedCriticalSection sect;
    DALI_UID transactionId;
public:
    IMPLEMENT_IINTERFACE;

    CNamedQueueConnection(SecurityToken _tok)
        : transactionId(0)
    {
        tok = _tok;
    }

    ~CNamedQueueConnection()
    {
        if(transactionId)
            rollback();
    }

    DALI_UID queryTransactionId() const { return transactionId; }

    // iface INamedQueueConnection
    IQueueChannel *open(const char *qname);

    void startTransaction()
    {
        CHECKEDCRITICALBLOCK(sect,60000);
        if(transactionId)
            throw MakeStringException(0, "Dali Named Queues: trying to start nested transaction frames");
        transactionId = queryCoven().getUniqueId();
    }

    bool commit()
    {
        return stopTransaction(MQR_COMMIT_TRANSACTION);
    }

    bool rollback()
    {
        return stopTransaction(MQR_ROLLBACK_TRANSACTION);
    }

private:
    bool stopTransaction(enum MQueueRequestKind kind)
    {
        CHECKEDCRITICALBLOCK(sect,60000);
        if(transactionId==0)
            return false;
        CMessageBuffer mb;
        mb.append((int)kind).append(transactionId);
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_NAMED_QUEUE_REQUEST);
        unsigned ret;
        mb.read(ret);
        if(mb.getPos()>mb.length())
        {
            StringAttr except;
            mb.read(except);
            throw MakeStringException(ret,"Dali Named Queues(3), Server Exception: %s",except.get());
        }
        if(ret == 0)
            return false;
        transactionId = 0;
        return true;
    }

};


class CNamedQueueHandler: public CInterface
{ 
    // client side simple handler
protected: friend class CNamedQueueSubscriptionProxy;
    SubscriptionId id;
    StringAttr name;
    int priority;
    bool oneshot;
    SessionId transaction;
public:
    CNamedQueueHandler(const char *_name, int _priority, bool _oneshot, SessionId _transaction)
        : name(_name)
    {
        id = queryCoven().getUniqueId();
        priority = _priority;
        oneshot = _oneshot;
        transaction = _transaction;
    }
    bool get( MemoryBuffer &mbout)
    { 
        // no timeout - 0 length q item returned equates with cancel
        bool osdummy = true; // serverside always one shot currently
        CMessageBuffer mb;
        mb.append((int)MQR_GET_QUEUE).append(id).append(name).append(priority).append(osdummy).append(transaction);
        if (!queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_NAMED_QUEUE_REQUEST)) 
            mb.clear();
        mbout.swapWith(mb);
        return mbout.length()!=0;
    }

    SubscriptionId getId()
    {
        return id;
    }
};

class CNamedQueueSubscriptionProxy: public Thread
{   
    // client side subscription handler
    CQueueChannel *owner;
    Linked<INamedQueueSubscription> subs;
    bool finished;
    CNamedQueueHandler handler;

public:

    CNamedQueueSubscriptionProxy(CQueueChannel *_owner,INamedQueueSubscription *_subs,const char *_name, int _priority, bool _oneshot, SessionId _transaction) 
        : handler(_name, _priority, _oneshot, _transaction), subs(_subs)
    {
        owner = _owner;
        finished = false;
        start();
    }

    ~CNamedQueueSubscriptionProxy();

    int run()
    {
        try {
            MemoryBuffer buf;
            bool ret;
            do {
                ret = handler.get(buf.clear());
                if (ret&&!finished)
                    subs->notify(handler.name,buf);
            } while (!handler.oneshot&&ret);
            finished = true;

        }
        catch (IException *e)
        {   
            // server error
            EXCLOG(e, "CNamedQueueSubscriptionProxy");
            e->Release();
            if (!finished) {
                subs->abort();
                finished = true;
            }
        }
        remove();
        return 0;
    }

    INamedQueueSubscription *querySubscription()
    {
        return subs;
    }

    SubscriptionId getId()
    {
        return handler.id;
    }

    void abort()
    {
        // no one calling yet
        subs->abort();
    }

    void stop();

    void remove();
};


class CQueueChannel: implements IQueueChannel, public CInterface
{ // Client side

    StringAttr name;
    Owned<CNamedQueueConnection> parent;
    SubscriptionId getsid;
    CheckedCriticalSection proxysect;
    CIArrayOf<CNamedQueueSubscriptionProxy> proxies;


public:
    IMPLEMENT_IINTERFACE;

    CQueueChannel(CNamedQueueConnection *_parent, const char *_name)
        : name(_name), parent(_parent)
    {
        parent->Link();
        getsid = 0;
    }

    ~CQueueChannel()
    {
        ForEachItemIn(i,proxies) {
            proxies.item(i).stop();
        }
        proxies.kill();
    }

    void put(MemoryBuffer &buf, int priority=0)
    {
        size32_t len = buf.length();
        CMessageBuffer mb;
        mb.append((int)MQR_ADD_QUEUE).append(name).append(parent->queryTransactionId()).append(priority).append(len).append(buf);
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_NAMED_QUEUE_REQUEST);
        unsigned ret;
        StringAttr except;
        mb.read(ret);
        if (ret) {
            mb.read(except);
            throw MakeStringException(ret,"Dali Named Queues(1), Server Exception: %s",except.get());
        }
    }

                                                        // puts message on queue, buf is clear on return
    bool get(MemoryBuffer &buf, int priority=0, unsigned timeout=WAIT_FOREVER)
    {
        CNamedQueueHandler handler(name,priority,true,parent->queryTransactionId());
        getsid = handler.getId();
        if (timeout==WAIT_FOREVER) {
            return handler.get(buf);
        }
        class cTimeoutThread: public Thread
        {
            CQueueChannel *parent;
            unsigned timeout;
        public:
            cTimeoutThread(CQueueChannel *_parent, unsigned _timeout)
                : Thread("CNamedQueueHandlerTimeout")
            {
                parent = _parent;
                timeout = _timeout;
            }
            Semaphore sem;
            int run()
            {
                if (!sem.wait(timeout)) {
                    parent->cancelGet();        // this should only cancel this get
                }
                return 0;
            }
        } timeoutthread(this,timeout);
        timeoutthread.start();
        bool ret = handler.get(buf);
        timeoutthread.sem.signal();
        timeoutthread.join();
        return ret;
    }


    SubscriptionId subscribe(INamedQueueSubscription *subs, int priority, bool oneshot)
    {
        CHECKEDCRITICALBLOCK(proxysect,60000);
        CNamedQueueSubscriptionProxy *proxy = new CNamedQueueSubscriptionProxy(this,subs,name,priority,oneshot,parent->queryTransactionId());
        proxies.append(*proxy);
        return proxy->getId();
    }

    virtual unsigned doprobe(MemoryBuffer *buf,PointerArray *ptrs)  // probes contents of queue
    {
        CMessageBuffer mb;
        mb.append((int)MQR_PROBE).append(name);
        if (buf) {
            size32_t maxsz = 0x100000;  // 1MB
            mb.append(maxsz);
        }
        queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_NAMED_QUEUE_REQUEST);
        unsigned ret;
        mb.read(ret);
        if (!buf||!ret) {
            if (mb.getPos()>mb.length()) {
                StringAttr except;
                mb.read(except);
                throw MakeStringException(ret,"Dali Named Queues(2), Server Exception: %s",except.get());
            }
        }
        else {
            buf->swapWith(mb);
            const byte *base = (const byte *)buf->toByteArray();
            size32_t *sizes = (size32_t *)(base + sizeof(size32_t));
            for (unsigned i=0;i<ret;i++) 
                ptrs->append((void *)(base+sizes[i]));
        }
        return ret;
    }

    
    unsigned probe()
    {
        return doprobe(NULL,NULL);
    }

    virtual unsigned probe(MemoryBuffer &buf,PointerArray &ptrs)  // probes contents of queue
    {
        return doprobe(&buf,&ptrs);
    }


    void cancelGet()
    {
        cancelSubscription(getsid); // getsid may be out of date but doesn't matter
    }

    void remove(CNamedQueueSubscriptionProxy *e)
    {
        Linked<CNamedQueueSubscriptionProxy> l = e; 
        {
            CHECKEDCRITICALBLOCK(proxysect,60000);
            proxies.zap(*e);
        }
    }

    void cancelSubscription(SubscriptionId id)
    {
        if (id) {
            try {
                CMessageBuffer mb;
                mb.append((int)MQR_CANCEL_SUB).append(id);
                queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_NAMED_QUEUE_REQUEST);
            }
            catch (IException *e)
            {   
                // server error
                EXCLOG(e, "cancelSubscription");
                e->Release();
            }
        }
        Sleep(10);  // avoid race, bit of a kludge
    }

    int changePriority(int newpriority, SubscriptionId id)
    {
        UNIMPLEMENTED; // TBD
        return 0;
    }
};


CNamedQueueSubscriptionProxy::~CNamedQueueSubscriptionProxy()
{
}

void CNamedQueueSubscriptionProxy::stop()
{
    if (!finished) {
        finished = true;
        CQueueChannel *o = owner;
        owner = NULL;               
        o->cancelSubscription(handler.id);
        join();
    }
}


void CNamedQueueSubscriptionProxy::remove()
{
    if (owner)
        owner->remove(this);    // object deleted by this
}

IQueueChannel *CNamedQueueConnection::open(const char *qname)
{
    CHECKEDCRITICALBLOCK(sect,60000);
    CQueueChannel *channel = new CQueueChannel(this,qname);
    return channel;
}

INamedQueueConnection *createNamedQueueConnection(SecurityToken tok)
{
    return new CNamedQueueConnection(tok);
}


class CDaliNamedQueueServer: public IDaliServer, public Thread, implements IConnectionMonitor
{  // Server side
    

    class CNamedQueueSubscriptionStub: public CInterface
    {   
        mptag_t replytag;
        SocketEndpoint client;
        SubscriptionId id;
        int priority;
        bool oneshot;
        StringAttr name;
        DALI_UID transactionId;
    public:
        CNamedQueueSubscriptionStub(CMessageBuffer &mb) // takes ownership
        {
            replytag = mb.getReplyTag();
            client = mb.getSender();
            mb.read(id).read(name).read(priority).read(oneshot).read(transactionId);
        }

        ~CNamedQueueSubscriptionStub() 
        {
            // abort? TBD
        }

        SubscriptionId getId() { return id; }
        int getPriority() { return priority; }
        const char * queryName() { return name.get(); }
        SocketEndpoint &queryClient() { return client; }
        void notify(MemoryBuffer &buf) 
        { 
            CMessageBuffer mb;
            mb.init(client,MPTAG_DALI_NAMED_QUEUE_REQUEST,replytag);
            mb.swapWith(buf);
            queryCoven().reply(mb);     // may fail
        }
        void cancel() 
        { 
            // just send back zero length
            MemoryBuffer mb;
            notify(mb);
        }
        bool isOneShot() { return oneshot; } // currently always true as subscriptions handled client side
        SessionId queryTransactionId() const { return transactionId; }

    };

    bool stopped;
    CIArrayOf<CNamedQueueSubscriptionStub> stubs; // kept in priority order
    CheckedCriticalSection subsect;
    int fn = 0;
public:
    IMPLEMENT_IINTERFACE;

    CDaliNamedQueueServer()
        : Thread("CDaliNamedQueueServer")
    {
        stopped = true;
        addMPConnectionMonitor(this);
    }

    ~CDaliNamedQueueServer()
    {
        stubs.kill();
        removeMPConnectionMonitor(this);
    }

    void start()
    {
        Thread::start();
    }

    void ready()
    {
    }
    
    void suspend()
    {
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryCoven().cancel(RANK_ALL,MPTAG_DALI_NAMED_QUEUE_REQUEST);
        }
        if (!join(1000*60*5)) {
            PROGLOG("CDaliNamedQueueServer::stop timed out - active function is %d",fn);
        }
    }

    int run()
    {
        ICoven &coven=queryCoven();
        CMessageBuffer mb;
        stopped = false;
        while (!stopped) {
            try {
                // 1) Need to increment nq_requests here
                // NB: it will never be decremented. This is total for life of this instance.

                // 2) Need to increment nq_active_requests here
                // and ensure it's scoped, such that it is guaranteed
                // to decrement when processMessage() is complete.
                // NB: NQ activeRequests are always synchronously handled by processMessage()

                mb.clear();
                if (coven.recv(mb,RANK_ALL,MPTAG_DALI_NAMED_QUEUE_REQUEST,NULL)) {
                    processMessage(mb); // synchronous to ensure queue operations handled in correct order
                }   
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "Named Queue Server");
                e->Release();
            }
        }
        return 0;
    }


    void processMessage(CMessageBuffer &mb)
    {
        // single threaded by caller
        ICoven &coven=queryCoven();
        mb.read(fn);
        unsigned ret = 0;
        try {
            switch (fn) {
            case MQR_ADD_QUEUE:                     
                {
                    bool add = true;
                    StringAttr name;
                    int priority;
                    size32_t qblen;
                    SessionId transactionId;
                    mb.read(name).read(transactionId).read(priority).read(qblen);
                    const byte *data = mb.readDirect(qblen);
                    if(transactionId==0) // see if can put without blinking, unless within transaction frame
                        add = !checkNotifySubscriptions(name, priority, qblen, data);
                    if(add)
                        addSDS(name,priority,qblen,data,transactionId,false);
                    ret = 0;
                }
                break;
            case MQR_GET_QUEUE:                         
                {   
                    add(mb);
                    return; // not reply wanted
                }
                break;
            case MQR_PROBE:                     
                {
                    CHECKEDCRITICALBLOCK(subsect,60000);                    
                    StringAttr name;
                    mb.read(name);
                    size32_t maxsz=0;
                    if (mb.length()>=mb.getPos()+sizeof(size32_t)) 
                        mb.read(maxsz);
                    probeSDSget(name,mb.clear(),maxsz);     
                    coven.reply(mb); 
                    return;
                }
                break;
            case MQR_CANCEL_SUB:                        
                {
                    CHECKEDCRITICALBLOCK(subsect,60000);                    
                    SubscriptionId id;
                    mb.read(id);
                    cancel(id);
                    ret = 0;
                }
                break;
            case MQR_COMMIT_TRANSACTION:               
                {
                    DALI_UID transactionId;
                    mb.read(transactionId);
                    ret = stopTransaction(transactionId, false);
                }
                break;
            case MQR_ROLLBACK_TRANSACTION:               
                {
                    DALI_UID transactionId;
                    mb.read(transactionId);
                    ret = stopTransaction(transactionId, true);
                }
                break;
            }
            mb.clear().append(ret);
        }
        catch (IException *e)
        {
            StringBuffer s;
            e->errorMessage(s);
            EXCLOG(e, "Named Queue Server - handleMessage");
            mb.clear();
            if (fn==MQR_PROBE)
                mb.append((unsigned)0).append(s.str()); // error will just return nothing from probe
            else if (fn!=MQR_GET_QUEUE) // error will just cancel get_queue 
                mb.append(e->errorCode()).append(s.str());
            e->Release();
        }
        fn = 0;
        coven.reply(mb); 

    }   

    bool checkNotifySubscriptions(char const * name, int priority, size32_t qblen, byte const * data)
    {
        CIArrayOf<CNamedQueueSubscriptionStub> toremove;
        CHECKEDCRITICALBLOCK(subsect,60000);                    
        MemoryBuffer qb;
        for(unsigned i=0; i<stubs.ordinality(); i++)
        {
            CNamedQueueSubscriptionStub &stub = stubs.item(i);
            if(strcmp(stub.queryName(),name)==0)
            {
                qb.clear().append(qblen,data);
                try
                {
                    stub.notify(qb);
                    if(stub.queryTransactionId()) addSDS(name, priority, qblen, data, stub.queryTransactionId(), true);
                }
                catch (IException *e)
                {
                    // should just be aborted exceptions, but remove subscription whatever the error.
                    LOG(MCdebugInfo, unknownJob, e, "Named Queue Server - MQR_ADD_QUEUE notify");                   
                    e->Release();
                    stubs.remove(i,true);
                    toremove.append(stub);
                    i--;
                    continue;
                }
                if (stub.isOneShot()) {
                    stubs.remove(i,true);
                    toremove.append(stub);
                    i--;
                }   
                toremove.kill();    
                return true;
            }
        }
        toremove.kill();    
        return false;
    }

    void nodeDown(rank_t rank)
    {
        assertex(!"TBD");
    }

    unsigned find(SubscriptionId id)
    {
        // called in subsect
        ForEachItemIn(i,stubs) {
            CNamedQueueSubscriptionStub &stub = stubs.item(i);
            if (stub.getId()==id)
                return i;
        }
        return NotFound;
    }

    void add(CMessageBuffer &mb)
    {
        CNamedQueueSubscriptionStub *nstub = new CNamedQueueSubscriptionStub(mb);
        bool rem;
        while (getSDS(nstub,rem)) { // see if can do without blinking
            if (rem) {
                nstub->Release();
                return;
            }
        }
        CHECKEDCRITICALBLOCK(subsect,60000);                    
        ForEachItemIn(i,stubs) {
            CNamedQueueSubscriptionStub &stub = stubs.item(i);
            if (stub.getPriority()<nstub->getPriority())
                break;
        }
        stubs.add(*nstub,i);
    }

    void remove(SubscriptionId id)
    {
        CHECKEDCRITICALBLOCK(subsect,60000);                    
        unsigned i=find(id);
        if (i!=NotFound) 
            stubs.remove(i);
    }

    void cancel(SubscriptionId id)
    {
        CHECKEDCRITICALBLOCK(subsect,60000);                    
        unsigned i=find(id);
        if (i!=NotFound)  {
            stubs.item(i).cancel(); 
            stubs.remove(i);
        }
    }


    void onClose(SocketEndpoint &ep)        // MP connection monitor
    {
        // mark subs closed
        CHECKEDCRITICALBLOCK(subsect,60000);                    
        ForEachItemInRev(i,stubs) {
            CNamedQueueSubscriptionStub &stub = stubs.item(i);
            if (ep.equals(stub.queryClient())) {
                stubs.remove(i);
            }
        }
    }

    IRemoteConnection *connectSDS(const char *name,bool create)
        // no longer needs lock as single threaded by message loop
    {
        // single threaded by caller
        StringBuffer path;
        path.appendf("/Queues/Queue[@name=\"%s\"]",name);
        IRemoteConnection *conn = querySDS().connect(path.str(),0,0,(unsigned)-1); 
        if (!conn&&create) {
            path.clear();
            conn = querySDS().connect("/Queues/Queue",0,RTM_CREATE_ADD,(unsigned)-1);
            assertex(conn);
            IPropertyTree * root = conn->queryRoot();
            root->setProp("@name",name);
            root->setPropInt("@count", 0);
            conn->commit();
        }
        return conn;
    }

    void addSDS(const char *name,int priority,size32_t qblen,const byte *data,DALI_UID transactionId,bool transactionIsGet)
    {
        Owned<IRemoteConnection> conn = connectSDS(name,true);
        IPropertyTree * root = conn->queryRoot();
        IPropertyTree *item = createPTree("Item");
        item->setPropBin("",qblen,data);
        if (priority) {
            item->setPropInt("@priority",priority);
            if (!root->getPropInt("@priorities",0))
                root->setPropInt("@priorities",1);
        }
        IPropertyTree * add;
        if(transactionId)
        {
            char const * branch = transactionIsGet ? "TransactionalGet" : "TransactionalPut";
            StringBuffer path;
            path.appendf("%s[@id=\"%" I64F "d\"]", branch, transactionId);
            add = root->queryPropTree(path.str());
            if(!add)
            {
                add = createPTree(branch);
                add->setPropInt64("@id", transactionId);
                add->setPropInt("@count", 0);
                root->addPropTree(branch, add);
                add = root->queryPropTree(path.str());
            }
        }
        else
            add = root;
        add->addPropTree("Item",item);
        add->setPropInt("@count", add->getPropInt("@count") + 1);
    }

    bool getSDS(CNamedQueueSubscriptionStub *stub,bool &oneshot) // returns false if unsubscribe
    {
        oneshot = false;
        Owned<IRemoteConnection> conn = connectSDS(stub->queryName(),true); // we needn't create but might as well
        IPropertyTree * root = conn->queryRoot();
        MemoryBuffer mb;
        bool usingpriorities = root->getPropInt("@priorities",0)!=0;
        int priority;
        if (usingpriorities) {
            int topidx = 0;
            int priority = 0;
            unsigned i = 0;
            Owned<IPropertyTree> topitem;
            {
                Owned<IPropertyTreeIterator> iter = root->getElements("Item");
                ForEach(*iter) { 
                    i++;
                    IPropertyTree &item = iter->query();
                    int p = item.getPropInt("@priority");
                    if (!topitem.get()||(p>priority)) {
                        priority = p;
                        topitem.set(&item);
                        topidx = i;
                    }
                }
            }
            if (topitem.get()) {
                topitem->getPropBin("",mb.clear());
                topitem.clear();
                stub->notify(mb);
                // lets delete it
                removeSDS(conn,topidx);
                conn.clear();
                if(stub->queryTransactionId())
                    addSDS(stub->queryName(), priority, mb.length(), (byte const *)mb.toByteArray(), stub->queryTransactionId(), true);
                oneshot = stub->isOneShot();
                return true;
            }
        }
        else {  // optimized when put priorities not used
            IPropertyTree *item = root->queryPropTree("Item[1]");
            if (item) {
                item->getPropBin("",mb.clear());
                priority = item->getPropInt("@priority");
                stub->notify(mb);
                // lets delete it
                removeSDS(conn,1);
                conn.clear();
                if(stub->queryTransactionId())
                    addSDS(stub->queryName(), priority, mb.length(), (byte const *)mb.toByteArray(), stub->queryTransactionId(), true);
                oneshot = stub->isOneShot();
                return true;
            }
        }
        return false;
    }

    void probeSDSget(const char *name,MemoryBuffer &mb,size32_t maxsz)
    {
        Owned<IRemoteConnection> conn = connectSDS(name,false);
        unsigned count = conn.get()?conn->queryRoot()->getPropInt("@count"):0;
        mb.append(count);
        if (count) {
            if (maxsz>(count+1)*sizeof(size32_t)) {
                mb.reserve(count*sizeof(size32_t));
                StringBuffer pname;
                Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("Item");
                unsigned i = 0;
                ForEach(*iter) { 
                    IPropertyTree &item = iter->query();
                    size32_t p = mb.length();
                    mb.writeDirect(i*sizeof(size32_t)+sizeof(unsigned),sizeof(size32_t),&p);
                    item.getPropBin(pname.str(),mb);
                    if (maxsz<mb.length()) {
                        mb.clear().append(count);
                        return;
                    }
                    i++;
                }
            }
        }
    }


    void removeSDS(IRemoteConnection *conn,unsigned idx)
    {
        StringBuffer pname;
        IPropertyTree * root = conn->queryRoot();
        root->removeProp(pname.appendf("Item[%d]",idx).str());
        root->setPropInt("@count", root->getPropInt("@count") - 1);
        if (!root->hasProp("Item[1]")) {
            conn->close(true); 
        }
    }
                
    unsigned countSDS()
    {
        // single threaded by caller
        Owned<IRemoteConnection> conn = querySDS().connect("/Queues", 0, RTM_LOCK_READ, (unsigned)-1);
        if(!conn) return 0;
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("Queue");
        unsigned count = 0;
        ForEach(*iter)
            count++;
        return count;
    }

    unsigned meanLengthSDS()
    {
        // single threaded by caller
        Owned<IRemoteConnection> conn = querySDS().connect("/Queues", 0, RTM_LOCK_READ, (unsigned)-1);
        if(!conn) return 0;
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("Queue");
        unsigned count = 0;
        unsigned total = 0;
        ForEach(*iter) {
            count++;
            total += iter->query().getPropInt("@count");
        }
        return count ? (unsigned)(double(total)/count + 0.5) : 0;
    }

    unsigned maxLengthSDS()
    {
        // single threaded by caller
        Owned<IRemoteConnection> conn = querySDS().connect("/Queues", 0, RTM_LOCK_READ, (unsigned)-1);
        if(!conn) return 0;
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("Queue");
        unsigned max = 0;
        unsigned cur;
        ForEach(*iter) {
            cur = iter->query().getPropInt("@count");
            if(cur>max) max = cur;
        }
        return max;
    }

    bool stopTransaction(DALI_UID transactionId, bool rollback)
    {
        {
            CHECKEDCRITICALBLOCK(subsect,60000);                    
            for(unsigned i=0; i<stubs.ordinality(); i++)
                if(stubs.item(i).queryTransactionId() == transactionId) return false;
        }
        Owned<IRemoteConnection> conn = querySDS().connect("/Queues", 0, 0, (unsigned)-1);  // no lock needed as single threaded
        if(!conn) return false;
        Owned<IPropertyTreeIterator> iter = conn->queryRoot()->getElements("Queue");
        ForEach(*iter) {
            IPropertyTree & q = iter->query();
            StringBuffer getPath;
            StringBuffer putPath;
            getPath.appendf("TransactionalGet[@id=\"%" I64F "d\"]", transactionId);
            putPath.appendf("TransactionalPut[@id=\"%" I64F "d\"]", transactionId);
            IPropertyTree * xferBranch;
            IPropertyTree * removeBranch;
            if(rollback)
            {
                xferBranch = q.queryPropTree(getPath.str());
                removeBranch = q.queryPropTree(putPath.str());
            }
            else
            {
                xferBranch = q.queryPropTree(putPath.str());
                removeBranch = q.queryPropTree(getPath.str());
            }
            if(xferBranch)
            {
                unsigned count = q.getPropInt("@count") + xferBranch->getPropInt("@count");
                Owned<IPropertyTreeIterator> iter2 = xferBranch->getElements("Item");
                MemoryBuffer mb;
                ForEach(*iter2) {
                    IPropertyTree & item = iter2->query();
                    item.getPropBin("", mb);
                    if(checkNotifySubscriptions(q.queryProp("@name"), item.getPropInt("@priority"), mb.length(), (byte const *)mb.toByteArray()))
                        count--;
                    else
                        q.addPropTree("Item", LINK(&item));
                }
                q.setPropInt("@count", count);
                q.removeTree(xferBranch);
            }
            if(removeBranch)
                q.removeTree(removeBranch);
        }
        return true;
    }

} *daliNamedQueueServer = NULL;

unsigned queryDaliNamedQueueCount()
{
    assertex(daliNamedQueueServer);
    return daliNamedQueueServer->countSDS();
}

unsigned queryDaliNamedQueueMeanLength()
{
    assertex(daliNamedQueueServer);
    return daliNamedQueueServer->meanLengthSDS();
}

unsigned queryDaliNamedQueueMaxLength()
{
    assertex(daliNamedQueueServer);
    return daliNamedQueueServer->maxLengthSDS();
}

IDaliServer *createDaliNamedQueueServer()
{
    assertex(!daliNamedQueueServer); // initialization problem
    daliNamedQueueServer = new CDaliNamedQueueServer();
    return daliNamedQueueServer;
}


