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

#define da_decl __declspec(dllexport)
#include "platform.h"
#include "jlib.hpp"
#include "jmisc.hpp"
#include "jsuperhash.hpp"
#include "daclient.hpp"

// TBD local Coven subscriptions

//#define SUPRESS_REMOVE_ABORTED
#define TRACE_QWAITING

#include "dacoven.hpp"
#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "daserver.hpp"

#include "dasubs.ipp"

#ifdef _MSC_VER
#pragma warning (disable : 4355)
#endif

enum MSubscriptionRequestKind { 
    MSR_REMOVE_SUBSCRIPTION_PRIMARY,
    MSR_ADD_SUBSCRIPTION_PRIMARY,
    MSR_REMOVE_SUBSCRIPTION_SECONDARY,
    MSR_ADD_SUBSCRIPTION_SECONDARY
};


class CSubscriptionStub: public CInterface, implements ISubscription
{ // Server (Coven)  side
    MemoryAttr data;
    unsigned tag;
    SubscriptionId sid;
    INode *dst;
    bool hasaborted;
public:
    IMPLEMENT_IINTERFACE;

    CSubscriptionStub(unsigned _tag,SubscriptionId _sid,size32_t _datalen, const byte *_data,INode *_dst) 
       : data(_datalen,_data)
    {
        tag = _tag;
        sid = _sid;
        dst = LINK(_dst);
        hasaborted = false;
    }

    virtual ~CSubscriptionStub()
    {
        unlink();
        dst->Release();
    }

    const MemoryAttr &queryData()
    {
        return data;
    }
    
    void notify(MemoryBuffer &returndata)   // if returns false should unsubscribe
    {
        if (hasaborted) {
            throw MakeStringException(-1,"Subscription notification aborted");
            return;
        }
        size32_t dlen = returndata.length();
        CMessageBuffer mb;
        mb.append(tag).append(sid).append(dlen).append(returndata);
        try {
            if (!queryWorldCommunicator().send(mb,dst,MPTAG_DALI_SUBSCRIPTION_FULFILL,1000*60*3))  {
                // Must reply in 3 Minutes
                // Kludge to avoid locking SDS on blocked client
                hasaborted = true;
                StringBuffer tmp;
                throw MakeStringException(-1,"Subscription notification to %s timed out",dst->endpoint().getUrlStr(tmp).str());
                return;
            }

        }
        catch (IMP_Exception *e) {
            PrintExceptionLog(e,"Dali CSubscriptionStub");

            hasaborted = true;
            throw;
        }
    }

    void abort()
    {
        hasaborted = true;
    }

    bool aborted()
    {
        return hasaborted;
    }

    void unlink();

    INode &queryNode() { return *dst; }
    unsigned queryTag() { return tag; }
    SubscriptionId querySubscriptionId() { return sid; }

    StringBuffer &getDetails(StringBuffer &buf)
    {
        StringBuffer ep;
        return buf.appendf("%16" I64F "X: %s %s",sid,dst->endpoint().getUrlStr(ep).str(),hasaborted?"aborted":"");
    }
};

static class CDaliPublisher
{
public:
    virtual ISubscriptionManager *queryManager(unsigned tag) = 0;
    virtual void stop() = 0;
    virtual ~CDaliPublisher() {}
} *DaliPublisher;

class CDaliPublisherServer: public IDaliServer, public Thread, implements CDaliPublisher, implements IConnectionMonitor
{
    ICopyArrayOf<CSubscriptionStub> stubs;
    IArrayOf<ISubscriptionManager> managers;
    UnsignedArray tags;
    CheckedCriticalSection tagsect;
    CheckedCriticalSection stubsect;
    bool stopped;
    ReadWriteLock processlock;
public:
    IMPLEMENT_IINTERFACE;

    CDaliPublisherServer()
        : Thread("CDaliPublisherServer")
    {
        stopped = true;
    }

    ~CDaliPublisherServer()
    {
        stopped = true;
        managers.kill();
    }

    void start()
    {
        Thread::start();
    }
    void ready()
    {
        addMPConnectionMonitor(this);
    }

    void suspend()
    {
        PROGLOG("Suspending subscriptions");
        removeMPConnectionMonitor(this);
        processlock.lockWrite();
        PROGLOG("Suspended subscriptions");
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryCoven().cancel(RANK_ALL,MPTAG_DALI_SUBSCRIPTION_REQUEST);
        }
        processlock.unlockWrite();
        join();
    }

    int run()
    {
        ICoven &coven=queryCoven();
        CMessageHandler<CDaliPublisherServer> handler("CDaliPublisherServer",this,&CDaliPublisherServer::processMessage,NULL, 100);
        CMessageBuffer mb;
        stopped = false;
        while (!stopped) {
            try {
                mb.clear();
#ifdef TRACE_QWAITING
                unsigned waiting = coven.probe(RANK_ALL,MPTAG_DALI_SUBSCRIPTION_REQUEST,NULL);
                if ((waiting!=0)&&(waiting%10==0))
                    DBGLOG("QPROBE: MPTAG_DALI_SUBSCRIPTION_REQUEST has %d waiting",waiting);
#endif
                if (coven.recv(mb,RANK_ALL,MPTAG_DALI_SUBSCRIPTION_REQUEST,NULL))
                    handler.handleMessage(mb);
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CDaliPublisherServer");
                e->Release();
            }
        }
        return 0;
    }

    void processMessage(CMessageBuffer &mb)
    {
        ReadLockBlock block(processlock);
        if (stopped)
            return;
        ICoven &coven=queryCoven();
        int fn;
        mb.read(fn);
        SubscriptionId sid;
        unsigned subtag;
        ISubscriptionManager *manager;
        switch (fn) {
        case MSR_ADD_SUBSCRIPTION_PRIMARY:
        case MSR_ADD_SUBSCRIPTION_SECONDARY:
            {
                Owned<IException> exception;
                Owned<CSubscriptionStub> sub;
                try
                {
                    SubscriptionId sid;
                    mb.read(subtag).read(sid);
                    Owned<INode> subscriber = deserializeINode(mb);
                    size32_t dsize;
                    mb.read(dsize);
                    sub.setown(new CSubscriptionStub(subtag,sid,dsize,mb.readDirect(dsize),subscriber));
                    mb.clear();
                    {
                        CHECKEDCRITICALBLOCK(stubsect,60000);
                        removeAborted();
                    }
                    manager = queryManager(subtag);
                    if (manager) {
                        if (fn==MSR_ADD_SUBSCRIPTION_PRIMARY) {
                            rank_t n = coven.queryGroup().ordinality();
                            rank_t mr = coven.queryGroup().rank();
                            for (rank_t r = 0;r<n;r++) {
                                if (r!=mr) {
                                    int fn = MSR_ADD_SUBSCRIPTION_SECONDARY;
                                    mb.clear().append(fn).append(subtag).append(sid);
                                    subscriber->serialize(mb);
                                    mb.append(dsize).append(dsize,sub->queryData().get());
                                    coven.sendRecv(mb,r,MPTAG_DALI_SUBSCRIPTION_REQUEST);
                                    // should check for server failure here
                                }
                            }
                        }
                        manager->add(sub.getLink(),sid);
                    }
                }
                catch (IException *e) {
                    exception.setown(e);
                    sub.clear();
                }
                unsigned retry=0;
                if (exception)
                    serializeException(exception, mb);
                while (!coven.reply(mb,60000)) {
                        StringBuffer eps;
                        DBGLOG("MSR_ADD_SUBSCRIPTION_PRIMARY reply timed out to %s try %d",mb.getSender().getUrlStr(eps).str(),retry+1);
                        if (retry++==3)
                            return;
                }
                if (sub)
                {
                    CHECKEDCRITICALBLOCK(stubsect,60000);
                    stubs.append(*sub);
                }
            }
            break;
        case MSR_REMOVE_SUBSCRIPTION_PRIMARY:
        case MSR_REMOVE_SUBSCRIPTION_SECONDARY:
            {
                unsigned tstart = msTick();
                {
                    CHECKEDCRITICALBLOCK(stubsect,60000);
                    removeAborted();
                    mb.read(subtag);
                    mb.read(sid);
                    manager = queryManager(subtag);
                    if (manager) {
                        if (fn==MSR_REMOVE_SUBSCRIPTION_PRIMARY) {
                            rank_t n = coven.queryGroup().ordinality();
                            rank_t mr = coven.queryGroup().rank();
                            for (rank_t r = 0;r<n;r++) {
                                if (r!=mr) {
                                    mb.clear().append(MSR_REMOVE_SUBSCRIPTION_SECONDARY).append(subtag).append(sid);
                                    coven.sendRecv(mb,r,MPTAG_DALI_SUBSCRIPTION_REQUEST);
                                    // should check for server failure here
                                }
                            }
                        }
                        manager->remove(sid);
                    }
                    mb.clear();
                }
                coven.reply(mb);
                unsigned telapsed=msTick()-tstart;
                if (telapsed>1000)
                    LOG(MCerror, unknownJob, "MSR_REMOVE_SUBSCRIPTION_PRIMARY.1 took %dms",telapsed);
            }
            break;
        }
    }   


    void nodeDown(rank_t rank)
    {
        assertex(!"TBD");
    }

    ISubscriptionManager *queryManager(unsigned tag)
    {
        CHECKEDCRITICALBLOCK(tagsect,60000);
        unsigned i = tags.find(tag);
        if (i==NotFound)
            return NULL;
        return &managers.item(i);
    }


    void registerSubscriptionManager(unsigned tag, ISubscriptionManager *manager)
    {
        CHECKEDCRITICALBLOCK(tagsect,60000);
        tags.append(tag);
        manager->Link();
        managers.append(*manager);
    }

    void unlink(CSubscriptionStub *stub)
    {
        unsigned tstart = msTick();
        {
            CHECKEDCRITICALBLOCK(stubsect,60000);
            stubs.zap(*stub);
        }
        unsigned telapsed=msTick()-tstart;
        if (telapsed>1000)
            LOG(MCerror, unknownJob, "CDaliPublisherServer::unlink took %dms",telapsed);
    }
    
    void onClose(SocketEndpoint &ep)
    {
        // mark stub closed
        unsigned tstart = msTick();
        {
            CHECKEDCRITICALBLOCK(stubsect,60000);
            ForEachItemIn(i, stubs)
            {
                CSubscriptionStub &stub = stubs.item(i);
                if (stub.queryNode().endpoint().equals(ep)) {
                    stub.abort();
                }
            }
            unsigned telapsed=msTick()-tstart;
            if (telapsed>1000)
                LOG(MCerror, unknownJob, "CDaliPublisherServer::onClose took %dms",telapsed);
        }
    }

    void removeAborted()
    {
#ifdef SUPRESS_REMOVE_ABORTED
        return;
#endif
        // called from critical section
        CIArrayOf<CSubscriptionStub> toremove;
        ForEachItemIn(i, stubs)
        {
            CSubscriptionStub &stub = stubs.item(i);
            if (stub.aborted()) {
                stub.Link();
                toremove.append(stub);
            }
        }
        if (toremove.ordinality()) {
            CHECKEDCRITICALUNBLOCK(stubsect,60000);
            ForEachItemIn(i2, toremove) {
                CSubscriptionStub &stub = toremove.item(i2);
                queryManager(stub.queryTag())->remove(stub.querySubscriptionId());
            }
        }
    }

    StringBuffer &getSubscriptionList(StringBuffer &buf)
    {
        unsigned tstart = msTick();
        {
            CHECKEDCRITICALBLOCK(stubsect,60000);
            ForEachItemIn(i, stubs)
            {
                CSubscriptionStub &stub = stubs.item(i);
                stub.getDetails(buf).append('\n');
            }
        }
        unsigned telapsed=msTick()-tstart;
        if (telapsed>1000)
            LOG(MCerror, unknownJob, "CDaliPublisherServer::getSubscriptionList took %dms",telapsed);
        return buf;
    }

} *daliPublisherServer = NULL;


StringBuffer &getSubscriptionList(StringBuffer &buf)
{
    if (daliPublisherServer)
        daliPublisherServer->getSubscriptionList(buf);
    return buf;
}


void CSubscriptionStub::unlink()
{
    if (daliPublisherServer)
        daliPublisherServer->unlink(this);
}



class CDaliSubscriptionManagerStub: public CInterface, implements ISubscriptionManager
{
    // Client side
    unsigned tag;
    IArrayOf<ISubscription> subscriptions;
    Int64Array ids;
    CriticalSection subscriptionsect;
public:
    IMPLEMENT_IINTERFACE;
    CDaliSubscriptionManagerStub(unsigned _tag)
    {
        tag = _tag;
    }
    ~CDaliSubscriptionManagerStub()
    {
        subscriptions.kill();
    }
    void add(ISubscription *subs,SubscriptionId id)
    {
        {
            CriticalBlock block(subscriptionsect);
            ids.append(id);
            subscriptions.append(*subs);
        }
        int fn = MSR_ADD_SUBSCRIPTION_PRIMARY;
        CMessageBuffer mb;
        mb.append(fn).append(tag).append(id);
        queryMyNode()->serialize(mb);
        const MemoryAttr &data = subs->queryData();
        size32_t dlen = (size32_t)data.length();
        mb.append(dlen);
        mb.append(dlen,data.get());
        try
        {
            queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SUBSCRIPTION_REQUEST);
            if (mb.length())
                throw deserializeException(mb);
        }
        catch (IException *e)
        {
            PrintExceptionLog(e,"Dali CDaliSubscriptionManagerStub::add");
            {
                CriticalBlock block(subscriptionsect);
                unsigned idx = ids.find(id);
                if (NotFound != idx)
                {
                    ids.remove(idx);
                    subscriptions.remove(idx);
                }
            }
            throw;
        }
    }

    void remove(SubscriptionId id)
    {
        CriticalBlock block(subscriptionsect);
        unsigned idx = ids.find(id);
        if (idx == NotFound)
            return;
        int fn = MSR_REMOVE_SUBSCRIPTION_PRIMARY;
        CMessageBuffer mb;
        mb.append(fn);
        mb.append(tag);
        mb.append(id);
        try {
            queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_SUBSCRIPTION_REQUEST);
        }
        catch (IDaliClient_Exception *e) {
            PrintExceptionLog(e,"Dali CDaliSubscriptionManagerStub::remove");
            e->Release();
        }
        subscriptions.remove(idx);
        ids.remove(idx);
    }
    void notify(SubscriptionId id,MemoryBuffer &mb)
    {
        Linked<ISubscription> item;
        {
            CriticalBlock block(subscriptionsect);
            unsigned i = ids.find(id);
            if (i == NotFound) 
                return;
            item.set(&subscriptions.item(i));
        }
        item->notify(mb);
    }
    void abort()
    {
        PrintLog("CDaliSubscriptionManagerStub aborting");
        CriticalBlock block(subscriptionsect);
        ForEachItemIn(i,subscriptions) {
            subscriptions.item(i).abort();
        }
        subscriptions.kill();
        ids.kill();
        PrintLog("CDaliSubscriptionManagerStub aborted");
    }
};




class CDaliPublisherClient: public Thread, public CDaliPublisher
{

    CIArrayOf<CDaliSubscriptionManagerStub> managers;
    UnsignedArray tags;
    CheckedCriticalSection tagsect;
    bool stopped;


public:

    CDaliPublisherClient()
        :   Thread("CDaliPublisherClient")
    {
        stopped = true;
        start();
    }

    ~CDaliPublisherClient()
    {
        managers.kill();
    }

    ISubscriptionManager *queryManager(unsigned tag)
    {
        CHECKEDCRITICALBLOCK(tagsect,60000);
        unsigned i = tags.find(tag);
        if (i!=NotFound) 
            return &managers.item(i);
        CDaliSubscriptionManagerStub *stub = new CDaliSubscriptionManagerStub(tag);
        tags.append(tag);
        managers.append(*stub);
        return stub;
    }

    int run()
    {
        ICoven &coven=queryCoven();
        CMessageHandler<CDaliPublisherClient> handler("CDaliPublisherClientMessages",this,&CDaliPublisherClient::processMessage);
        stopped = false;
        CMessageBuffer mb;
        stopped = false;
        while (!stopped) {
            mb.clear();
            try {
#ifdef TRACE_QWAITING
                unsigned waiting = coven.probe(RANK_ALL,MPTAG_DALI_SUBSCRIPTION_FULFILL,NULL);
                if ((waiting!=0)&&(waiting%10==0))
                    DBGLOG("QPROBE: MPTAG_DALI_SUBSCRIPTION_REQUEST has %d waiting",waiting);
    #endif
                if (coven.recv(mb,RANK_ALL,MPTAG_DALI_SUBSCRIPTION_FULFILL,NULL))
                    handler.handleMessage(mb);
                else
                    stopped = true;
            }
            catch (IException *e) {
                EXCLOG(e,"CDaliPublisherClient::run");
                e->Release();
                stopped = true;
            }
        }
        return 0;
    }

    void processMessage(CMessageBuffer &mb)
    {
        //ICoven &coven=queryCoven();
        //ICommunicator &comm=coven.queryComm();
        unsigned tag;
        mb.read(tag);
        SubscriptionId id;
        mb.read(id);
        unsigned i = tags.find(tag);
        if (i!=NotFound) {      
            MemoryBuffer qb;
            size32_t dlen;
            mb.read(dlen);
            qb.append(dlen,mb.readDirect(dlen)); // this is bit inefficient - perhaps could be improved
            managers.item(i).notify(id,qb);
        }
    }

    void ready()
    {
    }


    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryCoven().cancel(RANK_ALL,MPTAG_DALI_SUBSCRIPTION_FULFILL);
        }
        join();
    }

};


IDaliServer *createDaliPublisherServer()
{
    assertex(!daliPublisherServer); // initialization problem
    daliPublisherServer = new CDaliPublisherServer();
    DaliPublisher = daliPublisherServer;
    return daliPublisherServer;
}

static CriticalSection subscriptionCrit;


ISubscriptionManager *querySubscriptionManager(unsigned tag)
{
    CriticalBlock block(subscriptionCrit);
    if (!DaliPublisher) {
        ICoven &coven=queryCoven();
        assertex(!coven.inCoven()); // Check not Coven server (if occurs - not initialized correctly;
        DaliPublisher = new CDaliPublisherClient();
    }
    return DaliPublisher->queryManager(tag);
}

void closeSubscriptionManager()
{
    CriticalBlock block(subscriptionCrit);
    if (DaliPublisher) {
        try {
            DaliPublisher->stop();
        }
        catch (IMP_Exception *e)
        {
            if (e->errorCode()!=MPERR_link_closed)
                throw;
            e->Release();
        }
        catch (IDaliClient_Exception *e) {
            if (e->errorCode()!=DCERR_server_closed)
                throw;
            e->Release();
        }
        delete DaliPublisher;
        DaliPublisher = NULL;
    }
}


void registerSubscriptionManager(unsigned tag, ISubscriptionManager *manager)
{
    assertex(daliPublisherServer); // initialization order check
    daliPublisherServer->registerSubscriptionManager(tag,manager);
}
