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
#include "jlib.hpp"
#include "jsuperhash.hpp"
#include "jmisc.hpp"

static CBuildVersion _bv("$Name$ $Id: dalock.cpp 62376 2011-02-04 21:59:58Z sort $");

#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"
#include "dacoven.hpp"
#include "daserver.hpp"

#include "dalock.hpp"

interface IDistributedLockManager
{
    virtual ~IDistributedLockManager() { }
    virtual DistributedLockId createDistributedLockId()=0;
    virtual void releaseDistributedLockId(DistributedLockId id)=0;
    virtual bool localLock(DistributedLockId id,SessionId owner,bool exclusive=true,long timeout=-1) { return false; }
    virtual void localUnlock(DistributedLockId id,SessionId owner) {}
    virtual bool lock(DistributedLockId id,SessionId owner,bool exclusive=true,long timeout=-1) = 0;
    virtual void unlock(DistributedLockId id,SessionId owner) = 0;
    virtual void start() {} ;
    virtual void stop() {} ;
};

static IDistributedLockManager *DistributedLockManager=NULL;

#define LOCKREPLYTIMEOUT (3*60*1000)




class CLockState: public CInterface
{
public:
    Int64Array owners;
    Semaphore sem;
    CriticalSection sect;
    unsigned short waiting;
    unsigned exclusivenest;
    DistributedLockId id;

    CLockState(DistributedLockId _id)
    {
        id = _id;
        waiting = 0;
        exclusivenest = 0;
    }

    bool lock(SessionId owner,bool exclusive, unsigned timeout)
    {
        CTimeMon tm(timeout);
        sect.enter();
        loop {
            unsigned num = owners.ordinality();
            if (exclusive) {
                if (num==0) {
                    owners.append(owner);
                    exclusivenest = 1;
                    break;
                }
                else if (exclusivenest && (owners.item(0)==owner)) {
                    exclusivenest++;
                    break;
                }
            }
            else if (!exclusivenest) {
                owners.append(owner);
                break;
            }
            waiting++;
            sect.leave();
            unsigned remaining;
            if (tm.timedout(&remaining)||!sem.wait(remaining)) {
                sect.enter();
                if (!sem.wait(0)) {
                    waiting--;
                    sect.leave();
                    return false;
                }
            }
            else
                sect.enter();
        }
        sect.leave();
        return true;
    }

    void unlock(SessionId owner)
    {
        sect.enter();
        if (exclusivenest) {
            exclusivenest--;
            if (exclusivenest) { // still locked
                assertex(owners.item(0)==owner);
                sect.leave();
                return;
            }
        }
        verifyex(owners.zap(owner));
        if (owners.ordinality()==0) {
            exclusivenest = 0;
            if (waiting) {
                sem.signal(waiting);
                waiting = 0;
            }
        }
        else {
            assertex(!exclusivenest);
        }
        sect.leave();
    }
};

class CLockStateTable: private SuperHashTableOf<CLockState,DistributedLockId>
{
    CriticalSection sect;


    void onAdd(void *)
    {
        // not used
    }

    void onRemove(void *e)
    {
        CLockState &elem=*(CLockState *)e;      
        elem.Release();
    }

    unsigned getHashFromElement(const void *e) const
    {
        const CLockState &elem=*(const CLockState *)e;      
        DistributedLockId id=elem.id;
        return low(id)^(unsigned)high(id);
    }

    unsigned getHashFromFindParam(const void *fp) const
    {
        DistributedLockId id = *(const DistributedLockId *)fp;
        return low(id)^(unsigned)high(id);
    }

    const void * getFindParam(const void *p) const
    {
        const CLockState &elem=*(const CLockState *)p;      
        return &elem.id;
    }

    bool matchesFindParam(const void * et, const void *fp, unsigned) const
    {
        return ((CLockState *)et)->id==*(DistributedLockId *)fp;
    }

    IMPLEMENT_SUPERHASHTABLEOF_REF_FIND(CLockState,DistributedLockId);

public: 
    CLockStateTable()
    {
    }

    ~CLockStateTable()
    {
        releaseAll();
    }

    bool lock(DistributedLockId id,SessionId owner,bool excl,unsigned timeout)
    {
        CLockState * s;
        {
            CriticalBlock block(sect);
            s = find(id);
            if (!s) {
                s = new CLockState(id);
                add(*s);
            }           
        }
        return s->lock(owner,excl,timeout);
    }
    void unlock(DistributedLockId id,SessionId owner)
    {
        CLockState * s;
        {
            CriticalBlock block(sect);
            s = find(id);
            assertex(s);
        }
        s->unlock(owner);
    }
};



enum MLockRequestKind { 
    MLR_ALLOC_LOCK_ID, 
    MLR_FREE_LOCK_ID,
    MLR_PRIMARY_LOCK_REQUEST,
    MLR_SECONDARY_LOCK_REQUEST,
    MLR_PRIMARY_UNLOCK_REQUEST,
    MLR_SECONDARY_UNLOCK_REQUEST,
    MLR_EXIT // TBD
};



class CLockRequestServer: public Thread
{
    bool stopped;
    IDistributedLockManager &manager;
public:
    CLockRequestServer(IDistributedLockManager &_manager) 
        : Thread("Lock Manager, CLockRequestServer"), manager(_manager)
    {
        stopped = true;
    }

    int run()
    {
        ICoven &coven=queryDefaultDali()->queryCoven();
        ICommunicator &comm=coven.queryComm();
        CMessageHandler<CLockRequestServer> handler("CLockRequestServer",this,&CLockRequestServer::processMessage);
        stopped = false;
        CMessageBuffer mb;
        while (!stopped) {
            try {
                mb.clear();
                if (comm.recv(mb,RANK_ALL,MPTAG_DALI_LOCK_REQUEST,NULL)) {
                    handler.handleMessage(mb);
                }
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CLockRequestServer");
                e->Release();
            }
        }
        return 0;
    }

    void processMessage(CMessageBuffer &mb)
    {
        ICoven &coven=queryDefaultDali()->queryCoven();
        ICommunicator &comm=coven.queryComm();

        DistributedLockId id;
        SessionId session;
        bool exclusive;
        long timeout;
        int fn;
        mb.read(fn);
        switch (fn) {
        case MLR_ALLOC_LOCK_ID: {
                id = manager.createDistributedLockId();
                mb.clear().append(id);
                comm.reply(mb);
            }
            break;
        case MLR_FREE_LOCK_ID: {
                mb.read(id);
                manager.releaseDistributedLockId(id);
            }
            break;
        case MLR_PRIMARY_LOCK_REQUEST: {
                mb.read(id).read(session).read(exclusive).read(timeout);
                bool ret = manager.lock(id,session,exclusive,timeout);
                mb.clear().append(ret);
                comm.reply(mb);
            }
            break;
        case MLR_PRIMARY_UNLOCK_REQUEST: {
                mb.read(id).read(session);
                manager.unlock(id,session);
                mb.clear();
                comm.reply(mb);
            }
            break;
        case MLR_SECONDARY_LOCK_REQUEST: {
                mb.read(id).read(session).read(exclusive).read(timeout);
                bool ret = manager.localLock(id,session,exclusive,timeout);
                mb.clear().append(ret);
                comm.reply(mb);
            }
            break;
        case MLR_SECONDARY_UNLOCK_REQUEST: {
                mb.read(id).read(session);
                manager.localUnlock(id,session);
                mb.clear();
                comm.reply(mb);
            }
            break;
        }
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            queryDefaultDali()->queryCoven().queryComm().cancel(RANK_ALL, MPTAG_DALI_LOCK_REQUEST);
        }
        join();
    }
};



class CClientDistributedLockManager: implements IDistributedLockManager
{
public:
    CClientDistributedLockManager()
    {
    }
    ~CClientDistributedLockManager()
    {
    }

    DistributedLockId createDistributedLockId()
    {
        CMessageBuffer mb;
        mb.append((int)MLR_ALLOC_LOCK_ID);
        queryDefaultDali()->queryCoven().sendRecv(mb,RANK_RANDOM,MPTAG_DALI_LOCK_REQUEST);
        DistributedLockId ret;
        mb.read(ret);
        return ret;
    }

    void releaseDistributedLockId(DistributedLockId id)
    {
        // maybe some checking here?
        CMessageBuffer mb;
        mb.append((int)MLR_FREE_LOCK_ID).append(id);
        try {
            ICoven &coven=queryDefaultDali()->queryCoven();
            coven.queryComm().send(mb,coven.chooseServer(id),MPTAG_DALI_LOCK_REQUEST,MP_ASYNC_SEND);
        }
        catch (IMP_Exception *e) // ignore if fails
        {
            if (e->errorCode()!=MPERR_link_closed)
                throw;
            EXCLOG(e,"releaseDistributedLockId");
            e->Release();
        }
    }

    bool lock(DistributedLockId id,SessionId owner,bool exclusive=true,long timeout=-1)
    {
        CMessageBuffer mb;
        mb.append((int)MLR_PRIMARY_LOCK_REQUEST).append(id).append(owner).append(exclusive).append(timeout);
        ICoven &coven=queryDefaultDali()->queryCoven();
        coven.sendRecv(mb,coven.chooseServer(id),MPTAG_DALI_LOCK_REQUEST);
        bool ret;
        mb.read(ret);
        return ret;
    }

    void unlock(DistributedLockId id,SessionId owner)
    {
        CMessageBuffer mb;
        mb.append((int)MLR_PRIMARY_UNLOCK_REQUEST).append(id).append(owner);
        ICoven &coven=queryDefaultDali()->queryCoven();
        coven.sendRecv(mb,coven.chooseServer(id),MPTAG_DALI_LOCK_REQUEST);
    }



};



#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable : 4355) // 'this' : used in base member initializer list
#endif

class CCovenDistributedLockManager: implements IDistributedLockManager
{

    DistributedLockId   nextLockId;
    CLockRequestServer  lockrequestserver;
    CLockStateTable     lockstates;
    ICoven              &coven;

public:
    CCovenDistributedLockManager(ICoven &_coven)
        : coven(_coven), lockrequestserver(*this)
    {
        nextLockId = 0;
    }
    ~CCovenDistributedLockManager()
    {
    }

    DistributedLockId createDistributedLockId()
    {
        return coven.getUniqueId();
    }

    void releaseDistributedLockId(DistributedLockId id)
    {
        // should remove lock info etc TBD
#if 0  // TBD
        CovenServerId dst = coven.originUniqueId(id);
        if (dst==myid) { // my lock
            lockallocator.freeid(id);
        }
        else {
            CMessageBuffer mb;
            mb.append((int)MLR_FREE_LOCK_ID).append(id);
            queryDefaultDali()->queryCoven().send(mb,coven.getServerRank(dst),MPTAG_DALI_LOCK_REQUEST,MP_ASYNC_SEND);
        }
#endif
    }

    bool remoteLock(rank_t dst,DistributedLockId id,SessionId owner,bool exclusive=true,long timeout=-1)
    {
        CMessageBuffer mb;
        mb.append((int)MLR_SECONDARY_LOCK_REQUEST).append(id).append(owner).append(exclusive).append(timeout);
        queryDefaultDali()->queryCoven().sendRecv(mb,dst,MPTAG_DALI_LOCK_REQUEST);
        bool ret;
        mb.read(ret);
        return ret;
    }

    void remoteUnlock(rank_t dst,DistributedLockId id,SessionId owner)
    {
        CMessageBuffer mb;
        mb.append((int)MLR_SECONDARY_UNLOCK_REQUEST).append(id).append(owner);
        queryDefaultDali()->queryCoven().sendRecv(mb,dst,MPTAG_DALI_LOCK_REQUEST);
    }

    bool localLock(DistributedLockId id,SessionId owner,bool exclusive=true,long timeout=-1)
    {
        return lockstates.lock(id,owner,exclusive,timeout);
    }

            
    void localUnlock(DistributedLockId id,SessionId owner)
    {
        lockstates.unlock(id,owner);
    }


    bool lock(DistributedLockId id,SessionId owner,bool exclusive=true,long timeout=-1)
    {
        rank_t myrank = coven.getServerRank();
        rank_t ownerrank = coven.chooseServer(id);
        // first do owner
        if (myrank==ownerrank) {
            if (!localLock(id,owner,exclusive,timeout))
                return false;
        }
        else if (!remoteLock(ownerrank,id,owner,exclusive,timeout))
            return false;
        // all others should succeed quickly
        IGroup &grp = queryDefaultDali()->queryCoven().queryComm().queryGroup();
        ForEachOtherNodeInGroup(r,grp) {
            if (r!=ownerrank)
                remoteLock(r,id,owner,exclusive);
        }
        return true;
    }

            
    void unlock(DistributedLockId id,SessionId owner)
    {
        rank_t myrank = coven.getServerRank();
        rank_t ownerrank = coven.chooseServer(id);
        // first do owner
        if (myrank==ownerrank) {
            localUnlock(id,owner);
        }
        else 
            remoteUnlock(ownerrank,id,owner);
        // all others should succeed quickly
        ForEachOtherNodeInGroup(r,coven.queryComm().queryGroup()) {
            if (r!=ownerrank)
                remoteUnlock(r,id,owner);
        }
    }

    void start()
    {
        lockrequestserver.start();
    }

    void stop()
    {
        lockrequestserver.stop();
    }


};


#ifdef _MSC_VER
#pragma warning (pop) // warning 4355
#endif

IDistributedLockManager &queryDistributedLockManager()
{
    if (!DistributedLockManager) {
        assertex(!queryDefaultDali()->queryCoven().inCoven()); // Check not Coven server (if occurs - not initialized correctly;
        DistributedLockManager = new CClientDistributedLockManager();
    }
    return *DistributedLockManager;
}


DistributedLockId createDistributedLockId()
{
    return queryDistributedLockManager().createDistributedLockId();
}

void releaseDistributedLockId(DistributedLockId id)
{
    queryDistributedLockManager().releaseDistributedLockId(id);
}

DistributedLockId lookupDistributedLockId(const char *name)
{
    assertex(!"TBD");
    return 0;
}


class CDistributedLock: public CInterface, implements IDistributedLock
{
    DistributedLockId id;
    SessionId session;
public:
    IMPLEMENT_IINTERFACE;
    CDistributedLock(DistributedLockId _id, SessionId _session)
    {
        id = _id;
        session = _session;
    }

    bool lock(bool exclusive=true,long timeout=-1)
    {
        return queryDistributedLockManager().lock(id,session,exclusive,timeout);
    }
    void unlock()
    {
        queryDistributedLockManager().unlock(id,session);
    }
    bool relock(bool exclusive=true,long timeout=-1)
    {
        assertex(!"TBD");
        return false; // TBD
    }
    DistributedLockId getID()
    {
        return id;
    }
    SessionId getSession()
    {
        return session;
    }
};

IDistributedLock *createDistributedLock(DistributedLockId id, SessionId session)
{
    return new CDistributedLock(id,(session==0)?myProcessSession():session);
}


class CDaliLockServer: public CInterface, public IDaliServer
{
public:
    IMPLEMENT_IINTERFACE;


    void start()
    {
        ICoven  &coven=queryDefaultDali()->queryCoven();
        assertex(coven.inCoven()); // must be member of coven
        DistributedLockManager = new CCovenDistributedLockManager(coven);
        DistributedLockManager->start();
    }

    void ready()
    {
    }

    void suspend()
    {
    }

    void stop()
    {
        DistributedLockManager->stop();
        delete DistributedLockManager;
        DistributedLockManager = NULL;
    }

    void nodeDown(rank_t rank)
    {
        assertex("TBD");
    }

};

IDaliServer *createDaliLockServer()
{
    return new CDaliLockServer();
}




