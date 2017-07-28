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
#include <time.h>
#include "jlib.hpp"
#include "jexcept.hpp"
#include "jptree.hpp"
#include "jfile.hpp"
#include "jexcept.hpp"
#include "mputil.hpp"
#include "jmisc.hpp"
#include "daclient.hpp"
#include "daserver.hpp"
#include "dacoven.hpp"
#include "mpcomm.hpp"

extern void closedownDFS();

#define VERSION_REQUEST_TIMEOUT 30000
// Unique IDs are allocated in blocks of 64K in clients and blocks of 2^32 in server
// base is saved in store whenever block exhausted, so replacement coven servers can restart 

// server side versioning.
#define ServerVersion    "3.14"
#define MinClientVersion "1.5"


// client side default versioning.
static StringAttr ClientVersion("3.6");
static StringAttr MinServerVersion("3.1");      // when this upped check initClientProcess instances
static CDaliVersion _ServerVersion;


#define COVEN_SERVER_TIMEOUT (1000*120)

enum MCovenRequestKind { 
    MCR_UPDATE_DATA_STORE, 
    MCR_ALLOC_UNIQUE_IDS,
    MCR_GET_SERVER_ID,
    MCR_GET_COVEN_ID,
    MCR_GET_VERSION_INFO,
    MCR_EXIT = -1       // fixed value
};



#define UUID_BLOCK_SIZE_CLIENT 0x10000
#define UUID_BLOCK_SIZE_FOREIGN 0x1000000
#define UUID_BLOCK_SIZE_SERVER 0xffffffff


void CDaliVersion::set(const char *s)
{
    const char *dot = strchr(s, '.');
    StringAttr _major(s, dot-s);
    major = atoi(_major);
    minor = atoi(dot+1);
}

int CDaliVersion::compare(const CDaliVersion &other) const
{
    int d = major-other.queryMajor();
    if (d) return d;
    d = minor-other.queryMinor();
    return d;
}

int CDaliVersion::compare(const char *other) const
{
    CDaliVersion _other(other);
    return compare(_other);
}


StringBuffer &CDaliVersion::toString(StringBuffer &str) const
{
    str.append(major).append('.').append(minor);
    return str;
}

class CDaliUidAllocator: public CInterface
{
    __uint64        uidnext;
    unsigned        uidsremaining;
    SocketEndpoint  node;
    unsigned        banksize;
public:
    CriticalSection crit;

    CDaliUidAllocator(const SocketEndpoint &_node)
    {
        node = _node;
        uidsremaining = 0;
        uidnext = 0;
        banksize = 1024;
    }

    bool allocUIDs(DALI_UID &uid,unsigned num)
    {
        // called in crit
        if (uidsremaining<num) 
            return false;
        uid = (DALI_UID)uidnext; 
        uidnext += num;
        uidsremaining -= num;
        return true;
    }

    void addUIDs(__uint64 uids,unsigned num)
    {
        // called in crit
        if (uidnext<uids) {
            if (uidnext+uidsremaining==uids)
                uidsremaining += num;
            else {
                uidnext = uids;
                uidsremaining = num;
            }
        }
        else if (uids+num==uidnext) {   
            uidsremaining += num;
            uidnext = uids;
        }
    }


    static CDaliUidAllocator &find(CIArrayOf<CDaliUidAllocator> &uidallocators,const SocketEndpoint &foreignnode)
    {
        // called in crit
        ForEachItemIn(i,uidallocators) {
            if (uidallocators.item(i).node.equals(foreignnode)) 
                return uidallocators.item(i);
        }
        CDaliUidAllocator &ret = *new CDaliUidAllocator(foreignnode);
        uidallocators.append(ret);
        if (uidallocators.ordinality()>1) {
            StringBuffer eps;
            PROGLOG("Added foreign UID allocator for %s",ret.node.getUrlStr(eps).str());
        }
        return ret;
    }

    unsigned getBankSize()
    {
        // only used client side
        // called in crit
        unsigned ret = banksize;
        if (banksize<UUID_BLOCK_SIZE_CLIENT)
            banksize *= 2;
        return ret;
    }

};


class CCovenBase: implements ICoven, public CInterface
{
    unsigned        ord;
    Int64Array      serverIDs;
    DALI_UID        covenid;

protected: 
    ICommunicator  *comm;

public:
    static CriticalSection uidcrit;
    static CIArrayOf<CDaliUidAllocator> uidallocators;
    
    IMPLEMENT_IINTERFACE;
    CCovenBase(IGroup *grp,bool server)
    {
        comm = createCommunicator(grp,server); // if server outer == true as can recieve from outside
        ord = grp->ordinality();
        for (unsigned i=0;i<ord;i++)
            serverIDs.append(0);
        covenid = 0;

        if (server)
        {
            _ServerVersion.set(ServerVersion);
            LOG(MCdebugInfo(100), unknownJob, "Server Version = %s, required minimum client version %s", ServerVersion, MinClientVersion);
        }
        else
        {
            CMessageBuffer mb;
            mb.append(MCR_GET_VERSION_INFO);
            mb.append(ClientVersion);
            mb.append(MinServerVersion);
            if (!comm->sendRecv(mb, RANK_RANDOM, MPTAG_DALI_COVEN_REQUEST, VERSION_REQUEST_TIMEOUT))
                throw MakeStringException(-1, "failed retrieving version information from server, legacy server?");
            if (!mb.length())
                throw MakeStringException(-1, "Failed to receive server information (probably communicating to legacy server)");
            StringAttr serverVersion, minClientVersion;
            mb.read(serverVersion);
            _ServerVersion.set(serverVersion), 
            mb.read(minClientVersion);

            CDaliVersion clientV(ClientVersion), minClientV(minClientVersion);
            if (clientV.compare(minClientV) < 0)
            {
                StringBuffer s("Client version ");
                s.append(ClientVersion).append(", server requires minimum client version ").append(minClientVersion);
                throw createClientException(DCERR_version_incompatibility, s.str());
            }
            CDaliVersion minServerV(MinServerVersion);
            if (_ServerVersion.compare(minServerV) < 0)
            {
                StringBuffer s("Server version ");
                s.append(serverVersion).append(", client requires minimum server version ").append(MinServerVersion);
                throw createClientException(DCERR_version_incompatibility, s.str());
            }
        }
    }
    ~CCovenBase()
    {
        comm->Release();
    }

    unsigned size() 
    { 
        return ord; 
    }



    rank_t chooseServer(DALI_UID uid, int tag=0)
    {
        rank_t r = hashc((const unsigned char *)&uid,sizeof(uid),tag)%ord;
        return r; // more TBD if coven server down
    }

    virtual DALI_UID getServerId(rank_t server)
    {
        return serverIDs.item(server);
    }

    void setServerId(rank_t server,DALI_UID id)
    {
        serverIDs.replace(id,server);
    }

    DALI_UID getCovenId()
    {
        return covenid;
    }

    void setCovenId(DALI_UID id)
    {
        covenid = id;
    }

    rank_t getServerRank(DALI_UID id)
    {
        for (rank_t r=0;r<size();r++)
            if (getServerId(r)==id)
                return r;
        return 0;
    }

    virtual rank_t  getServerRank()
    {
        return 0;
    }

    bool inCoven(INode *node)
    {
        ICoven &coven=queryCoven();
        return (coven.queryGroup().rank(node)!=RANK_NULL);
    }

    bool inCoven(SocketEndpoint &ep)
    {
        ICoven &coven=queryCoven();
        return (coven.queryGroup().rank(ep)!=RANK_NULL);
    }

    virtual void barrier(void)
    {
        assertex(comm);
        return comm->barrier();
    }

    virtual bool verifyConnection(rank_t rank, unsigned timeout=1000*60*5)
    {
        assertex(comm);
        return comm->verifyConnection(rank,timeout);
    }


    virtual bool verifyAll(bool duplex=false, unsigned timeout=1000*60*30)
    {
        assertex(comm);
        return comm->verifyAll(duplex,timeout);
    }
    
    // receive, returns senders rank or false if no message available in time given or cancel called

    virtual IGroup &queryGroup()
    {
        assertex(comm);
        return comm->queryGroup();
    }
    virtual IGroup *getGroup()
    {
        assertex(comm);
        return comm->getGroup();
    }

    virtual void flush  (mptag_t tag)
    {
        if (comm)
            comm->flush(tag);
    }

    virtual void cancel  (rank_t srcrank, mptag_t tag)
    {
        if (comm)
            comm->cancel(srcrank,tag);
    }
};


CriticalSection CCovenBase::uidcrit;
CIArrayOf<CDaliUidAllocator> CCovenBase::uidallocators;



class CCovenServer: public CCovenBase
{
    rank_t          myrank;
    DALI_UID        uidnext;
    bool            stopped;
    StringAttr      storename;
    StringAttr      backupname;
    CriticalSection updatestorecrit;
    Semaphore       done;

    IPropertyTree   *store;

protected:

    void processMessage(CMessageBuffer &mb) 
    {
        ICoven &coven=queryCoven();
        int fn;
        mb.read(fn);
        switch (fn) {
        case MCR_UPDATE_DATA_STORE: {
                CriticalBlock block(updatestorecrit);
                mergeStore(store,mb,false);
                writeStore(store);
                mb.clear();
                coven.reply(mb);
            }
            break;
        case MCR_ALLOC_UNIQUE_IDS: {
                unsigned num;
                mb.read(num);
                try {
                    SocketEndpoint foreignnode;
                    DALI_UID uid;
                    if (mb.remaining()) {
                        foreignnode.deserialize(mb);
                        uid=getUniqueIds(num,&foreignnode);
                    }
                    else
                        uid=getUniqueIds(num,NULL);
                    mb.clear().append(uid);
                }
                catch (IException *e) {
                    DALI_UID uid = 0;
                    serializeException(e,mb.clear().append(uid));
                }
                coven.reply(mb);
            }
            break;
        case MCR_GET_SERVER_ID: {
                rank_t r;
                mb.read(r);
                DALI_UID uid=getServerId(r);
                mb.clear().append(uid);
                coven.reply(mb);
            }
            break;
        case MCR_GET_COVEN_ID: {
                DALI_UID uid=getCovenId();
                mb.clear().append(uid);
                coven.reply(mb);
            }
            break;
        case MCR_GET_VERSION_INFO: {
                mb.clear().append(ServerVersion).append(MinClientVersion);
                coven.reply(mb);
            }
            break;
        case MCR_EXIT: 
            Sleep(10);
            PROGLOG("Stop request received");
            stop();
            break;
        default:
            ERRLOG("Unknown coven command request"); //MORE: I think this should be a user error?
            mb.clear();
            coven.reply(mb);
            break;
        }
    }


public:

    CCovenServer(IGroup *grp,const char *_storename, const char *_backupname) 
        : CCovenBase(grp,true), storename(_storename), backupname(_backupname)
    {
        myrank = grp->rank();
        stopped = true;
        store = readStore(); // coven needs separate store from SDS as required before SDS initialized
        CMessageBuffer mb;
        for (rank_t r=0;r<size();r++) {
            if (r<myrank) {
                store->serialize(mb.clear());
                if (!sendRecv(mb,r,MPTAG_DALI_COVEN_REQUEST, COVEN_SERVER_TIMEOUT)) {
                    StringBuffer str;
                    throw MakeStringException(-1,"Could not connect to %s",grp->queryNode(r).endpoint().getUrlStr(str).str());
                }   
                mergeStore(store,mb,true);
            }
            else if (r>myrank) {
                rank_t sender;
                StringBuffer str;
                for (;;)
                {
                    if (!recv(mb,r,MPTAG_DALI_COVEN_REQUEST,&sender,COVEN_SERVER_TIMEOUT)) {
                        throw MakeStringException(-1,"Could not connect to %s",grp->queryNode(r).endpoint().getUrlStr(str).str());
                    }
                    if (RANK_NULL==sender)
                        processMessage(mb);
                    else break;
                }
                mergeStore(store,mb,true);
                reply(mb); 
            }
        }
        writeStore(store);
    }

    ~CCovenServer()
    {
        store->Release();
    }

    IPropertyTree *readStore()
    {
        IPropertyTree  *t=NULL;
        OwnedIFile ifile = createIFile(storename.get());
        if (ifile->exists())
            t = createPTree(*ifile, ipt_caseInsensitive);
        if (t) {
            setServerId(myrank,t->getPropInt64("ServerID"));
            setCovenId(t->getPropInt64("CovenID"));
        }
        else {
            t = createPTree("Coven");
            t->setPropInt("UIDbase",1);
            t->setPropInt("SDSedition",0);
            if (myrank==0) {
                DALI_UID covenid = 0;
                while (covenid==0)
                    covenid = makeint64(getRandom()&0x7fffffff,queryMyNode()->getHash());
                t->setPropInt64("CovenID",covenid);
                setCovenId(covenid);
            }
            DALI_UID servid = 0;
            while (servid==0)
                servid = makeint64(getRandom()&0x7fffffff,queryMyNode()->getHash());
            t->setPropInt64("ServerID",servid);
            setServerId(myrank,servid);
        }
        return t;
    }

    void writeStore(IPropertyTree *t)
    {
        StringBuffer xml;
        toXML(t, xml);
        Owned<IFile> f = createIFile(storename.get());
        Owned<IFileIO> io = f->open(IFOcreate);
        io->write(0, xml.length(), xml.str());
        io.clear();
        if (!backupname.isEmpty()) {
            try {
                f.setown(createIFile(backupname.get()));
                io.setown(f->open(IFOcreate));
                io->write(0, xml.length(), xml.str());
                io.clear();
            }
            catch (IException *e) {
                EXCLOG(e,"writing coven backup");
                e->Release();
            }
        }
    }

    void mergeStore(IPropertyTree *t,MemoryBuffer &mb,bool check)
    {
        Owned<IPropertyTree> mt = createPTree(mb);
        String str;
        DALI_UID covenid = mt->getPropInt64("CovenID");
        if (covenid!=0) {
            DALI_UID mycovenid = t->getPropInt64("CovenID");
            if (mycovenid==0) {
                t->setPropInt64("CovenID",covenid);
                setCovenId(covenid);
            }
            else if (covenid!=mycovenid)
                throw MakeStringException(-1,"Dali mismatched Coven servers");
        }
        if (check) {
            if (mt->getPropInt("UIDbase")!=t->getPropInt("UIDbase"))
                throw MakeStringException(-1,"UID base incompatibility amongst Coven servers");
            if (mt->getPropInt("SDSedition")!=t->getPropInt("SDSedition"))
                throw MakeStringException(-1,"SDS edition incompatibility amongst Coven Servers");
        }
        else {
            t->setPropInt("UIDbase",mt->getPropInt("UIDbase"));
            t->setPropInt("SDSedition",mt->getPropInt("SDSedition"));
        }
    }

    void updateDataStore()
    {
        for (rank_t r = 0; r<size(); r++) {
            if (r!=myrank) {
                CMessageBuffer mb;
                mb.append((int)MCR_UPDATE_DATA_STORE);
                store->serialize(mb);
                sendRecv(mb,r,MPTAG_DALI_COVEN_REQUEST);
            }
        }
        writeStore(store);
    }

    bool serverIsDown(rank_t)
    {
        return false; // TBD
    }

    rank_t getPrimary(unsigned ofs) 
    {
        rank_t r; 
        do {
            r = ofs%size();
        } while (serverIsDown(r));
        return r;
    }



    DALI_UID    getUniqueId(SocketEndpoint *foreignnode)
    {
        return getUniqueIds(1,foreignnode);
    }

    DALI_UID    getUniqueIds(unsigned num,SocketEndpoint *_foreignnode)
    {
        if (num==0)
            return 0;
        SocketEndpoint foreignnode;
        if (_foreignnode&&!_foreignnode->isNull()) {
            foreignnode.set(*_foreignnode);
            if (foreignnode.port==0)
                foreignnode.port = DALI_SERVER_PORT;
        }
        DALI_UID uid;
        uidcrit.enter();
        CDaliUidAllocator &uidAllocator = CDaliUidAllocator::find(uidallocators,foreignnode);
        uidcrit.leave();
        CriticalBlock block(uidAllocator.crit);
        while (!uidAllocator.allocUIDs(uid,num)) {
            rank_t primary=getPrimary(0);
            if ((primary==myrank)&&foreignnode.isNull()) {
                CriticalBlock block2(updatestorecrit);
                unsigned next = (unsigned)store->getPropInt("UIDbase");
                uidAllocator.addUIDs(((__uint64)next)<<32,UUID_BLOCK_SIZE_SERVER);
                next++;
                if (!next) {
                    next++;
                    ERRLOG("Unique ID overflow!!"); //unlikely to happen in my lifetime
                }
                store->setPropInt("UIDbase",(int)next);
                updateDataStore();
            }
            else {
                unsigned n = UUID_BLOCK_SIZE_FOREIGN;
                if (n<num) 
                    n = num*2;
                CMessageBuffer mb;
                mb.append((int)MCR_ALLOC_UNIQUE_IDS);
                mb.append(n);
                Owned<ICommunicator> foreign;
                ICommunicator *comm = &queryComm();
                if (!foreignnode.isNull()) {
                    Owned<IGroup> group = createIGroup(1,&foreignnode); 
                    foreign.setown(createCommunicator(group));
                    comm = foreign.get();
                    primary = 0;
                }
                comm->sendRecv(mb,primary,MPTAG_DALI_COVEN_REQUEST);
                DALI_UID next;
                mb.read(next);
                uidAllocator.addUIDs((__uint64)next,n);
            }
        }
        return uid;
    }


    DALI_UID getServerId(rank_t server)
    {
        return CCovenBase::getServerId(server);
    }

    DALI_UID getServerId()
    {
        return getServerId(myrank);
    }

    DALI_UID getCovenId()
    {
        return CCovenBase::getCovenId();
    }


    rank_t  getServerRank()
    {
        return myrank;
    }

    bool inCoven()
    {
        return TRUE;
    }

    void main() 
    {
        ICoven &coven=queryCoven();
        CMessageHandler<CCovenServer> handler("CCovenServer",this,&CCovenServer::processMessage);
        stopped = false;
        CMessageBuffer mb;
        while (!stopped) {
            try {
                mb.clear();
                if (coven.recv(mb,RANK_ALL,MPTAG_DALI_COVEN_REQUEST,NULL)) {
                    handler.handleMessage(mb);
                }
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "main (Coven request)");
                e->Release();
            }
        }
        done.signal();
    }

    void stop()
    {
        if (!stopped) {
            stopped = true;
            PROGLOG("Stopping server");
            queryCoven().cancel(RANK_ALL, MPTAG_DALI_COVEN_REQUEST);
        }
        done.wait();
        done.signal();  // in case anyone else calls stop
    }

    unsigned getInitSDSNodes()
    {
        return (unsigned)store->getPropInt("SDSNodes");
    }
    void setInitSDSNodes(unsigned e)
    {
        CriticalBlock block(updatestorecrit);
        store->setPropInt("SDSNodes",(int)e);
        updateDataStore();
    }

    virtual bool send (CMessageBuffer &mbuf, rank_t dstrank, mptag_t tag, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(comm);
        return comm->send(mbuf,dstrank,tag,timeout);
    }


    bool sendRecv(CMessageBuffer &mbuff, rank_t sendrank, mptag_t sendtag, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(comm);
        return comm->sendRecv(mbuff,sendrank,sendtag,timeout);
    }


    virtual unsigned probe(rank_t srcrank, mptag_t tag, rank_t *sender=NULL, unsigned timeout=0)
    {
        return comm->probe(srcrank,tag,sender,timeout);
    }
    
    virtual bool recv(CMessageBuffer &mbuf, rank_t srcrank, mptag_t tag, rank_t *sender=NULL, unsigned timeout=MP_WAIT_FOREVER)
    {
        return comm->recv(mbuf,srcrank,tag,sender,timeout);
    }

    virtual bool reply   (CMessageBuffer &mbuff, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(comm);
        return comm->reply(mbuff,timeout);
    }


    virtual void disconnect(INode *node)
    {
        assertex(comm);
        comm->disconnect(node);
    }


};
    
#define CATCH_MPERR_link_closed \
        catch (IMP_Exception *e) \
        {  \
            if (e->errorCode()!=MPERR_link_closed) \
                throw; \
            e->Release(); \
            throw createClientException(DCERR_server_closed); \
        }

class CCovenClient: public CCovenBase
{
public:
    CCovenClient(IGroup *grp) : CCovenBase(grp,false)
    {
    }

    DALI_UID    getUniqueId(SocketEndpoint *foreignnode)
    {
        return getUniqueIds(1,foreignnode);
    }

    DALI_UID    getUniqueIds(unsigned num,SocketEndpoint *_foreignnode)
    {
        if (num==0)
            return 0;
        SocketEndpoint foreignnode;
        if (_foreignnode&&!_foreignnode->isNull()) {
            foreignnode.set(*_foreignnode);
            if (foreignnode.port==0)
                foreignnode.port=DALI_SERVER_PORT;
            if (CCovenBase::inCoven(foreignnode))
                foreignnode.set(NULL,0);
        }
        uidcrit.enter();
        CDaliUidAllocator &uidAllocator = CDaliUidAllocator::find(uidallocators,foreignnode);
        uidcrit.leave();
        DALI_UID uid;
        CriticalBlock block(uidAllocator.crit);
        while (!uidAllocator.allocUIDs(uid,num)) {
            unsigned n = uidAllocator.getBankSize();
            if (n<num) 
                n = num*2;
            DALI_UID next;
            CMessageBuffer mb;
            mb.append((int)MCR_ALLOC_UNIQUE_IDS);
            mb.append(n);
            if (!foreignnode.isNull())
                foreignnode.serialize(mb);
            sendRecv(mb,RANK_RANDOM,MPTAG_DALI_COVEN_REQUEST);
            mb.read(next);
            if ((next==0)&&mb.remaining())  // server exception
                throw deserializeException(mb);
            uidAllocator.addUIDs((__uint64)next,n);
        }
        return uid;
    }

    DALI_UID getServerId(rank_t server)
    {
        DALI_UID uid=CCovenBase::getServerId(server);
        if (!uid) {
            CMessageBuffer mb;
            mb.append((int)MCR_GET_SERVER_ID);
            mb.append(server);
            sendRecv(mb,RANK_RANDOM,MPTAG_DALI_COVEN_REQUEST);
            mb.read(uid);
            CCovenBase::setServerId(server,uid);
        }
        return uid;
    }

    DALI_UID getCovenId()
    {
        DALI_UID uid=CCovenBase::getCovenId();
        if (!uid) {
            CMessageBuffer mb;
            mb.append((int)MCR_GET_COVEN_ID);
            sendRecv(mb,RANK_RANDOM,MPTAG_DALI_COVEN_REQUEST);
            mb.read(uid);
            CCovenBase::setCovenId(uid);
        }
        return uid;
    }

    DALI_UID getServerId()
    {
        return 0;
    }


    bool inCoven()
    {
        return FALSE;
    }

    unsigned getInitSDSNodes()
    {
        assertex(!"getInitSDSNodes not allowed in client");
        return 0;
    }
    void setInitSDSNodes(unsigned e)
    {
        assertex(!"setInitSDSNodes not allowed in client");
    }

    virtual bool send (CMessageBuffer &mbuf, rank_t dstrank, mptag_t tag, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(comm);
        try {
            return comm->send(mbuf,dstrank,tag,timeout);
        }
        CATCH_MPERR_link_closed
    }


    bool sendRecv(CMessageBuffer &mbuff, rank_t sendrank, mptag_t sendtag, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(comm);
        try {
            return comm->sendRecv(mbuff,sendrank,sendtag,timeout);
        }
        CATCH_MPERR_link_closed
    }


    virtual unsigned probe(rank_t srcrank, mptag_t tag, rank_t *sender=NULL, unsigned timeout=0)
    {
        assertex(comm);
        try {
            return comm->probe(srcrank,tag,sender,timeout);
        }
        CATCH_MPERR_link_closed
    }
    
    virtual bool recv(CMessageBuffer &mbuf, rank_t srcrank, mptag_t tag, rank_t *sender=NULL, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(comm);
        try {
            return comm->recv(mbuf,srcrank,tag,sender,timeout);
        }
        CATCH_MPERR_link_closed
    }

    virtual bool reply   (CMessageBuffer &mbuff, unsigned timeout=MP_WAIT_FOREVER)
    {
        assertex(comm);
        try {
            return comm->reply(mbuff,timeout);
        }
        CATCH_MPERR_link_closed
    }

    virtual void disconnect(INode *node)
    {
        assertex(comm);
        try {
            comm->disconnect(node);
        }
        catch (IMP_Exception *e) 
        {  
            if (e->errorCode()!=MPERR_link_closed) 
                throw; 
            e->Release(); 
        }
    }


};

static CCovenServer *covenServer=NULL;
static ICoven *coven=NULL;

ICoven &queryCoven()
{
    if (coven==NULL)
    {
        Owned<IException> e = MakeStringException(-1, "No access to Dali - this normally means a plugin call is being called from a thorslave");
        EXCLOG(e, NULL);
        throw e.getClear();
    }
    return *coven;
}

bool isCovenActive()
{
    return coven != NULL;
}

void initCoven(IGroup *grp,IPropertyTree *config,const char *clientVersion,const char *minServerVersion)
{
    assertex(!coven);
    if (clientVersion) ClientVersion.set(clientVersion);
    if (minServerVersion) MinServerVersion.set(minServerVersion);
    if (config&&(grp->rank()!=RANK_NULL) )
    {
        const char *s="dalicoven.xml";
        IPropertyTree *covenstore = config->queryPropTree("Coven");
        if (covenstore) {
            const char *t = covenstore->queryProp("@store");
            if (t&&*t)
                s = t;
        }
        const char *backupPath = config->queryProp("SDS/@remoteBackupLocation");
        StringBuffer b;
        if (backupPath&&*backupPath) {
            b.append(backupPath);
            addPathSepChar(b);
            b.append(s);
        }
        const char *dataPath = config->queryProp("@dataPath");
        StringBuffer covenPath;
        if (dataPath)
        {
            covenPath.append(dataPath).append(s);
            s = covenPath.str();
        }

        covenServer = new CCovenServer(grp,s,b.str());
        coven = covenServer;
    }
    else {
        covenServer = NULL;
        coven = new CCovenClient(grp);
    }
}

bool verifyCovenConnection(unsigned timeout)
{
    if (!coven) return false;
    return coven->verifyAll(true, timeout);
}

void covenMain()
{ 
    assertex(covenServer);
    covenServer->main();
}

void closeCoven()
{
    if (coven) {
        closedownDFS();                 // TBD convert DFS to IDaliProcess
        ::Release(coven);
        coven = NULL;
    }
}

const CDaliVersion &queryDaliServerVersion()
{
    return _ServerVersion;
}


DALI_UID getGlobalUniqueIds(unsigned num,SocketEndpoint *_foreignnode)
{
    if (num==0)
        return 0;
    if (coven)
        return coven->getUniqueIds(num,_foreignnode);
    if (!_foreignnode||_foreignnode->isNull())
        throw MakeStringException(99,"getUniqueIds: Not connected to dali");
    SocketEndpoint foreignnode;
    foreignnode.set(*_foreignnode);
    if (foreignnode.port==0)
        foreignnode.port=DALI_SERVER_PORT;
    CCovenBase::uidcrit.enter();
    CDaliUidAllocator &uidAllocator = CDaliUidAllocator::find(CCovenBase::uidallocators,foreignnode);
    CCovenBase::uidcrit.leave();
    DALI_UID uid;
    CriticalBlock block(uidAllocator.crit);
    while (!uidAllocator.allocUIDs(uid,num)) {
        unsigned n = uidAllocator.getBankSize();
        if (n<num) 
            n = num*2;
        DALI_UID next;
        CMessageBuffer mb;
        mb.append((int)MCR_ALLOC_UNIQUE_IDS);
        mb.append(n);
        Owned<ICommunicator> foreign;
        Owned<IGroup> group = createIGroup(1,&foreignnode); 
        foreign.setown(createCommunicator(group));
        foreign->sendRecv(mb,RANK_RANDOM,MPTAG_DALI_COVEN_REQUEST);
        mb.read(next);
        if ((next==0)&&mb.remaining())  // server exception
            throw deserializeException(mb);
        uidAllocator.addUIDs((__uint64)next,n);
    }
    return uid;
}
