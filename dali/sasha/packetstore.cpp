#define sa_decl DECL_EXPORT
#include "platform.h"
#include "jlib.hpp"
#include "jsuperhash.hpp"
#include "jmisc.hpp"
#include "jbuff.hpp"

/* TBD
  single packet
  delete
  copy/multicopy
  rename
  compressed store
  get changed
*/

#include "mpbuff.hpp"
#include "mpcomm.hpp"
#include "mputil.hpp"

#include "packetstore.hpp"

enum PacketStoreRequestKind 
{ 
    PSR_GET,
    PSR_LOCK_TRANSACTION,
    PSR_LOCK_TRANSACTION_SECONDARY,
    PSR_PUT,
    PSR_PUT_SECONDARY,
    PSR_PUT_COMMIT,
    PSR_PUT_COMMIT_SECONDARY,
    PSR_DELETE,
    PSR_DELETE_SECONDARY,
    PSR_EXIT // TBD
};

enum PacketStoreReturnKind 
{
    PSRET_OK,
    PSRET_LOCK_TIMEOUT = 1,
    PSRET_NOT_FOUND = 2,
    PSRET_EXCEPTION = 3,
    PSRET_SEND_FAIL = 4
};

#define PRIMARY_SEND_TIMEOUT (5*60*1000)        // 5 mins
#define PRIMARY_RECEIVE_TIMEOUT (5*60*1000)     // 5 mins   
#define SECONDARY_SEND_TIMEOUT (5*60*1000)      // 5 mins
#define SECONDARY_REPLY_TIMEOUT (1*60*1000)     // 1 min
#define SECONDARY_CONFIRM_TIMEOUT (1*60*1000)   // 1 min
#define PUT_DEQUEUE_TIMEOUT (1*60*1000)         // 1 min

//=========================================================


class CDataPacket: extends CInterface
{
    StringAttr key;
    unsigned keyhash;
    count_t edition;
protected: friend class CDataPacketTransaction;
    int lock;               // -1, exclusive, 0 unlocked, +ve non-exclusive
public:

    CDataPacket(const char * _key)
        : key(_key)
    {
        keyhash = hashc((const byte *)key.get(),key.length(),0);
        lock = 0;
        edition = 0;
    }

    unsigned queryHash()
    {
        return keyhash;
    }

    const char *queryKey()
    {
        return key.get();
    }

    count_t getEdition()
    {
        return edition;
    }

    void bumpEdition()
    {
        edition++;
    }

    MemoryAttr value;
    

};


class CDataPacketStore: private SuperHashTableOf<CDataPacket, const char>
{
    CriticalSection sect;
protected: friend class CDataPacketTransaction;
    CriticalSection lockingsect;
    Semaphore lockingsem;
    unsigned lockingwaiting;
public:

    CDataPacketStore()
        : SuperHashTableOf<CDataPacket, const char>() 
    { 
    }

    ~CDataPacketStore()
    {
    }

    CDataPacket &find(const char *key) // links
    {
        CriticalBlock block(sect);
        CDataPacket *ret=SuperHashTableOf<CDataPacket, const char>::find(key);
        if (!ret) {
            ret = new CDataPacket(key);
            SuperHashTableOf<CDataPacket, const char>::add(*ret);
        }
        ret->Link();
        return *ret;
    }

    void clear()
    {
        CriticalBlock block(sect);
        _releaseAll();
    }

protected:
// SuperHashTable definitions
    virtual void onAdd(void *e) 
    { 
    }

    virtual void onRemove(void *e) 
    { 
        CDataPacket &elem=*(CDataPacket *)e;        
        elem.Release();
        
    }

    virtual unsigned getHashFromElement(const void *e) const
    {
        return ((CDataPacket *) e)->queryHash();
    }

    virtual unsigned getHashFromFindParam(const void *fp) const
    {
        return hashc((const unsigned char *)fp, strlen((const char *)fp), 0);
    }

    virtual const void *getFindParam(const void *e) const
    {
        return ((CDataPacket *) e)->queryKey();
    }

    virtual bool matchesFindParam(const void *e, const void *fp, unsigned fphash) const
    {
        return (0 == strcmp(((CDataPacket *) e)->queryKey(), (const char *)fp));
    }

};


class CDataPacketTransaction: extends CInterface
{
    CDataPacketStore &store;
    CIArrayOf<CDataPacket> transaction;
    unsigned ret; // use for storing return
    bool locked;
    SocketEndpoint sender;      // 
    unsigned transactionid;     // these globally identify

    void dounlock()
    {
        if (locked) {
            unsigned count = transaction.ordinality();
            bool notify=false;
            while (count--) {
                CDataPacket &item = transaction.item(count);
                assertex(item.lock!=0);
                if (item.lock<=1) {
                    notify = true;
                    item.lock = 0;
                }
                else
                    item.lock--;
            }
            if (notify) {
                store.lockingsem.signal(store.lockingwaiting);
                store.lockingwaiting=0;
            }
            locked = false;
        }
    }
public:
    CDataPacketTransaction(CDataPacketStore &_store)
        : store(_store)
    {
        ret = PSRET_OK;
        locked = false;
    }

    ~CDataPacketTransaction()
    {
        clear();
        unlock();
    }

    void clear()
    {
        transaction.kill();
    }

    void add(const char *key)
    {
        CDataPacket &item = store.find(key);
        transaction.append(item);
    }

    void deserialize(MemoryBuffer &mb)
    {
        // format  count,{names}
        unsigned count;
        mb.read(count);
        while (count--) {           // tiny bit slow (allocating every id)
            StringAttr id;
            mb.read(id);
            add(id.get());
        }
    }

    bool lockRead(unsigned timeout)
    {
        CriticalBlock block(store.lockingsect);
        if (locked) {
            LOG(MCuserError, unknownJob, "Warning: transaction previously locked (lockRead)");
            dounlock();
        }
        int count = (int)transaction.ordinality();
        int i;
        for (i=0;i<count;i++) {
            CDataPacket &item = transaction.item(i);
            if (item.lock<0) {
                while (i-->0)  // resets i
                    transaction.item(i).lock--;     // reset what we have done
                store.lockingwaiting++;
                bool b;
                {
                    CriticalUnblock unblock(store.lockingsect);
                    b = store.lockingsem.wait(timeout);
                }
                if (!b && !store.lockingsem.wait(0)) {
                    store.lockingwaiting--; // no longer waiting
                    return false;
                }
                // try again
            }
            else
                item.lock++;
        }
        locked = true;
        return true;
    }   

    bool lockWrite(unsigned timeout)
    {
        CriticalBlock block(store.lockingsect);
        if (locked) {
            LOG(MCuserError, unknownJob, "Warning: transaction previously locked (lockWrite)");
            dounlock();
        }
        int count = (int)transaction.ordinality();
        int i;
        for (i=0;i<count;i++) {
            CDataPacket &item = transaction.item(i);
            if (item.lock!=0) {
                while (i-->0)  // resets i
                    transaction.item(i).lock=0;     // reset what we have done
                bool b;
                store.lockingwaiting++;
                {
                    CriticalUnblock unblock(store.lockingsect);
                    b = store.lockingsem.wait(timeout);
                }
                if (!b && !store.lockingsem.wait(0)) {
                    store.lockingwaiting--; // no longer waiting
                    return false;
                }
                // try again
            }
            else
                item.lock=-1;
        }
        locked = true;
        return true;
    }   

    void unlock()       // can unlock either a read or a write lock
    {
        CriticalBlock block(store.lockingsect);
        dounlock();
    }

    bool isLocked()
    {
        CriticalBlock block(store.lockingsect);
        return locked;
    }

    void setReturn(unsigned _ret)
    {
        ret = _ret;
    }

    unsigned getReturn()
    {
        return ret;
    }

    void putData(MemoryBuffer &buf) 
    {
        unsigned count=transaction.ordinality();
        unsigned i;
        size32_t *sizes = (size32_t *)buf.readDirect(count*sizeof(size32_t));
        for (i=0;i<count; i++) {
            const byte *ptr = buf.readDirect(sizes[i]);
            CDataPacket &item = transaction.item(i);
            item.value.set(sizes[i],ptr);
            item.bumpEdition();
        }
    }

    void getData(MemoryBuffer &buf) 
    {
        unsigned count=transaction.ordinality();
        unsigned i;
        size32_t total=0;
        for (i=0;i<count;i++) {
            size32_t size = transaction.item(i).value.length();
            buf.append(size);
            total+=size;
        }
        byte *p = (byte *)buf.reserve(total);
        for (i=0;i<count; i++) {
            CDataPacket &item = transaction.item(i);
            size32_t size = item.value.length();
            const void *ptr = item.value.get();
            memcpy(p,ptr,size);
            p+=size;
        }
    }

    void setTransactionId(SocketEndpoint _sender,unsigned _transactionid)
    {
        transactionid = _transactionid;
        sender = _sender;
    }

    bool testTransactionId(SocketEndpoint _sender,unsigned _transactionid)
    {
        if (transactionid != _transactionid)
            return false;
        return sender.equals(_sender);
    }

};



class CPutTransactionQueue 
{
    // orphans TBD
    CriticalSection sect;
    CIArrayOf<CDataPacketTransaction> queue;
    unsigned addwaiting;
    Semaphore addwaitingsem;


    unsigned dofind(SocketEndpoint &sender,unsigned transactionid)
    {
        ForEachItemIn(i,queue) {
            CDataPacketTransaction &item = queue.item(i);
            if (item.testTransactionId(sender,transactionid)) 
                return i;
        }
        return NotFound;
    }

    bool doremove(SocketEndpoint &sender,unsigned transactionid)
    {
        unsigned i=dofind(sender,transactionid);
        if (i!=NotFound) {
            queue.remove(i);
            return true;
        }
        return false;
    }


public:

    CPutTransactionQueue()
    {
        addwaiting = 0;
    }

    void enqueue(SocketEndpoint &sender,unsigned transactionid,CDataPacketTransaction *p)   // takes ownership
    {
        CriticalBlock block(sect);
        if (doremove(sender,transactionid)) {
            StringBuffer s;
            LOG(MCuserError, unknownJob, "Warning: duplicate transaction detected from %s", sender.getUrlStr(s).str());
            exit(0);
        }
        p->setTransactionId(sender,transactionid);
        queue.append(*p);
        if (addwaiting) {
            addwaitingsem.signal(addwaiting);
            addwaiting = 0;
        }
    }

    bool remove(SocketEndpoint &sender,unsigned transactionid)
    {
        CriticalBlock block(sect);
        return doremove(sender,transactionid);
    }


    CDataPacketTransaction *dequeue(SocketEndpoint &sender,unsigned transactionid,unsigned timeout)
    {
        CriticalBlock block(sect);
        CTimeMon tm(timeout);
        unsigned remaining;
        while (!tm.timedout(&remaining)) {
            unsigned i = dofind(sender,transactionid);
            if (i!=NotFound) {
                CDataPacketTransaction *ret = &queue.item(i);
                queue.remove(i,true);
                return ret;
            }
            addwaiting++;
            bool b;
            {
                CriticalUnblock unblock(sect);
                b = addwaitingsem.wait(remaining);
            }
            if (!b&&!addwaitingsem.wait(0)) {
                addwaiting--;
                break;
            }
        }
        return NULL;
    }
};


class CPacketStoreServer: public CInterface
{
    CDataPacketStore store;
    bool stopped;
    CriticalSection lockingwritesect;       // only used when calling CDataPacketTransaction lock functions
    Owned<ICommunicator> comm;
    CPutTransactionQueue putinprogress;
    unsigned numservers;
    unsigned myrank;
public:

    CPacketStoreServer(IGroup *grp)
    {
        comm.setown(createCommunicator(grp,true));
        numservers = grp->ordinality();
        myrank = grp->rank();
    }

    void mainloop()
    {
        CMessageHandler<CPacketStoreServer> handler("CPacketStoreServer",this,&CPacketStoreServer::processMessage);
        stopped = false;
        CMessageBuffer mb;
        while (!stopped) {
            try {
                mb.clear();
                if (comm->recv(mb,RANK_ALL,MPTAG_PACKET_STORE_REQUEST,NULL)) {
                    handler.handleMessage(mb);
                }
                else
                    stopped = true;
            }
            catch (IException *e)
            {
                EXCLOG(e, "CPacketStoreServer");
                e->Release();
            }
        }
    }

    void processMessage(CMessageBuffer &mb)
    {
        unsigned short *fn = (unsigned short *)mb.readDirect(sizeof(unsigned short));

        unsigned ret = PSRET_OK;
        Owned<CDataPacketTransaction> transaction;
        try {
            // lock and process
            // loop TBD
            bool doloop;
            do {
                doloop=false;
                    
                switch (*fn) {
                case PSR_GET: {
                        transaction.setown(new CDataPacketTransaction(store));
                        unsigned timeout;
                        mb.read(timeout);
                        transaction->deserialize(mb);
                        mb.clear();
                        if (!transaction->lockRead(timeout)) {
                            // locally lock packet items
                            LOG(MCuserError, unknownJob, "CPacketStoreServer: lockRead Timeout");
                            ret = PSRET_LOCK_TIMEOUT;
                            mb.append(ret);
                        }
                        else {
                            mb.append(ret);
                            transaction->getData(mb);
                        }
                        transaction->unlock();
                    }
                    break;
                case PSR_LOCK_TRANSACTION: 
                case PSR_LOCK_TRANSACTION_SECONDARY: {
                        transaction.setown(new CDataPacketTransaction(store));
                        unsigned timeout;
                        mb.read(timeout);
                        SocketEndpoint sender;
                        sender.deserialize(mb);
                        unsigned transactionid;
                        mb.read(transactionid);
                        transaction->deserialize(mb);
                        unsigned *term = (unsigned *)mb.readDirect(sizeof(unsigned));
                        unsigned *retp = (unsigned *)mb.readDirect(sizeof(unsigned));
                        if (*fn==PSR_LOCK_TRANSACTION) {
                            unsigned subfn = *term;
                            *term = myrank;
                            *fn = PSR_LOCK_TRANSACTION_SECONDARY;
                            *retp = PSRET_OK;
                            if (numservers>1) { // start chain;
                                mb.reset();
                                if (!comm->send(mb,(myrank+1)%numservers,MPTAG_PACKET_STORE_REQUEST,SECONDARY_SEND_TIMEOUT)) {
                                    LOG(MCuserError, unknownJob, "CPacketStoreServer: Timeout sending secondary put header (1)");
                                    *retp = PSRET_SEND_FAIL;
                                    // error recovery TBD
                                }
                                else
                                    return;
                            }
                        }
                        if (*retp==PSRET_OK) {
                            if (!transaction->lockWrite(timeout)) {
                                LOG(MCuserError, unknownJob, "CPacketStoreServer: lockWrite Timeout"); // better tracing TBD
                                *retp = PSRET_LOCK_TIMEOUT;
                            }
                        }
                        if (*term!=myrank) {            
                            mb.reset();
                            if (!comm->send(mb,(myrank+1)%numservers,MPTAG_PACKET_STORE_REQUEST,SECONDARY_SEND_TIMEOUT)) {
                                LOG(MCuserError, unknownJob, "CPacketStoreServer: Timeout sending secondary put header");
                                *retp = PSRET_SEND_FAIL;
                                // error recovery TBD
                            }
                        }
                        transaction->setReturn(*retp);
                        transaction->Link();
                        putinprogress.enqueue(sender,transactionid,transaction.get());
                    }
                    // no reply
                    return;
                case PSR_PUT: 
                case PSR_PUT_SECONDARY: {
                        SocketEndpoint sender;
                        sender.deserialize(mb);
                        unsigned transactionid;
                        mb.read(transactionid);
                        unsigned *term = (unsigned *)mb.readDirect(sizeof(unsigned));
                        unsigned *retp = (unsigned *)mb.readDirect(sizeof(unsigned));
                        CDataPacketTransaction *p=putinprogress.dequeue(sender,transactionid,PUT_DEQUEUE_TIMEOUT);
                        if (!p) {
                            ret = PSRET_LOCK_TIMEOUT;
                            LOG(MCuserError, unknownJob, "CPacketStoreServer: Timeout dequeuing transaction");
                            // error handling TBD
                            mb.clear().append(ret);
                            break;
                        }
                        transaction.setown(p);
                        ret = transaction->getReturn();
                        if (ret==PSRET_OK) 
                            ret = *retp;
                        if ((ret==PSRET_OK)&&transaction->isLocked()) {
                            // do update
                            transaction->putData(mb);
                        }
                        if (*fn==PSR_PUT) {
                            *term = myrank;
                            *retp = ret;
                            *fn = PSR_PUT_SECONDARY;
                            MemoryBuffer data;
                            data.swapWith(mb);
                            mb.append(ret);
                            comm->reply(mb); // can return reply here
                            // check timeout TBD
                            data.swapWith(mb);
                        }
                        transaction->unlock();
                        rank_t next = (myrank+1)%numservers;
                        if (*term!=next) { // chain
                            mb.reset();
                            if (!comm->send(mb,next,MPTAG_PACKET_STORE_REQUEST,SECONDARY_SEND_TIMEOUT)) {
                                LOG(MCuserError, unknownJob, "CPacketStoreServer: Timeout sending secondary put header");
                                // error recovery TBD
                            }
                        }                           
                        return; // reply either already sent or no reply
                    }
                }                              
            } while (doloop);
        }
        catch (IException *e) {
            EXCLOG(e, "CPacketStoreServer::processMessage");
            ret = PSRET_EXCEPTION;
            mb.clear().append(ret);
            e->Release();
            if (transaction)
                transaction->unlock();
        }
        comm->reply(mb);
    }
    

};


void runPacketStoreServer(IGroup *grp)
{
    CPacketStoreServer server(grp);
    server.mainloop();
}


// ==============================================================================================================
// Client Side

class CPacketStoreClient: implements IPacketStore, public CInterface
{
    Owned<ICommunicator> comm;
    SocketEndpoint myep;
    CriticalSection sect;
    unsigned nservers;

    unsigned nextTransactionId()
    {
        static CriticalSection transactionsect;
        CriticalBlock block(transactionsect);
        static unsigned nexttransactionid=0;
        return nexttransactionid++;
    }

    bool multiTransactionLock(CMessageBuffer &mb,rank_t r,unsigned transactionid,unsigned subfn,unsigned count, const char **id,unsigned timeout)
    {
        unsigned ret=PSRET_OK;
        unsigned short fn=PSR_LOCK_TRANSACTION;
        mb.append(fn).append(timeout);
        myep.serialize(mb);
        mb.append(transactionid);
        mb.append(count);
        unsigned i;
        for (i=0;i<count;i++)
            mb.append(id[i]);
        mb.append(subfn);           
        mb.append(ret); 
        return comm->send(mb,r,MPTAG_PACKET_STORE_REQUEST,PRIMARY_SEND_TIMEOUT);
    }

public:
    IMPLEMENT_IINTERFACE;



    CPacketStoreClient(IGroup *grp)
    {
        comm.setown(createCommunicator(grp,true));
        comm->verifyAll(false);
        myep = queryMyNode()->endpoint();
        nservers = grp->ordinality();
    }
    
    bool put(const char *id, size32_t packetsize, const void *packetdata, unsigned timeout)
    {
        return multiPut(1,&id,&packetsize,&packetdata,timeout);
    }

    bool multiPut(unsigned count, const char **id, const size32_t *packetsize, const void **packetdata, unsigned timeout)
    {
        if (count==0)
            return true;
        try {
            CriticalBlock block(sect);
            rank_t r = (nservers>1)?(getRandom()%nservers):0;
            unsigned transactionid = nextTransactionId();
            unsigned short fn=PSR_PUT;
            CMessageBuffer mb;
            multiTransactionLock(mb,r,transactionid,fn,count,id,timeout);
            unsigned ret=PSRET_OK;
            mb.clear().append(fn);
            myep.serialize(mb);
            mb.append(transactionid);
            mb.append(ret);         // couple of slots used by secondary
            mb.append(ret);
            size32_t total=0;
            unsigned i;
            for (i=0;i<count;i++) {
                mb.append(packetsize[i]);
                total += packetsize[i];
            }
            byte *p = (byte *)mb.reserve(total);
            for (i=0;i<count;i++) {
                memcpy(p,packetdata[i],packetsize[i]);
                p+= packetsize[i];
            }
            if (!comm->sendRecv(mb,r,MPTAG_PACKET_STORE_REQUEST,PRIMARY_SEND_TIMEOUT))
                return false;
            mb.read(ret);
            return (ret==PSRET_OK);
        }
        catch (IException *e) {
            EXCLOG(e, "CPacketStoreClient::multiPut");
            // error handling TBD

        }
        return false; // TBD
    }

    bool multiDelete(unsigned count, const char **id, unsigned timeout)
    {
        if (count==0)
            return true;
        try {
            CriticalBlock block(sect);
            rank_t r = (nservers>1)?(getRandom()%nservers):0;
            unsigned transactionid = nextTransactionId();
            unsigned short fn=PSR_DELETE;
            CMessageBuffer mb;
            multiTransactionLock(mb,r,transactionid,fn,count,id,timeout);
            unsigned ret;
            mb.read(ret);
            return (ret==PSRET_OK);
        }
        catch (IException *e) {
            EXCLOG(e, "CPacketStoreClient::multiPut");
            // error handling TBD

        }
        return false; // TBD
    }


    bool get(const char *id, MemoryBuffer &buf, unsigned timeout)
    {
        size32_t size;
        size32_t offset;
        return multiGet(1,&id,buf,&size,&offset,timeout);
    }

    bool multiGet(unsigned count, const char **id, MemoryBuffer &buf, size32_t *sizes, size32_t *offsets, unsigned timeout)
    {   // this could avoid extra copy if we knew buf clear on entry
        if (count==0)
            return true;
        try {
            CriticalBlock block(sect);      // not sure if this really needed
            rank_t r = (nservers>1)?(getRandom()%nservers):0;
            unsigned ret=PSRET_OK;
            CMessageBuffer mb;
            unsigned short fn=PSR_GET;
            mb.append(fn).append(timeout);
            mb.append(count);
            unsigned i;
            for (i=0;i<count;i++)
                mb.append(id[i]);
            if (comm->sendRecv(mb,r,MPTAG_PACKET_STORE_REQUEST,PRIMARY_RECEIVE_TIMEOUT)) {
                mb.read(ret);
                if (ret==PSRET_OK) {
                    size32_t *sizes = (size32_t *)mb.readDirect(count*sizeof(size32_t));
                    size32_t ofs = mb.length();
                    size32_t total = 0;
                    for (i=0;i<count;i++) {
                        offsets[i] = total+ofs;
                        total += sizes[i];
                    }
                    buf.append(total,mb.readDirect(total));
                    return true;
                }
            }
            return false; 
        }
        catch (IException *e) {
            EXCLOG(e, "CPacketStoreClient::multiPut");
            // error handling TBD

        }
        return false; 
    }

    bool remove(const char *id, unsigned timeout)
    {
        UNIMPLEMENTED;
        return false; // TBD
    }

    bool rename(const char *from, const char *to, unsigned timeout)
    {
        UNIMPLEMENTED;
        return false; // TBD
    }

    bool copy(const char *from, const char *to, unsigned timeout)
    {
        UNIMPLEMENTED;
        return false; // TBD
    }

    pkt_edition_t getEdition(const char *id)
    {
        UNIMPLEMENTED;
        return 0; // TBD
    }

    pkt_edition_t getChanged(const char *id, pkt_edition_t lastedition, MemoryBuffer &mb, unsigned timeout)
    {
        UNIMPLEMENTED;
        return 0; // TBD
    }

    IPacketStoreEnumerator * getEnumerator(const char *mask)
    {
        UNIMPLEMENTED;
        return NULL; // TBD
    }

    IPacketStoreValueEnumerator * getValueEnumerator(const char *mask)
    {
        UNIMPLEMENTED;
        return NULL; // TBD
    }

};


extern sa_decl IPacketStore * connectPacketStore(IGroup *psgroup)
{
    return new CPacketStoreClient(psgroup);
}

