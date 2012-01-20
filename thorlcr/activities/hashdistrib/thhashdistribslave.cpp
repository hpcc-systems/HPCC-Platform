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

#include "platform.h"
#include "limits.h"
#include "slave.ipp"

#include "thhashdistribslave.ipp"
#include "thorport.hpp"
#include "jio.hpp"
#include "jflz.hpp"
#include "jsort.hpp"
#include "jdebug.hpp"
#include "thsortu.hpp"
#include "thactivityutil.ipp"
#include "thmem.hpp"
#include "tsorta.hpp"
#include "thormisc.hpp"
#include "javahash.hpp"
#include "javahash.tpp"
#include "mpcomm.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"
#include "jhtree.hpp"
#include "thalloc.hpp"

#ifdef _DEBUG
//#define TRACE_UNIQUE
//#define FULL_TRACE
//#define TRACE_MP
#endif

//--------------------------------------------------------------------------------------------
// HashDistributeSlaveActivity
//





#define NUMSLAVEPORTS       2
#define DEFAULTCONNECTTIMEOUT 10000
#define DEFAULT_OUT_BUFFER_SIZE 0x100000        // 1MB
#define DEFAULT_IN_BUFFER_SIZE  0x100000*32  // 32MB input buffer

#define DISK_BUFFER_SIZE 0x10000 // 64K
#define DEFAULT_TIMEOUT (1000*60*60)

static bool isMaster(unsigned src,unsigned dst)
{
    // simple way of connecting up directed graph in balanced fashion
    if (src>dst)
        return !isMaster(dst,src);
    return (src&1)==(dst&1);
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning( disable : 4355 ) // 'this' : used in base member initializer list
#endif

class PtrElemAllocator;

#define PTRBUFSIZE 10000    // meta is assumed not *too* big (and fixed)
class PtrElem
{
    PtrElem *next;
    const void *row;
    //byte meta[1]; // and following
public:
    static inline size32_t size(size32_t metasize) { return sizeof(PtrElem*)+sizeof(const void *)+metasize; }
    inline void deserialize(IEngineRowAllocator *allocator,IOutputRowDeserializer *deserializer,IRowDeserializerSource &dsz, size32_t metasize)
    {
        RtlDynamicRowBuilder rowBuilder(allocator);
        size32_t sz = deserializer->deserialize(rowBuilder,dsz);
        row = rowBuilder.finalizeRowClear(sz);
        dsz.read(metasize,getMetaPtr());
    }
    inline void serialize(IOutputRowSerializer *serializer,IRowSerializerTarget &sz, size32_t metasize)
    {
        serializer->serialize(sz,(const byte *)row);
        sz.put(metasize,getMetaPtr());
    }

    inline void clear()
    {
        if (row) {
            ReleaseThorRow(row);
            row = NULL;
        }
        // NB don't clear next
    }

    inline const void *queryRow() const
    {
        return row;
    }
    inline PtrElem *queryNext() const
    {
        return next;
    }
    inline void setNext(PtrElem * _next)
    {
        next = _next;
    }

    inline void setRow( const void *_row)
    {
        row = _row;
    }

    inline void *getMetaPtr() const
    {
        return ((byte *)&row)+sizeof(row);
    }

};

class PtrElemAllocator: public CSimpleInterface, implements ISRBRowInterface
{
    size32_t metasize;
    IOutputRowSerializer *serializer;
    IOutputRowDeserializer *deserializer;
    SpinLock lock;
    CFixedSizeAllocator fsallocator;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
    PtrElem *addRowMeta(IRowStreamWithMetaData &in)
    {
        //SpinBlock block(lock);
        PtrElem *ret = (PtrElem *)fsallocator.alloc();
        ret->setNext(NULL);
        const void *row = NULL;
        if (!in.nextRow(row,ret->getMetaPtr())) {
            fsallocator.dealloc(ret);
            return NULL;
        }
        ret->setRow(row);
        return ret;
    }

    PtrElem *addRow(IRowStream &in)
    {
        PtrElem *ret = (PtrElem *)fsallocator.alloc();
        ret->setNext(NULL);
        const void *row = in.nextRow();
        if (!row)
        {
            row = in.nextRow();
            if (!row)
            {
                fsallocator.dealloc(ret);
                return NULL;
            }
        }
        ret->setRow(row);
        return ret;
    }

    PtrElem *deserializeRow(IEngineRowAllocator *allocator, CThorStreamDeserializerSource &dsz)
    {
        PtrElem *ret = (PtrElem *)fsallocator.alloc();
        ret->setNext(NULL);
        ret->deserialize(allocator,deserializer,dsz,metasize); // NB deserializer is thor row deserializer
        return ret;
    }

    const void *rowGetClear(PtrElem *e)
    {
        const void *ret = e->queryRow();
        fsallocator.dealloc(e);
        return ret;
    }

    const void *rowGetClear(PtrElem *e,void *meta, size32_t _metasize)
    {
        assertex(metasize==_metasize);
        memcpy(meta,e->getMetaPtr(),metasize);
        const void *ret = e->queryRow();
        fsallocator.dealloc(e);
        return ret;
    }


    void releaseRow(const void *r)
    {
        PtrElem *e = (PtrElem *)r;
        ReleaseThorRow(e->queryRow());
        fsallocator.dealloc(e);
    }


    void linkRow(const void *r)
    {
        assertex(!"PtrElemAllocator::linkRow");
    }

    size32_t rowMemSize(const void *r)
    {
//      SpinBlock block(lock);                      // OK
        const PtrElem *e = (const PtrElem *)r;
        size32_t ret = e->size(metasize);
        if (e->queryRow()) 
            ret += thorRowMemoryFootprint(e->queryRow());
        return ret;
    }

    void init(size32_t _metasize,IOutputRowSerializer *_serializer,IOutputRowDeserializer *_deserializer)
    {
        metasize = _metasize;
        serializer = _serializer;
        deserializer = _deserializer;
        fsallocator.init(PtrElem::size(metasize));
    }

    inline const void *getMetaPtr(const PtrElem *e,size32_t _metasize)
    {
        assertex(metasize==_metasize);
        return e->getMetaPtr();
    }

    inline size32_t queryMetaSize() { return metasize; }


};



class CDistributorBase : public CSimpleInterface, implements IHashDistributor
{
    Linked<IEngineRowAllocator> allocator;
    Linked<IOutputRowSerializer> serializer;
    Linked<IOutputRowDeserializer> deserializer;
    const bool &abort;
    IHash *ihash;
    Owned<IRowStreamWithMetaData> inm;
    Owned<IRowStream> innm;

    size32_t metasize;
    Semaphore distribdone;
    ICompare *icompare;
    CMessageBuffer outputbuf;
    size32_t outputBufferSize;
    bool dedup;
    StringBuffer tempname;
    int outfh;
    Owned<IException> sendexc;
    Owned<IException> recvexc;
    Semaphore localfinished;
    bool connected;
    IStopInput *istop;
    unsigned nodedupcount;
    unsigned nodedupsample;
    bool *sendfinished;
    unsigned numsendfinished;
    bool selfstopped;
    Owned<IRowWriter> pipewr;               // NB not Thor rows!
protected:
    PtrElemAllocator ptrallocator;


    class cPipeOutWrapper: public CSimpleInterface, implements IRowStreamWithMetaData, implements IRowLinkCounter, implements IOutputRowSerializer
    {
        // this is a weird one as it is asymetric
    public:
        ISmartRowBuffer *piperd;
        size32_t metasize;
        PtrElemAllocator *ptrallocator;
        IOutputRowSerializer *rowserializer;

        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        void init(ISmartRowBuffer *_piperd,IOutputRowSerializer *_serializer, PtrElemAllocator *_ptrallocator)
        {
            metasize = _ptrallocator->queryMetaSize();
            piperd = _piperd;
            rowserializer = _serializer;
            ptrallocator = _ptrallocator;
        }


        bool nextRow(const void *&row, void * meta)
        {
            PtrElem * e = (PtrElem *)piperd->nextRow();
            if (!e)
                return false;
            row = (meta&&metasize)?ptrallocator->rowGetClear(e,meta,metasize):ptrallocator->rowGetClear(e);
            return true;
        }

        const void *nextRow()
        {
            const void *row;
            if (nextRow(row,NULL))
                return row;
            return NULL;
        }


        void stop()
        {
            piperd->stop();
        }

        void linkRow(const void *r)
        {
            assertex(!"cPipeOutWrapper::linkRow");
        }

        void releaseRow(const void *r)
        {
            ptrallocator->releaseRow(r);
        }

        void serialize(IRowSerializerTarget & out, const byte * r)
        {
            if (!r)
                return;
            const PtrElem * e = (const PtrElem *)r;
            if (metasize)
                out.put(metasize,ptrallocator->getMetaPtr(e,metasize));
            rowserializer->serialize(out,(const byte *)e->queryRow());
        }

    };



    Owned<cPipeOutWrapper> pipeout;
    Owned<ISmartRowBuffer> piperd;


    class cRecvThread: public Thread
    {
        CDistributorBase *parent;
    public:
        cRecvThread(CDistributorBase *_parent)
            : Thread("CDistributorBase::cRecvThread")
        {
            parent = _parent;
        }
        int run()
        {
            parent->recvloop();
            parent->recvloopdone();
            return 0;
        }
        void stop()
        {
            parent->stopRecv();
        }
    } recvthread;

    class cSendThread: public Thread
    {
        CDistributorBase *parent;
    public:
        cSendThread(CDistributorBase *_parent)
            : Thread("CDistributorBase::cSendThread")
        {
            parent = _parent;
        }
        int run()
        {
            parent->sendloop();
            return 0;
        }
    } sendthread;

    inline bool selfPush(unsigned i)
    {
        return (i==self)&&!pull;
    }

    void checkCount(PtrElem *list,unsigned count)
    {
        unsigned i=0;
        const PtrElem *e = list;
        while (e) {
            i++;
            e = e->queryNext();
        }
        assertex(count==i);
    }

protected:
    CActivityBase *activity;
    size32_t inputBufferSize, pullBufferSize;
    unsigned self;
    unsigned numnodes;
    Owned<IRandomNumberGenerator> irandom;
    CriticalSection putsect;
    bool pull;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDistributorBase(CActivityBase *_activity, IRowInterfaces *_rowif,const bool &_abort,bool _dedup, IStopInput *_istop)
        : activity(_activity), allocator(_rowif->queryRowAllocator()), serializer(_rowif->queryRowSerializer()), deserializer(_rowif->queryRowDeserializer()), abort(_abort), recvthread(this), sendthread(this)
    {
        connected = false;
        dedup = _dedup;
        numnodes = 0;
        icompare = NULL;

        outfh = -1;
        outputBufferSize = globals->getPropInt("@hd_out_buffer_size", DEFAULT_OUT_BUFFER_SIZE);
        outputbuf.reserveTruncate(outputBufferSize+2);  // 2 for trailing flag
        nodedupcount = 0;
        nodedupsample = 0;
        istop = _istop;
        inputBufferSize = globals->getPropInt("@hd_in_buffer_size", DEFAULT_IN_BUFFER_SIZE);
        pullBufferSize = DISTRIBUTE_PULL_BUFFER_SIZE;
        selfstopped = false;
        sendfinished = NULL;
        numsendfinished = 0;
        pull = false;
    }

    ~CDistributorBase()
    {
        try {
            disconnect(true); // in case exception
            removetemp();
        }
        catch (IException *e)
        {
            ActPrintLog(activity, e, "HDIST: CDistributor");
            e->Release();
        }
        free(sendfinished);
    }

    virtual void setBufferSizes(unsigned _inputBufferSize, unsigned _outputBufferSize, unsigned _pullBufferSize)
    {
        if (_inputBufferSize) inputBufferSize = _inputBufferSize;
        if (_outputBufferSize) outputBufferSize = _outputBufferSize;
        if (_pullBufferSize) pullBufferSize = _pullBufferSize;
    }

    virtual IRowStreamWithMetaData *doconnect(size32_t _metasize, IHash *_ihash, ICompare *_icompare)
    {
        ActPrintLog(activity, "HASHDISTRIB: connect");
        metasize = _metasize;
        ptrallocator.init(metasize,serializer,deserializer);
        ihash = _ihash;
        icompare = _icompare;
        irandom.setown(createRandomNumberGenerator());
        irandom->seed(self);
        pipeout.setown(new cPipeOutWrapper);
        piperd.setown(createSmartInMemoryBuffer(activity,pullBufferSize,&ptrallocator));
        pipeout->init(piperd,serializer,&ptrallocator);
        pipewr.set(pipeout->piperd->queryWriter());
        connected = true;
        selfstopped = false;

        nodedupcount = 0;
        nodedupsample = 0;
        outputbuf.clear();

        if (sendfinished)
            free(sendfinished);
        sendfinished = (bool *)calloc(sizeof(bool),numnodes);
        numsendfinished = 0;
        sendexc.clear();
        recvexc.clear();
        start();
        ActPrintLog(activity, "HASHDISTRIB: connected");
        return pipeout.getLink();
    }

    virtual IRowStreamWithMetaData *connect(IRowStreamWithMetaData *_in, size32_t _metasize, IHash *_ihash, ICompare *_icompare)
    {
        if (_metasize)
            inm.set(_in);
        else
            innm.set(_in);
        return doconnect(_metasize, _ihash,_icompare);
    }

    virtual IRowStream *connect(IRowStream *_in, IHash *_ihash, ICompare *_icompare)
    {
        innm.set(_in);
        return doconnect(0, _ihash,_icompare);
    }


    virtual void disconnect(bool stop)
    {
        if (connected) {
            connected = false;
            if (stop) {
                recvthread.stop();
                selfstopped = true;
            }
            distribdone.wait();
            if (sendexc.get()) {
                recvthread.join(1000*60);       // hopefully the others will close down
                throw sendexc.getClear();
            }
            recvthread.join();
            if (recvexc.get())
                throw recvexc.getClear();

        }
    }

    virtual void removetemp()
    {
        if (outfh!=-1) {
            close(outfh);
            outfh = -1;
        }
        if (tempname.length()) {
            remove(tempname.toCharArray());
            tempname.clear();
        }
    }

    virtual void recvloop()
    {
        MemoryBuffer tempMb;
        try {
            ActPrintLog(activity, "Read loop start");
            CMessageBuffer recvMb;
            Owned<ISerialStream> stream = createMemoryBufferSerialStream(tempMb);
            CThorStreamDeserializerSource rowSource;
            rowSource.setStream(stream);
            unsigned left=numnodes-1;
            while (left) {
#ifdef _FULL_TRACE
                ActPrintLog("HDIST: Receiving block");
#endif
                unsigned n = recvBlock(recvMb);
                if (n==(unsigned)-1)
                    break;
#ifdef _FULL_TRACE
                ActPrintLog(activity, "HDIST: Received block %d from slave %d",recvMb.length(),n+1);
#endif
                if (recvMb.length()) {
                    try { fastLZDecompressToBuffer(tempMb.clear(),recvMb); }
                    catch (IException *e)
                    {
                        StringBuffer senderStr;
                        activity->queryContainer().queryJob().queryJobGroup().queryNode(n+1).endpoint().getUrlStr(senderStr);
                        IException *e2 = MakeActivityException(activity, e, "Received from node: %s", senderStr.str());
                        e->Release();
                        throw e2;
                    }
                    {
                        CriticalBlock block(putsect);
                        while (!rowSource.eos()) 
                            pipewr->putRow(ptrallocator.deserializeRow(allocator,rowSource));      
                    }
                }
                else {
                    left--;
                    ActPrintLog(activity, "HDIST: finished slave %d, %d left",n+1,left);
                }
#ifdef _FULL_TRACE
                ActPrintLog(activity, "HDIST: Put block %d from slave %d",recvMb.length(),n+1);
#endif
            }
        }
        catch (IException *e)
        {
            setRecvExc(e);
        }
#ifdef _FULL_TRACE
        ActPrintLog(activity, "HDIST: waiting localfinished");
#endif

    }

    void recvloopdone()
    {
        localfinished.wait();
        pipewr->flush();
        if (pipeout)
            pipeout->stop();
        pipewr.clear();
        ActPrintLog(activity, "HDIST: Read loop done");

    }

    static CriticalSection dedupcrit;
    static ICompare *dedupcompare;
    static int comparePtrElem(const void * left, const void * right)
    {
        return dedupcompare->docompare(((const PtrElem *)left)->queryRow(),((const PtrElem *)right)->queryRow());
    }

    void deduplist(PtrElem *&list,PtrElem *&tail,size32_t &bucketsize,size32_t &totalsz)
    {
        if ((nodedupsample==10)&&(nodedupcount==0))
            return; // heuristic (if none of the first 10 blocks have more than 10% dups then don't dedup)
        assertex(bucketsize);
        PointerArray ptrs;
        PtrElem *e1 = list;
        while (e1) {
            ptrs.append((void *)e1);
            e1 = e1->queryNext();
        }
        unsigned c = ptrs.ordinality();
        if (c<2)
            return;
        {
            CriticalBlock block(dedupcrit);
            dedupcompare = icompare;
            qsortvec((void **)ptrs.getArray(), c, comparePtrElem);
        }
        PtrElem *n = NULL;
        tail = NULL;
        unsigned precount = c;
        for (unsigned i = c; i>0;) {
            PtrElem *e = (PtrElem *)ptrs.item(--i);
            if (n&&(icompare->docompare(n->queryRow(),e->queryRow())==0)) {
                size32_t rsz = ptrallocator.rowMemSize(e);
                totalsz -= rsz;
                bucketsize -= rsz;
                ptrallocator.releaseRow(e);
                c--;
            }
            else {
                if (!n)
                    tail = e;
                e->setNext(n);
                n = e;
            }
        }
        list = n;
        if (nodedupsample<10) {
            if (c<precount*9/10)
                nodedupcount++;
            nodedupsample++;
            ActPrintLog(activity, "pre-dedup sample %d - %d unique out of %d",nodedupsample,c,precount);
            if ((nodedupsample==10)&&(nodedupcount==0)) {
                ActPrintLog(activity, "disabling distribute pre-dedup");
                dedup = false;
            }
        }
    }

    void doSendBlock(unsigned i,CMessageBuffer &mb)
    {
        if (!sendfinished[i]) {
            if (selfPush(i))
                assertex(i!=self);
            if (!sendBlock(i,mb)) {
                ActPrintLog(activity, "CDistributorBase::sendBlock stopped slave %d",i+1);
                sendfinished[i] = true;
                numsendfinished++;
            }
        }
        mb.clear();
    }


    void writelist(unsigned h,PtrElem *&list,PtrElem *&tail,size32_t &bucketsize,bool all,size32_t &totalsz)
        // writes everything and puts list on free list
    {

        if (!list) {
            assertex(bucketsize==0);
            return;
        }
        if (dedup) 
            deduplist(list,tail,bucketsize,totalsz);
        assertex(bucketsize!=0);    // can't dedup all!
        PtrElem *e=list;
        if (selfPush(h)||sendfinished[h]) {

            if (selfstopped&&!sendfinished[self]) {
                sendfinished[self] = true;
                numsendfinished++;
            }
            {
                CriticalBlock block(putsect);
                do {    
                    PtrElem *n = e->queryNext();
                    totalsz -= ptrallocator.rowMemSize(e);
                    if (sendfinished[h]) 
                        ptrallocator.releaseRow(e);
                    else
                        pipewr->putRow(e);
                    e = n;
                } while (e);
            }
            list = NULL;
            tail = NULL;
            bucketsize = 0;
        }
        else {
            outputbuf.clear();
            CMemoryRowSerializer sz(outputbuf);
            MemoryBuffer rowbuf;
            MemoryAttr prevrowma;
            size32_t bufrd = 0;
            do {
                PtrElem *n = e->queryNext();
                bufrd += ptrallocator.rowMemSize(e);
                e->serialize(serializer,sz,metasize);
                ptrallocator.releaseRow(e);
                if (outputbuf.length()>outputBufferSize) {
                    MemoryBuffer tempMb;
                    fastLZCompressToBuffer(tempMb,outputbuf.length(),outputbuf.bufferBase());
                    outputbuf.swapWith(tempMb);
                    tempMb.clear();
                    assertex(totalsz>=bufrd);
                    totalsz -= bufrd;
                    assertex(bucketsize>=bufrd);
                    bucketsize -= bufrd;
                    doSendBlock(h,outputbuf);
                    if (!all) {
                        list = n;
                        if (!n)
                            tail = NULL;
                        return;
                    }
                    bufrd = 0;
                }
                e = n;
            } while(e);
            list = NULL;
            tail = NULL;
            if (outputbuf.length()) {
                MemoryBuffer tempMb;
                fastLZCompressToBuffer(tempMb,outputbuf.length(),outputbuf.bufferBase());
                outputbuf.swapWith(tempMb);
                tempMb.clear();
                assertex(totalsz>=bufrd);
                totalsz -= bufrd;
                assertex(bucketsize==bufrd);
                bucketsize = 0;
                doSendBlock(h,outputbuf);
            }
        }
    }


    void sendloop()
    {
        ActPrintLog(activity, "Distribute send start");
        MemoryAttr ma;
        size32_t *sizes=(unsigned *)ma.allocate(
            sizeof(size32_t)*numnodes +
            numnodes*PtrElem::size(metasize)*2
        );
        memset(sizes,0,ma.length());
        UnsignedArray candidates;


        size32_t totalsz=0;
        PtrElem **heads = (PtrElem **)(sizes+numnodes);
        PtrElem **tails = heads+numnodes;
        unsigned tot=0;
        try {
            unsigned shuffleinterval=0;
            do {
                if (totalsz<inputBufferSize) {
                    PtrElem *n = metasize?ptrallocator.addRowMeta(*inm):ptrallocator.addRow(*innm);
                    if (!n)
                        break;
                    unsigned h = ihash->hash(n->queryRow()?n->queryRow():ptrallocator.getMetaPtr(n,metasize))%numnodes; //????
                    if (!sendfinished[h]) {
                        PtrElem *&tail = tails[h];
                        if (tail)
                            tail->setNext(n);
                        else
                            heads[h] = n;
                        tail = n;
                        n->setNext(NULL);
                        tot++;
                        size32_t rs = ptrallocator.rowMemSize(n);
                        totalsz += rs;
                        sizes[h] += rs;
                    }
                    else
                        ptrallocator.releaseRow(n);
                }
                else {
                    unsigned i;
                    unsigned maxsz=0;
                    for (i=0;i<numnodes;i++)
                        if (sizes[i]>maxsz)
                            maxsz = sizes[i];
                    candidates.kill();
                    bool seenself = false;
                    for (i=0;i<numnodes;i++)
                        if (sizes[i]>maxsz/2)
                            if (i==self)
                                seenself = true;
                            else
                                candidates.append(i);
                    assertex(seenself||candidates.ordinality()); // must be at least one!
                    if (candidates.ordinality()) {
                        unsigned h;
                        if (candidates.ordinality()==1)
                            h = candidates.item(0);
                        else
                            h = candidates.item(getRandom()%candidates.ordinality());
                        writelist(h,heads[h],tails[h],sizes[h],false,totalsz);
                    }
                    if (seenself)
                        writelist(self,heads[self],tails[self],sizes[self],true,totalsz);
                }
            } while (numsendfinished<numnodes);
            ActPrintLog(activity, "Distribute send finishing");
            Owned<IShuffledIterator> iter = createShuffledIterator(numnodes);
            ForEach(*iter) {
                unsigned h=iter->get();
                if (sizes[h])
                    writelist(h,heads[h],tails[h],sizes[h],true,totalsz);
            }
        }
        catch (IException *e)
        {
            ActPrintLog(activity, e, "HDIST: sendloop");
            sendexc.setown(e);
        }
        // Time limit here if exception

        closewrite();
        ActPrintLog(activity, "HDIST: Send loop %s %d rows sent",sendexc.get()?"aborted":"finished",tot);
        if (istop) {
            if (sendexc.get()) { // ignore secondary fault
                try {
                    istop->stopInput();
                }
                catch (IException *e) {
                    ActPrintLog(activity, e, "HDIST: follow on");
                    e->Release();
                }
            }
            else
                istop->stopInput();
        }
        distribdone.signal();
    }

    void closewrite()
    {
        unsigned i;
        CMessageBuffer nullmb;
        for (i=0;i<numnodes;i++) {
            if (!selfPush(i)) {
                try {
                    nullmb.clear();
                    sendBlock(i,nullmb);
                }
                catch (IException *e)
                {
                    ActPrintLog(activity, e, "HDIST: closewrite");
                    if (sendexc.get())
                        e->Release();
                    else
                        sendexc.setown(e);
                }
            }
        }
        localfinished.signal();
    }

    virtual void startTX()=0;
    void start()
    {
        startTX();
        recvthread.start();
        sendthread.start();
    }

    virtual void join() // probably does nothing
    {
        sendthread.join();
        recvthread.join();
    }

    inline unsigned numNodes()
    {
        return numnodes;
    }

    inline ICompare * queryCompare()
    {
        return icompare;
    }

    inline IEngineRowAllocator *queryAllocator()
    {
        return allocator;
    }

    inline PtrElemAllocator &queryPtrAllocator()
    {
        return ptrallocator;
    }

    inline IRowWriter &output()
    {
        return *pipewr;
    }

    void setRecvExc(IException *e)
    {
        ActPrintLog(activity, e, "HDIST: recvloop");
        if (recvexc.get())
            e->Release();
        else
            recvexc.setown(e);
    }
    virtual unsigned recvBlock(CMessageBuffer &mb,unsigned i=(unsigned)-1) = 0;
    virtual void stopRecv() = 0;
    virtual bool sendBlock(unsigned i,CMessageBuffer &mb) = 0;
};

CriticalSection CDistributorBase::dedupcrit;
ICompare *CDistributorBase::dedupcompare;



// protocol is:
// 1) 0 byte block              - indicates end of input - no ack required
// 2) 1 byte block {1}          - request to send - ack required
// 3) >1 byte block {...,0} - block sent following RTS received - no ack required
// 4) >1 byte block {...,1}    - block sent - ack required
// ack is always a single byte 0 for stop and 1 for continue



class CRowDistributor: public CDistributorBase
{
    mptag_t tag;
    ICommunicator &comm;
    bool stopping;
public:

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRowDistributor(CActivityBase *activity, ICommunicator &_comm, mptag_t _tag, IRowInterfaces *_rowif, const bool &abort, bool dedup, IStopInput *istop)
        : CDistributorBase(activity, _rowif, abort, dedup, istop), comm(_comm), tag(_tag)
    {
        self = comm.queryGroup().rank()-1;
        numnodes = comm.queryGroup().ordinality()-1;
        stopping = false;
    }
    virtual IRowStreamWithMetaData *doconnect(size32_t metasize, IHash *ihash, ICompare *icompare)
    {
        stopping = false;
        return CDistributorBase::doconnect(metasize, ihash, icompare);
    }
    virtual unsigned recvBlock(CMessageBuffer &msg, unsigned)
        // does not append to msg
    {
Restart:
        msg.clear();
        rank_t sender;
        CMessageBuffer ack;
#ifdef TRACE_MP
        unsigned waiting = comm.probe(RANK_ALL,tag,NULL);
        ActPrintLog(activity, "HDIST MP recv(%d) waiting %d",(int)tag, waiting);
#endif
        if (!comm.recv(msg, RANK_ALL, tag, &sender)) {
#ifdef TRACE_MP
            ActPrintLog(activity, "HDIST MP recv failed");
#endif
            return (unsigned)-1;
        }
#ifdef TRACE_MP
        waiting = comm.probe(RANK_ALL,tag,NULL);
        ActPrintLog(activity, "HDIST MP received %d from %d reply tag %d, waiting %d",msg.length(), (int)sender, (int)msg.getReplyTag(),waiting);
#endif
        size32_t sz=msg.length();
        while (sz) {
            sz--;
            byte flag = *((byte *)msg.toByteArray()+sz);
            msg.setLength(sz);
            if (flag==1) {
                // want an ack, so send
                flag = stopping?0:1;
                ack.clear().append(flag);
#ifdef _FULL_TRACE
                ActPrintLog("HDIST MP sent CTS to %d",(int)sender);
#endif
                comm.send(ack,sender,msg.getReplyTag());
                if (sz!=0)  // wasn't an RTS so return data
                    break;
                if (stopping)
                    goto Restart;
                // receive the data from whoever sent RTS
                msg.clear();
                comm.recv(msg,sender,tag);
                sz = msg.length();
#ifdef _FULL_TRACE
                ActPrintLog("HDIST MP received block from %d size %d",(int)sender,sz);
#endif
            }
            else
                break;
        }
        return sender-1;
    }

    virtual bool sendBlock(unsigned i, CMessageBuffer &msg)
    {
#ifdef TRACE_MP
        ActPrintLog(activity, "HDIST MP send(%d,%d,%d)",i+1,(int)tag,msg.length());
#endif
        byte flag=0;

        // if 0 length then eof so don't send RTS
        if (msg.length()>0) {
            // send request to send
            CMessageBuffer rts;
            flag = 1;               // want ack
            rts.append(flag);
#ifdef _FULL_TRACE
            ActPrintLog(activity, "HDIST MP sending RTS to %d",i+1);
#endif

            comm.sendRecv(rts, i+1, tag);
            rts.read(flag);
#ifdef _FULL_TRACE
            ActPrintLog(activity, "HDIST MP got CTS from %d, %d",i+1,(int)flag);
#endif
            if (flag==0)
                return false;           // other end stopped
            flag = 0;                   // no ack
            msg.append(flag);
        }
        if (flag==0) {                  // no ack
            comm.send(msg, i+1, tag);
            return true;
        }
        // this branch not yet used
        assertex(false);
        comm.sendRecv(msg, i+1, tag);
        msg.read(flag);             // whether stopped
        return flag!=0;
    }

    virtual void stopRecv()
    {
#ifdef TRACE_MP
        ActPrintLog(activity, "HDIST MP stopRecv");
#endif
        stopping = true;
    }

    void startTX()
    {
        // not used
    }
};

class CRowPullDistributor: public CDistributorBase
{
    struct cBuf
    {
        cBuf *next;
        offset_t pos;
        size32_t size;
    };

    ICommunicator &comm;
    mptag_t tag;
    CriticalSection sect;
    cBuf **diskcached;
    CMessageBuffer *bufs;
    bool *hasbuf;
    Owned<IFile> cachefile;
    Owned<IFileIO> cachefileio;
    offset_t diskpos;
    mptag_t *waiting;
    bool *donerecv;
    bool *donesend;
    Semaphore selfready;
    Semaphore selfdone;
    bool stopping;

    class cTxThread: public Thread
    {
        CRowPullDistributor &parent;
    public:
        cTxThread(CRowPullDistributor &_parent)
            : Thread("CRowPullDistributor::cTxThread"),parent(_parent)
        {
        }
        int run()
        {
            parent.txrun();
            return 0;
        }
    } *txthread;

    class cSortedDistributeMerger: public CSimpleInterface, implements IRowProvider
    {
        CDistributorBase &parent;
        Owned<IRowStream> out;
        MemoryBuffer *bufs;
        CThorStreamDeserializerSource *dszs;
        unsigned numnodes;
        ICompare &cmp;
        IEngineRowAllocator &allocator;
        PtrElemAllocator &ptrallocator;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        cSortedDistributeMerger(CDistributorBase &_parent,unsigned &_numnodes,ICompare &_cmp, IEngineRowAllocator &_allocator, PtrElemAllocator &_ptrallocator)
            : parent(_parent),cmp(_cmp), allocator(_allocator), ptrallocator(_ptrallocator)
        {
            numnodes = _numnodes;
            bufs = new MemoryBuffer[numnodes];
            dszs = new CThorStreamDeserializerSource[numnodes];
            for (unsigned node=0; node < numnodes; node++)
            {
                Owned<ISerialStream> stream = createMemoryBufferSerialStream(bufs[node]);
                dszs[node].setStream(stream);
            }
            out.setown(createRowStreamMerger(numnodes,*this,&cmp));
        }

        ~cSortedDistributeMerger()
        {
            out.clear();
            delete [] dszs;
            delete [] bufs;
        }

        inline IRowStream &merged() { return *out; }

        void linkRow(const void *row) {}
        void releaseRow(const void *row) {}

        const void *nextRow(unsigned idx)
        {
            assertex(idx<numnodes);
            if (!dszs[idx].eos()) 
                return ptrallocator.deserializeRow(&allocator,dszs[idx]);
            CMessageBuffer mb;
            mb.swapWith(bufs[idx]);
            mb.clear();
            bufs[idx].clear();
            parent.recvBlock(mb,idx);
            if (mb.length()==0)
                return NULL;
            fastLZDecompressToBuffer(bufs[idx],mb);
            return nextRow(idx);
        }

        void stop(unsigned idx)
        {
            assertex(idx<numnodes);
            bufs[idx].resetBuffer();
        }

    };
    void clean()
    {
        for (unsigned i=0;i<numnodes;i++)
        {
            while (diskcached[i]) {
                cBuf *cb = diskcached[i];
                diskcached[i] = cb->next;
                delete cb;
            }
            bufs[i].clear();
            waiting[i] = TAG_NULL;
        }
        memset(diskcached, 0, sizeof(cBuf *)*numnodes);
        memset(hasbuf, 0, sizeof(bool)*numnodes);
        memset(donerecv, 0, sizeof(bool)*numnodes);
        memset(donesend, 0, sizeof(bool)*numnodes);

        if (cachefileio.get()) // not sure really have to
        {
            cachefileio.clear();
            cachefile->remove();
        }
    }
public:
    CRowPullDistributor(CActivityBase *activity, ICommunicator &_comm, mptag_t _tag, IRowInterfaces *_rowif,const bool &abort, bool dedup, IStopInput *istop)
        : CDistributorBase(activity, _rowif, abort, dedup, istop), comm(_comm), tag(_tag)
    {
        pull = true;
        tag = _tag;
        numnodes = comm.queryGroup().ordinality()-1;
        self = comm.queryGroup().rank()-1;
        diskcached = (cBuf **)calloc(numnodes,sizeof(cBuf *));
        bufs = new CMessageBuffer[numnodes];
        hasbuf = (bool *)calloc(numnodes,sizeof(bool));
        donerecv = (bool *)calloc(numnodes,sizeof(bool));
        donesend = (bool *)calloc(numnodes,sizeof(bool));
        waiting = (mptag_t *)malloc(sizeof(mptag_t)*numnodes);
        for (unsigned i=0;i<numnodes;i++)
            waiting[i] = TAG_NULL;
        txthread = NULL;
        stopping = false;
        diskpos = 0;
    }
    ~CRowPullDistributor()
    {
        stop();
        for (unsigned i=0;i<numnodes;i++) {
            while (diskcached[i]) {
                cBuf *cb = diskcached[i];
                diskcached[i] = cb->next;
                delete cb;
            }
        }
        free(diskcached);
        diskcached = NULL;
        delete [] bufs;
        free(hasbuf);
        free(donesend);
        free(donerecv);
        free(waiting);
        if (cachefileio.get()) {
            cachefileio.clear();
            cachefile->remove();
        }
    }
    virtual IRowStreamWithMetaData *doconnect(size32_t metasize, IHash *ihash, ICompare *icompare)
    {
        clean();
        return CDistributorBase::doconnect(metasize, ihash, icompare);
    }
    void recvloop()
    {
        class cCmp: public ICompare
        {
            ICompare *compare;
        public:
            cCmp(ICompare *_compare)
            {
                compare = _compare;
            }
            int docompare(const void * left, const void * right) const
            {
                return compare->docompare(((const PtrElem *)left)->queryRow(),((const PtrElem *)right)->queryRow());
            }
        } cmp(queryCompare());
        try {
            Owned<cSortedDistributeMerger> merger = new cSortedDistributeMerger(*this,numnodes,cmp,*queryAllocator(),queryPtrAllocator());
            ActPrintLog(activity, "Read loop start");
            loop {
                const void *row = merger->merged().nextRow();
                if (!row)
                    break;
                output().putRow(row);  
            }
        }
        catch (IException *e)
        {
            ActPrintLog(activity, e, "HDIST: recvloop");
            setRecvExc(e);
        }
    }


    virtual bool sendBlock(unsigned i, CMessageBuffer &msg)
    {
        CriticalBlock block(sect);
        if (donesend[i]&&(msg.length()==0)) {
            return false;
        }
        assertex(!donesend[i]);
        bool done = msg.length()==0;
        if (!hasbuf[i]) {
            hasbuf[i] = true;
            bufs[i].swapWith(msg);
            if (i==self)
                selfready.signal();
            else
                doSend(i);
        }
        else {
            size32_t sz = msg.length();
            if (sz==0) {
                assertex(bufs[i].length()!=0);
            }
            else {
                if (!cachefileio.get()) {
                    StringBuffer tempname;
                    GetTempName(tempname,"hashdistspill",true);
                    cachefile.setown(createIFile(tempname.str()));
                    cachefileio.setown(cachefile->open(IFOcreaterw));
                    if (!cachefileio)
                        throw MakeStringException(-1,"CRowPullDistributor: Could not create disk cache");
                    diskpos = 0;
                    ActPrintLog(activity, "CRowPullDistributor spilling to %s",tempname.str());
                }
                cachefileio->write(diskpos,sz,msg.bufferBase());
            }
            cBuf *cb = new cBuf;
            cb->pos = diskpos;
            cb->size = sz;
            diskpos += sz;
            cBuf *prev = diskcached[i];
            if (prev) {
                while (prev->next)
                    prev = prev->next;
                prev->next = cb;
            }
            else
                diskcached[i] = cb;
            cb->next = NULL;
            msg.clear();
        }
        if (done)
            donesend[i] = true;
        return true;
    }
    virtual unsigned recvBlock(CMessageBuffer &msg,unsigned i)
    {
        assertex(i<numnodes);
        if (i==self) {
            msg.clear();
            selfready.wait();
        }
        else {
            msg.clear().append((byte)1); // rts
            if (!comm.sendRecv(msg, i+1, tag)) {
                return i;
            }
        }

        CriticalBlock block(sect);
        assertex(!donerecv[i]);
        if (self==i) {
            if (stopping)
                return (unsigned)-1;
            if (hasbuf[i]) {
                bufs[i].swapWith(msg);
                cBuf *cb = diskcached[i];
                if (cb) {
                    diskcached[i] = cb->next;
                    if (cb->size)
                        cachefileio->read(cb->pos,cb->size,bufs[i].reserve(cb->size));
                    delete cb;
                    selfready.signal(); // next time round
                }
                else
                    hasbuf[i] = false;
            }
        }
        if (msg.length()==0) {
            donerecv[i] = true;
            if (i==self)
                selfdone.signal();
            else {
                // confirm done
                CMessageBuffer confirm;
                comm.send(confirm, i+1, tag);
            }
        }
#ifdef TRACE_MP
        ActPrintLog(activity, "HDIST MPpull recv done(%d)",i);
#endif
        return i;
    }
    bool doSend(unsigned target)
    {
        // called in crit
        if (hasbuf[target]&&(waiting[target]!=TAG_NULL)) {
#ifdef TRACE_MP
            ActPrintLog(activity, "HDIST MP dosend(%d,%d)",i,bufs[target].length());
#endif
            size32_t sz = bufs[target].length();
            // TBD compress here?

            comm.send(bufs[target],(rank_t)target+1,waiting[target]);
            waiting[target]=TAG_NULL;
            bufs[target].clear();
            cBuf *cb = diskcached[target];
            if (cb) {
                diskcached[target] = cb->next;
                if (cb->size)
                    cachefileio->read(cb->pos,cb->size,bufs[target].reserve(cb->size));
                delete cb;
            }
            else
                hasbuf[target] = false;
            if (!sz) {
                assertex(!hasbuf[target]);
            }
            return true;
        }
        return false;
    }
    void txrun()
    {
        CriticalBlock block(sect);
        unsigned done = 1; // self not sent
        while (done<numnodes) {
            rank_t sender;
            CMessageBuffer rts;
            {
                CriticalUnblock block(sect);
                if (!comm.recv(rts, RANK_ALL, tag, &sender)) {
                    return;
                }

            }
            if (rts.length()==0) {
                done++;
            }
            else {
                unsigned i = (unsigned)sender-1;
                assertex(i<numnodes);
                assertex(waiting[i]==TAG_NULL);
                waiting[i] = rts.getReplyTag();
                doSend(i);
            }
        }
    }
    void startTX()
    {
        delete txthread;
        txthread = new cTxThread(*this);
        txthread->start();
    }

    virtual void join() // probably does nothing
    {
        CDistributorBase::join();
        if (txthread) {
            txthread->join();
            delete txthread;
            txthread = NULL;
        }
    }
    void stop()
    {
        selfdone.wait();
        if (txthread) {
            txthread->join();
            delete txthread;
        }
    }
    virtual void stopRecv()
    {
#ifdef TRACE_MP
        ActPrintLog(activity, "HDIST MPpull stopRecv");
#endif
        stopping = true;
        selfready.signal();
    }
};

//==================================================================================================
// Activity Implementation
//==================================================================================================


IHashDistributor *createHashDistributor(CActivityBase *activity, ICommunicator &comm, mptag_t tag, IRowInterfaces *_rowif, const bool &abort,bool dedup,IStopInput *istop)
{
    return new CRowDistributor(activity, comm, tag, _rowif, abort, dedup, istop);
}

IHashDistributor *createPullHashDistributor(CActivityBase *activity, ICommunicator &comm, mptag_t tag, IRowInterfaces *_rowif, const bool &abort,bool dedup,IStopInput *istop)
{
    return new CRowPullDistributor(activity, comm, tag,  _rowif, abort, dedup, istop);
}


#ifdef _MSC_VER
#pragma warning(pop)
#endif



#ifdef _MSC_VER
#pragma warning(push)
#pragma warning( disable : 4355 ) // 'this' : used input base member initializer list
#endif
class HashDistributeSlaveBase : public CSlaveActivity, public CThorDataLink, implements IStopInput
{
    IHashDistributor *distributor;
    IThorDataLink *input;
    Owned<IRowStream> out;
    bool inputstopped;
    CriticalSection stopsect;
    mptag_t mptag;

public:
    Owned<IRowStream> instrm;
    IHash *ihash;
    ICompare *mergecmp;     // if non-null is merge distribute
    bool eofin;

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    HashDistributeSlaveBase(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        input = NULL;
        eofin = false;
        mptag = TAG_NULL;
        distributor = NULL;
        mergecmp = NULL;
        ihash = NULL;
    }
    ~HashDistributeSlaveBase()
    {
        out.clear();
        if (distributor)
        {
            distributor->disconnect(false);
            distributor->removetemp();
            distributor->join();
            distributor->Release();
        }
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        mptag = container.queryJob().deserializeMPTag(data);
        ActPrintLog("HASHDISTRIB: %sinit tag %d",mergecmp?"merge, ":"",(int)mptag);
        if (mergecmp)
            distributor = createPullHashDistributor(this, container.queryJob().queryJobComm(), mptag, this, abortSoon, false, this);
        else
            distributor = createHashDistributor(this, container.queryJob().queryJobComm(), mptag, this, abortSoon, false, this);
        inputstopped = true;
    }
    void stopInput()
    {
        CriticalBlock block(stopsect);  // can be called async by distribute
        if (!inputstopped) {
            CSlaveActivity::stopInput(input);
            inputstopped = true;
        }
    }
    void start(bool passthrough)
    {
        // bit messy
        eofin = false;
        if (!instrm.get()) {    // derived class may override
            input = inputs.item(0);
            startInput(input);
            inputstopped = false;
            instrm.set(input);
            if (passthrough)
                out.set(instrm);
        }
        else if (passthrough) {
            out.set(instrm);
        }
        if (!passthrough) {
            out.setown(distributor->connect(instrm,ihash,mergecmp));
        }
        dataLinkStart("HASHDISTRIB", container.queryId());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        start(false);
    }
    void stop()
    {
        ActPrintLog("HASHDISTRIB: stopping");
        if (out) {
            out->stop();
            out.clear();
        }
        if (distributor) {
            distributor->disconnect(true);
            distributor->removetemp();
            distributor->join();
        }
        stopInput();
        instrm.clear();
        dataLinkStop();
    }
    void kill()
    {
        ActPrintLog("HASHDISTRIB: kill");
        CSlaveActivity::kill();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL); // careful not to call again in derivatives
        if (abortSoon||eofin) {
            eofin = true;
            return NULL;
        }
        OwnedConstThorRow row = out->ungroupedNextRow();
        if (!row.get()) {
            eofin =  true;
            return NULL;
        }
        dataLinkIncrement();
        return row.getClear();
    }
    virtual bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canStall = true; // currently
        info.unknownRowsOutput = true; // mixed about
    }
};


//===========================================================================

class HashDistributeSlaveActivity : public HashDistributeSlaveBase
{
public:
    HashDistributeSlaveActivity(CGraphElementBase *container) : HashDistributeSlaveBase(container) { }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        HashDistributeSlaveBase::init(data, slaveData);
        IHThorHashDistributeArg *distribargs = (IHThorHashDistributeArg *)queryHelper();
        ihash = distribargs->queryHash();
    }
};

//===========================================================================

class HashDistributeMergeSlaveActivity : public HashDistributeSlaveBase
{
public:
    HashDistributeMergeSlaveActivity(CGraphElementBase *container) : HashDistributeSlaveBase(container) { }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        IHThorHashDistributeArg *distribargs = (IHThorHashDistributeArg *)queryHelper();
        mergecmp = distribargs->queryMergeCompare();
        HashDistributeSlaveBase::init(data, slaveData);
        ihash = distribargs->queryHash();
    }

};

//===========================================================================
class CHDRproportional: public CSimpleInterface, implements IHash
{
    CActivityBase *activity;
    Owned<IFile> tempfile;
    Owned<IRowStream> tempstrm;
    IHThorHashDistributeArg *args;
    mptag_t statstag;
    offset_t *sizes;
    double skew;
    offset_t tot;
    unsigned self;
    unsigned n;
    Owned<IOutputRowSerializer> serializer;

    int isSkewed(offset_t av, offset_t sz)
    {
        double r = ((double)sz-(double)av)/(double)av;
        if (r>=skew)
            return 1;
        if (-r>skew)
            return -1;
        return 0;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CHDRproportional(CActivityBase *_activity, IHThorHashDistributeArg *_args,mptag_t _mastertag) : activity(_activity)
    {
        args = _args;
        statstag = _mastertag;
        sizes = NULL;
        skew = args->getSkew();
        tot = 0;
    }

    ~CHDRproportional()
    {
        try {
            tempstrm.clear();
            if (tempfile)
                tempfile->remove();
        }
        catch (IException *e) {
            EXCLOG(e,"REDISTRIBUTE");
            e->Release();
        }
        free(sizes);
    }

    IRowStream *calc(CSlaveActivity *activity, IThorDataLink *in,bool &passthrough)
    {
        // first - find size
        serializer.set(activity->queryRowSerializer());
        Owned<IRowStream> ret;
        passthrough = true;
        n = activity->queryContainer().queryJob().querySlaves();
        self = activity->queryContainer().queryJob().queryJobComm().queryGroup().rank()-1;
        ThorDataLinkMetaInfo info;
        in->getMetaInfo(info);
        offset_t sz = info.byteTotal;
        if (sz==(offset_t)-1) {
            // not great but hopefully exception not rule!
            sz = 0;
            StringBuffer tempname;
            GetTempName(tempname,"hdprop",true); // use alt temp dir
            tempfile.setown(createIFile(tempname.str()));
            {
                ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "REDISTRIBUTE size unknown, spilling to disk");
                MemoryAttr ma;
                activity->startInput(in);
                Owned<IExtRowWriter> out = createRowWriter(tempfile,serializer,activity->queryRowAllocator(),false, false, false);
                if (!out)
                    throw MakeStringException(-1,"Could not created file %s",tempname.str());
                loop {
                    const void * row = in->ungroupedNextRow();
                    if (!row)
                        break;
                    out->putRow(row);
                }
                out->flush();
                sz = out->getPosition();
                activity->stopInput(in);
            }
            ret.setown(createSimpleRowStream(tempfile,activity));
        }
        CMessageBuffer mb;
        mb.append(sz);
        ActPrintLog(activity, "REDISTRIBUTE sending size %"I64F"d to master",sz);
        if (!activity->queryContainer().queryJob().queryJobComm().send(mb, (rank_t)0, statstag)) {
            ActPrintLog(activity, "REDISTRIBUTE send to master failed");
            throw MakeStringException(-1, "REDISTRIBUTE send to master failed");
        }
        mb.clear();
        if (!activity->queryContainer().queryJob().queryJobComm().recv(mb, (rank_t)0, statstag)) {
            ActPrintLog(activity, "REDISTRIBUTE recv from master failed");
            throw MakeStringException(-1, "REDISTRIBUTE recv from master failed");
        }
        ActPrintLog(activity, "REDISTRIBUTE received sizes from master");
        offset_t *insz = (offset_t *)mb.readDirect(n*sizeof(offset_t));
        sizes = (offset_t *)calloc(n,sizeof(offset_t));
        offset_t tsz=0;
        unsigned i;
        // each node does this calculation (and hopefully each gets same results!)
        for (i=0;i<n;i++) {
            tsz += insz[i];
        }
        offset_t avg = tsz/n;
        for (i=0;i<n;i++) {
            offset_t s = insz[i];
            int cmp = isSkewed(avg,s);
            if (cmp!=0)
                passthrough = false; // currently only pass through if no skews
            if (cmp>0) {
                offset_t adj = s-avg;
                for (unsigned j=(i+1)%n;j!=i;) {
                    offset_t t = insz[j];
                    cmp = isSkewed(avg,t);
                    if (cmp<0) {
                        offset_t adj2 = (avg-t);
                        if (adj2>adj)
                            adj2 = adj;
                        if (i==self)
                            sizes[j] = adj2;
                        insz[j] += adj2;
                        adj -= adj2;
                        s -= adj2;
                        if (adj==0)
                            break;
                    }
                    j++;
                    if (j==n)
                        j = 0;
                }
            }
            // check if that redistrib did the trick
            cmp = isSkewed(avg,s);
            if (cmp>0) { // ok redistribute to the *non* skewed nodes now
                offset_t adj = s-avg;
                for (unsigned j=(i+1)%n;j!=i;) {
                    offset_t t = insz[j];
                    if (t<avg) {
                        offset_t adj2 = (avg-t);
                        if (adj2>adj)
                            adj2 = adj;
                        if (i==self)
                            sizes[j] = adj2;
                        insz[j] += adj2;
                        adj -= adj2;
                        s -= adj2;
                        if (adj==0)
                            break;
                    }
                    j++;
                    if (j==n)
                        j = 0;
                }
            }

            insz[i] = s;
            if (i==self)
                sizes[i] = s;
        }
        for (i=0;i<n;i++) {
#ifdef _DEBUG
            ActPrintLog(activity, "after Node %d has %"I64F"d",i, insz[i]);
#endif
        }
        tot = 0;
        for (i=0;i<n;i++) {
            if (sizes[i]) {
                if (i==self)
                    ActPrintLog(activity, "Keep %"I64F"d local",sizes[i]);
                else
                    ActPrintLog(activity, "Redistribute %"I64F"d to %d",sizes[i],i);
            }
            tot += sizes[i];
        }
        return ret.getClear();
    }

    unsigned hash(const void *row)
    {
        if (tot<=1)
            return self;
        offset_t r = getRandom();
        if (tot>(unsigned)-1) {
            r = (r<<31);            // 31 because cautious of sign
            r ^= getRandom();
        }
        r %= tot;
        CSizingSerializer ssz;
        serializer->serialize(ssz,(const byte *)row);
        size32_t rs = ssz.size();
        tot -= rs;
        for (unsigned i=0;i<n;i++) {
            offset_t v = sizes[i];
            if ((v>=r)&&(v>=rs)) {
                sizes[i] -= rs;
                return i;
            }
            r -= v;
        }
        if (sizes[self]<rs)
            sizes[self] = 0;
        else
            sizes[self] -= rs;
        return self;
    }

};


class ReDistributeSlaveActivity : public HashDistributeSlaveBase
{
    Owned<CHDRproportional> partitioner;
public:
    ReDistributeSlaveActivity(CGraphElementBase *container) : HashDistributeSlaveBase(container) { }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        HashDistributeSlaveBase::init(data, slaveData);
        mptag_t tag = container.queryJob().deserializeMPTag(data);
        IHThorHashDistributeArg *distribargs = (IHThorHashDistributeArg *)queryHelper();
        partitioner.setown(new CHDRproportional(this, distribargs,tag));
        ihash = partitioner;
    }

    void start()
    {
        bool passthrough;
        {
            ActivityTimer s(totalCycles, timeActivities, NULL);
            instrm.setown(partitioner->calc(this,inputs.item(0),passthrough));  // may return NULL
        }
        HashDistributeSlaveBase::start(passthrough);
    }

    void stop()
    {
        HashDistributeSlaveBase::stop();
        if (instrm) {
            instrm.clear();
            // should remove here rather than later?
        }
    }
};

//===========================================================================

class IndexDistributeSlaveActivity : public HashDistributeSlaveBase
{
    struct _tmp { virtual ~_tmp() { } };
    class CKeyLookup : implements IHash, public _tmp
    {
        IndexDistributeSlaveActivity &owner;
        Owned<IKeyIndex> tlk;
        Owned<IKeyManager> tlkManager;
        IHThorKeyedDistributeArg *helper;
        unsigned numslaves;
    public:
        CKeyLookup(IndexDistributeSlaveActivity &_owner, IHThorKeyedDistributeArg *_helper, IKeyIndex *_tlk)
            : owner(_owner), helper(_helper), tlk(_tlk)
        {
            tlkManager.setown(createKeyManager(tlk, tlk->keySize(), NULL));
            numslaves = owner.queryContainer().queryJob().querySlaves();
        }
        unsigned hash(const void *data)
        {
            helper->createSegmentMonitors(tlkManager, data);
            tlkManager->finishSegmentMonitors();
            tlkManager->reset();
            verifyex(tlkManager->lookup(false));
            tlkManager->releaseSegmentMonitors();
            offset_t partNo = tlkManager->queryFpos();
            if (partNo)
                partNo--; // note that partNo==0 means lower than anything in the key - should be treated same as partNo==1 here
            return ((unsigned)partNo % numslaves);
        }
    };
    _tmp *lookup;

public:
    IndexDistributeSlaveActivity(CGraphElementBase *container) : HashDistributeSlaveBase(container), lookup(NULL)
    {
    }
    ~IndexDistributeSlaveActivity()
    {
        if (lookup)
        {
            delete lookup;
            ihash = NULL;
        }
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        HashDistributeSlaveBase::init(data, slaveData);

        IHThorKeyedDistributeArg *helper = (IHThorKeyedDistributeArg *) queryHelper();

        offset_t tlkSz;
        data.read(tlkSz);
        Owned<IFileIO> iFileIO = createIFileI((size32_t)tlkSz, data.readDirect((size32_t)tlkSz));

        StringBuffer name(helper->getIndexFileName());
        name.append("_tlk"); // MORE - this does not look right!
        CKeyLookup *l = new CKeyLookup(*this, helper, createKeyIndex(name.str(), 0, *iFileIO, true, false)); // MORE - crc is not 0...
        ihash = l;
        lookup = l;

    }
friend class CKeyLookup;
};

//===========================================================================


class HashDedupSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    IRowStream *input;      // can be changed
    bool inputstopped;
    const char *actTxt;
    CThorRowArray htabrows;
    const void **htab;
    IHThorHashDedupArg *dedupargs;
    unsigned htsize;
    unsigned htremaining;   // remaining slots left
    ICompare *icompare;
    IHash *ihash;
    ICompare *ikeycompare;
    Owned<IEngineRowAllocator> keyallocator;
    Owned<IOutputRowSerializer> keyserializer;
public:

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    HashDedupSlaveActivityBase(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        htsize = 0;
        inputstopped = false;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        dedupargs = (IHThorHashDedupArg *)queryHelper();
        ihash = dedupargs->queryHash();
        icompare = dedupargs->queryCompare();
        appendOutputLinked(this);
        IOutputMetaData* km = dedupargs->queryKeySize();
        if (km&&(km!=dedupargs->queryOutputMeta())) {
            ikeycompare = dedupargs->queryKeyCompare();
            keyallocator.setown(createThorRowAllocator(km,queryActivityId()));
            keyserializer.setown(km->createRowSerializer(queryCodeContext(),queryActivityId()));
            htabrows.setSizing(true,true);
        }
        else {
            htabrows.setSizing(true,true);
            ikeycompare = NULL;
        }
        htabrows.setMaxTotal(queryLargeMemSize());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        inputstopped = false;
        input = inputs.item(0);
        htabrows.clear();
        htsize = 0;
        startInput(inputs.item(0));
        dataLinkStart(actTxt, container.queryId());
    }
    bool eqKey(const void *k1, const void *k2)
    {
        if (ikeycompare)
            return (ikeycompare->docompare(k1,k2)==0);
        return (icompare->docompare(k1,k2)==0);
    }
    bool addHash(const void *row)
    {
        // NB assume key size constant
        // TBD use CThorRowArray with sizing better?
        OwnedConstThorRow key;
        if (keyallocator) {
            RtlDynamicRowBuilder krow(keyallocator);
            size32_t sz = dedupargs->recordToKey(krow,row);
            assertex(sz);
            key.setown(krow.finalizeRowClear(sz));
        }
        else
            key.set(row);
        if (htsize==0) {
            CSizingSerializer ssz;
            if (keyserializer)
                keyserializer->serialize(ssz,(const byte *)key.get());
            else
                queryRowSerializer()->serialize(ssz,(const byte *)row);
            // following is very rough guess of how many will fit in memory (will work best for fixed)
            size32_t ks = ssz.size();
            size32_t divsz = (ks<16)?16:ks;
            // JCSMORE if child query assume low memory usage??
            unsigned total = (container.queryOwnerId() ? (queryLargeMemSize()/10) : queryLargeMemSize()) /(divsz+sizeof(void *)*3);
            htsize = total+10;
            ActPrintLog("%s: reserving hash table of size %d",actTxt,htsize);
            htabrows.reserve(htsize);
            htab = (const void **)htabrows.base();
            htremaining = htsize*9/10;
        }
        unsigned h = ihash->hash(row)%htsize;
        loop {
            const void *htk=htab[h];
            if (!htk)
                break;
            if (eqKey(htk,key))
                return false;
            if (++h==htsize)
                h = 0;
        }
        if (htremaining==0)
            throw MakeActivityException(this, TE_TooMuchData, "%s: hash table overflow (out of memory)",actTxt);
        htremaining--;

        htabrows.setRow(h,key.getClear());
        return true;
    }
    void stopInput()
    {
        if (!inputstopped) {
            CSlaveActivity::stopInput(inputs.item(0));
            inputstopped = true;
        }
    }
    void kill()
    {
        ActPrintLog("%s: kill", actTxt);
        htabrows.clear();
        CSlaveActivity::kill();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while (!abortSoon) {
            OwnedConstThorRow row = input->ungroupedNextRow();
            if (!row)
                break;
            if (addHash(row)) {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }

    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
};


class LocalHashDedupSlaveActivity : public HashDedupSlaveActivityBase
{

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    LocalHashDedupSlaveActivity(CGraphElementBase *container)
        : HashDedupSlaveActivityBase(container)
    {
        actTxt = "LOCALHASHDEDUP";
    }

    ~LocalHashDedupSlaveActivity()
    {
    }


    void stop()
    {
        ActPrintLog("%s: stopping", actTxt);
        stopInput();
        dataLinkStop();
    }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
    }
};

class GlobalHashDedupSlaveActivity : public HashDedupSlaveActivityBase, implements IStopInput
{
    mptag_t mptag;
    CriticalSection stopsect;
    IHashDistributor *distributor;
    Owned<IRowStream> instrm;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    GlobalHashDedupSlaveActivity(CGraphElementBase *container)
        : HashDedupSlaveActivityBase(container)
    {
        actTxt = "HASHDEDUP";
        distributor = NULL;
        mptag = TAG_NULL;
    }

    ~GlobalHashDedupSlaveActivity()
    {
        instrm.clear();
        if (distributor)
        {
            distributor->disconnect(false);
            distributor->removetemp();
            distributor->join();
            distributor->Release();
        }
    }

    void stopInput()
    {
        CriticalBlock block(stopsect);  // can be called async by distribute
        HashDedupSlaveActivityBase::stopInput();
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        HashDedupSlaveActivityBase::init(data, slaveData);
        mptag = container.queryJob().deserializeMPTag(data);
        distributor = createHashDistributor(this, container.queryJob().queryJobComm(), mptag, this, abortSoon,true, this);
    }

    void start()
    {
        HashDedupSlaveActivityBase::start();
        ActivityTimer s(totalCycles, timeActivities, NULL);
        instrm.setown(distributor->connect(input,ihash,icompare));
        input = instrm.get();
    }

    void stop()
    {
        ActPrintLog("%s: stopping", actTxt);
        if (instrm) {
            instrm->stop();
            instrm.clear();
        }
        distributor->disconnect(true);
        distributor->removetemp();
        distributor->join();
        stopInput();
        dataLinkStop();
    }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canStall = true;
        info.unknownRowsOutput = true;
    }
};

//===========================================================================


class HashJoinSlaveActivity : public CSlaveActivity, public CThorDataLink, implements IStopInput
{
    IThorDataLink *inL;
    IThorDataLink *inR;
    MemoryBuffer ptrbuf;
    IHThorHashJoinArg *joinargs;
    Owned<IJoinHelper> joinhelper;
    bool eof;
    Owned<IRowStream> strmL;
    Owned<IRowStream> strmR;
    Owned<IThorRowSortedLoader> loaderL;
    Owned<IThorRowSortedLoader> loaderR;
    CriticalSection joinHelperCrit;
    CriticalSection stopsect;
    rowcount_t lhsProgressCount;
    rowcount_t rhsProgressCount;
    bool inputLstopped;
    bool inputRstopped;
    bool leftdone;
    mptag_t mptag;
    mptag_t mptag2;
    CThorRowArray rows;

public:

    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    HashJoinSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        lhsProgressCount = rhsProgressCount = 0;
        mptag = TAG_NULL;
        mptag2 = TAG_NULL;
    }
    ~HashJoinSlaveActivity()
    {
        strmL.clear();
        strmR.clear();
        joinhelper.clear();
        loaderL.clear();
        loaderR.clear();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        joinargs = (IHThorHashJoinArg *)queryHelper();
        appendOutputLinked(this);
        mptag = container.queryJob().deserializeMPTag(data);
        mptag2 = container.queryJob().deserializeMPTag(data);
        ActPrintLog("HASHJOIN: init tags %d,%d",(int)mptag,(int)mptag2);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        inputLstopped = true;
        inputRstopped = true;
        leftdone = false;
        eof = false;
        ActPrintLog("HASHJOIN: starting");
        inL = inputs.item(0);
        startInput(inL);
        inputLstopped = false;
        inR = inputs.item(1);
        startInput(inR);
        inputRstopped = false;
        IHash *ihashL = joinargs->queryHashLeft();
        IHash *ihashR = joinargs->queryHashRight();
        ICompare *icompareL = joinargs->queryCompareLeft();
        ICompare *icompareR = joinargs->queryCompareRight();
        Owned<IHashDistributor> distributor;
        distributor.setown(createHashDistributor(this, container.queryJob().queryJobComm(), mptag, queryRowInterfaces(inL), abortSoon,false, this));
        Owned<IRowStream> reader = distributor->connect(inL,ihashL,icompareL);
        loaderL.setown(createThorRowSortedLoader(rows));
        bool isemptylhs;
        strmL.setown(loaderL->load(reader,queryRowInterfaces(inL),icompareL,true,abortSoon,isemptylhs,"HASHJOIN(L)",true));
        reader.clear();
        stopInputL();
        distributor->disconnect(false);
        distributor->removetemp();
        distributor->join();
        distributor.clear();
        rows.clear();
        loaderR.setown(createThorRowSortedLoader(rows));
        leftdone = true;
        distributor.setown(createHashDistributor(this, container.queryJob().queryJobComm(), mptag2, queryRowInterfaces(inR), abortSoon,false, this));
        reader.setown(distributor->connect(inR,ihashR,icompareR));
        bool isemptyrhs;
        strmR.setown(loaderR->load(reader,queryRowInterfaces(inR),icompareR,false,abortSoon,isemptyrhs,"HASHJOIN(R)",true));
        reader.clear();
        stopInputR();
        distributor->disconnect(false);
        distributor->removetemp();
        distributor->join();
        distributor.clear();
        { CriticalBlock b(joinHelperCrit);
            switch(container.getKind())
            {
                case TAKhashjoin:
                    {
                        bool hintparallelmatch = container.queryXGMML().getPropInt("hint[@name=\"parallel_match\"]")!=0;
                        bool hintunsortedoutput = container.queryXGMML().getPropInt("hint[@name=\"unsorted_output\"]")!=0;
                        joinhelper.setown(createJoinHelper(joinargs, "HASHJOIN", container.queryId(), queryRowAllocator(),hintparallelmatch,hintunsortedoutput));
                    }
                    break;
                case TAKhashdenormalize:
                case TAKhashdenormalizegroup:
                    joinhelper.setown(createDenormalizeHelper(joinargs, "HASHDENORMALIZE", container.getKind(), container.queryId(), queryRowAllocator()));
                    break;
                default:
                    throwUnexpected();
            }
        }
        joinhelper->init(strmL, strmR, ::queryRowAllocator(inL), ::queryRowAllocator(inR), ::queryRowMetaData(inL), &abortSoon);
        dataLinkStart("HASHJOIN", container.queryId());
    }
    void stopInput()
    {
        if (leftdone)
            stopInputR();
        else
            stopInputL();
    }
    void stopInputL()
    {
        CriticalBlock block(stopsect);  // can be called async by distribute
        if (!inputLstopped) {
            CSlaveActivity::stopInput(inL);
            inputLstopped = true;
        }
    }
    void stopInputR()
    {
        CriticalBlock block(stopsect);  // can be called async by distribute
        if (!inputRstopped) {
            CSlaveActivity::stopInput(inR);
            inputRstopped = true;
        }
    }
    void stop()
    {
        ActPrintLog("HASHJOIN: stopping");
        stopInputL();
        stopInputR();
        lhsProgressCount = joinhelper->getLhsProgress();
        rhsProgressCount = joinhelper->getRhsProgress();
        strmL.clear();
        strmR.clear();
        joinhelper.clear();
        dataLinkStop();
    }
    void kill()
    {
        ActPrintLog("HASHJOIN: kill");
        rows.clear();
        CSlaveActivity::kill();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!eof) {
            OwnedConstThorRow row = joinhelper->nextRow();
            if (row) {
                dataLinkIncrement();
                return row.getClear();
            }
            eof = true;
        }
        return NULL;
    }
    virtual bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canStall = true;
        info.unknownRowsOutput = true;
    }
    void serializeStats(MemoryBuffer &mb)
    {
        CSlaveActivity::serializeStats(mb);
        CriticalBlock b(joinHelperCrit);
        if (!joinhelper) // bit odd, but will leave as was for now.
        {
            mb.append(lhsProgressCount);
            mb.append(rhsProgressCount);
        }
        else
        {
            mb.append(joinhelper->getLhsProgress());
            mb.append(joinhelper->getRhsProgress());
        }
    }
};
#ifdef _MSC_VER
#pragma warning(pop)
#endif


//===========================================================================

CThorRowAggregator *mergeLocalAggs(CActivityBase &activity, IHThorRowAggregator &helper, IHThorHashAggregateExtra &helperExtra, CThorRowAggregator *localAggTable, mptag_t mptag, memsize_t maxMem, bool grow, bool ordered)
{
    Owned<IHashDistributor> distributor;
    Owned<IRowStream> strm;
    Owned<CThorRowAggregator> globalAggTable = new CThorRowAggregator(activity, helperExtra, helper, maxMem, grow);
    globalAggTable->start(activity.queryRowAllocator());
    __int64 readCount = 0;
    if (ordered)
    {
        class CRowAggregatedStream : public CInterface, implements IRowStream
        {
            CActivityBase &activity;
            IRowInterfaces *rowIf;
            Linked<CThorRowAggregator> localAggregated;
            RtlDynamicRowBuilder outBuilder;
            size32_t node;
        public:
            IMPLEMENT_IINTERFACE;
            CRowAggregatedStream(CActivityBase &_activity, IRowInterfaces *_rowIf, CThorRowAggregator *_localAggregated) : activity(_activity), rowIf(_rowIf), localAggregated(_localAggregated), outBuilder(_rowIf->queryRowAllocator())
            {
                node = activity.queryContainer().queryJob().queryMyRank();
            }
            // IRowStream impl.
            virtual const void *nextRow()
            {
                Owned<AggregateRowBuilder> next = localAggregated->nextResult();
                if (!next) return NULL;
                byte *outPtr = outBuilder.getSelf();
                memcpy(outPtr, &node, sizeof(node));
                const void *nextRow = next->finalizeRowClear();
                memcpy(outPtr+sizeof(node), &nextRow, sizeof(const void *));
                return outBuilder.finalizeRowClear(sizeof(node)+sizeof(const void *));
            }
            virtual void stop() { }
        };
        Owned<IOutputMetaData> nodeRowMeta = createOutputMetaDataWithChildRow(activity.queryRowAllocator(), sizeof(size32_t));
        Owned<IRowInterfaces> nodeRowMetaRowIf = createRowInterfaces(nodeRowMeta, activity.queryActivityId(), activity.queryCodeContext());
        Owned<IRowStream> localAggregatedStream = new CRowAggregatedStream(activity, nodeRowMetaRowIf, localAggTable);
        class CNodeCompare : implements ICompare, implements IHash
        {
            IHash *baseHash;
        public:
            CNodeCompare(IHash *_baseHash) : baseHash(_baseHash) { }
            virtual int docompare(const void *l,const void *r) const
            {
                size32_t lNode, rNode;
                memcpy(&lNode, l, sizeof(size32_t));
                memcpy(&rNode, r, sizeof(size32_t));
                return (int)lNode-(int)rNode;
            }
            virtual unsigned hash(const void *rowMeta)
            {
                const void *row;
                memcpy(&row, ((const byte *)rowMeta)+sizeof(size32_t), sizeof(const void *));
                return baseHash->hash(row);
            }
        } nodeCompare(helperExtra.queryHashElement());
        distributor.setown(createPullHashDistributor(&activity, activity.queryContainer().queryJob().queryJobComm(), mptag, nodeRowMetaRowIf, activity.queryAbortSoon(), false, NULL));
        strm.setown(distributor->connect(localAggregatedStream, &nodeCompare, &nodeCompare));
        loop
        {
            OwnedConstThorRow rowMeta = strm->nextRow();
            if (!rowMeta)
                break;
            readCount++;
            const void *row;
            memcpy(&row, ((const byte *)rowMeta.get())+sizeof(size32_t), sizeof(const void *));
            globalAggTable->mergeElement(row);
        }
    }
    else
    {
        class CRowAggregatedStream : public CInterface, implements IRowStream
        {
            Linked<CThorRowAggregator> localAggregated;
        public:
            IMPLEMENT_IINTERFACE;
            CRowAggregatedStream(CThorRowAggregator *_localAggregated) : localAggregated(_localAggregated)
            {
            }
            // IRowStream impl.
            virtual const void *nextRow()
            {
                Owned<AggregateRowBuilder> next = localAggregated->nextResult();
                if (!next) return NULL;
                return next->finalizeRowClear();
            }
            virtual void stop() { }
        };
        Owned<IRowStream> localAggregatedStream = new CRowAggregatedStream(localAggTable);
        distributor.setown(createHashDistributor(&activity, activity.queryContainer().queryJob().queryJobComm(), mptag, &activity, activity.queryAbortSoon(), false, NULL));
        strm.setown(distributor->connect(localAggregatedStream, helperExtra.queryHashElement(), NULL));
        loop
        {
            OwnedConstThorRow row = strm->nextRow();
            if (!row)
                break;
            readCount++;
            globalAggTable->mergeElement(row);
        }
    }
    strm->stop();
    strm.clear();
    distributor->disconnect(true);
    distributor->removetemp();
    distributor->join();
    distributor.clear();

    activity.ActPrintLog("HASHAGGREGATE: Read %"RCPF"d records to build hash table", readCount);
    StringBuffer str("HASHAGGREGATE: After distribution merge contains ");
    activity.ActPrintLog("%s", str.append(globalAggTable->elementCount()).append("entries").str());
    return globalAggTable.getClear();
}

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning( disable : 4355 ) // 'this' : used in base member initializer list
#endif
class CHashAggregateSlave : public CSlaveActivity, public CThorDataLink, implements IHThorRowAggregator
{
    IHThorHashAggregateArg *helper;
    IThorDataLink *input;
    mptag_t mptag;
    Owned<CThorRowAggregator> localAggTable;
    bool eos;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CHashAggregateSlave(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        mptag = TAG_NULL;
        eos = true;
    }
    void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorHashAggregateArg *> (queryHelper());
        appendOutputLinked(this);

        if (!container.queryLocal())
            mptag = container.queryJob().deserializeMPTag(data);
        ActPrintLog("HASHAGGREGATE: init tags %d",(int)mptag);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        localAggTable.setown(new CThorRowAggregator(*this, *helper, *helper, queryLargeMemSize()/10, container.queryOwnerId()==0));
        localAggTable->start(queryRowAllocator());

        input = inputs.item(0);
        startInput(input);
        try
        {
            dataLinkStart("HASHAGGREGATE", container.queryId());
            while (!abortSoon)
            {
                OwnedConstThorRow row = input->ungroupedNextRow();
                if (!row)
                    break;
                localAggTable->addRow(row);
            }
            StringBuffer str("HASHAGGREGATE: Table before distribution contains ");
            ActPrintLog("%s", str.append(localAggTable->elementCount()).append(" entries").str());
        }
        catch (IException *)
        {
            stopInput(input);
            throw;
        }
        stopInput(input);
        if (abortSoon)
            return;
        if (!container.queryLocal() && container.queryJob().querySlaves()>1)
        {
            bool ordered = 0 != (TAForderedmerge & helper->getAggregateFlags());
            localAggTable.setown(mergeLocalAggs(*this, *helper, *helper, localAggTable, mptag, queryLargeMemSize()/10, container.queryOwnerId()==0, ordered));
        }
        eos = false;
    }
    void stop()
    {
        ActPrintLog("HASHAGGREGATE: stopping");
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eos) return NULL;
        Owned<AggregateRowBuilder> next = localAggTable->nextResult();
        if (next)
        {
            dataLinkIncrement();
            return next->finalizeRowClear();
        }
        eos = true;
        return NULL;
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canStall = true;
        // maybe more?
    }
// IHThorRowAggregator impl. - JCSMORE more until aggregator allows selectInterface to return.
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) { return helper->clearAggregate(rowBuilder); }
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src) { return helper->processFirst(rowBuilder, src); }
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src) { return helper->processNext(rowBuilder, src); }
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) { return helper->mergeAggregate(rowBuilder, src); }
};
#ifdef _MSC_VER
#pragma warning(pop)
#endif

//===========================================================================


CActivityBase *createHashDistributeSlave(CGraphElementBase *container)
{
    if (container&&(((IHThorHashDistributeArg *)container->queryHelper())->queryHash()==NULL))
        return createReDistributeSlave(container);
    ActPrintLog(container, "HASHDISTRIB: createHashDistributeSlave");
    return new HashDistributeSlaveActivity(container);
}

CActivityBase *createHashDistributeMergeSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "HASHDISTRIB: createHashDistributeMergeSlave");
    return new HashDistributeMergeSlaveActivity(container);
}

CActivityBase *createHashDedupSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "HASHDEDUP: createHashDedupSlave");
    return new GlobalHashDedupSlaveActivity(container);
}

CActivityBase *createHashLocalDedupSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "LOCALHASHDEDUP: createHashLocalDedupSlave");
    return new LocalHashDedupSlaveActivity(container);
}

CActivityBase *createHashJoinSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "HASHJOIN: createHashJoinSlave");
    return new HashJoinSlaveActivity(container);
}

CActivityBase *createHashAggregateSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "HASHAGGREGATE: createHashAggregateSlave");
    return new CHashAggregateSlave(container);
}

CActivityBase *createIndexDistributeSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "DISTRIBUTEINDEX: createIndexDistributeSlave");
    return new IndexDistributeSlaveActivity(container);
}

CActivityBase *createReDistributeSlave(CGraphElementBase *container)
{
    ActPrintLog(container, "REDISTRIBUTE: createReDistributeSlave");
    return new ReDistributeSlaveActivity(container);
}


