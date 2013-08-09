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

#include "platform.h"
#include "limits.h"
#include <math.h>

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
#define DEFAULT_WRITEPOOLSIZE 16
#define DISK_BUFFER_SIZE 0x10000 // 64K
#define DEFAULT_TIMEOUT (1000*60*60)

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning( disable : 4355 ) // 'this' : used in base member initializer list
#endif

#ifdef _DEBUG
#define HDSendPrintLog(M) PROGLOG(M)
#define HDSendPrintLog2(M,P1) PROGLOG(M,P1)
#define HDSendPrintLog3(M,P1,P2) PROGLOG(M,P1,P2)
#define HDSendPrintLog4(M,P1,P2,P3) PROGLOG(M,P1,P2,P3)
#else
#define HDSendPrintLog(M)
#define HDSendPrintLog2(M,P1)
#define HDSendPrintLog3(M,P1,P2)
#define HDSendPrintLog4(M,P1,P2,P3)
#endif

class CDistributorBase : public CSimpleInterface, implements IHashDistributor, implements IExceptionHandler
{
    Linked<IRowInterfaces> rowIf;
    IEngineRowAllocator *allocator;
    IOutputRowSerializer *serializer;
    IOutputMetaData *meta;
    IHash *ihash;
    Owned<IRowStream> input;

    Semaphore distribDoneSem, localFinishedSem;
    ICompare *iCompare;
    size32_t bucketSendSize;
    bool doDedup, allowSpill, connected, selfstopped;
    Owned<IException> sendException, recvException;
    IStopInput *istop;
    size32_t fixedEstSize;
    Owned<IRowWriter> pipewr;
    roxiemem::IRowManager *rowManager;
    Owned<ISmartRowBuffer> piperd;

    /*
     * CSendBucket - a collection of rows destinate for a particular destination target(slave)
     */
    class CSendBucket : public CSimpleInterface, implements IRowStream
    {
        CDistributorBase &owner;
        size32_t total;
        ThorRowQueue rows;
        unsigned destination;
        CThorExpandingRowArray dedupList;
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSendBucket(CDistributorBase &_owner, unsigned _destination) : owner(_owner), destination(_destination),
                dedupList(*owner.activity, owner.rowIf)
        {
            total = 0;
        }
        ~CSendBucket()
        {
            loop
            {
                const void *row = rows.dequeue();
                if (!row)
                    break;
                ReleaseThorRow(row);
            }
        }
        unsigned count() const { return rows.ordinality(); }
        bool dedup(ICompare *iCompare) // returns true if reduces by >= 10%
        {
            unsigned c = rows.ordinality();
            if (c<2)
                return false;
            for (unsigned i=0; i<c; i++)
                dedupList.append(rows.item(i));
            rows.clear(); // NB: dedupList took ownership
            dedupList.sort(*iCompare, owner.activity->queryMaxCores());
            OwnedConstThorRow prev;
            for (unsigned i = c; i>0;)
            {
                OwnedConstThorRow row = dedupList.getClear(--i);
                if ((NULL != prev.get()) && (0 == iCompare->docompare(prev, row)))
                {
                    size32_t rsz = owner.rowMemSize(row);
                    total -= rsz;
                }
                else
                {
                    prev.set(row);
                    rows.enqueue(row.getClear());
                }
            }
            dedupList.clearRows();
            return true; // attempted
        }
        void add(const void *row, size32_t &rs)
        {
            rs = owner.rowMemSize(row);
            total += rs;
            rows.enqueue(row);
        }
        unsigned queryDestination() const { return destination; }
        size32_t querySize() const { return total; }
        bool serializeClear(MemoryBuffer &mb, size32_t limit) // returns true if sent all
        {
            CMemoryRowSerializer memSerializer(mb);
            loop
            {
                OwnedConstThorRow row = nextRow();
                if (!row)
                    break;
                owner.serializer->serialize(memSerializer, (const byte *)row.get());
                if (mb.length()>=limit)
                    return false;
            }
            return true;
        }
    // IRowStream impl.
        virtual const void *nextRow()
        {
            return rows.dequeue();
        }
        virtual void stop() { }
    };
    typedef SimpleInterThreadQueueOf<CSendBucket, false> CSendBucketQueue;
    /*
     * CSender, main send loop functionality
     * processes input, constructs CSendBucket's and manages creation CWriteHandler threads
     */
    class CSender : public CSimpleInterface, implements IThreadFactory, implements IExceptionHandler
    {
        /*
         * CWriterHandler, a per thread class and member of the writerPool
         * a write handler, is given an initial CSendBucket and handles the dedup(if applicable)
         * compression and transmission to the target.
         * If the size serialized, is below a threshold, it will see if more has been queued
         * in the interim and serialize that compress and searilize within the same send/recv cycle
         * When done, it will see if more queue available.
         * NB: There will be at most 1 writer per destination target (up to thread pool limit)
         */
        class CWriteHandler : public CSimpleInterface, implements IPooledThread
        {
            CSender &owner;
            CDistributorBase &distributor;
            Owned<CSendBucket> _sendBucket;
            unsigned nextPending;
            bool aborted;

        public:
            IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

            CWriteHandler(CSender &_owner) : owner(_owner), distributor(_owner.owner)
            {
                aborted = false;
            }
            void init(void *startInfo)
            {
                nextPending = getRandom()%distributor.numnodes;
                _sendBucket.setown((CSendBucket *)startInfo);
                owner.setActiveWriter(_sendBucket->queryDestination(), this);
                aborted = false;
            }
            void main()
            {
                Owned<CSendBucket> sendBucket = _sendBucket.getClear();
                unsigned dest = sendBucket->queryDestination();
                size32_t writerTotalSz = 0;
                size32_t sendSz = 0;
                MemoryBuffer mb;
                while (!aborted)
                {
                    writerTotalSz += sendBucket->querySize();
                    owner.dedup(sendBucket); // conditional

                    if (owner.selfPush(dest))
                        distributor.addLocal(sendBucket);
                    else // remote
                    {
                        bool wholeBucket = sendBucket->serializeClear(mb, distributor.bucketSendSize);
                        sendSz = mb.length();
                        // NB: buckets will typically be large enough already, if not check pending buckets
                        if (sendSz < distributor.bucketSendSize)
                        {
                            // more added to dest I'm processing?
                            {
                                CriticalBlock b(owner.activeWritersLock);
                                sendBucket.setown(owner.pendingBuckets.item(dest)->dequeueNow());
                            }
                            if (sendBucket)
                            {
                                HDSendPrintLog3("CWriteHandler, pending(b=%d) rolled, size=%d", sendBucket->queryDestination(), sendBucket->querySize());
                                // NB: if was just < bucketSendSize and pending is ~ bucketSendSize, could mean we send is ~2*bucketSendSize, but that's ok.
                                continue; // NB: it will flow into else "remote" arm
                            }
                        }
                        while (!aborted)
                        {
                            // JCSMORE check if worth compressing
                            CMessageBuffer msg;
                            fastLZCompressToBuffer(msg, mb.length(), mb.bufferBase());
                            mb.clear();
                            owner.send(dest, msg);
                            sendSz = 0;
                            if (wholeBucket)
                                break;
                            wholeBucket = sendBucket->serializeClear(mb, distributor.bucketSendSize);
                        }
                    }
                    owner.decTotal(writerTotalSz);
                    writerTotalSz = 0;
                    // see if others to process
                    // NB: this will never start processing a bucket for a destination which already has an active writer.
                    CriticalBlock b(owner.activeWritersLock);
                    owner.setActiveWriter(dest, NULL);
                    sendBucket.setown(owner.getAnotherBucket(nextPending));
                    if (!sendBucket)
                        break;
                    dest = sendBucket->queryDestination();
                    owner.setActiveWriter(dest, this);
                    HDSendPrintLog3("CWriteHandler, now dealing with (b=%d), size=%d", sendBucket->queryDestination(), sendBucket->querySize());
                }
            }
            bool canReuse() { return true; }
            bool stop() { return true; }
            void abort()
            {
                aborted = true;
            }
        } **activeWriters;

        CDistributorBase &owner;
        CriticalSection activeWritersLock;
        mutable SpinLock totalSzLock;
        SpinLock doDedupLock;
        PointerIArrayOf<CSendBucket> buckets;
        UnsignedArray candidates;
        size32_t totalSz;
        bool senderFull, doDedup, aborted, initialized;
        Semaphore senderFullSem;
        Linked<IException> exception;
        OwnedMalloc<bool> senderFinished;
        unsigned numFinished, dedupSamples, dedupSuccesses, self;
        Owned<IThreadPool> writerPool;
        PointerArrayOf<CSendBucketQueue> pendingBuckets;
        unsigned numActiveWriters;

        void init()
        {
            unsigned n;
            for (unsigned n=0; n<owner.numnodes; n++)
                buckets.append(NULL);
            totalSz = 0;
            senderFull = false;
            senderFinished.allocateN(owner.numnodes, true);
            numFinished = 0;
            dedupSamples = dedupSuccesses = 0;
            doDedup = owner.doDedup;
            writerPool.setown(createThreadPool("HashDist writer pool", this, this, owner.writerPoolSize, 5*60*1000));
            self = owner.activity->queryJob().queryMyRank()-1;
            for (n=0; n<owner.numnodes; n++)
                pendingBuckets.append(new CSendBucketQueue);
            numActiveWriters = 0;
            aborted = false;
            activeWriters = new CWriteHandler *[owner.numnodes];
            memset(activeWriters, 0, owner.numnodes * sizeof(CWriteHandler *));
            initialized = true;
        }
        void reset()
        {
            assertex(0 == numActiveWriters);
            // unless it was aborted, there shouldn't be any pending or non-null buckets
            for (unsigned n=0; n<owner.numnodes; n++)
            {
                CSendBucketQueue *queue = pendingBuckets.item(n);
                loop
                {
                    CSendBucket *bucket = queue->dequeueNow();
                    if (!bucket)
                        break;
                    ::Release(bucket);
                }
                buckets.replace(NULL, n);
            }
            totalSz = 0;
            senderFull = false;
            numFinished = 0;
            aborted = false;
        }
        void reinit()
        {
            if (initialized)
                reset();
            else
                init();
        }
        void dedup(CSendBucket *sendBucket)
        {
            {
                SpinBlock b(doDedupLock);
                if (!doDedup)
                    return;
            }
            unsigned preCount = sendBucket->count();
            CCycleTimer dedupTimer;
            if (sendBucket->dedup(owner.iCompare))
            {
                unsigned tookMs = dedupTimer.elapsedMs();
                unsigned postCount = sendBucket->count();
                SpinBlock b(doDedupLock);
                if (dedupSamples<10)
                {
                    if (postCount<preCount*9/10)
                        dedupSuccesses++;
                    dedupSamples++;
                    ActPrintLog(owner.activity, "pre-dedup sample %d : %d unique out of %d, took: %d ms", dedupSamples, postCount, preCount, tookMs);
                    if ((10 == dedupSamples) && (0 == dedupSuccesses))
                    {
                        ActPrintLog(owner.activity, "disabling distribute pre-dedup");
                        doDedup = false;
                    }
                }
            }
        }
        CSendBucket *queryBucket(unsigned n)
        {
            return buckets.item(n);
        }
        CSendBucket *queryBucketCreate(unsigned n)
        {
            CSendBucket *bucket = buckets.item(n);
            if (!bucket)
            {
                bucket = new CSendBucket(owner, n);
                buckets.replace(bucket, n);
            }
            return bucket;
        }
        CSendBucket *getBucketClear(unsigned n)
        {
            Linked<CSendBucket> bucket = buckets.item(n);
            if (!bucket)
                return NULL;
            buckets.replace(NULL, n);
            return bucket.getClear();
        }
        void send(unsigned dest, CMessageBuffer &mb)
        {
            if (!senderFinished[dest])
            {
                if (selfPush(dest))
                    assertex(dest != self);
                if (!owner.sendBlock(dest, mb))
                {
                    ActPrintLog(owner.activity, "CDistributorBase::sendBlock stopped slave %d", dest+1);
                    senderFinished[dest] = true;
                    numFinished++;
                }
            }
        }
        void decTotal(size32_t sz)
        {
            SpinBlock b(totalSzLock);
            HDSendPrintLog2("decTotal - %d", sz);
            totalSz -= sz;
            if (sz && senderFull)
            {
                senderFull = false;
                senderFullSem.signal();
            }
        }
        size32_t queryTotalSz() const
        {
            SpinBlock b(totalSzLock);
            return totalSz;
        }
        inline bool selfPush(unsigned i)
        {
            return (i==self)&&!owner.pull;
        }
        void closeWrite()
        {
            unsigned i;
            CMessageBuffer nullMsg;
            for (i=0; i<owner.numnodes; i++)
            {
                if (!selfPush(i))
                {
                    try
                    {
                        nullMsg.clear();
                        owner.sendBlock(i, nullMsg);
                    }
                    catch (IException *e)
                    {
                        ActPrintLog(owner.activity, e, "HDIST: closeWrite");
                        owner.fireException(e);
                        e->Release();
                    }
                }
            }
        }
    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

        CSender(CDistributorBase &_owner) : owner(_owner)
        {
            initialized = false;
        }
        ~CSender()
        {
            if (initialized)
            {
                delete [] activeWriters;
                for (unsigned n=0; n<owner.numnodes; n++)
                {
                    CSendBucketQueue *queue = pendingBuckets.item(n);
                    loop
                    {
                        CSendBucket *bucket = queue->dequeueNow();
                        if (!bucket)
                            break;
                        ::Release(bucket);
                    }
                    delete queue;
                }
            }
        }
        CSendBucket *getAnotherBucket(unsigned &next)
        {
            // NB: called inside activeWritersLock
            unsigned start = next;
            loop
            {
                unsigned current=next;
                unsigned c = pendingBuckets.item(current)->ordinality();
                ++next;
                if (next>=owner.numnodes)
                    next = 0;
                if (c)
                {
                    if (NULL == activeWriters[current])
                        return pendingBuckets.item(current)->dequeueNow();
                }
                if (next == start)
                    return NULL;
            }
        }
        void add(CSendBucket *bucket)
        {
            if (owner.selfstopped && !senderFinished[self])
            {
                senderFinished[self] = true;
                ++numFinished;
            }
            unsigned dest = bucket->queryDestination();
            if (senderFinished[dest])
            {
                HDSendPrintLog2("CSender::add disposing of bucket [finished(%d)]", dest);
                bucket->Release();
            }
            else
            {
                CriticalBlock b(activeWritersLock);
                CWriteHandler *writer = activeWriters[dest];
                if (!writer && (numActiveWriters < owner.writerPoolSize))
                {
                    HDSendPrintLog2("CSender::add (new thread), dest=%d", dest);
                    writerPool->start(bucket);
                }
                else // an existing writer will pick up
                    pendingBuckets.item(dest)->enqueue(bucket);
            }
        }
        void setActiveWriter(unsigned n, CWriteHandler *writer)
        {
            if (writer)
            {
                assertex(!activeWriters[n]);
                ++numActiveWriters;
            }
            else
            {
                assertex(activeWriters[n]);
                --numActiveWriters;
            }
            activeWriters[n] = writer;
        }
        unsigned getSendCandidate(bool &doSelf)
        {
            unsigned i;
            unsigned maxsz=0;
            for (i=0;i<owner.numnodes;i++)
            {
                CSendBucket *bucket = queryBucket(i);
                if (bucket)
                {
                    size32_t bucketSz = bucket->querySize();
                    if (bucketSz > maxsz)
                        maxsz = bucketSz;
                }
            }
            doSelf = false;
            if (0 == maxsz)
                return NotFound;
            candidates.kill();
            for (i=0; i<owner.numnodes; i++)
            {
                CSendBucket *bucket = queryBucket(i);
                if (bucket)
                {
                    size32_t bucketSz = bucket->querySize();
                    if (bucketSz > maxsz/2)
                    {
                        if (i==self)
                            doSelf = true;
                        else
                            candidates.append(i);
                    }
                }
            }
            if (0 == candidates.ordinality())
                return NotFound;
            unsigned h;
            if (candidates.ordinality()==1)
                h = candidates.item(0);
            else
                h = candidates.item(getRandom()%candidates.ordinality());
            return h;
        }
        void process(IRowStream *input)
        {
            ActPrintLog(owner.activity, "Distribute send start");
            reinit();
            CCycleTimer timer;
            rowcount_t totalSent = 0;
            try
            {
                while (!aborted && numFinished < owner.numnodes)
                {
                    while (queryTotalSz() >= owner.inputBufferSize)
                    {
                        if (aborted)
                            break;

                        HDSendPrintLog("process exceeded inputBufferSize");
                        bool doSelf;
                        unsigned which = getSendCandidate(doSelf);
                        if (NotFound != which)
                        {
                            HDSendPrintLog3("process exceeded: send to %d, size=%d", which, queryBucket(which)->querySize());
                            add(getBucketClear(which));
                        }
                        if (doSelf)
                        {
                            HDSendPrintLog2("process exceeded: doSelf, size=%d", queryBucket(self)->querySize());
                            add(getBucketClear(self));
                        }
                        else if (NotFound == which) // i.e. none
                        {
                            HDSendPrintLog("process exceeded inputBufferSize, none to send");
                            {
                                SpinBlock b(totalSzLock);
                                // some may have been freed after lock
                                if (totalSz < owner.inputBufferSize)
                                    break;
                                senderFull = true;
                            }
                            loop
                            {
                                if (timer.elapsedCycles() >= queryOneSecCycles()*10)
                                    ActPrintLog(owner.activity, "HD sender, waiting for space");
                                timer.reset();

                                if (senderFullSem.wait(10000))
                                    break;
                                if (aborted)
                                    break;
                            }
                        }
                    }
                    if (aborted)
                        break;
                    const void *row = input->ungroupedNextRow();
                    if (!row)
                        break;
                    unsigned dest = owner.ihash->hash(row)%owner.numnodes;
                    if (senderFinished[dest]) // does this need to be thread safe?
                        ReleaseThorRow(row);
                    else
                    {
                        CSendBucket *bucket = queryBucketCreate(dest);
                        size32_t rs;
                        bucket->add(row, rs);
                        totalSent++;
                        {
                            SpinBlock b(totalSzLock);
                            totalSz += rs;
                        }
                        if (bucket->querySize() >= owner.bucketSendSize)
                        {
                            HDSendPrintLog3("adding new bucket: %d, size = %d", bucket->queryDestination(), bucket->querySize());
                            add(getBucketClear(dest));
                        }
                    }
                }
            }
            catch (IException *e)
            {
                ActPrintLog(owner.activity, e, "HDIST: sender.process");
                owner.fireException(e);
                e->Release();
            }

            ActPrintLog(owner.activity, "Distribute send finishing");
            if (!aborted)
            {
                // send remainder
                Owned<IShuffledIterator> iter = createShuffledIterator(owner.numnodes);
                ForEach(*iter)
                {
                    unsigned dest=iter->get();
                    Owned<CSendBucket> bucket = getBucketClear(dest);
                    HDSendPrintLog4("Looking at last bucket(%d): %d, size = %d", dest, bucket.get()?bucket->queryDestination():0, bucket.get()?bucket->querySize():-1);
                    if (bucket && bucket->querySize())
                    {
                        HDSendPrintLog3("Sending last bucket(s): %d, size = %d", bucket->queryDestination(), bucket->querySize());
                        add(bucket.getClear());
                    }
                }
            }
            ActPrintLog(owner.activity, "HDIST: waiting for threads");
            writerPool->joinAll();
            ActPrintLog(owner.activity, "HDIST: calling closeWrite()");
            closeWrite();

            ActPrintLog(owner.activity, "HDIST: Send loop %s %"RCPF"d rows sent", exception.get()?"aborted":"finished", totalSent);
        }
        void abort()
        {
            if (aborted)
                return;
            aborted = true;
            senderFullSem.signal();
            if (initialized)
            {
                CriticalBlock b(activeWritersLock);
                for (unsigned w=0; w<owner.numnodes; w++)
                {
                    CWriteHandler *writer = activeWriters[w];
                    if (writer)
                        writer->abort();
                }
            }
        }
    // IThreadFactory impl.
        virtual IPooledThread *createNew()
        {
            return new CWriteHandler(*this);
        }
    // IExceptionHandler impl.
        virtual bool fireException(IException *e)
        {
            ActPrintLog(owner.activity, e, "HDIST: CSender");
            if (!aborted)
            {
                abort();
                exception.set(e);
                senderFullSem.signal();
            }
            return owner.fireException(e);
        }
        friend class CWriteHandler;
    };

protected:
    IOutputRowDeserializer *deserializer;

    class cRecvThread: implements IThreaded
    {
        CDistributorBase *parent;
        CThreadedPersistent threaded;
    public:
        cRecvThread(CDistributorBase *_parent)
            : threaded("CDistributorBase::cRecvThread", this)
        {
            parent = _parent;
        }
        void start() { threaded.start(); }
        void join(unsigned timeout=INFINITE) { threaded.join(timeout); }
        void stop()
        {
            parent->stopRecv();
        }
    // IThreaded impl.
        virtual void main()
        {
            parent->recvloop();
            parent->recvloopdone();
        }
    } recvthread;

    class cSendThread: implements IThreaded
    {
        CDistributorBase *parent;
        CThreadedPersistent threaded;
    public:
        cSendThread(CDistributorBase *_parent)
            : threaded("CDistributorBase::cSendThread", this)
        {
            parent = _parent;
        }
        void start() { threaded.start(); }
        void join(unsigned timeout=INFINITE) { threaded.join(timeout); }
    // IThreaded impl.
        virtual void main()
        {
            parent->sendloop();
        }
    } sendthread;

    void addLocal(CSendBucket *bucket)
    {
        CriticalBlock block(putsect); // JCSMORE - probably doesn't need for this long
        HDSendPrintLog3("addLocal (b=%d), size=%d", bucket->queryDestination(), bucket->querySize());
        loop
        {
            const void *row = bucket->nextRow();
            if (!row)
                break;
            pipewr->putRow(row);
        }
    }
protected:
    CActivityBase *activity;
    size32_t inputBufferSize, pullBufferSize;
    unsigned writerPoolSize;
    unsigned self;
    unsigned numnodes;
    CriticalSection putsect;
    bool pull, aborted;
    CSender sender;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDistributorBase(CActivityBase *_activity, bool _doDedup, IStopInput *_istop)
        : activity(_activity), recvthread(this), sendthread(this), sender(*this)
    {
        aborted = connected = false;
        doDedup = _doDedup;
        self = activity->queryJob().queryMyRank() - 1;
        numnodes = activity->queryJob().querySlaves();
        iCompare = NULL;
        ihash = NULL;
        fixedEstSize = 0;
        bucketSendSize = globals->getPropInt("@hd_out_buffer_size", DEFAULT_OUT_BUFFER_SIZE);
        istop = _istop;
        inputBufferSize = globals->getPropInt("@hd_in_buffer_size", DEFAULT_IN_BUFFER_SIZE);
        pullBufferSize = DISTRIBUTE_PULL_BUFFER_SIZE;
        selfstopped = false;
        pull = false;
        rowManager = activity->queryJob().queryRowManager();

        allowSpill = activity->getOptBool(THOROPT_HDIST_SPILL, true);
        if (allowSpill)
            ActPrintLog(activity, "Using spilling buffer (will spill if overflows)");
        writerPoolSize = activity->getOptUInt(THOROPT_HDIST_WRITE_POOL_SIZE, DEFAULT_WRITEPOOLSIZE);
        if (writerPoolSize>numnodes)
            writerPoolSize = numnodes; // no point in more
        ActPrintLog(activity, "Writer thread pool size : %d", writerPoolSize);
    }

    ~CDistributorBase()
    {
        try
        {
            disconnect(true); // in case exception
        }
        catch (IException *e)
        {
            ActPrintLog(activity, e, "HDIST: CDistributor");
            e->Release();
        }
    }

    size32_t rowMemSize(const void *row)
    {
        if (fixedEstSize)
            return fixedEstSize;
        CSizingSerializer ssz;
        serializer->serialize(ssz, (const byte *)row);
        return ssz.size();
    }

    virtual void setBufferSizes(unsigned _inputBufferSize, unsigned _bucketSendSize, unsigned _pullBufferSize)
    {
        if (_inputBufferSize) inputBufferSize = _inputBufferSize;
        if (_bucketSendSize) bucketSendSize = _bucketSendSize;
        if (_pullBufferSize) pullBufferSize = _pullBufferSize;
    }

    virtual IRowStream *connect(IRowInterfaces *_rowIf, IRowStream *_input, IHash *_ihash, ICompare *_iCompare)
    {
        ActPrintLog(activity, "HASHDISTRIB: connect");

        rowIf.set(_rowIf);
        allocator = _rowIf->queryRowAllocator();
        meta = _rowIf->queryRowMetaData();
        serializer = _rowIf->queryRowSerializer();
        deserializer = _rowIf->queryRowDeserializer();

        fixedEstSize = meta->querySerializedDiskMeta()->getFixedSize();

        input.set(_input);
        ihash = _ihash;
        iCompare = _iCompare;
        if (allowSpill)
        {
            StringBuffer temp;
            GetTempName(temp,"hddrecvbuff", true);
            piperd.setown(createSmartBuffer(activity, temp.toCharArray(), pullBufferSize, rowIf));
        }
        else
            piperd.setown(createSmartInMemoryBuffer(activity, rowIf, pullBufferSize));

        pipewr.set(piperd->queryWriter());
        connected = true;
        selfstopped = false;
        aborted = false;

        sendException.clear();
        recvException.clear();
        start();
        ActPrintLog(activity, "HASHDISTRIB: connected");
        return piperd.getLink();
    }

    virtual void disconnect(bool stop)
    {
        if (connected)
        {
            connected = false;
            if (stop)
            {
                recvthread.stop();
                selfstopped = true;
            }
            distribDoneSem.wait();
            if (sendException.get())
            {
                recvthread.join(1000*60);       // hopefully the others will close down
                throw sendException.getClear();
            }
            recvthread.join();
            if (recvException.get())
                throw recvException.getClear();
        }
        rowIf.clear();
        allocator = NULL;;
        meta = NULL;;
        serializer = NULL;;
        deserializer = NULL;;
        fixedEstSize = 0;
        input.clear();
        piperd.clear();
        pipewr.clear();
        ihash = NULL;
        iCompare = NULL;
    }
    virtual void abort()
    {
        if (!aborted)
        {
            aborted = true;
            sender.abort();
        }
    }
    virtual void recvloop()
    {
        CCycleTimer timer;
        MemoryBuffer tempMb;
        try
        {
            ActPrintLog(activity, "Read loop start");
            CMessageBuffer recvMb;
            Owned<ISerialStream> stream = createMemoryBufferSerialStream(tempMb);
            CThorStreamDeserializerSource rowSource;
            rowSource.setStream(stream);
            unsigned left=numnodes-1;
            while (left && !aborted)
            {
#ifdef _FULL_TRACE
                ActPrintLog("HDIST: Receiving block");
#endif
                unsigned n = recvBlock(recvMb);
                if (n==(unsigned)-1)
                    break;
#ifdef _FULL_TRACE
                ActPrintLog(activity, "HDIST: Received block %d from slave %d",recvMb.length(),n+1);
#endif
                if (recvMb.length())
                {
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
                        while (!rowSource.eos() && !aborted)
                        {
                            timer.reset();
                            RtlDynamicRowBuilder rowBuilder(allocator);
                            size32_t sz = deserializer->deserialize(rowBuilder, rowSource);
                            const void *row = rowBuilder.finalizeRowClear(sz);
                            cycle_t took=timer.elapsedCycles();
                            if (took>=queryOneSecCycles())
                                DBGLOG("RECVLOOP row deserialization blocked for : %d second(s)", (unsigned)(cycle_to_nanosec(took)/1000000000));
                            timer.reset();
                            pipewr->putRow(row);
                            took=timer.elapsedCycles();
                            if (took>=queryOneSecCycles())
                                DBGLOG("RECVLOOP pipewr->putRow blocked for : %d second(s)", (unsigned)(cycle_to_nanosec(took)/1000000000));
                        }
                    }
                }
                else
                {
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
        ActPrintLog(activity, "HDIST: waiting localFinishedSem");
#endif
    }

    void recvloopdone()
    {
        localFinishedSem.wait();
        pipewr->flush();
        if (piperd)
            piperd->stop();
        pipewr.clear();
        ActPrintLog(activity, "HDIST: Read loop done");

    }

    void sendloop()
    {
        // NB: keeps sending until all receivers including self have requested stop
        try
        {
            sender.process(input);
        }
        catch (IException *e)
        {
            if (!sendException.get())
                sendException.setown(e);
        }
        localFinishedSem.signal();
        if (istop)
        {
            try { istop->stopInput(); }
            catch (IException *e)
            {
                if (!sendException.get())
                    sendException.setown(e);
                else
                {
                    ActPrintLog(activity, e, "HDIST: follow on");
                    e->Release();
                }
            }
        }
        distribDoneSem.signal();
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

    inline ICompare *queryCompare()
    {
        return iCompare;
    }

    inline IEngineRowAllocator *queryAllocator()
    {
        return allocator;
    }

    inline IRowWriter &output()
    {
        return *pipewr;
    }

    void setRecvExc(IException *e)
    {
        ActPrintLog(activity, e, "HDIST: recvloop");
        abort();
        if (recvException.get())
            e->Release();
        else
            recvException.setown(e);
    }
    bool sendRecv(ICommunicator &comm, CMessageBuffer &mb, rank_t r, mptag_t tag)
    {
        loop
        {
            if (aborted)
                return false;
            if (comm.sendRecv(mb, r, tag, MEDIUMTIMEOUT))
                return true;
            // try again
        }
    }
    virtual unsigned recvBlock(CMessageBuffer &mb,unsigned i=(unsigned)-1) = 0;
    virtual void stopRecv() = 0;
    virtual bool sendBlock(unsigned i,CMessageBuffer &mb) = 0;

    // IExceptionHandler impl.
    virtual bool fireException(IException *e)
    {
        if (!sendException.get())
            sendException.set(e);
        return true;
    }
};



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

    CRowDistributor(CActivityBase *activity, ICommunicator &_comm, mptag_t _tag, bool doDedup, IStopInput *istop)
        : CDistributorBase(activity, doDedup, istop), comm(_comm), tag(_tag)
    {
        stopping = false;
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
        if (!comm.recv(msg, RANK_ALL, tag, &sender))
        {
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
        while (sz)
        {
            sz--;
            byte flag = *((byte *)msg.toByteArray()+sz);
            msg.setLength(sz);
            if (flag==1)
            {
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
        if (msg.length()>0)
        {
            // send request to send
            CMessageBuffer rts;
            flag = 1;               // want ack
            rts.append(flag);
#ifdef _FULL_TRACE
            ActPrintLog(activity, "HDIST MP sending RTS to %d",i+1);
#endif

            if (!sendRecv(comm, rts, i+1, tag))
                return false;
            rts.read(flag);
#ifdef _FULL_TRACE
            ActPrintLog(activity, "HDIST MP got CTS from %d, %d",i+1,(int)flag);
#endif
            if (flag==0)
                return false;           // other end stopped
            flag = 0;                   // no ack
            msg.append(flag);
        }
        if (flag==0)
        {                  // no ack
            comm.send(msg, i+1, tag);
            return true;
        }
        // this branch not yet used
        assertex(false);
        if (!sendRecv(comm, msg, i+1, tag))
            return false;
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
        stopping = false;
    }
    virtual void abort()
    {
        CDistributorBase::abort();
        stopRecv();
        comm.cancel(RANK_ALL, tag);
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

    class cSortedDistributeMerger : public CSimpleInterface, implements IRowProvider
    {
        CDistributorBase &parent;
        Owned<IRowStream> out;
        MemoryBuffer *bufs;
        CThorStreamDeserializerSource *dszs;
        unsigned numnodes;
        ICompare *cmp;
        IEngineRowAllocator *allocator;
        IOutputRowDeserializer *deserializer;

    public:
        IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
        cSortedDistributeMerger(CDistributorBase &_parent, unsigned &_numnodes, ICompare *_cmp, IEngineRowAllocator *_allocator, IOutputRowDeserializer *_deserializer)
            : parent(_parent), cmp(_cmp), allocator(_allocator), deserializer(_deserializer)
        {
            numnodes = _numnodes;
            bufs = new MemoryBuffer[numnodes];
            dszs = new CThorStreamDeserializerSource[numnodes];
            for (unsigned node=0; node < numnodes; node++)
            {
                Owned<ISerialStream> stream = createMemoryBufferSerialStream(bufs[node]);
                dszs[node].setStream(stream);
            }
            out.setown(createRowStreamMerger(numnodes, *this, cmp));
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
            {
                RtlDynamicRowBuilder rowBuilder(allocator);
                size32_t sz = deserializer->deserialize(rowBuilder, dszs[idx]);
                return rowBuilder.finalizeRowClear(sz);
            }
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
        diskpos = 0;
        if (txthread)
        {
            delete txthread;
            txthread = NULL;
        }
        // JCSMORE - shouldn't really be necessary - pull distributor needs revisiting
        selfready.reinit();
        selfdone.reinit();
    }
public:
    CRowPullDistributor(CActivityBase *activity, ICommunicator &_comm, mptag_t _tag, bool doDedup, IStopInput *istop)
        : CDistributorBase(activity, doDedup, istop), comm(_comm), tag(_tag)
    {
        pull = true;
        tag = _tag;
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
        for (unsigned i=0;i<numnodes;i++)
        {
            while (diskcached[i])
            {
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
    void recvloop()
    {
        try
        {
            Owned<cSortedDistributeMerger> merger = new cSortedDistributeMerger(*this, numnodes, queryCompare(), queryAllocator(), deserializer);
            ActPrintLog(activity, "Read loop start");
            while (!aborted)
            {
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
        if (donesend[i]&&(msg.length()==0))
            return false;
        assertex(!donesend[i]);
        bool done = msg.length()==0;
        if (!hasbuf[i])
        {
            hasbuf[i] = true;
            bufs[i].swapWith(msg);
            if (i==self)
                selfready.signal();
            else
                doSend(i);
        }
        else
        {
            size32_t sz = msg.length();
            if (sz==0)
            {
                assertex(bufs[i].length()!=0);
            }
            else
            {
                if (!cachefileio.get())
                {
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
            if (prev)
            {
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
        if (i==self)
        {
            msg.clear();
            selfready.wait();
            if (aborted)
                return (unsigned)-1;
        }
        else
        {
            msg.clear().append((byte)1); // rts
            if (!sendRecv(comm, msg, i+1, tag))
            {
                return i;
            }
        }

        CriticalBlock block(sect);
        assertex(!donerecv[i]);
        if (self==i)
        {
            if (stopping)
            {
                selfdone.signal();
                return (unsigned)-1;
            }
            if (hasbuf[i])
            {
                bufs[i].swapWith(msg);
                cBuf *cb = diskcached[i];
                if (cb)
                {
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
        if (msg.length()==0)
        {
            donerecv[i] = true;
            if (i==self)
                selfdone.signal();
            else
            {
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
        if (hasbuf[target]&&(waiting[target]!=TAG_NULL))
        {
#ifdef TRACE_MP
            ActPrintLog(activity, "HDIST MP dosend(%d,%d)",i,bufs[target].length());
#endif
            size32_t sz = bufs[target].length();
            // TBD compress here?

            comm.send(bufs[target],(rank_t)target+1,waiting[target]);
            waiting[target]=TAG_NULL;
            bufs[target].clear();
            cBuf *cb = diskcached[target];
            if (cb)
            {
                diskcached[target] = cb->next;
                if (cb->size)
                    cachefileio->read(cb->pos,cb->size,bufs[target].reserve(cb->size));
                delete cb;
            }
            else
                hasbuf[target] = false;
            if (!sz)
            {
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
        while (done<numnodes)
        {
            rank_t sender;
            CMessageBuffer rts;
            {
                CriticalUnblock block(sect);
                if (!comm.recv(rts, RANK_ALL, tag, &sender))
                {
                    return;
                }

            }
            if (rts.length()==0)
            {
                done++;
            }
            else
            {
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
        clean();
        stopping = false;
        txthread = new cTxThread(*this);
        txthread->start();
    }

    virtual void join() // probably does nothing
    {
        CDistributorBase::join();
        if (txthread)
        {
            txthread->join();
            delete txthread;
            txthread = NULL;
        }
    }
    void stop()
    {
        selfdone.wait();
        if (txthread)
        {
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
    virtual void abort()
    {
        CDistributorBase::abort();
        comm.cancel(RANK_ALL, tag);
    }
};

//==================================================================================================
// Activity Implementation
//==================================================================================================


IHashDistributor *createHashDistributor(CActivityBase *activity, ICommunicator &comm, mptag_t tag, bool doDedup, IStopInput *istop)
{
    return new CRowDistributor(activity, comm, tag, doDedup, istop);
}

IHashDistributor *createPullHashDistributor(CActivityBase *activity, ICommunicator &comm, mptag_t tag, bool doDedup, IStopInput *istop)
{
    return new CRowPullDistributor(activity, comm, tag, doDedup, istop);
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
protected:
    Owned<IRowStream> instrm;
    IHash *ihash;
    ICompare *mergecmp;     // if non-null is merge distribute
    bool eofin;
public:
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
            distributor = createPullHashDistributor(this, container.queryJob().queryJobComm(), mptag, false, this);
        else
            distributor = createHashDistributor(this, container.queryJob().queryJobComm(), mptag, false, this);
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
        if (!instrm.get()) // derived class may override
        {
            input = inputs.item(0);
            startInput(input);
            inputstopped = false;
            instrm.set(input);
            if (passthrough)
                out.set(instrm);
        }
        else if (passthrough)
        {
            out.set(instrm);
        }
        if (!passthrough)
        {
            Owned<IRowInterfaces> myRowIf = getRowInterfaces(); // avoiding circular link issues
            out.setown(distributor->connect(myRowIf, instrm, ihash, mergecmp));
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
        if (out)
        {
            out->stop();
            out.clear();
        }
        if (distributor)
        {
            distributor->disconnect(true);
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
    void abort()
    {
        CSlaveActivity::abort();
        if (distributor)
            distributor->abort();
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
        if (sz==(offset_t)-1)
        {
            // not great but hopefully exception not rule!
            unsigned rwFlags = DEFAULT_RWFLAGS;
            sz = 0;
            StringBuffer tempname;
            GetTempName(tempname,"hdprop",true); // use alt temp dir
            tempfile.setown(createIFile(tempname.str()));
            {
                ActPrintLogEx(&activity->queryContainer(), thorlog_null, MCwarning, "REDISTRIBUTE size unknown, spilling to disk");
                MemoryAttr ma;
                activity->startInput(in);
                if (activity->getOptBool(THOROPT_COMPRESS_SPILLS, true))
                    rwFlags |= rw_compress;
                Owned<IExtRowWriter> out = createRowWriter(tempfile, activity, rwFlags);
                if (!out)
                    throw MakeStringException(-1,"Could not created file %s",tempname.str());
                loop
                {
                    const void * row = in->ungroupedNextRow();
                    if (!row)
                        break;
                    out->putRow(row);
                }
                out->flush();
                sz = out->getPosition();
                activity->stopInput(in);
            }
            ret.setown(createRowStream(tempfile, activity, rwFlags));
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
    class CKeyLookup : implements IHash
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
    } *lookup;

public:
    IndexDistributeSlaveActivity(CGraphElementBase *container) : HashDistributeSlaveBase(container), lookup(NULL)
    {
    }
    ~IndexDistributeSlaveActivity()
    {
        if (lookup)
            delete lookup;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        HashDistributeSlaveBase::init(data, slaveData);

        IHThorKeyedDistributeArg *helper = (IHThorKeyedDistributeArg *) queryHelper();

        offset_t tlkSz;
        data.read(tlkSz);
        Owned<IFileIO> iFileIO = createIFileI((size32_t)tlkSz, data.readDirect((size32_t)tlkSz));

        // NB: this TLK is an in-memory TLK serialized from the master - the name is for tracing by the key code only
        OwnedRoxieString indexFileName(helper->getIndexFileName());
        StringBuffer name(indexFileName);
        name.append("_tlk");
        lookup = new CKeyLookup(*this, helper, createKeyIndex(name.str(), 0, *iFileIO, true, false)); // MORE - crc is not 0...
        ihash = lookup;
    }
};

//===========================================================================

#define HTSIZE_LIMIT_PC 75 // %
#define HASHDEDUP_HT_INIT_MAX_SIZE 0x600000
#define HASHDEDUP_MINSPILL_THRESHOLD 1000
#define HASHDEDUP_HT_BUCKET_SIZE 0x10000 // 64k (rows)
#define HASHDEDUP_HT_INC_SIZE 0x10000 // 64k (rows)
#define HASHDEDUP_BUCKETS_MIN 11 // (NB: prime #)
#define HASHDEDUP_BUCKETS_MAX 9973 // (NB: prime #)
#define HASHDEDUP_BUCKET_POSTSPILL_PRIORITY 5 // very high, by this stage it's cheap to dispose of

class HashDedupSlaveActivityBase;
class CBucket;
class CHashTableRowTable : private CThorExpandingRowArray
{
    CBucket *owner;
    HashDedupSlaveActivityBase &activity;
    IHash *iRowHash, *iKeyHash;
    ICompare *iCompare;
    rowidx_t htElements, htMax;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CHashTableRowTable(HashDedupSlaveActivityBase &_activity, IRowInterfaces *rowIf, IHash *_iRowHash, IHash *_iKeyHash, ICompare *_iCompare);
    inline const void *query(rowidx_t i) const { return CThorExpandingRowArray::query(i); }
    inline void setOwner(CBucket *_owner) { owner = _owner; }
    bool kill()
    {
        if (!rows)
            return false;
        CThorExpandingRowArray::kill();
        htMax = htElements = 0;
        return true;
    }
    bool clear()
    {
        if (0 == htElements)
            return false;
        CThorExpandingRowArray::clearRows();
        memset(rows, 0, maxRows * sizeof(void *));
        htElements = 0;
        htMax = maxRows * HTSIZE_LIMIT_PC / 100;
        return true;
    }
    void init(rowidx_t sz);
    const void *allocateNewTable()
    {
        rowidx_t newMaxRows = maxRows+HASHDEDUP_HT_INC_SIZE;
        return allocateRowTable(newMaxRows);
    }
    void rehash(const void **newRows);
    bool lookupRow(unsigned htPos, const void *row) const // return true == match
    {
        rowidx_t s = htPos;
        loop
        {
            const void *htKey = rows[htPos];
            if (!htKey)
                break;
            if (0 == iCompare->docompare(row, htKey))
                return true;
            if (++htPos==maxRows)
                htPos = 0;
            if (htPos == s)
                ThrowStringException(0, "lookupRow() HT full - infinite loop!");
        }
        return false;
    }
    inline void addRow(unsigned htPos, const void *row)
    {
        while (NULL != rows[htPos])
        {
            if (++htPos==maxRows)
                htPos = 0;
        }
        // similar to underlying CThorExpandingRowArray::setRow, but cheaper, as know no old row to dispose of
        rows[htPos] = row;
        ++htElements;
        if (htPos+1>numRows) // keeping high water mark
            numRows = htPos+1;
    }
    inline const void *getRowClear(unsigned htPos)
    {
        dbgassertex(htPos < maxRows);
        const void *ret = CThorExpandingRowArray::getClear(htPos);
        if (ret)
            --htElements;
        return ret;
    }
    inline rowidx_t queryHtElements() const { return htElements; }
    inline bool hasRoom() const { return htElements < htMax; }
    inline rowidx_t queryMaxRows() const { return CThorExpandingRowArray::queryMaxRows(); }
    inline const void **getRowArray() { return CThorExpandingRowArray::getRowArray(); }
};

class CSpill : public CSimpleInterface, implements IRowWriter
{
    CActivityBase &owner;
    IRowInterfaces *rowIf;
    rowcount_t count;
    Owned<CFileOwner> spillFile;
    IRowWriter *writer;
    StringAttr desc;
    unsigned bucketN, rwFlags;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSpill(CActivityBase &_owner, IRowInterfaces *_rowIf, const char *_desc, unsigned _bucketN)
        : owner(_owner), rowIf(_rowIf), desc(_desc), bucketN(_bucketN)
    {
        count = 0;
        writer = NULL;
        rwFlags = DEFAULT_RWFLAGS;
    }
    ~CSpill()
    {
        ::Release(writer);
    }
    void init()
    {
        dbgassertex(NULL == writer);
        count = 0;
        StringBuffer tempname, prefix("hashdedup_bucket");
        prefix.append(bucketN).append('_').append(desc);
        GetTempName(tempname, prefix.str(), true);
        OwnedIFile iFile = createIFile(tempname.str());
        spillFile.setown(new CFileOwner(iFile.getLink()));
        if (owner.getOptBool(THOROPT_COMPRESS_SPILLS, true))
            rwFlags |= rw_compress;
        writer = createRowWriter(iFile, rowIf, rwFlags);
    }
    IRowStream *getReader(rowcount_t *_count=NULL) // NB: also detatches ownership of 'fileOwner'
    {
        assertex(NULL == writer); // should have been closed
        Owned<CFileOwner> fileOwner = spillFile.getClear();
        if (!fileOwner)
            return NULL;
        Owned<IExtRowStream> strm = createRowStream(&fileOwner->queryIFile(), rowIf, rwFlags);
        Owned<CStreamFileOwner> fileStream = new CStreamFileOwner(fileOwner, strm);
        if (_count)
            *_count = count;
        return fileStream.getClear();
    }
    rowcount_t getCount() const { return count; }
    void close()
    {
        if (NULL == writer)
            return;
        flush();
        ::Release(writer);
        writer = NULL;
    }
// IRowWriter
    virtual void putRow(const void *row)
    {
        writer->putRow(row);
        ++count; // NULL's too (but there won't be any in usage of this impl.)
    }
    virtual void flush()
    {
        writer->flush();
    }
};

class CBucket : public CSimpleInterface, implements IInterface
{
    HashDedupSlaveActivityBase &owner;
    IRowInterfaces *rowIf, *keyIf;
    IHash *iRowHash, *iKeyHash;
    ICompare *iCompare;
    Owned<IEngineRowAllocator> _keyAllocator;
    IEngineRowAllocator *keyAllocator;
    CHashTableRowTable *htRows;
    bool extractKey, spilt;
    CriticalSection lock;
    unsigned bucketN;
    CSpill rowSpill, keySpill;

    void doSpillHashTable();
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CBucket(HashDedupSlaveActivityBase &_owner, IRowInterfaces *_rowIf, IRowInterfaces *_keyIf, IHash *_iRowHash, IHash *_iKeyHash, ICompare *_iCompare, bool _extractKey, unsigned _bucketN, CHashTableRowTable *_htRows);
    bool addKey(const void *key, unsigned hashValue);
    bool addRow(const void *row, unsigned hashValue);
    void clear();
    bool clearHashTable(bool ptrTable) // returns true if freed mem
    {
        dbgassertex(htRows);
        if (ptrTable)
            return htRows->kill();
        else
            return htRows->clear();
    }
    bool spillHashTable(); // returns true if freed mem
    bool flush(bool critical);
    bool rehash();
    void close()
    {
        rowSpill.close();
        keySpill.close();
    }
    inline IRowStream *getRowStream(rowcount_t *count) { return rowSpill.getReader(count); }
    inline IRowStream *getKeyStream(rowcount_t *count) { return keySpill.getReader(count); }
    inline rowidx_t getKeyCount() const { return htRows->queryHtElements(); }
    inline rowcount_t getSpiltRowCount() const { return rowSpill.getCount(); }
    inline rowcount_t getSpiltKeyCount() const { return keySpill.getCount(); }
    inline bool isSpilt() const { return spilt; }
    inline unsigned queryBucketNumber() const { return bucketN; }
};

class CBucketHandler : public CSimpleInterface, implements IInterface, implements roxiemem::IBufferedRowCallback
{
    HashDedupSlaveActivityBase &owner;
    IRowInterfaces *rowIf, *keyIf;
    IHash *iRowHash, *iKeyHash;
    ICompare *iCompare;
    bool extractKey;
    unsigned numBuckets, currentBucket, nextSpilledBucketFlush;
    unsigned depth, div;
    unsigned nextToSpill;
    PointerArrayOf<CBucket> _buckets;
    CBucket **buckets;
    mutable rowidx_t peakKeyCount;

    rowidx_t getTotalBucketCount() const
    {
        rowidx_t totalCount = 0;
        for (unsigned i=0; i<numBuckets; i++)
            totalCount += buckets[i]->getKeyCount();
        return totalCount;
    }
    void recalcPeakKeyCount() const
    {
        rowidx_t newPeakKeyCount = getTotalBucketCount();
        if ((RCIDXMAX == peakKeyCount) || (newPeakKeyCount > peakKeyCount))
            peakKeyCount = newPeakKeyCount;
    }

    class CPostSpillFlush : implements roxiemem::IBufferedRowCallback
    {
        CBucketHandler &owner;
    public:
        CPostSpillFlush(CBucketHandler &_owner) : owner(_owner)
        {
        }
    // IBufferedRowCallback
        virtual unsigned getPriority() const
        {
            return HASHDEDUP_BUCKET_POSTSPILL_PRIORITY;
        }
        virtual bool freeBufferedRows(bool critical)
        {
            if (NotFound == owner.nextSpilledBucketFlush)
                return false;
            unsigned startNum = owner.nextSpilledBucketFlush;
            loop
            {
                CBucket *bucket = owner.buckets[owner.nextSpilledBucketFlush];
                ++owner.nextSpilledBucketFlush;
                if (owner.nextSpilledBucketFlush == owner.numBuckets)
                    owner.nextSpilledBucketFlush = 0;
                if (bucket->flush(critical))
                    return true;
                if (startNum == owner.nextSpilledBucketFlush)
                    break;
            }
            return false;
        }
    } postSpillFlush;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CBucketHandler(HashDedupSlaveActivityBase &_owner, IRowInterfaces *_rowIf, IRowInterfaces *_keyIf, IHash *_iRowHash, IHash *_iKeyHash, ICompare *_iCompare, bool _extractKey, unsigned _depth, unsigned _div);
    ~CBucketHandler();
    unsigned getBucketEstimate(rowcount_t totalRows) const;
    unsigned getBucketEstimateWithPrev(rowcount_t totalRows, rowidx_t prevPeakKeys, rowidx_t keyCount) const;
    void init(unsigned numBuckets, IRowStream *seeKeys=NULL);
    inline rowcount_t getPeakCount() const
    {
        if (NotFound == nextToSpill)
            recalcPeakKeyCount();
        return peakKeyCount;
    }
    void flushBuckets();
    bool spillBucket(bool critical) // spills a bucket
    {
        if (NotFound == nextToSpill)
        {
            nextToSpill = 0;
            recalcPeakKeyCount();
            // post spill, turn on, on higher priority, flushes spilt buckets that have accrued keys
            nextSpilledBucketFlush = 0;
        }
        unsigned start=nextToSpill;
        do
        {
            // NB: lock ensures exclusivity to write to bucket inside spillHashTable
            // Another thread could create a bucket at this time, but
            // access to 'buckets' (which can be assigned to by other thread) should be atomic, on Intel at least.
            CBucket *bucket = buckets[nextToSpill++];
            if (nextToSpill == numBuckets)
                nextToSpill = 0;
            if (!bucket->isSpilt() && (critical || (bucket->getKeyCount() >= HASHDEDUP_MINSPILL_THRESHOLD)))
            {
                // spill whole bucket unless last
                // The one left, will be last bucket standing and grown to fill mem
                // it is still useful to use as much as poss. of remaining bucket HT as filter
                if (bucket->spillHashTable())
                    return true;
                else if (critical && bucket->clearHashTable(true))
                    return true;
            }
        }
        while (nextToSpill != start);
        return false;
    }
    CBucketHandler *getNextBucketHandler(Owned<IRowStream> &nextInput);
public:
    bool addRow(const void *row);
    inline unsigned calcBucket(unsigned hashValue) const
    {
        // JCSMORE - if huge # of rows 32bit has may not be enough?
        return (hashValue / div) % numBuckets;
    }
// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return SPILL_PRIORITY_HASHDEDUP;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        return spillBucket(critical);
    }
};

class HashDedupSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    IRowStream *input;      // can be changed
    IRowStream *initialInput;
    Owned<IRowStream> currentInput;
    bool inputstopped, eos, lastEog, extractKey, local, isVariable, grouped;
    const char *actTxt;
    IHThorHashDedupArg *helper;
    IHash *iHash, *iKeyHash;
    ICompare *iCompare, *rowKeyCompare;
    Owned<IRowInterfaces> _keyRowInterfaces;
    IRowInterfaces *keyRowInterfaces;
    Owned<CBucketHandler> bucketHandler;
    IArrayOf<CBucketHandler> bucketHandlerStack;
    SpinLock stopSpin;
    PointerArrayOf<CHashTableRowTable> _hashTables;
    CHashTableRowTable **hashTables;
    unsigned numHashTables, initialNumBuckets;
    roxiemem::RoxieHeapFlags allocFlags;

    inline CHashTableRowTable &queryHashTable(unsigned n) const { return *hashTables[n]; }
    void ensureNumHashTables(unsigned _numHashTables)
    {
        rowidx_t initSize = HASHDEDUP_HT_INIT_MAX_SIZE / _numHashTables;
        if (initSize > HASHDEDUP_HT_INC_SIZE)
            initSize = HASHDEDUP_HT_INC_SIZE;
        if (_numHashTables <= numHashTables)
        {
            unsigned i=0;
            for (; i<_numHashTables; i++)
                hashTables[i]->init(initSize);
            for (; i<numHashTables; i++)
            {
                ::Release(hashTables[i]);
                hashTables[i] = NULL;
            }
        }
        else if (_numHashTables > numHashTables)
        {
            _hashTables.ensure(_numHashTables);
            hashTables = (CHashTableRowTable **)_hashTables.getArray();
            for (unsigned i=numHashTables; i<_numHashTables; i++)
            {
                hashTables[i] = new CHashTableRowTable(*this, keyRowInterfaces, iHash, iKeyHash, rowKeyCompare);
                hashTables[i]->init(initSize);
            }
        }
        numHashTables = _numHashTables;
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    HashDedupSlaveActivityBase(CGraphElementBase *_container, bool _local)
        : CSlaveActivity(_container), CThorDataLink(this), local(_local)
    {
        input = initialInput = NULL;
        actTxt = NULL;
        initialNumBuckets = 0;
        inputstopped = eos = lastEog = extractKey = local = isVariable = grouped = false;
        helper = NULL;
        iHash = iKeyHash = NULL;
        iCompare = rowKeyCompare = NULL;
        keyRowInterfaces = NULL;
        hashTables = NULL;
        numHashTables = initialNumBuckets = 0;
        roxiemem::RoxieHeapFlags allocFlags = roxiemem::RHFnone;
    }
    ~HashDedupSlaveActivityBase()
    {
        for (unsigned i=0; i<numHashTables; i++)
            ::Release(hashTables[i]);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorHashDedupArg *)queryHelper();
        iHash = helper->queryHash();
        appendOutputLinked(this);
        iCompare = helper->queryCompare();
        allocFlags = queryJob().queryThorAllocator()->queryFlags();

        // JCSMORE - it may not be worth extracting the key,
        // if there's an upstream activity that holds onto rows for periods of time (e.g. sort)
        IOutputMetaData *km = helper->queryKeySize();
        extractKey = (0 == (HFDwholerecord & helper->getFlags()));
        if (extractKey)
        {
            // if key and row are fixed length, check that estimated memory sizes make it worth extracting key per row
            isVariable = km->isVariableSize();
            if (!isVariable && helper->queryOutputMeta()->isFixedSize())
            {
                roxiemem::IRowManager *rM = queryJob().queryRowManager();
                memsize_t keySize = rM->getExpectedCapacity(km->getMinRecordSize(), allocFlags);
                memsize_t rowSize = rM->getExpectedCapacity(helper->queryOutputMeta()->getMinRecordSize(), allocFlags);
                if (keySize >= rowSize)
                    extractKey = false;
            }
        }
        if (extractKey)
        {
            _keyRowInterfaces.setown(createRowInterfaces(km, queryActivityId(), queryCodeContext()));
            keyRowInterfaces = _keyRowInterfaces;
            rowKeyCompare = helper->queryRowKeyCompare();
            iKeyHash = helper->queryKeyHash();
        }
        else
        {
            isVariable = helper->queryOutputMeta()->isVariableSize();
            keyRowInterfaces = this;
            rowKeyCompare = iCompare;
            iKeyHash = iHash;
        }
        grouped = container.queryGrouped();
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        inputstopped = false;
        eos = lastEog = false;
        startInput(inputs.item(0));
        ThorDataLinkMetaInfo info;
        inputs.item(0)->getMetaInfo(info);
        initialInput = input = inputs.item(0);
        unsigned div = local ? 1 : queryJob().querySlaves(); // if global, hash values already modulated by # slaves
        bucketHandler.setown(new CBucketHandler(*this, this, keyRowInterfaces, iHash, iKeyHash, rowKeyCompare, extractKey, 0, div));
        initialNumBuckets = container.queryXGMML().getPropInt("hint[@name=\"num_buckets\"]/@value");
        if (0 == initialNumBuckets)
        {
            if (grouped)
                initialNumBuckets = HASHDEDUP_BUCKETS_MIN;
            else
                initialNumBuckets = bucketHandler->getBucketEstimate(info.totalRowsMax); // will use default if no meta total
        }
        ensureNumHashTables(initialNumBuckets);
        bucketHandler->init(initialNumBuckets);
        dataLinkStart(actTxt, container.queryId());
    }
    void stopInput()
    {
        if (!inputstopped)
        {
            SpinBlock b(stopSpin);
            CSlaveActivity::stopInput(inputs.item(0));
            inputstopped = true;
        }
    }
    void kill()
    {
        ActPrintLog("%s: kill", actTxt);
        currentInput.clear();
        bucketHandler.clear();
        bucketHandlerStack.kill();
        CSlaveActivity::kill();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eos)
            return NULL;
        // bucket handlers, stream out non-duplicates (1st entry in HT)
        loop
        {
            OwnedConstThorRow row;
            {
                SpinBlock b(stopSpin);
                row.setown(grouped?input->nextRow():input->ungroupedNextRow());
            }
            if (row)
            {
                lastEog = false;
                if (bucketHandler->addRow(row)) // true if new, i.e. non-duplicate (does not take ownership)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
            else
            {
                // If spill event occurred, disk buckets + key buckets will have been created by this stage.
                bucketHandler->flushBuckets();

                Owned<CBucketHandler> nextBucketHandler;
                loop
                {
                    // pop off parents until one has a bucket left to read
                    Owned<IRowStream> nextInput;
                    nextBucketHandler.setown(bucketHandler->getNextBucketHandler(nextInput));
                    if (!nextBucketHandler)
                    {
                        if (!bucketHandlerStack.ordinality())
                        {
                            currentInput.clear();
                            bucketHandler.clear();
                            if (grouped)
                            {
                                if (lastEog)
                                    eos = true;
                                else
                                {
                                    lastEog = true;
                                    // reset for next group
                                    input = initialInput;
                                    bucketHandler.setown(new CBucketHandler(*this, this, keyRowInterfaces, iHash, iKeyHash, rowKeyCompare, extractKey, 0, 1));
                                    ensureNumHashTables(initialNumBuckets); // resets
                                    bucketHandler->init(initialNumBuckets);
                                }
                            }
                            else
                                eos = true;
                            return NULL;
                        }
                        bucketHandler.setown(&bucketHandlerStack.popGet());
                    }
                    else
                    {
                        // NB: if wanted threading, then each sibling bucket could be handled on own thread,
                        // but they'd be competing for disk and memory and involve more locking
                        bucketHandlerStack.append(*bucketHandler.getClear()); // push current handler on to stack
                        bucketHandler.setown(nextBucketHandler.getClear());
                        currentInput.setown(nextInput.getClear());
                        break;
                    }
                }
                assertex(currentInput);
                input = currentInput;
            }
        }
    }

    virtual bool isGrouped() { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) = 0;
friend class CBucketHandler;
friend class CHashTableRowTable;
friend class CBucket;
};

CHashTableRowTable::CHashTableRowTable(HashDedupSlaveActivityBase &_activity, IRowInterfaces *rowIf, IHash *_iRowHash, IHash *_iKeyHash, ICompare *_iCompare)
    : CThorExpandingRowArray(_activity, rowIf, true),
      activity(_activity), iRowHash(_iRowHash), iKeyHash(_iKeyHash), iCompare(_iCompare)
{
    htMax = htElements = 0;
    owner = NULL;
}

void CHashTableRowTable::init(rowidx_t sz)
{
    // reinitialize if need bigger or if requested size is much smaller than existing
    rowidx_t newMaxRows = activity.queryJob().queryRowManager()->getExpectedCapacity(sz * sizeof(rowidx_t *), activity.allocFlags) / sizeof(rowidx_t *);
    if (newMaxRows <= maxRows && ((maxRows-newMaxRows) <= HASHDEDUP_HT_INC_SIZE))
        return;
    ReleaseThorRow(rows);
    OwnedConstThorRow newRows = allocateRowTable(sz);
    if (!newRows)
        throw MakeActivityException(&activity, -1, "Failed to initialize initial memory for hash tables");
    rows = (const void **)newRows.getClear();
    maxRows = RoxieRowCapacity(rows) / sizeof(void *);
    memset(rows, 0, maxRows * sizeof(void *));
    numRows = 0;
    htMax = maxRows * HTSIZE_LIMIT_PC / 100;
}

void CHashTableRowTable::rehash(const void **newRows)
{
    OwnedConstThorRow _newRows = newRows;

    dbgassertex(iKeyHash);
    rowidx_t newMaxRows = RoxieRowCapacity(newRows) / sizeof(void *);
    memset(newRows, 0, newMaxRows * sizeof(void *));
    rowidx_t newNumRows=0;
    for (rowidx_t i=0; i<maxRows; i++)
    {
        const void *row = rows[i];
        if (row)
        {
            unsigned h = iKeyHash->hash(row) % newMaxRows;
            while (NULL != newRows[h])
            {
                if (++h == newMaxRows)
                    h = 0;
            }
            newRows[h] = row;
            if (h>=newNumRows)
                newNumRows = h+1;
        }
    }

    if (maxRows)
        ActPrintLog(&activity, "Rehashed bucket %d - old size = %d, new size = %d, elements = %d", owner->queryBucketNumber(), maxRows, newMaxRows, htElements);

    const void **oldRows = rows;
    rows = (const void **)_newRows.getClear();
    ReleaseThorRow(oldRows);
    maxRows = newMaxRows;
    numRows = newNumRows;
    htMax = maxRows * HTSIZE_LIMIT_PC / 100;
}

//

CBucket::CBucket(HashDedupSlaveActivityBase &_owner, IRowInterfaces *_rowIf, IRowInterfaces *_keyIf, IHash *_iRowHash, IHash *_iKeyHash, ICompare *_iCompare, bool _extractKey, unsigned _bucketN, CHashTableRowTable *_htRows)
    : owner(_owner), rowIf(_rowIf), keyIf(_keyIf), iRowHash(_iRowHash), iKeyHash(_iKeyHash), iCompare(_iCompare), extractKey(_extractKey), bucketN(_bucketN), htRows(_htRows),
      rowSpill(owner, _rowIf, "rows", _bucketN), keySpill(owner, _keyIf, "keys", _bucketN)

{
    spilt = false;
    // ideally want rows in bucket to be contiguous, so when it spills, pages will be released
    if (extractKey)
    {   // use own allocator
        unsigned flags = owner.allocFlags | roxiemem::RHFunique;
        _keyAllocator.setown(owner.queryJob().getRowAllocator(keyIf->queryRowMetaData(), owner.queryActivityId(), (roxiemem::RoxieHeapFlags)flags));
        keyAllocator = _keyAllocator;
    }
    else
        keyAllocator = keyIf->queryRowAllocator();
}

void CBucket::clear()
{
    // bucket read-only after this (getKeyStream/getRowStream etc.)
    if (htRows)
    {
        clearHashTable(false);
        htRows = NULL;
    }
}

void CBucket::doSpillHashTable()
{
    if (isSpilt())
        return;
    spilt = true;
    rowSpill.init();
    keySpill.init();
    rowidx_t maxRows = htRows->queryMaxRows();
    for (rowidx_t i=0; i<maxRows; i++)
    {
        OwnedConstThorRow key = htRows->getRowClear(i);
        if (key)
            keySpill.putRow(key.getClear());
    }
}

bool CBucket::spillHashTable()
{
    CriticalBlock b(lock);
    rowidx_t removeN = htRows->queryHtElements();
    if (0 == removeN || spilt) // NB: if split, will be handled by CBucket on different priority
        return false; // signal nothing to spill
    doSpillHashTable();
    ActPrintLog(&owner, "Spilt bucket %d - %d elements of hash table", bucketN, removeN);
    return true;
}

bool CBucket::flush(bool critical)
{
    CriticalBlock b(lock);
    if (isSpilt())
    {
        rowidx_t count = getKeyCount();
        // want to avoid flushing tiny buckets (unless critical), to make room for a few rows repeatedly
        if (critical || (count >= HASHDEDUP_MINSPILL_THRESHOLD))
        {
            if (clearHashTable(critical))
            {
                PROGLOG("Flushed(%s) bucket %d - %d elements", critical?"(critical)":"", queryBucketNumber(), count);
                return true;
            }
        }
    }
    return false;
}

bool CBucket::addKey(const void *key, unsigned hashValue)
{
    {
        CriticalBlock b(lock);
        if (!isSpilt())
        {
            bool doAdd = true;
            if (!htRows->hasRoom())
            {
                // attempt rehash
                if (!rehash())
                {
                    // no room to rehash, ensure spilt
                    doSpillHashTable(); // NB: may have spilt already when allocating for rehash
                    doAdd = false;
                }
            }
            if (doAdd)
            {
                unsigned htPos = hashValue % htRows->queryMaxRows();
                LinkThorRow(key);
                htRows->addRow(htPos, key);
                return true;
            }
        }
    }
    LinkThorRow(key);
    keySpill.putRow(key);
    return false;
}

// NB: always called inside a CriticalBlock b(lock)
// NB2: returns true if okay to proceed to use HT (basically if !spilt)
bool CBucket::rehash()
{
    // Returns true, if there's room in HT

    // Have to be careful not to block 'lock' when allocating here.
    // Because, spillHashTable() needs to block 'lock'
    OwnedConstThorRow newHtRows;
    {
        CriticalUnblock b(lock); // allocate may cause spill
        newHtRows.setown(htRows->allocateNewTable());
    }
    if (!newHtRows)
        return false;
    htRows->rehash((const void **)newHtRows.getClear());
    return true;
}

bool CBucket::addRow(const void *row, unsigned hashValue)
{
    {
        CriticalBlock b(lock);
        bool doAdd = true;
        bool needRehash = !htRows->hasRoom();
        if (needRehash)
        {
            if (isSpilt())
                doAdd = false; // don't rehash if full and already spilt
        }
        if (htRows->queryMaxRows()) // might be 0, if HT cleared
        {
            // Even if not adding, check HT for dedupping purposes upfront
            unsigned htPos = hashValue % htRows->queryMaxRows();
            if (htRows->lookupRow(htPos, row))
                return false; // dedupped

            if (doAdd)
            {
                if (needRehash)
                {
                    if (rehash()) // even if rehash fails, there may be room to continue (following a flush)
                        htPos = hashValue % htRows->queryMaxRows();
                    else
                    {
                        // no room to rehash, ensure spilt
                        doSpillHashTable(); // NB: may have spilt already when allocating for rehash
                    }
                }
                if (htRows->hasRoom())
                {
                    OwnedConstThorRow key;
                    if (extractKey)
                    {
                        CriticalUnblock b(lock); // will allocate, might cause spill
                        RtlDynamicRowBuilder krow(keyAllocator);
                        size32_t sz = owner.helper->recordToKey(krow, row);
                        assertex(sz);
                        key.setown(krow.finalizeRowClear(sz));
                    }
                    else
                        key.set(row);
                    if (htRows->queryMaxRows())
                        htRows->addRow(htPos, key.getClear());
                    if (!isSpilt()) // could have spilt whilst extracting key
                        return true;

                    // if spilt, then still added/used to dedup, but have to commit row to disk
                    // as no longer know it's 1st/unique
                }
            }
        }
    }
    LinkThorRow(row);
    rowSpill.putRow(row);
    return false;
}

//

CBucketHandler::CBucketHandler(HashDedupSlaveActivityBase &_owner, IRowInterfaces *_rowIf, IRowInterfaces *_keyIf, IHash *_iRowHash, IHash *_iKeyHash, ICompare *_iCompare, bool _extractKey, unsigned _depth, unsigned _div)
    : owner(_owner), rowIf(_rowIf), keyIf(_keyIf), iRowHash(_iRowHash), iKeyHash(_iKeyHash), iCompare(_iCompare), extractKey(_extractKey), depth(_depth), div(_div), postSpillFlush(*this)
{
    currentBucket = 0;
    nextToSpill = NotFound;
    peakKeyCount = RCIDXMAX;
    nextSpilledBucketFlush = NotFound;
}

CBucketHandler::~CBucketHandler()
{
    owner.queryJob().queryRowManager()->removeRowBuffer(this);
    owner.queryJob().queryRowManager()->removeRowBuffer(&postSpillFlush);
    for (unsigned i=0; i<numBuckets; i++)
        ::Release(buckets[i]);
}

void CBucketHandler::flushBuckets()
{
    owner.queryJob().queryRowManager()->removeRowBuffer(this);
    owner.queryJob().queryRowManager()->removeRowBuffer(&postSpillFlush);
    for (unsigned i=0; i<numBuckets; i++)
    {
        CBucket &bucket = *buckets[i];
        bucket.clear();
        // close stream now, to flush rows out in write streams
        if (bucket.isSpilt())
            bucket.close();
    }
}

unsigned CBucketHandler::getBucketEstimateWithPrev(rowcount_t totalRows, rowidx_t prevPeakKeys, rowidx_t keyCount) const
{
    // how many buckets would the # records we managed to squeeze in last round fit it:
    unsigned retBuckets = 0;
    if (prevPeakKeys)
    {
        retBuckets = (unsigned)(((keyCount+totalRows)+prevPeakKeys-1) / prevPeakKeys);
        retBuckets = retBuckets * 125 / 100;
    }
    if (retBuckets < HASHDEDUP_BUCKETS_MIN)
        retBuckets = HASHDEDUP_BUCKETS_MIN;
    else if (retBuckets > HASHDEDUP_BUCKETS_MAX)
        retBuckets = HASHDEDUP_BUCKETS_MAX;
    return retBuckets;
}

unsigned CBucketHandler::getBucketEstimate(rowcount_t totalRows) const
{
    // Try and estimate the number of buckets that the hash dedup should use.
    // If the number of buckets is too high the hash dedup may write to too many individual disk files - potentially causing many excessive seeks.
    // It will also not use all the memory in the next phase.
    // I'm not sure either are significant problems (output is buffered, and second point may be a bonus).
    // If the number of buckets is too low then we might need to do another round of spilling - which is painful.
    //
    // The estimation below, assumes there are no duplicates, i.e. the worse case, as such, it is very likely to be an over-estimate.
    // Over-estimating is likely to be preferable to underestimating, which would cause more phases.
    //
    // Lower and upper bounds are defined for # buckets (HASHDEDUP_BUCKETS_MIN/HASHDEDUP_BUCKETS_MAX)
    // NB: initial estimate is bypassed completely if "num_buckets" hint specifics starting # buckets.

    unsigned retBuckets = HASHDEDUP_BUCKETS_MIN;
    // only guess in fixed case and if totalRows known and >0
    if (RCMAX != totalRows && totalRows>0 && !owner.isVariable)
    {
        // Rough estimate for # buckets to start with
        // likely to be way off for variable

        // JCSMORE - will need to change based on whether upstream keeps packed or not.
        roxiemem::IRowManager *rM = owner.queryJob().queryRowManager();

        memsize_t availMem = roxiemem::getTotalMemoryLimit()-0x500000;
        memsize_t initKeySize = rM->getExpectedCapacity(keyIf->queryRowMetaData()->getMinRecordSize(), owner.allocFlags);
        memsize_t minBucketSpace = retBuckets * rM->getExpectedCapacity(HASHDEDUP_HT_BUCKET_SIZE * sizeof(void *), owner.allocFlags);

        rowcount_t _maxRowGuess = (availMem-minBucketSpace) / initKeySize; // without taking into account ht space / other overheads
        rowidx_t maxRowGuess;
        if (_maxRowGuess >= RCIDXMAX/sizeof(void *)) // because can't allocate a block bigger than 32bit
            maxRowGuess = (rowidx_t)RCIDXMAX/sizeof(void *);
        else
            maxRowGuess = (rowidx_t)_maxRowGuess;
        memsize_t bucketSpace = retBuckets * rM->getExpectedCapacity(((maxRowGuess+retBuckets-1)/retBuckets) * sizeof(void *), owner.allocFlags);
        // now rebase maxRowGuess
        _maxRowGuess = (availMem-bucketSpace) / initKeySize;
        if (_maxRowGuess >= RCIDXMAX/sizeof(void *))
            maxRowGuess = (rowidx_t)RCIDXMAX/sizeof(void *);
        else
            maxRowGuess = (rowidx_t)_maxRowGuess;
        maxRowGuess = (rowidx_t) (((float)maxRowGuess / 100) * HTSIZE_LIMIT_PC); // scale down to ht limit %
        memsize_t rowMem = maxRowGuess * initKeySize;
        if (rowMem > (availMem-bucketSpace))
        {
            rowMem = availMem - minBucketSpace; // crude
            maxRowGuess = rowMem / initKeySize;
        }
        retBuckets = (unsigned)((totalRows+maxRowGuess-1) / maxRowGuess);
    }
    if (retBuckets < HASHDEDUP_BUCKETS_MIN)
        retBuckets = HASHDEDUP_BUCKETS_MIN;
    else if (retBuckets > HASHDEDUP_BUCKETS_MAX)
        retBuckets = HASHDEDUP_BUCKETS_MAX;
    return retBuckets;
}

void CBucketHandler::init(unsigned _numBuckets, IRowStream *keyStream)
{
    numBuckets = _numBuckets;
    _buckets.ensure(numBuckets);
    buckets = (CBucket **)_buckets.getArray();
    for (unsigned i=0; i<numBuckets; i++)
    {
        CHashTableRowTable &htRows = owner.queryHashTable(i);
        buckets[i] = new CBucket(owner, rowIf, keyIf, iRowHash, iKeyHash, iCompare, extractKey, i, &htRows);
        htRows.setOwner(buckets[i]);
    }
    ActPrintLog(&owner, "Max %d buckets, current depth = %d", numBuckets, depth+1);
    owner.queryJob().queryRowManager()->addRowBuffer(this);
    // postSpillFlush not needed until after 1 spill event, but not safe to add within callback
    owner.queryJob().queryRowManager()->addRowBuffer(&postSpillFlush);
    if (keyStream)
    {
        loop
        {
            OwnedConstThorRow key = keyStream->nextRow();
            if (!key)
                break;
            unsigned hashValue = iKeyHash->hash(key);
            unsigned bucketN = calcBucket(hashValue);
            buckets[bucketN]->addKey(key, hashValue);
        }
    }
}

CBucketHandler *CBucketHandler::getNextBucketHandler(Owned<IRowStream> &nextInput)
{
    if (NotFound == nextToSpill) // no spilling
        return NULL;
    while (currentBucket<numBuckets)
    {
        CBucket *bucket = buckets[currentBucket];
        if (bucket->isSpilt())
        {
            rowcount_t keyCount, count;
            // JCSMORE ideally, each key and row stream, would use a unique allocator per destination bucket
            // thereby keeping rows/keys together in pages, making it easier to free pages on spill requests
            Owned<IRowStream> keyStream = bucket->getKeyStream(&keyCount);
            Owned<CBucketHandler> newBucketHandler = new CBucketHandler(owner, rowIf, keyIf, iRowHash, iKeyHash, iCompare, extractKey, depth+1, div*numBuckets);
            ActPrintLog(&owner, "Created bucket handler %d, depth %d", currentBucket, depth+1);
            nextInput.setown(bucket->getRowStream(&count));
            // Use peak in mem keys as estimate for next round of buckets.
            unsigned nextNumBuckets = getBucketEstimateWithPrev(count, (rowidx_t)getPeakCount(), (rowidx_t)keyCount);
            owner.ensureNumHashTables(nextNumBuckets);
            newBucketHandler->init(nextNumBuckets, keyStream);
            ++currentBucket;
            return newBucketHandler.getClear();
        }
        ++currentBucket;
    }
    return NULL;
}

bool CBucketHandler::addRow(const void *row)
{
    unsigned hashValue = iRowHash->hash(row);
    unsigned bucketN = calcBucket(hashValue);
    return buckets[bucketN]->addRow(row, hashValue);
}

class LocalHashDedupSlaveActivity : public HashDedupSlaveActivityBase
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    LocalHashDedupSlaveActivity(CGraphElementBase *container)
        : HashDedupSlaveActivityBase(container, true)
    {
        actTxt = "LOCALHASHDEDUP";
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
        : HashDedupSlaveActivityBase(container, false)
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
        distributor = createHashDistributor(this, container.queryJob().queryJobComm(), mptag, true, this);
    }
    void start()
    {
        HashDedupSlaveActivityBase::start();
        ActivityTimer s(totalCycles, timeActivities, NULL);
        Owned<IRowInterfaces> myRowIf = getRowInterfaces(); // avoiding circular link issues
        instrm.setown(distributor->connect(myRowIf, input, iHash, iCompare));
        input = instrm.get();
    }
    void stop()
    {
        ActPrintLog("%s: stopping", actTxt);
        if (instrm)
        {
            instrm->stop();
            instrm.clear();
        }
        distributor->disconnect(true);
        distributor->join();
        stopInput();
        dataLinkStop();
    }
    void abort()
    {
        HashDedupSlaveActivityBase::abort();
        if (distributor)
            distributor->abort();
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
    CriticalSection joinHelperCrit;
    CriticalSection stopsect;
    rowcount_t lhsProgressCount;
    rowcount_t rhsProgressCount;
    bool inputLstopped;
    bool inputRstopped;
    bool leftdone;
    mptag_t mptag;
    mptag_t mptag2;
    Owned<IHashDistributor> lhsDistributor, rhsDistributor;

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
        if (!lhsDistributor)
            lhsDistributor.setown(createHashDistributor(this, container.queryJob().queryJobComm(), mptag, false, this));
        Owned<IRowStream> reader = lhsDistributor->connect(queryRowInterfaces(inL), inL, ihashL, icompareL);
        Owned<IThorRowLoader> loaderL = createThorRowLoader(*this, ::queryRowInterfaces(inL), icompareL, true, rc_allDisk, SPILL_PRIORITY_HASHJOIN);
        strmL.setown(loaderL->load(reader, abortSoon));
        loaderL.clear();
        reader.clear();
        stopInputL();
        lhsDistributor->disconnect(false);
        lhsDistributor->join();
        leftdone = true;
        if (!rhsDistributor)
            rhsDistributor.setown(createHashDistributor(this, container.queryJob().queryJobComm(), mptag2, false, this));
        reader.setown(rhsDistributor->connect(queryRowInterfaces(inR), inR, ihashR, icompareR));
        Owned<IThorRowLoader> loaderR = createThorRowLoader(*this, ::queryRowInterfaces(inR), icompareR, true, rc_mixed, SPILL_PRIORITY_HASHJOIN);;
        strmR.setown(loaderR->load(reader, abortSoon));
        loaderR.clear();
        reader.clear();
        stopInputR();
        rhsDistributor->disconnect(false);
        rhsDistributor->join();
        { CriticalBlock b(joinHelperCrit);
            switch(container.getKind())
            {
                case TAKhashjoin:
                    {
                        bool hintparallelmatch = container.queryXGMML().getPropInt("hint[@name=\"parallel_match\"]/@value")!=0;
                        bool hintunsortedoutput = container.queryXGMML().getPropInt("hint[@name=\"unsorted_output\"]/@value")!=0;
                        joinhelper.setown(createJoinHelper(*this, joinargs, this, hintparallelmatch, hintunsortedoutput));
                    }
                    break;
                case TAKhashdenormalize:
                case TAKhashdenormalizegroup:
                    joinhelper.setown(createDenormalizeHelper(*this, joinargs, this));
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
        {
            CriticalBlock b(joinHelperCrit);
            joinhelper.clear();
        }
        dataLinkStop();
    }
    void kill()
    {
        ActPrintLog("HASHJOIN: kill");
        CSlaveActivity::kill();
    }
    void abort()
    {
        CSlaveActivity::abort();
        if (lhsDistributor)
            lhsDistributor->abort();
        if (rhsDistributor)
            rhsDistributor->abort();
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

CThorRowAggregator *mergeLocalAggs(Owned<IHashDistributor> &distributor, CActivityBase &activity, IHThorRowAggregator &helper, IHThorHashAggregateExtra &helperExtra, CThorRowAggregator *localAggTable, mptag_t mptag, bool ordered)
{
    Owned<IRowStream> strm;
    Owned<CThorRowAggregator> globalAggTable = new CThorRowAggregator(activity, helperExtra, helper);
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
        if (!distributor)
            distributor.setown(createPullHashDistributor(&activity, activity.queryContainer().queryJob().queryJobComm(), mptag, false, NULL));
        strm.setown(distributor->connect(nodeRowMetaRowIf, localAggregatedStream, &nodeCompare, &nodeCompare));
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
        if (!distributor)
            distributor.setown(createHashDistributor(&activity, activity.queryContainer().queryJob().queryJobComm(), mptag, false, NULL));
        Owned<IRowInterfaces> rowIf = activity.getRowInterfaces(); // create new rowIF / avoid using activities IRowInterface, otherwise suffer from circular link
        strm.setown(distributor->connect(rowIf, localAggregatedStream, helperExtra.queryHashElement(), NULL));
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
    distributor->join();

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
    Owned<IHashDistributor> distributor;

    bool doNextGroup()
    {
        localAggTable->start(queryRowAllocator());
        while (!abortSoon)
        {
            OwnedConstThorRow row = input->nextRow();
            if (!row)
            {
                if (container.queryGrouped())
                    break;
                row.setown(input->nextRow());
                if (!row)
                    break;
            }
            localAggTable->addRow(row);
        }
        return 0 != localAggTable->elementCount();
    }

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

        if (!container.queryLocalOrGrouped())
        {
            mptag = container.queryJob().deserializeMPTag(data);
            ActPrintLog("HASHAGGREGATE: init tags %d",(int)mptag);
        }
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        localAggTable.setown(new CThorRowAggregator(*this, *helper, *helper));
        doNextGroup(); // or local set if !grouped
        if (!container.queryGrouped())
            ActPrintLog("Table before distribution contains %d entries", localAggTable->elementCount());
        if (!container.queryLocalOrGrouped() && container.queryJob().querySlaves()>1)
        {
            bool ordered = 0 != (TAForderedmerge & helper->getAggregateFlags());
            localAggTable.setown(mergeLocalAggs(distributor, *this, *helper, *helper, localAggTable, mptag, ordered));
            ActPrintLog("Table after distribution contains %d entries", localAggTable->elementCount());
        }
        eos = false;
        dataLinkStart("HASHAGGREGATE", container.queryId());
    }
    void stop()
    {
        ActPrintLog("HASHAGGREGATE: stopping");
        stopInput(input);
        dataLinkStop();
    }
    void abort()
    {
        CSlaveActivity::abort();
        if (distributor)
            distributor->abort();
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
        if (container.queryGrouped())
        {
            localAggTable->reset();
            if (!doNextGroup())
                eos = true;
        }
        else
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

