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

#include "thactivityutil.ipp"
#include "thcompressutil.hpp"
#include "thexception.hpp"
#include "thbufdef.hpp"
#include "jbuff.hpp"
#include "jset.hpp"
#include "jisem.hpp"

#include "thorxmlwrite.hpp"
#include "../hashdistrib/thhashdistribslave.ipp"
#include "thsortu.hpp"

#ifdef _DEBUG
#define _TRACEBROADCAST
#endif


enum join_t { JT_Undefined, JT_Inner, JT_LeftOuter, JT_RightOuter, JT_LeftOnly, JT_RightOnly, JT_LeftOnlyTransform };


#define MAX_SEND_SIZE 0x100000 // 1MB
#define MAX_QUEUE_BLOCKS 5

enum broadcast_code { bcast_none, bcast_send, bcast_sendStopping, bcast_stop };
enum broadcast_flags { bcastflag_null=0, bcastflag_spilt=0x100, bcastflag_standardjoin=0x200 };
#define BROADCAST_CODE_MASK 0x00FF
#define BROADCAST_FLAG_MASK 0xFF00
class CSendItem : public CSimpleInterface
{
    CMessageBuffer msg;
    unsigned info;
    unsigned origin, headerLen;
public:
    CSendItem(broadcast_code _code, unsigned _origin) : info((unsigned)_code), origin(_origin)
    {
        msg.append(info);
        msg.append(origin);
        headerLen = msg.length();
    }
    CSendItem(CMessageBuffer &_msg)
    {
        msg.swapWith(_msg);
        msg.read(info);
        msg.read(origin);
        headerLen = msg.getPos();
    }
    unsigned length() const { return msg.length(); }
    void reset() { msg.setLength(headerLen); }
    CMessageBuffer &queryMsg() { return msg; }
    broadcast_code queryCode() const { return (broadcast_code)(info & BROADCAST_CODE_MASK); }
    unsigned queryOrigin() const { return origin; }
    broadcast_flags queryFlags() const { return (broadcast_flags)(info & BROADCAST_FLAG_MASK); }
    void setFlag(broadcast_flags _flag)
    {
        info = (info & ~BROADCAST_FLAG_MASK) | ((short)_flag);
        msg.writeDirect(0, sizeof(info), &info); // update
    }
};


interface IBCastReceive
{
    virtual void bCastReceive(CSendItem *sendItem) = 0;
};

/*
 * CBroadcaster, is a utility class, that sends CSendItem packets to sibling nodes, which in turn resend to others,
 * ensuring the data is broadcast to all other nodes.
 * sender and receiver threads are employed to handle the receipt/resending of packets.
 * CBroadcaster should be started on all receiving nodes, each receiver will receive CSendItem packets
 * through IBCastReceive::bCastReceive calls.
 */
class CBroadcaster : public CSimpleInterface
{
    ICommunicator &comm;
    CActivityBase &activity;
    mptag_t mpTag;
    unsigned myNode, slaves;
    IBCastReceive *recvInterface;
    InterruptableSemaphore allDoneSem;
    CriticalSection allDoneLock, bcastOtherCrit;
    bool allDone, allDoneWaiting, allRequestStop, stopping, stopRecv;
    broadcast_flags stopFlag;
    Owned<IBitSet> slavesDone, slavesStopping;

    class CRecv : implements IThreaded
    {
        CBroadcaster &broadcaster;
        CThreadedPersistent threaded;
        bool aborted;
    public:
        CRecv(CBroadcaster &_broadcaster) : threaded("CBroadcaster::CRecv", this), broadcaster(_broadcaster)
        {
            aborted = false;
        }
        void start()
        {
            aborted = false;
            threaded.start();
        }
        void abort(bool join)
        {
            if (aborted)
                return;
            aborted = true;
            broadcaster.cancelReceive();
            if (join)
                threaded.join();
        }
        void wait()
        {
            threaded.join();
        }
    // IThreaded
        virtual void main()
        {
            try
            {
                broadcaster.recvLoop();
            }
            catch (IException *e)
            {
                EXCLOG(e, "CRecv");
                abort(false);
                broadcaster.cancel(e);
                e->Release();
            }
        }
    } receiver;
    class CSend : implements IThreaded
    {
        CBroadcaster &broadcaster;
        CThreadedPersistent threaded;
        SimpleInterThreadQueueOf<CSendItem, true> broadcastQueue;
        Owned<IException> exception;
        bool aborted;
        void clearQueue()
        {
            loop
            {
                Owned<CSendItem> sendItem = broadcastQueue.dequeueNow();
                if (NULL == sendItem)
                    break;
            }
        }
    public:
        CSend(CBroadcaster &_broadcaster) : threaded("CBroadcaster::CSend", this), broadcaster(_broadcaster)
        {
            aborted = false;
        }
        ~CSend()
        {
            clearQueue();
        }
        void addBlock(CSendItem *sendItem)
        {
            if (exception)
            {
                if (sendItem)
                    sendItem->Release();
                throw exception.getClear();
            }
            broadcastQueue.enqueue(sendItem); // will block if queue full
        }
        void start()
        {
            aborted = false;
            exception.clear();
            threaded.start();
        }
        void abort(bool join)
        {
            if (aborted)
                return;
            aborted = true;
            broadcastQueue.stop();
            clearQueue();
            if (join)
                threaded.join();
        }
        void wait()
        {
            ActPrintLog(&broadcaster.activity, "CSend::wait(), messages to send: %d", broadcastQueue.ordinality());
            addBlock(NULL);
            threaded.join();
        }
    // IThreaded
        virtual void main()
        {
            try
            {
                while (!broadcaster.activity.queryAbortSoon())
                {
                    Owned<CSendItem> sendItem = broadcastQueue.dequeue();
                    if (NULL == sendItem)
                        break;
                    broadcaster.broadcastToOthers(sendItem);
                }
            }
            catch (IException *e)
            {
                EXCLOG(e, "CSend");
                exception.setown(e);
                abort(false);
                broadcaster.cancel(e);
            }
            ActPrintLog(&broadcaster.activity, "Sender stopped");
        }
    } sender;

    // NB: returns true if all except me(myNode) are done
    bool slaveStop(unsigned slave)
    {
        CriticalBlock b(allDoneLock);
        bool done = slavesDone->testSet(slave, true);
        assertex(false == done);
        unsigned which = slavesDone->scan(0, false);
        if (which == slaves) // i.e. got all
        {
            allDone = true;
            if (allDoneWaiting)
            {
                allDoneWaiting = false;
                allDoneSem.signal();
            }
        }
        else if (which == (myNode-1))
        {
            if (slavesDone->scan(which+1, false) == slaves)
                return true; // all done except me
        }
        return false;
    }
    unsigned target(unsigned i, unsigned node)
    {
        // For a tree broadcast, calculate the next node to send the data to. i represents the ith copy sent from this node.
        // node is a 0 based node number.
        // It returns a 1 based node number of the next node to send the data to.
        unsigned n = node;
        unsigned j=0;
        while (n)
        {
            j++;
            n /= 2;
        }
        return ((1<<(i+j))+node)+1;
    }
    void broadcastToOthers(CSendItem *sendItem)
    {
        mptag_t rt = activity.queryMPServer().createReplyTag();
        unsigned origin = sendItem->queryOrigin();
        unsigned psuedoNode = (myNode<origin) ? slaves-origin+myNode : myNode-origin;
        CMessageBuffer replyMsg;
        // sends to all in 1st pass, then waits for ack from all
        CriticalBlock b(bcastOtherCrit);
        for (unsigned sendRecv=0; sendRecv<2 && !activity.queryAbortSoon(); sendRecv++)
        {
            unsigned i = 0;
            while (!activity.queryAbortSoon())
            {
                unsigned t = target(i++, psuedoNode);
                if (t>slaves)
                    break;
                t += (origin-1);
                if (t>slaves)
                    t -= slaves;
                unsigned sendLen = sendItem->length();
                if (0 == sendRecv) // send
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sending to node %d, origin %d, size %d, code=%d", myNode, t, origin, sendLen, (unsigned)sendItem->queryCode());
#endif
                    CMessageBuffer &msg = sendItem->queryMsg();
                    msg.setReplyTag(rt); // simulate sendRecv
                    comm.send(msg, t, mpTag);
                }
                else // recv reply
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sent to node %d, origin %d, size %d, code=%d - received ack", myNode, t, origin, sendLen, (unsigned)sendItem->queryCode());
#endif
                    if (!activity.receiveMsg(replyMsg, t, rt))
                        break;
                }
            }
        }
    }
    // called by CRecv thread
    void cancelReceive()
    {
        stopRecv = true;
        activity.cancelReceiveMsg(RANK_ALL, mpTag);
    }
    void recvLoop()
    {
        CMessageBuffer msg;
        while (!stopRecv && !activity.queryAbortSoon())
        {
            rank_t sendRank;
            if (!activity.receiveMsg(msg, RANK_ALL, mpTag, &sendRank))
                break;
            mptag_t replyTag = msg.getReplyTag();
            CMessageBuffer ackMsg;
            Owned<CSendItem> sendItem = new CSendItem(msg);
#ifdef _TRACEBROADCAST
            ActPrintLog(&activity, "Broadcast node %d received from node %d, origin node %d, size %d, code=%d", myNode, (unsigned)sendRank, sendItem->queryOrigin(), sendItem->length(), (unsigned)sendItem->queryCode());
#endif
            comm.send(ackMsg, sendRank, replyTag); // send ack
            sender.addBlock(sendItem.getLink());
            assertex(myNode != sendItem->queryOrigin());
            switch (sendItem->queryCode())
            {
                case bcast_stop:
                {
                    CriticalBlock b(allDoneLock);
                    if (slaveStop(sendItem->queryOrigin()-1) || allDone)
                    {
                        recvInterface->bCastReceive(sendItem.getClear());
                        ActPrintLog(&activity, "recvLoop, received last slaveStop");
                        // NB: this slave has nothing more to receive.
                        // However the sender will still be re-broadcasting some packets, including these stop packets
                        return;
                    }
                    break;
                }
                case bcast_sendStopping:
                {
                    slavesStopping->set(sendItem->queryOrigin()-1, true);
                    // allRequestStop=true, if I'm stopping and all others have requested also
                    allRequestStop = slavesStopping->scan(0, false) == slaves;
                    // fall through
                }
                case bcast_send:
                {
                    if (!allRequestStop) // don't care if all stopping
                        recvInterface->bCastReceive(sendItem.getClear());
                    break;
                }
                default:
                    throwUnexpected();
            }
        }
    }
public:
    CBroadcaster(CActivityBase &_activity) : activity(_activity), receiver(*this), sender(*this), comm(_activity.queryJobChannel().queryJobComm())
    {
        allDone = allDoneWaiting = allRequestStop = stopping = stopRecv = false;
        myNode = activity.queryJobChannel().queryMyRank();
        slaves = activity.queryJob().querySlaves();
        slavesDone.setown(createThreadSafeBitSet());
        slavesStopping.setown(createThreadSafeBitSet());
        mpTag = TAG_NULL;
        recvInterface = NULL;
        stopFlag = bcastflag_null;
    }
    void start(IBCastReceive *_recvInterface, mptag_t _mpTag, bool _stopping)
    {
        stopping = _stopping;
        if (stopping)
            setStopping();
        recvInterface = _recvInterface;
        stopRecv = false;
        mpTag = _mpTag;
        receiver.start();
        sender.start();
    }
    void reset()
    {
        allDone = allDoneWaiting = allRequestStop = stopping = false;
        stopFlag = bcastflag_null;
        slavesDone->reset();
        slavesStopping->reset();
    }
    CSendItem *newSendItem(broadcast_code code)
    {
        if (stopping && (bcast_send==code))
            code = bcast_sendStopping;
        return new CSendItem(code, myNode);
    }
    void resetSendItem(CSendItem *sendItem)
    {
        sendItem->reset();
    }
    void waitReceiverDone()
    {
        {
            CriticalBlock b(allDoneLock);
            slaveStop(myNode-1);
            if (allDone)
                return;
            allDoneWaiting = true;
        }
        allDoneSem.wait();
    }
    void end()
    {
        waitReceiverDone();
        receiver.wait(); // terminates when received stop from all others
        sender.wait(); // terminates when any remaining packets, including final stop packets have been re-broadcast
    }
    void cancel(IException *e=NULL)
    {
        allDoneWaiting = false;
        allDone = true;
        receiver.abort(true);
        sender.abort(true);
        if (e)
        {
            allDoneSem.interrupt(LINK(e));
            activity.fireException(e);
        }
        else
            allDoneSem.signal();
    }
    bool send(CSendItem *sendItem)
    {
        broadcastToOthers(sendItem);
        return !allRequestStop;
    }
    bool isStopping()
    {
        return slavesStopping->test(myNode-1);
    }
    broadcast_flags queryStopFlag() { return stopFlag; }
    bool stopRequested()
    {
        if (bcastflag_null != queryStopFlag()) // if this node has requested to stop immediately
            return true;
        return allRequestStop; // if not, if all have request to stop
    }
    void setStopping()
    {
        slavesStopping->set(myNode-1, true);
    }
    void stop(broadcast_flags flag)
    {
        setStopping();
        stopFlag = flag;
    }
};

/* CMarker processes a sorted set of rows, comparing every adjacent row.
 * It creates a bitmap, where 1 represents row N mismatches N+1
 * Multiple threads are used to process blocks of the table in parallel
 * When complete, it knows how many unique values there are in the table
 * which are subsequently stepped through via findNextBoundary() to build
 * up a hash table
 */
class CMarker
{
    CActivityBase &activity;
    NonReentrantSpinLock lock;
    ICompare *cmp;
    OwnedConstThorRow bitSetMem; // for thread unsafe version
    Owned<IBitSet> bitSet;
    const void **base;
    rowidx_t nextChunkStartRow; // Updated as threads request next chunk
    rowidx_t rowCount, chunkSize; // There are configured at start of calculate()
    rowidx_t parallelMinChunkSize, parallelChunkSize; // Constant, possibly configurable in future
    unsigned threadCount;

    class CCompareThread : public CInterface, implements IThreaded
    {
        CMarker &parent;
        CThreaded threaded;
        rowidx_t startRow, endRow, chunkUnique;
    public:
        CCompareThread(CMarker &_parent, rowidx_t _startRow, rowidx_t _endRow)
            : parent(_parent), startRow(_startRow), endRow(_endRow), threaded("CCompareThread", this)
        {
            chunkUnique = 0;
        }
        rowidx_t getUnique() const { return chunkUnique; }
        void start() { threaded.start(); }
        void join() { threaded.join(); }
    // IThreaded
        virtual void main()
        {
            chunkUnique = parent.run(startRow, endRow);
        }
    };

    rowidx_t getMore(rowidx_t &startRow) // NB: returns end row #
    {
        NonReentrantSpinBlock block(lock);
        if (nextChunkStartRow == rowCount)
            return 0;
        startRow = nextChunkStartRow;
        if (rowCount-nextChunkStartRow <= chunkSize)
            nextChunkStartRow = rowCount;
        else
            nextChunkStartRow += chunkSize;
        return nextChunkStartRow; // and end row for this particular chunk request
    }
    inline void mark(rowidx_t i)
    {
        // NB: Thread safe, because markers are dealing with discrete parts of bitSetMem (alighted to bits_t boundaries)
        bitSet->set(i); // mark boundary
    }
    rowidx_t doMarking(rowidx_t myStart, rowidx_t myEnd)
    {
        // myStart must be on bits_t boundary
        dbgassertex(0 == (myStart % BitsPerItem));

        rowidx_t chunkUnique = 0;
        const void **rows = base+myStart;
        rowidx_t i=myStart;
        for (; i<(myEnd-1); i++, rows++)
        {
            int r = cmp->docompare(*rows, *(rows+1));
            if (r)
            {
                ++chunkUnique;
                mark(i);
            }
            /* JCSMORE - could I binchop ahead somehow, to process duplicates more quickly..
             * i.e. if same cur+mid = cur, then no need to check intermediates..
             */
        }
        if (myEnd != rowCount)
        {
            // final row, cross boundary with next chunk, i.e. { last-row-of-my-chunk , first-row-of-next }
            int r = cmp->docompare(*rows, *(rows+1));
            if (r)
            {
                ++chunkUnique;
                mark(i);
            }
        }
        return chunkUnique;
    }
    rowidx_t run(rowidx_t myStart, rowidx_t myEnd)
    {
        rowidx_t chunkUnique = 0;
        loop
        {
            chunkUnique += doMarking(myStart, myEnd);
            myEnd = getMore(myStart);
            if (0 == myEnd)
                break; // done
        }
        return chunkUnique;
    }
public:
    CMarker(CActivityBase &_activity) : activity(_activity)
    {
        cmp = NULL;
        base = NULL;
        nextChunkStartRow = rowCount = chunkSize = 0;
        // perhaps should make these configurable..
        parallelMinChunkSize = 1024;
        parallelChunkSize = 10*parallelMinChunkSize;
        threadCount = activity.getOptInt(THOROPT_JOINHELPER_THREADS, activity.queryMaxCores());
        if (0 == threadCount)
            threadCount = getAffinityCpus();
    }
    bool init(rowidx_t rowCount)
    {
        bool threadSafeBitSet = activity.getOptBool("threadSafeBitSet", false); // for testing only
        if (threadSafeBitSet)
        {
            DBGLOG("Using Thread safe variety of IBitSet");
            bitSet.setown(createThreadSafeBitSet());
        }
        else
        {
            size32_t bitSetMemSz = getBitSetMemoryRequirement(rowCount);
            void *pBitSetMem = activity.queryRowManager().allocate(bitSetMemSz, activity.queryContainer().queryId(), SPILL_PRIORITY_LOW);
            if (!pBitSetMem)
                return false;

            bitSetMem.setown(pBitSetMem);
            bitSet.setown(createBitSet(bitSetMemSz, pBitSetMem));
        }
        return true;
    }
    void reset()
    {
        bitSet.clear();
    }
    rowidx_t calculate(CThorExpandingRowArray &rows, ICompare *_cmp, bool doSort)
    {
        CCycleTimer timer;
        assertex(bitSet);
        cmp = _cmp;
        if (doSort)
            rows.sort(*cmp, threadCount);
        rowCount = rows.ordinality();
        if (0 == rowCount)
            return 0;
        base = rows.getRowArray();

        rowidx_t uniqueTotal = 0;
        if ((1 == threadCount) || (rowCount < parallelMinChunkSize))
            uniqueTotal = doMarking(0, rowCount);
        else
        {
            nextChunkStartRow = 0;
            chunkSize = rowCount / threadCount;
            if (chunkSize > parallelChunkSize)
                chunkSize = parallelChunkSize;
            else if (chunkSize < parallelMinChunkSize)
            {
                chunkSize = parallelMinChunkSize;
                threadCount = rowCount / chunkSize;
            }
            // Must be multiple of sizeof BitsPerItem
            chunkSize = ((chunkSize + (BitsPerItem-1)) / BitsPerItem) * BitsPerItem; // round up to nearest multiple of BitsPerItem

            /* This is yet another case of requiring a set of small worker threads
             * Thor should really use a common pool of lightweight threadlets made available to all
             * where any particular instances (e.g. lookup) can stipulate min/max it requires etc.
             */
            CIArrayOf<CCompareThread> threads;
            for (unsigned t=0; t<threadCount; t++)
            {
                if (nextChunkStartRow+chunkSize >= rowCount)
                {
                    threads.append(* new CCompareThread(*this, nextChunkStartRow, rowCount));
                    nextChunkStartRow = rowCount;
                    break;
                }
                else
                {
                    rowidx_t s = nextChunkStartRow;
                    nextChunkStartRow += chunkSize;
                    threads.append(* new CCompareThread(*this, s, nextChunkStartRow));
                }
            }
            ForEachItemIn(t, threads)
                threads.item(t).start();
            ForEachItemIn(t2, threads)
            {
                CCompareThread &compareThread = threads.item(t2);
                compareThread.join();
                uniqueTotal += compareThread.getUnique();
            }
        }
        ++uniqueTotal;
        mark(rowCount-1); // last row is implicitly end of group
        cmp = NULL;
        DBGLOG("CMarker::calculate - uniqueTotal=%" RIPF "d, took=%d ms", uniqueTotal, timer.elapsedMs());
        return uniqueTotal;
    }
    rowidx_t findNextBoundary(rowidx_t start)
    {
        if (start==rowCount)
            return 0;
        return bitSet->scan(start, true)+1;
    }
};


/*
 * Template class to minimize virtual calls
 * This class becomes a base of CInMemJoinBase, that is base of LookupJoin or AllJoin implementations
 */
template <class HELPER>
class CAllOrLookupHelper
{
protected:
    HELPER *helper;

public:
    CAllOrLookupHelper(HELPER *_helper) : helper(_helper) { }
    inline bool match(const void *lhs, const void *rhsrow)
    {
        return helper->match(lhs, rhsrow);
    }
    inline const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned numRows, const void **rows)
    {
        return helper->transform(rowBuilder, left, right, numRows, rows);
    }
    inline const size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned count)
    {
        return helper->transform(rowBuilder, left, right, count);
    }
};



/* 
    These activities load the RHS into a table, therefore
    the right hand stream -should- contain the fewer records

    Inner, left outer and left only joins supported
*/

/* Base common to:
 * 1) Lookup Many
 * 2) Lookup
 * 3) All
 *
 * Handles the initialization, broadcast and processing (decompression, deserialization, population) of RHS
 * and base common functionality for all and lookup varieties
 */
template <class HTHELPER, class HELPER>
class CInMemJoinBase : public CSlaveActivity, public CThorDataLink, public CAllOrLookupHelper<HELPER>, implements ISmartBufferNotify, implements IBCastReceive
{
    Semaphore leftstartsem;
    Owned<IException> leftexception;

    bool eos, eog, someSinceEog;
    SpinLock rHSRowSpinLock;

protected:
    typedef CAllOrLookupHelper<HELPER> HELPERBASE;

    using HELPERBASE::helper;

    /* Utility class, that is called from the broadcaster to queue up received blocks
     * It will block if it has > MAX_QUEUE_BLOCKS to process (on the queue)
     * Processing will decompress the incoming blocks and add them to a row array per slave
     */
    class CRowProcessor : implements IThreaded
    {
        CThreadedPersistent threaded;
        CInMemJoinBase &owner;
        bool stopped;
        SimpleInterThreadQueueOf<CSendItem, true> blockQueue;
        Owned<IException> exception;

        void clearQueue()
        {
            loop
            {
                Owned<CSendItem> sendItem = blockQueue.dequeueNow();
                if (NULL == sendItem)
                    break;
            }
        }
    public:
        CRowProcessor(CInMemJoinBase &_owner) : threaded("CRowProcessor", this), owner(_owner)
        {
            stopped = false;
            blockQueue.setLimit(MAX_QUEUE_BLOCKS);
        }
        ~CRowProcessor()
        {
            blockQueue.stop();
            clearQueue();
            wait();
        }
        void start()
        {
            stopped = false;
            clearQueue();
            exception.clear();
            threaded.start();
        }
        void abort()
        {
            if (!stopped)
            {
                stopped = true;
                blockQueue.enqueue(NULL);
            }
        }
        void wait()
        {
            threaded.join();
            if (exception)
                throw exception.getClear();
        }
        void addBlock(CSendItem *sendItem)
        {
            if (exception)
            {
                if (sendItem)
                    sendItem->Release();
                throw exception.getClear();
            }
            blockQueue.enqueue(sendItem); // will block if queue full
        }
    // IThreaded
        virtual void main()
        {
            try
            {
                while (!stopped)
                {
                    Owned<CSendItem> sendItem = blockQueue.dequeue();
                    if (stopped || (NULL == sendItem))
                        break;
                    MemoryBuffer expandedMb;
                    ThorExpand(sendItem->queryMsg(), expandedMb);
                    owner.processRHSRows(sendItem->queryOrigin(), expandedMb);
                }
            }
            catch (IException *e)
            {
                exception.setown(e);
                EXCLOG(e, "CRowProcessor");
            }
        }
    } rowProcessor;

    CBroadcaster broadcaster;
    rowidx_t rhsTableLen;
    HTHELPER table;
    OwnedConstThorRow leftRow;

    IThorDataLink *leftITDL, *rightITDL;
    Owned<IRowStream> left, right;
    Owned<IEngineRowAllocator> rightAllocator;
    Owned<IEngineRowAllocator> leftAllocator;
    Owned<IEngineRowAllocator> allocator;
    Owned<IOutputRowSerializer> rightSerializer;
    Owned<IOutputRowDeserializer> rightDeserializer;
    bool gotRHS;
    join_t joinType;
    OwnedConstThorRow defaultRight;
    bool needGlobal;
    unsigned flags;
    bool exclude;
    const void *rhsNext;
    CThorExpandingRowArray rhs;
    Owned<IOutputMetaData> outputMeta;

    PointerArrayOf<CThorSpillableRowArray> rhsNodeRows;

    rowidx_t nextRhsRow;
    unsigned keepLimit;
    unsigned joined;
    unsigned joinCounter;
    OwnedConstThorRow defaultLeft;

    bool leftMatch, grouped;
    bool fuzzyMatch, returnMany;
    rank_t myNode;
    unsigned numNodes;

    inline bool isGroupOp() const
    {
        switch (container.getKind())
        {
            case TAKlookupdenormalizegroup:
            case TAKsmartdenormalizegroup:
            case TAKalldenormalizegroup:
                return true;
        }
        return false;
    }
    inline bool isDenormalize() const
    {
        switch (container.getKind())
        {
            case TAKlookupdenormalize:
            case TAKlookupdenormalizegroup:
            case TAKalldenormalize:
            case TAKalldenormalizegroup:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                return true;
        }
        return false;
    }
    StringBuffer &getJoinTypeStr(StringBuffer &str)
    {
        switch(joinType)
        {
            case JT_Undefined:  return str.append("UNDEFINED");
            case JT_Inner:      return str.append("INNER");
            case JT_LeftOuter:  return str.append("LEFT OUTER");
            case JT_RightOuter: return str.append("RIGHT OUTER");
            case JT_LeftOnly:   return str.append("LEFT ONLY");
            case JT_RightOnly:  return str.append("RIGHT ONLY");
        }
        return str.append("---> Unknown Join Type <---");
    }
    void clearRHS()
    {
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                rows->kill();
        }
        rhs.kill();
    }
    void clearHT()
    {
        rhsTableLen = 0;
        table.reset();
    }
    void processRHSRows(unsigned slave, MemoryBuffer &mb)
    {
        /* JCSMORE - I wonder if this should be done asynchronously on a few threads (<=1 per target)
         * It also might be better to use hash the rows now and assign to rhsNodeRows arrays, it's a waste of hash() calls
         * if it never spills, but will make flushing non-locals simpler if spilling occurs.
         */
        CThorSpillableRowArray &rows = *rhsNodeRows.item(slave-1);
        RtlDynamicRowBuilder rowBuilder(rightAllocator);
        CThorStreamDeserializerSource memDeserializer(mb.length(), mb.toByteArray());
        while (!memDeserializer.eos())
        {
            size32_t sz = rightDeserializer->deserialize(rowBuilder, memDeserializer);
            OwnedConstThorRow fRow = rowBuilder.finalizeRowClear(sz);
            // NB: If spilt, addRHSRow will filter out non-locals
            if (!addRHSRow(rows, fRow)) // NB: in SMART case, must succeed
                throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
        }
    }
    void broadcastRHS() // broadcasting local rhs
    {
        Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_send);
        MemoryBuffer mb;
        try
        {
            CThorSpillableRowArray &localRhsRows = *rhsNodeRows.item(queryJobChannel().queryMyRank()-1);
            CMemoryRowSerializer mbser(mb);
            while (!abortSoon)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;

                    if (!addRHSRow(localRhsRows, row)) // may cause broadcaster to be told to stop (for isStopping() to become true)
                        throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");

                    rightSerializer->serialize(mbser, (const byte *)row.get());
                    if (mb.length() >= MAX_SEND_SIZE || broadcaster.stopRequested())
                        break;
                }
                if (0 == mb.length())
                    break;
                if (broadcaster.stopRequested())
                    sendItem->setFlag(broadcaster.queryStopFlag());
                ThorCompress(mb, sendItem->queryMsg());
                if (!broadcaster.send(sendItem))
                    break;
                if (broadcaster.stopRequested())
                    break;
                mb.clear();
                broadcaster.resetSendItem(sendItem);
            }
        }
        catch (IException *e)
        {
            ActPrintLog(e, "CInMemJoinBase::broadcastRHS: exception");
            throw;
        }

        sendItem.setown(broadcaster.newSendItem(bcast_stop));
        if (broadcaster.stopRequested())
            sendItem->setFlag(broadcaster.queryStopFlag());
        ActPrintLog("Sending final RHS broadcast packet");
        broadcaster.send(sendItem); // signals stop to others
    }
    inline void resetRhsNext()
    {
        nextRhsRow = 0;
        joined = 0;
        leftMatch = false;
    }
    inline const void *denormalizeNextRow()
    {
        ConstPointerArray filteredRhs;
        while (rhsNext)
        {
            if (abortSoon)
                return NULL;
            if (!fuzzyMatch || (HELPERBASE::match(leftRow, rhsNext)))
            {
                leftMatch = true;
                if (exclude)
                {
                    rhsNext = NULL;
                    break;
                }
                ++joined;
                filteredRhs.append(rhsNext);
            }
            if (!returnMany || joined == keepLimit)
            {
                rhsNext = NULL;
                break;
            }
            rhsNext = table.getNextRHS();
        }
        if (filteredRhs.ordinality() || (!leftMatch && 0!=(flags & JFleftouter)))
        {
            unsigned rcCount = 0;
            OwnedConstThorRow ret;
            RtlDynamicRowBuilder rowBuilder(allocator);
            unsigned numRows = filteredRhs.ordinality();
            const void *rightRow = numRows ? filteredRhs.item(0) : defaultRight.get();
            if (isGroupOp())
            {
                size32_t sz = HELPERBASE::joinTransform(rowBuilder, leftRow, rightRow, numRows, filteredRhs.getArray());
                if (sz)
                    ret.setown(rowBuilder.finalizeRowClear(sz));
            }
            else
            {
                ret.set(leftRow);
                if (filteredRhs.ordinality())
                {
                    size32_t rowSize = 0;
                    loop
                    {
                        const void *rightRow = filteredRhs.item(rcCount);
                        size32_t sz = HELPERBASE::joinTransform(rowBuilder, ret, rightRow, ++rcCount);
                        if (sz)
                        {
                            rowSize = sz;
                            ret.setown(rowBuilder.finalizeRowClear(sz));
                        }
                        if (rcCount == filteredRhs.ordinality())
                            break;
                        rowBuilder.ensureRow();
                    }
                    if (!rowSize)
                        ret.clear();
                }
            }
            return ret.getClear();
        }
        else
            return NULL;
    }
    const void *lookupNextRow()
    {
        if (!abortSoon && !eos)
        {
            loop
            {
                if (NULL == rhsNext)
                {
                    leftRow.setown(left->nextRow());
                    joinCounter = 0;
                    if (leftRow)
                    {
                        eog = false;
                        if (rhsTableLen)
                        {
                            resetRhsNext();
                            const void *failRow = NULL;
                            rhsNext = table.getFirstRHSMatch(leftRow, failRow); // also checks abortLimit/atMost
                            if (failRow)
                                return failRow;
                        }
                    }
                    else
                    {
                        if (eog)
                            eos = true;
                        else
                        {
                            eog = true;
                            if (!someSinceEog)
                                continue; // skip empty 'group'
                            someSinceEog = false;
                        }
                        break;
                    }
                }
                OwnedConstThorRow ret;
                if (isDenormalize())
                    ret.setown(denormalizeNextRow());
                else
                {
                    RtlDynamicRowBuilder rowBuilder(allocator);
                    while (rhsNext)
                    {
                        if (!fuzzyMatch || HELPERBASE::match(leftRow, rhsNext))
                        {
                            leftMatch = true;
                            if (!exclude)
                            {
                                size32_t sz = HELPERBASE::joinTransform(rowBuilder, leftRow, rhsNext, ++joinCounter);
                                if (sz)
                                {
                                    OwnedConstThorRow row = rowBuilder.finalizeRowClear(sz);
                                    someSinceEog = true;
                                    if (++joined == keepLimit)
                                        rhsNext = NULL;
                                    else if (!returnMany)
                                        rhsNext = NULL;
                                    else
                                        rhsNext = table.getNextRHS();
                                    return row.getClear();
                                }
                            }
                        }
                        rhsNext = table.getNextRHS();
                    }
                    if (!leftMatch && NULL == rhsNext && 0!=(flags & JFleftouter))
                    {
                        size32_t sz = HELPERBASE::joinTransform(rowBuilder, leftRow, defaultRight, 0);
                        if (sz)
                            ret.setown(rowBuilder.finalizeRowClear(sz));
                    }
                }
                if (ret)
                {
                    someSinceEog = true;
                    return ret.getClear();
                }
            }
        }
        return NULL;
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CInMemJoinBase(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this),
        broadcaster(*this), rowProcessor(*this), HELPERBASE((HELPER *)queryHelper()), rhs(*this, NULL)
    {
        gotRHS = false;
        nextRhsRow = 0;
        rhsNext = NULL;
        myNode = queryJobChannel().queryMyRank();
        numNodes = queryJob().querySlaves();
        needGlobal = !container.queryLocal() && (container.queryJob().querySlaves() > 1);

        rhsTableLen = 0;
        leftITDL = rightITDL = NULL;

        joined = 0;
        joinCounter = 0;
        leftMatch = false;
        returnMany = false;

        eos = eog = someSinceEog = false;

        flags = helper->getJoinFlags();
        grouped = helper->queryOutputMeta()->isGrouped();
        fuzzyMatch = 0 != (JFmatchrequired & flags);
        exclude = 0 != (JFexclude & flags);
        keepLimit = helper->getKeepLimit();
        if (0 == keepLimit)
            keepLimit = (unsigned)-1;
        if (flags & JFleftouter)
            joinType = exclude ? JT_LeftOnly : JT_LeftOuter;
        else
            joinType = JT_Inner;
    }
    ~CInMemJoinBase()
    {
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                delete rows;
        }
    }
// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);

        StringBuffer str;
        ActPrintLog("Join type is %s", getJoinTypeStr(str).str());

        if (!container.queryLocal())
            mpTag = container.queryJobChannel().deserializeMPTag(data);

        unsigned slaves = container.queryJob().querySlaves();
        rhsNodeRows.ensure(slaves);
        while (slaves--)
            rhsNodeRows.append(new CThorSpillableRowArray(*this, NULL));
    }
    virtual void start()
    {
        assertex(inputs.ordinality() == 2);
        gotRHS = false;
        nextRhsRow = 0;
        joined = 0;
        joinCounter = 0;
        leftMatch = false;
        rhsNext = NULL;
        rhsTableLen = 0;
        leftITDL = inputs.item(0);
        rightITDL = inputs.item(1);
        allocator.set(queryRowAllocator());
        leftAllocator.set(::queryRowAllocator(leftITDL));
        outputMeta.set(leftITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta());
        for (unsigned s=0; s<container.queryJob().querySlaves(); s++)
            rhsNodeRows.item(s)->setup(queryRowInterfaces(rightITDL));

        eos = eog = someSinceEog = false;

        right.set(rightITDL);
        rightAllocator.set(::queryRowAllocator(rightITDL));
        rightSerializer.set(::queryRowSerializer(rightITDL));
        rightDeserializer.set(::queryRowDeserializer(rightITDL));

        if ((flags & JFonfail) || (flags & JFleftouter))
        {
            RtlDynamicRowBuilder rr(rightAllocator);
            rr.ensureRow();
            size32_t rrsz = helper->createDefaultRight(rr);
            defaultRight.setown(rr.finalizeRowClear(rrsz));
        }

        leftITDL = createDataLinkSmartBuffer(this,leftITDL,LOOKUPJOINL_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(leftITDL->queryFromActivity()),grouped,RCUNBOUND,this,false,&container.queryJob().queryIDiskUsage());
        left.setown(leftITDL);
        startInput(leftITDL);

        try
        {
            startInput(rightITDL);
        }
        catch (CATCHALL)
        {
            leftstartsem.wait();
            left->stop();
            throw;
        }
        leftstartsem.wait();
        if (leftexception)
        {
            right->stop();
            throw leftexception.getClear();
        }
        dataLinkStart();
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
        broadcaster.cancel();
        rowProcessor.abort();
    }
    virtual void stop()
    {
        // JCS->GH - if in a child query, it would be good to preserve RHS.. would need tip/flag from codegen that constant
        clearRHS();
        clearHT();
        if (right)
        {
            stopInput(right, "(R)");
            right.clear();
        }
        broadcaster.reset();
        stopInput(left, "(L)");
        left.clear();
        dataLinkStop();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canStall = true;
    }
    void doBroadcastRHS(bool stopping)
    {
        rowProcessor.start();
        broadcaster.start(this, mpTag, stopping);
        broadcastRHS();
        broadcaster.end();
        rowProcessor.wait();
    }
    void doBroadcastStop(mptag_t tag, broadcast_flags flag)
    {
        broadcaster.start(this, tag, false);
        Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_stop);
        if (flag)
            sendItem->setFlag(flag);
        broadcaster.send(sendItem); // signals stop to others
        broadcaster.end();
    }
    rowidx_t getGlobalRHSTotal()
    {
        rowcount_t rhsRows = 0;
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
            rows.flush();
            rhsRows += rows.numCommitted();
            if (rhsRows > RIMAX)
                throw MakeActivityException(this, 0, "Too many RHS rows: %" RCPF "d", rhsRows);
        }
        return (rowidx_t)rhsRows;
    }
    virtual bool addRHSRow(CThorSpillableRowArray &rhsRows, const void *row)
    {
        LinkThorRow(row);
        {
            SpinBlock b(rHSRowSpinLock);
            if (rhsRows.append(row))
                return true;
        }
        ReleaseThorRow(row);
        return false;
    }
// ISmartBufferNotify
    virtual void onInputStarted(IException *except)
    {
        leftexception.set(except);
        leftstartsem.signal();
    }
    virtual bool startAsync()
    {
        return true;
    }
    virtual void onInputFinished(rowcount_t count)
    {
        ActPrintLog("LHS input finished, %" RCPF "d rows read", count);
    }
};



/* Base class for:
 * 1) Lookup Many
 * 2) Lookup
 * They use different hash table representations, Lookup Many represents entries as {index, count}
 * Where as Lookup represents as simply hash table to rows
 * The main activity class
 * Both varieties do common work in this base class
 *
 * It performs the join, by 1st ensuring all RHS data is on all nodes, creating a hash table of this gathered set
 * then it streams the LHS through, matching against the RHS hash table entries.
 * It also handles match conditions where there is no hard match (, ALL), in those cases no hash table is needed.
 *
 * This base class also handles the 'SMART' functionality.
 * If RHS doesn't fit into memory, this class handles failover to local lookupjoin, by hashing the RHS to local
 * and hash distributing the LHS.
 * If the local RHS table still doesn't fit into memory, it will failover to a standard hash join, i.e. it will
 * need to sort both sides
 *
 * TODO: right outer/only joins
 */

template <class HTHELPER>
class CLookupJoinActivityBase : public CInMemJoinBase<HTHELPER, IHThorHashJoinArg>, implements roxiemem::IBufferedRowCallback
{
    typedef CInMemJoinBase<HTHELPER, IHThorHashJoinArg> PARENT;
protected:
    using PARENT::container;
    using PARENT::queryJobChannel;
    using PARENT::myNode;
    using PARENT::numNodes;
    using PARENT::ActPrintLog;
    using PARENT::right;
    using PARENT::left;
    using PARENT::rightITDL;
    using PARENT::leftITDL;
    using PARENT::rhsNodeRows;
    using PARENT::rhsTableLen;
    using PARENT::table;
    using PARENT::flags;
    using PARENT::outputMeta;
    using PARENT::leftMatch;
    using PARENT::gotRHS;
    using PARENT::needGlobal;
    using PARENT::leftRow;
    using PARENT::allocator;
    using PARENT::defaultRight;
    using PARENT::grouped;
    using PARENT::abortSoon;
    using PARENT::leftAllocator;
    using PARENT::rightAllocator;
    using PARENT::returnMany;
    using PARENT::fuzzyMatch;
    using PARENT::keepLimit;
    using PARENT::doBroadcastRHS;
    using PARENT::doBroadcastStop;
    using PARENT::getGlobalRHSTotal;
    using PARENT::getOptBool;
    using PARENT::broadcaster;
    using PARENT::inputs;
    using PARENT::queryHelper;
    using PARENT::totalCycles;
    using PARENT::timeActivities;
    using PARENT::fireException;
    using PARENT::lookupNextRow;
    using PARENT::rowProcessor;
    using PARENT::dataLinkIncrement;
    using PARENT::helper;
    using PARENT::clearHT;
    using PARENT::rhs;
    using PARENT::queryRowManager;

    IHash *leftHash, *rightHash;
    ICompare *compareRight, *compareLeftRight;

    unsigned abortLimit, atMost;
    bool dedup, stable;

    mptag_t lhsDistributeTag, rhsDistributeTag, broadcast2MpTag, broadcast3MpTag;

    // Handling failover to a) hashed local lookupjoin b) hash distributed standard join
    bool smart;
    bool rhsCollated, rhsCompacted;
    bool compressSpills;
    Owned<IHashDistributor> lhsDistributor, rhsDistributor;
    ICompare *compareLeft;
    UnsignedArray flushedRowMarkers;
    CriticalSection overflowCrit;
    Owned<CFileOwner> overflowWriteFile;
    Owned<IRowWriter> overflowWriteStream;
    rowcount_t overflowWriteCount;
    atomic_t localLookup, standardJoin;
    CriticalSection broadcastSpillingLock;
    Owned<IJoinHelper> joinHelper;

    inline bool isSmart() const { return smart; }
    inline bool isLocalLookup() const { return 0 != atomic_read(&localLookup); }
    inline void setLocalLookup(bool tf) { atomic_set(&localLookup, (int)tf); }
    inline bool isStandardJoin() const { return 0 != atomic_read(&standardJoin); }
    inline void setStandardJoin(bool tf) { atomic_set(&standardJoin, (int)tf); }
    rowidx_t clearNonLocalRows(CThorSpillableRowArray &rows, rowidx_t startPos)
    {
        CThorArrayLockBlock block(rows);
        rowidx_t clearedRows = 0;
        rowidx_t numRows = rows.numCommitted();
        for (rowidx_t r=startPos; r<numRows; r++)
        {
            unsigned hv = rightHash->hash(rows.query(r));
            if ((myNode-1) != (hv % numNodes))
            {
                OwnedConstThorRow row = rows.getClear(r); // dispose of
                ++clearedRows;
            }
        }
        return clearedRows;
    }
    // Annoyingly similar to above, used post broadcast when rhsNodeRows collated into 'rhs'
    rowidx_t clearNonLocalRows(CThorExpandingRowArray &rows, rowidx_t startPos)
    {
        rowidx_t clearedRows = 0;
        rowidx_t numRows = rows.ordinality();
        for (rowidx_t r=startPos; r<numRows; r++)
        {
            unsigned hv = rightHash->hash(rows.query(r));
            if ((myNode-1) != (hv % numNodes))
            {
                OwnedConstThorRow row = rows.getClear(r); // dispose of
                ++clearedRows;
            }
        }
        return clearedRows;
    }
    bool clearAllNonLocalRows(const char *msg)
    {
        // This is likely to free memory, so block others (threads) until done
        // NB: This will not block appends
        CriticalBlock b(broadcastSpillingLock);
        if (isLocalLookup())
            return false;
        setLocalLookup(true);
        ActPrintLog("Clearing non-local rows - cause: %s", msg);

        broadcaster.stop(bcastflag_spilt); // signals to broadcast to start stopping immediately and to signal spilt to others

        rowidx_t clearedRows = 0;
        if (rhsCollated)
        {
            // This only needs to be done once, no rows will be added after collated
            clearedRows += clearNonLocalRows(rhs, 0);
            rhs.compact();
            rhsCompacted = true;
        }
        else
        {
            /* NB: It is likely that there will be unflushed rows in the rhsNodeRows arrays after we are done here.
             * These will need flushing when all is done and clearNonLocalRows will need recalling to process rest
             */
            ForEachItemIn(a, rhsNodeRows)
            {
                CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                /* Record point to which clearNonLocalRows will reach
                 * so that can resume from that point, when done adding/flushing
                 */
                flushedRowMarkers.append(rows.numCommitted());
                clearedRows += clearNonLocalRows(rows, 0);
            }
        }
        ActPrintLog("handleLowMem: clearedRows = %" RIPF "d", clearedRows);
        return 0 != clearedRows;
    }
    bool setupHT(rowidx_t size)
    {
        if (size < 10)
            size = 16;
        else
        {
            rowcount_t res = size/3*4; // make HT 1/3 bigger than # rows
            if ((res < size) || (res > RIMAX)) // check for overflow, or result bigger than rowidx_t size
                throw MakeActivityException(this, 0, "Too many rows on RHS for hash table: %" RCPF "d", res);
            size = (rowidx_t)res;
        }
        try
        {
            table.setup(this, size, leftHash, rightHash, compareLeftRight);
        }
        catch (IException *e)
        {
            if (!isSmart())
                throw;
            switch (e->errorCode())
            {
            case ROXIEMM_MEMORY_POOL_EXHAUSTED:
            case ROXIEMM_MEMORY_LIMIT_EXCEEDED:
                e->Release();
                return false;
            default:
                throw;
            }
        }
        rhsTableLen = size;
        return true;
    }
    void getRHS(bool stopping)
    {
        if (gotRHS)
            return;
        gotRHS = true;
/*
 * Global LOOKUP:
 * 1) distributes RHS (using broadcaster)
 * 2) sizes the hash table
 * 3) If there is no OOM event, it is done and the RHS hash table is built.
 *    ELSE -
 * 4) If during 1) or 2) an OOM event occurs, all other slaves are notified.
 *    If in the middle of broadcasting, it will stop sending RHS
 *    The spill event will flush out all rows that do not hash to local slave
 * 5) Hash distributor streams are setup for the [remaining] RHS and unread LHS.
 * 6) The broadcast rows + the remaining RHS distributed stream are consumed into a single row array.
 * 7) When done if it has not spilt, the RHS hash table is sized.
 * 8) If there is no OOM event, the RHS is done and the RHS hash table is built
 *    The distributed LHS stream is used to perform a local lookup join.
 *    ELSE -
 * 9) If during 6) or 7) an OOM event occurs, the stream loader, will spill and sort as necessary.
 * 10) The LHS side is loaded and spilt and sorted if necessary
 * 11) A regular join helper is created to perform a local join against the two hash distributed sorted sides.
 */
        bool rhsAlreadySorted = false;
        CMarker marker(*this);
        if (needGlobal)
        {
            rhsAlreadySorted = helper->isRightAlreadySorted();
            doBroadcastRHS(stopping);
            rowidx_t rhsRows;
            {
                CriticalBlock b(broadcastSpillingLock);
                rhsRows = getGlobalRHSTotal(); // total count of flushed rows
            }
            overflowWriteStream.clear(); // if created, would imply already localLookup=true, there cannot be anymore after broadcast complete
            if (!isLocalLookup())
            {
                if (stable && !rhsAlreadySorted)
                    rhs.setup(NULL, false, stableSort_earlyAlloc);
                bool success=false;
                try
                {
                    if (marker.init(rhsRows))
                    {
                        // NB: If this ensure returns false, it will have called the MM callbacks and have setup isLocalLookup() already
                        success = rhs.resize(rhsRows, SPILL_PRIORITY_LOW); // NB: Could OOM, handled by exception handler
                    }
                }
                catch (IException *e)
                {
                    if (!isSmart())
                        throw;
                    switch (e->errorCode())
                    {
                    case ROXIEMM_MEMORY_POOL_EXHAUSTED:
                    case ROXIEMM_MEMORY_LIMIT_EXCEEDED:
                        e->Release();
                        break;
                    default:
                        throw;
                    }
                    success = false;
                }
                if (!success)
                {
                    ActPrintLog("Out of memory trying to size the global RHS row table for a SMART join, will now attempt a distributed local lookup join");
                    if (!isLocalLookup())
                    {
                        clearAllNonLocalRows("OOM on sizing global row table"); // NB: someone else could have provoked callback already
                        dbgassertex(isLocalLookup());
                    }
                }
            }

            // NB: no more rows can be added to rhsNodeRows at this point, but they could still be flushed

            // For testing purposes only
            if (isSmart() && getOptBool(THOROPT_LKJOIN_LOCALFAILOVER, getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)))
                clearAllNonLocalRows("testing");

            rowidx_t uniqueKeys = 0;
            {
                /* NB: This does not allocate/will not provoke spilling, but spilling callback still active
                 * and need to protect rhsNodeRows access
                 */
                CriticalBlock b(broadcastSpillingLock);
                if (!isLocalLookup())
                {
                    // If spilt, don't size ht table now, if local rhs fits, ht will be sized later
                    ForEachItemIn(a, rhsNodeRows)
                    {
                        CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                        rhs.appendRows(rows, true); // NB: This should not cause spilling, rhs is already sized and we are only copying ptrs in
                        rows.kill(); // free up ptr table asap
                    }
                    // Have to keep broadcastSpillingLock locked until sort and calculate are done
                    uniqueKeys = marker.calculate(rhs, compareRight, !rhsAlreadySorted);
                    rhsCollated = true;
                    ActPrintLog("Collated all RHS rows");

                    if (stable && !rhsAlreadySorted)
                    {
                        ActPrintLog("Clearing rhs stable ptr table");
                        rhs.setup(NULL, false, stableSort_none); // don't need stable ptr table anymore
                    }
                }
            }
            if (!isLocalLookup()) // check again after processing above
            {
                if (!setupHT(uniqueKeys)) // NB: Sizing can cause spilling callback to be triggered or OOM in case of !smart
                {
                    ActPrintLog("Out of memory trying to size the global hash table for a SMART join, will now attempt a distributed local lookup join");
                    clearAllNonLocalRows("OOM on sizing global hash table"); // NB: setupHT should have provoked callback already
                    dbgassertex(isLocalLookup());
                }
            }
            if (isSmart())
            {
                /* NB: Potentially one of the slaves spilt late after broadcast and rowprocessor finished
                 * NB2: This is also to let others know of this slaves spill state.
                 * Need to remove spill callback and broadcast one last message to know.
                 */

                queryRowManager().removeRowBuffer(this);

                ActPrintLog("Broadcasting final split status");
                broadcaster.reset();
                doBroadcastStop(broadcast2MpTag, isLocalLookup() ? bcastflag_spilt : bcastflag_null);
            }

            /* All slaves now know whether any one spilt or not, i.e. whether to perform local hash join or not
             * If any have, still need to distribute rest of RHS..
             */

            if (!isLocalLookup())
            {
                if (stopping) // broadcast done and no-one spilt, this node can now stop
                    return;
            }
            else
            {
                ActPrintLog("Spilt whilst broadcasting, will attempt distribute local lookup join");

                // Either it has collated already and remaining rows are sorted, or if didn't get that far, can't rely on previous state of rhsAlreadySorted
                rhsAlreadySorted = rhsCollated;

                // NB: lhs ordering and grouping lost from here on..
                if (grouped)
                    throw MakeActivityException(this, 0, "Degraded to distributed lookup join, LHS order cannot be preserved");

                // If HT sized already and now spilt, too big clear and size when local size known
                clearHT();
                marker.reset();

                if (stopping)
                {
                    ActPrintLog("getRHS stopped");
                    /* NB: Can only stop now, after distributors are setup
                     * since other slaves may not be stopping and are dependent on the distributors
                     * The distributor will not actually stop until everyone else does.
                     */
                    return;
                }
            }
        }
        else
        {
            rhsAlreadySorted = helper->isRightAlreadyLocallySorted();
            if (stopping) // if local can stop now
                return;
            setLocalLookup(true);
        }

        if (isLocalLookup())
        {
            Owned<IThorRowLoader> rowLoader;
            if (!needGlobal || !rhsCollated) // If global && rhsCollated, then no need for rowLoader
            {
                ICompare *cmp = rhsAlreadySorted ? NULL : compareRight;
                if (isSmart())
                {
                    dbgassertex(!stable);
                    if (getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)) // for testing only (force to disk, as if spilt)
                        rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), cmp, stableSort_none, rc_allDisk, SPILL_PRIORITY_LOOKUPJOIN));
                    else
                        rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), cmp, stableSort_none, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN));
                }
                else
                {
                    // i.e. will fire OOM if runs out of memory loading local right
                    rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), cmp, stable ? stableSort_lateAlloc : stableSort_none, rc_allMem, SPILL_PRIORITY_DISABLE));
                }
            }
            Owned<IRowStream> rightStream;
            if (needGlobal)
            {
                if (rhsCollated)
                {
                    if (rhsCompacted) // compacted whilst spillable, couldn't reallocate row pointer at that stage, can now.
                        rhs.resize(rhs.ordinality(), SPILL_PRIORITY_LOW);
                }
                else // NB: If spilt after rhsCollated, callback will have cleared and compacted, rows will still be sorted
                {
                    /* NB: If cleared before rhsCollated, then need to clear non-locals that were added after spill
                     * There should not be many, as broadcast starts to stop as soon as a slave notifies it is spilling
                     * and ignores all non-locals.
                     */

                    // First identify largest row set from nodes, clear (non-locals) and compact.
                    rowidx_t largestRowCount = 0;
                    CThorSpillableRowArray *largestRhsNodeRows = NULL;
                    ForEachItemIn(a, rhsNodeRows)
                    {
                        CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                        clearNonLocalRows(rows, flushedRowMarkers.item(a));

                        ActPrintLog("Compacting rhsNodeRows[%d], has %" RIPF "d rows", a, rows.numCommitted());
                        rows.compact();

                        rowidx_t c = rows.numCommitted();
                        if (c > largestRowCount)
                        {
                            largestRowCount = c;
                            largestRhsNodeRows = &rows;
                        }
                    }
                    /* NB: The collected broadcast rows thus far (in rhsNodeRows or rhs) were ordered/deterministic.
                     * However, the rest of the rows received via the distributor are non-deterministic.
                     * Therefore the order overall is non-deterministic from this point on.
                     * For that reason, the rest of the RHS (distributed) rows will be processed ahead of the
                     * collected [broadcast] rows in the code below for efficiency reasons.
                     */
                    IArrayOf<IRowStream> rightStreams;
                    if (largestRhsNodeRows) // unlikely, but could happen if 0 local rhs rows left
                    {
                        // add largest directly into loader, so it can alleviate some memory immediately. NB: doesn't allocate
                        rowLoader->transferRowsIn(*largestRhsNodeRows);

                        /* the others + the rest of the RHS (distributed) are streamed into loader
                         * the streams have a lower spill priority than the loader, IOW, they will spill before the loader spills.
                         * If the loader has to spill, then we have to failover to standard join
                         */
                        ForEachItemIn(a, rhsNodeRows)
                        {
                            CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                            if (&rows == largestRhsNodeRows)
                                continue;
                            /* NB: will kill array when stream exhausted or if spilt
                             * Ensure spill priority of these spillable streams is lower than the stream in the loader in the next stage
                             */
                            rightStreams.append(*rows.createRowStream(SPILL_PRIORITY_SPILLABLE_STREAM, compressSpills)); // NB: default SPILL_PRIORITY_SPILLABLE_STREAM is lower than SPILL_PRIORITY_LOOKUPJOIN
                        }
                    }
                    // NB: 'right' deliberately added after rhsNodeRow streams, so that rhsNodeRow can be consumed into loader 1st
                    right.setown(rhsDistributor->connect(queryRowInterfaces(rightITDL), right.getClear(), rightHash, NULL));
                    rightStreams.append(*right.getLink()); // what remains of 'right' will be read through distributor
                    if (overflowWriteFile)
                    {
                        unsigned rwFlags = DEFAULT_RWFLAGS;
                        if (getOptBool(THOROPT_COMPRESS_SPILLS, true))
                            rwFlags |= rw_compress;
                        ActPrintLog("Reading overflow RHS broadcast rows : %" RCPF "d", overflowWriteCount);
                        Owned<IRowStream> overflowStream = createRowStream(&overflowWriteFile->queryIFile(), queryRowInterfaces(rightITDL), rwFlags);
                        rightStreams.append(* overflowStream.getClear());
                    }
                    if (rightStreams.ordinality()>1)
                        right.setown(createConcatRowStream(rightStreams.ordinality(), rightStreams.getArray()));
                    else
                        right.set(&rightStreams.item(0));
                    rightStream.setown(rowLoader->load(right, abortSoon, false, &rhs, NULL, false));
                }
            }
            else
                rightStream.setown(rowLoader->load(right, abortSoon, false, &rhs));

            if (!rightStream)
            {
                ActPrintLog("RHS local rows fitted in memory, count: %" RIPF "d", rhs.ordinality());
                // all fitted in memory, rows were transferred out back into 'rhs' and sorted

                /* Now need to size HT.
                 * If HT sizing DOESN'T cause spill, then rows will be transferred back into 'rhs'
                 * If HT sizing DOES cause spill, sorted rightStream will be created.
                 * transfer rows back into a spillable container
                 */

                rowLoader.clear();

                bool success;
                try
                {
                    success = marker.init(rhs.ordinality());
                }
                catch (IException *e)
                {
                    if (!isSmart())
                        throw;
                    switch (e->errorCode())
                    {
                    case ROXIEMM_MEMORY_POOL_EXHAUSTED:
                    case ROXIEMM_MEMORY_LIMIT_EXCEEDED:
                        e->Release();
                        break;
                    default:
                        throw;
                    }
                    success = false;
                }
                if (success)
                {
                    // Either was already sorted, or rowLoader->load() sorted on transfer out to rhs
                    rowidx_t uniqueKeys = marker.calculate(rhs, compareRight, false);
                    success = setupHT(uniqueKeys);
                    if (!success)
                    {
                        if (!isSmart())
                            throw MakeActivityException(this, 0, "Failed to allocate [LOCAL] hash table");
                    }
                }
                if (!success)
                {
                    ActPrintLog("Out of memory trying to allocate [LOCAL] tables for a SMART join (%" RIPF "d rows), will now failover to a std hash join", rhs.ordinality());
                    Owned<IThorRowCollector> collector;
                    if (isSmart())
                        collector.setown(createThorRowCollector(*this, queryRowInterfaces(rightITDL), NULL, stableSort_none, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN));
                    else
                        collector.setown(createThorRowCollector(*this, queryRowInterfaces(rightITDL), NULL, stableSort_none, rc_allMem, SPILL_PRIORITY_DISABLE));

                    collector->setOptions(rcflag_noAllInMemSort); // If fits into memory, don't want it resorted
                    collector->transferRowsIn(rhs); // can spill after this
                    rightStream.setown(collector->getStream());
                }
            }
            if (rightStream)
            {
                ActPrintLog("Local RHS spilt to disk. Standard Join will be used");
                setStandardJoin(true);
            }

            // Barrier point, did all slaves succeed in building local RHS HT?
            if (needGlobal && isSmart())
            {
                bool localRequiredStandardJoin = isStandardJoin();
                ActPrintLog("Checking other slaves for local RHS lookup status, this slaves RHS %s", localRequiredStandardJoin?"did NOT fit" : "DID fit");
                broadcaster.reset();
                broadcast_flags flag = localRequiredStandardJoin ? bcastflag_standardjoin : bcastflag_null;
                doBroadcastStop(broadcast3MpTag, flag);
                if (!localRequiredStandardJoin && isStandardJoin())
                {
                    ActPrintLog("Other slaves did NOT fit, consequently this slave must fail over to standard join as well");
                    dbgassertex(!rightStream);
                    rightStream.setown(rhs.createRowStream());
                }

                // start LHS distributor, needed either way, by local lookup or full join
                left.setown(lhsDistributor->connect(queryRowInterfaces(leftITDL), left.getClear(), leftHash, NULL));
            }

            if (isStandardJoin()) // NB: returned stream, implies (spilt or setupHT OOM'd) AND sorted, if not, 'rhs' is filled
            {
                ActPrintLog("Performing standard join");

                marker.reset();
                // NB: lhs ordering and grouping lost from here on.. (will have been caught earlier if global)
                if (grouped)
                    throw MakeActivityException(this, 0, "Degraded to standard join, LHS order cannot be preserved");

                rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(leftITDL), helper->isLeftAlreadyLocallySorted() ? NULL : compareLeft));
                left.setown(rowLoader->load(left, abortSoon, false));
                leftITDL = inputs.item(0); // reset
                ActPrintLog("LHS loaded/sorted");

                // rightStream is sorted
                // so now going to do a std. join on distributed sorted streams
                switch(container.getKind())
                {
                    case TAKlookupjoin:
                    case TAKsmartjoin:
                    {
                        bool hintunsortedoutput = getOptBool(THOROPT_UNSORTED_OUTPUT, TAKsmartjoin == container.getKind());
                        bool hintparallelmatch = getOptBool(THOROPT_PARALLEL_MATCH, hintunsortedoutput); // i.e. unsorted, implies use parallel by default, otherwise no point
                        joinHelper.setown(createJoinHelper(*this, helper, this, hintparallelmatch, hintunsortedoutput));
                        break;
                    }
                    case TAKlookupdenormalize:
                    case TAKlookupdenormalizegroup:
                    case TAKsmartdenormalize:
                    case TAKsmartdenormalizegroup:
                        joinHelper.setown(createDenormalizeHelper(*this, helper, this));
                        break;
                    default:
                        throwUnexpected();
                }
                joinHelper->init(left, rightStream, leftAllocator, rightAllocator, ::queryRowMetaData(leftITDL));
                return;
            }
            else
                ActPrintLog("Local RHS loaded to memory, performing local lookup join");
            // If got this far, without turning into a standard fully distributed join, then all rows are in rhs
        }
        table.addRows(rhs, marker);
        ActPrintLog("rhs table: %d elements", rhsTableLen);
    }
public:
    using PARENT::queryJob;

    static bool needDedup(IHThorHashJoinArg *helper)
    {
        unsigned flags = helper->getJoinFlags();
        bool dedup = false;
        if (0 == (flags & (JFtransformMaySkip|JFmatchrequired)))
        {
            if (0 == (JFmanylookup & flags))
                dedup = true;
            else
            {
                unsigned keepLimit = helper->getKeepLimit();
                unsigned abortLimit = helper->getMatchAbortLimit();
                unsigned atMost = helper->getJoinLimit();
                dedup = (1==keepLimit) && (0==atMost) && (0==abortLimit);
            }
        }
        return dedup;
    }
    CLookupJoinActivityBase(CGraphElementBase *_container) : PARENT(_container)
    {
        rhsCollated = rhsCompacted = false;
        broadcast2MpTag = broadcast3MpTag = lhsDistributeTag = rhsDistributeTag = TAG_NULL;
        setLocalLookup(false);
        setStandardJoin(false);

        leftHash = helper->queryHashLeft();
        rightHash = helper->queryHashRight();
        compareRight = helper->queryCompareRight();
        compareLeft = helper->queryCompareLeft();
        compareLeftRight = helper->queryCompareLeftRight();
        stable = 0 == (JFunstable & flags);
        dedup = false;
        abortLimit = helper->getMatchAbortLimit();
        atMost = helper->getJoinLimit();
        if (0 == abortLimit)
            abortLimit = (unsigned)-1;
        if (0 == atMost)
            atMost = (unsigned)-1;
        if (abortLimit < atMost)
            atMost = abortLimit;

        switch (container.getKind())
        {
            case TAKsmartjoin:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                smart = true;
                break;
            default:
                smart = false;
                break;
        }
        overflowWriteCount = 0;
        compressSpills = getOptBool(THOROPT_COMPRESS_SPILLS, true);
        ActPrintLog("Smart join = %s", smart?"true":"false");
    }
    bool exceedsLimit(rowidx_t count, const void *left, const void *right, const void *&failRow)
    {
        if ((unsigned)-1 != atMost)
        {
            failRow = NULL;
            if (count>abortLimit)
            {
                if (0 == (JFmatchAbortLimitSkips & flags))
                {
                    Owned<IException> e;
                    try
                    {
                        if (helper)
                            helper->onMatchAbortLimitExceeded();
                        CommonXmlWriter xmlwrite(0);
                        if (outputMeta && outputMeta->hasXML())
                            outputMeta->toXML((const byte *) leftRow.get(), xmlwrite);
                        throw MakeActivityException(this, 0, "More than %d match candidates in join for row %s", abortLimit, xmlwrite.str());
                    }
                    catch (IException *_e)
                    {
                        if (0 == (JFonfail & flags))
                            throw;
                        e.setown(_e);
                    }
                    RtlDynamicRowBuilder ret(allocator);
                    size32_t transformedSize = helper->onFailTransform(ret, leftRow, defaultRight, e.get());
                    if (transformedSize)
                        failRow = ret.finalizeRowClear(transformedSize);
                }
                else
                    leftMatch = true; // there was a lhs match, even though rhs group exceeded limit. Therefore this lhs will not be considered left only/left outer
                return true;
            }
            else if (count>atMost)
                return true;
        }
        return false;
    }
// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        PARENT::init(data, slaveData);

        if (!container.queryLocal())
        {
            broadcast2MpTag = container.queryJobChannel().deserializeMPTag(data);
            broadcast3MpTag = container.queryJobChannel().deserializeMPTag(data);
            lhsDistributeTag = container.queryJobChannel().deserializeMPTag(data);
            rhsDistributeTag = container.queryJobChannel().deserializeMPTag(data);
            rhsDistributor.setown(createHashDistributor(this, queryJobChannel().queryJobComm(), rhsDistributeTag, false, NULL, "RHS"));
            lhsDistributor.setown(createHashDistributor(this, queryJobChannel().queryJobComm(), lhsDistributeTag, false, NULL, "LHS"));
        }
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();
        if (isSmart())
        {
            setLocalLookup(false);
            setStandardJoin(false);
            rhsCollated = rhsCompacted = false;
            flushedRowMarkers.kill();

            if (needGlobal)
            {
                overflowWriteCount = 0;
                overflowWriteFile.clear();
                overflowWriteStream.clear();
                queryRowManager().addRowBuffer(this);
            }
        }
        else
        {
            bool inputGrouped = leftITDL->isGrouped();
            dbgassertex(inputGrouped == grouped); // std. lookup join expects these to match
        }

    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!gotRHS)
        {
            getRHS(false);
            StringBuffer msg;
            if (isSmart())
            {
                msg.append("SmartJoin - ");
                if (isStandardJoin())
                    msg.append("Failed over to standard join");
                else if (needGlobal && isLocalLookup())
                    msg.append("Failed over to hash distributed local lookup join");
                else
                    msg.append("All in memory lookup join");
            }
            else
                msg.append("LookupJoin");
            ActPrintLog("%s", msg.str());
        }
        OwnedConstThorRow row;
        if (joinHelper) // regular join (hash join)
            row.setown(joinHelper->nextRow());
        else
            row.setown(lookupNextRow());
        if (!row.get())
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void abort()
    {
        PARENT::abort();
        if (rhsDistributor)
            rhsDistributor->abort();
        if (lhsDistributor)
            lhsDistributor->abort();
        if (joinHelper)
            joinHelper->stop();
    }
    virtual void stop()
    {
        if (!gotRHS && needGlobal)
            getRHS(true); // If global, need to handle RHS until all are slaves stop

        if (rhsDistributor)
        {
            rhsDistributor->disconnect(true);
            rhsDistributor->join();
        }
        if (lhsDistributor)
        {
            lhsDistributor->disconnect(true);
            lhsDistributor->join();
        }
        joinHelper.clear();
        PARENT::stop();
    }
    virtual bool isGrouped()
    {
        return isSmart() ? false : inputs.item(0)->isGrouped();
    }
// IBCastReceive
    virtual void bCastReceive(CSendItem *sendItem)
    {
        if (0 != (sendItem->queryFlags() & bcastflag_spilt))
        {
            VStringBuffer msg("Notification that slave %d spilt", sendItem->queryOrigin());
            clearAllNonLocalRows(msg.str());
        }
        else if (0 != (sendItem->queryFlags() & bcastflag_standardjoin))
        {
            VStringBuffer msg("Notification that slave %d required standard join", sendItem->queryOrigin());
            setStandardJoin(true);
        }
        if (bcast_stop == sendItem->queryCode())
        {
            sendItem->Release();
            sendItem = NULL; // NB: NULL indicates end
        }
        rowProcessor.addBlock(sendItem);
    }
// IBufferedRowCallback
    virtual unsigned getSpillCost() const
    {
        return SPILL_PRIORITY_LOOKUPJOIN;
    }
    virtual unsigned getActivityId() const
    {
        return this->queryActivityId();
    }
    virtual bool freeBufferedRows(bool critical)
    {
        // NB: only installed if lookup join and global
        return clearAllNonLocalRows("Out of memory callback");
    }
    virtual bool addRHSRow(CThorSpillableRowArray &rhsRows, const void *row)
    {
        /* NB: If PARENT::addRHSRow fails, it will cause clearAllNonLocalRows() to have been triggered and localLookup to be set
         * When all is done, a last pass is needed to clear out non-locals
         */
        if (!overflowWriteFile)
        {
            if (!isLocalLookup() && PARENT::addRHSRow(rhsRows, row))
                return true;
            dbgassertex(isLocalLookup());
            // keep it only if it hashes to my node
            unsigned hv = rightHash->hash(row);
            if ((myNode-1) != (hv % numNodes))
                return true; // throw away non-local row
            if (PARENT::addRHSRow(rhsRows, row))
                return true;

            /* Could OOM whilst still failing over to local lookup again, dealing with last row, or trailing
             * few rows being received. Unlikely since all local rows will have been cleared, but possible,
             * particularly if last rows end up causing row ptr table expansion here.
             *
             * Need to stash away somewhere to allow it continue.
             */
            CriticalBlock b(overflowCrit); // could be coming from broadcaster or receiver
            if (!overflowWriteFile)
            {
                unsigned rwFlags = DEFAULT_RWFLAGS;
                if (getOptBool(THOROPT_COMPRESS_SPILLS, true))
                    rwFlags |= rw_compress;
                StringBuffer tempFilename;
                GetTempName(tempFilename, "lookup_local", true);
                ActPrintLog("Overflowing RHS broadcast rows to spill file: %s", tempFilename.str());
                OwnedIFile iFile = createIFile(tempFilename.str());
                overflowWriteFile.setown(new CFileOwner(iFile.getLink()));
                overflowWriteStream.setown(createRowWriter(iFile, queryRowInterfaces(rightITDL), rwFlags));
            }
        }
        ++overflowWriteCount;
        LinkThorRow(row);
        CriticalBlock b(overflowCrit); // could be coming from broadcaster or receiver
        overflowWriteStream->putRow(row);
        return true;
    }
};


class CHTBase
{
protected:
    OwnedConstThorRow htMemory;
    IHash *leftHash, *rightHash;
    ICompare *compareLeftRight;
    rowidx_t htSize;

public:
    CHTBase()
    {
        reset();
    }
    void setup(CSlaveActivity *activity, rowidx_t size, IHash *_leftHash, IHash *_rightHash, ICompare *_compareLeftRight)
    {
        unsigned __int64 _sz = sizeof(const void *) * ((unsigned __int64)size);
        memsize_t sz = (memsize_t)_sz;
        if (sz != _sz) // treat as OOM exception for handling purposes.
            throw MakeStringException(ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Unsigned overflow, trying to allocate hash table of size: %" I64F "d ", _sz);
        void *ht = activity->queryRowManager().allocate(sz, activity->queryContainer().queryId(), SPILL_PRIORITY_LOW);
        memset(ht, 0, sz);
        htMemory.setown(ht);
        htSize = size;
        leftHash = _leftHash;
        rightHash = _rightHash;
        compareLeftRight = _compareLeftRight;
    }
    void reset()
    {
        htMemory.clear();
        htSize = 0;
        leftHash = rightHash = NULL;
        compareLeftRight = NULL;
    }
};

class CLookupHT : public CHTBase
{
    CLookupJoinActivityBase<CLookupHT> *activity;
    const void **ht;

    const void *findFirst(const void *left)
    {
        unsigned h = leftHash->hash(left)%htSize;
        loop
        {
            const void *right = ht[h];
            if (!right)
                break;
            if (0 == compareLeftRight->docompare(left, right))
                return right;
            h++;
            if (h>=htSize)
                h = 0;
        }
        return NULL;
    }
    void releaseHTRows()
    {
        roxiemem::ReleaseRoxieRowArray(htSize, ht);
    }
public:
    CLookupHT()
    {
        reset();
    }
    ~CLookupHT()
    {
        releaseHTRows();
    }
    void setup(CLookupJoinActivityBase<CLookupHT> *_activity, rowidx_t size, IHash *leftHash, IHash *rightHash, ICompare *compareLeftRight)
    {
        activity = _activity;
        CHTBase::setup(activity, size, leftHash, rightHash, compareLeftRight);
        ht = (const void **)htMemory.get();
    }
    void reset()
    {
        releaseHTRows();
        CHTBase::reset();
        ht = NULL;
    }
    inline void addEntry(const void *row, unsigned hash)
    {
        loop
        {
            const void *&htRow = ht[hash];
            if (!htRow)
            {
                LinkThorRow(row);
                htRow = row;
                break;
            }
            hash++;
            if (hash>=htSize)
                hash = 0;
        }
    }
    inline const void *getNextRHS()
    {
        return NULL; // no next in LOOKUP without MANY
    }
    inline const void *getFirstRHSMatch(const void *leftRow, const void *&failRow)
    {
        failRow = NULL;
        return findFirst(leftRow);
    }
    virtual void addRows(CThorExpandingRowArray &_rows, CMarker &marker)
    {
        const void **rows = _rows.getRowArray();
        rowidx_t pos=0;
        loop
        {
            rowidx_t nextPos = marker.findNextBoundary(pos);
            if (0 == nextPos)
                break;
            const void *row = rows[pos];
            unsigned h = rightHash->hash(row)%htSize;
            addEntry(row, h);
            pos = nextPos;
        }
        // Rows now in hash table, rhs arrays no longer needed
        _rows.kill();
        marker.reset();
    }
};

struct HtEntry { rowidx_t index, count; };
class CLookupManyHT : public CHTBase
{
    CLookupJoinActivityBase<CLookupManyHT> *activity;
    HtEntry *ht;
    /* To be multithreaded, will need to avoid having 'curren't member.
     * Will need to pass into getNextRHS instead
     */
    HtEntry currentHashEntry;
    const void **rows;

    inline HtEntry *lookup(unsigned hash)
    {
        HtEntry *e = ht+hash;
        if (0 == e->count)
            return NULL;
        return e;
    }
    const void *findFirst(const void *left)
    {
        unsigned h = leftHash->hash(left)%htSize;
        loop
        {
            HtEntry *e = lookup(h);
            if (!e)
                break;
            const void *right = rows[e->index];
            if (0 == compareLeftRight->docompare(left, right))
            {
                currentHashEntry = *e;
                return right;
            }
            h++;
            if (h>=htSize)
                h = 0;
        }
        return NULL;
    }
public:
    CLookupManyHT()
    {
        reset();
    }
    void setup(CLookupJoinActivityBase<CLookupManyHT> *_activity, rowidx_t size, IHash *leftHash, IHash *rightHash, ICompare *compareLeftRight)
    {
        activity = _activity;
        CHTBase::setup(activity, size, leftHash, rightHash, compareLeftRight);
        ht = (HtEntry *)htMemory.get();
    }
    inline void addEntry(const void *row, unsigned hash, rowidx_t index, rowidx_t count)
    {
        loop
        {
            HtEntry &e = ht[hash];
            if (!e.count)
            {
                e.index = index;
                e.count = count;
                break;
            }
            hash++;
            if (hash>=htSize)
                hash = 0;
        }
    }
    void reset()
    {
        CHTBase::reset();
        ht = NULL;
        rows = NULL;
    }
    inline const void *getNextRHS()
    {
        if (1 == currentHashEntry.count)
            return NULL;
        --currentHashEntry.count;
        return rows[++currentHashEntry.index];
    }
    inline const void *getFirstRHSMatch(const void *leftRow, const void *&failRow)
    {
        const void *right = findFirst(leftRow);
        if (right)
        {
            if (activity->exceedsLimit(currentHashEntry.count, leftRow, right, failRow))
                return NULL;
        }
        return right;
    }
    virtual void addRows(CThorExpandingRowArray &_rows, CMarker &marker)
    {
        rows = _rows.getRowArray();
        rowidx_t pos=0;
        rowidx_t pos2;
        loop
        {
            pos2 = marker.findNextBoundary(pos);
            if (0 == pos2)
                break;
            rowidx_t count = pos2-pos;
            /* JCS->GH - Could you/do you spot LOOKUP MANY, followed by DEDUP(key) ?
             * It feels like we should only dedup if code gen spots, rather than have LOOKUP without MANY option
             * i.e. feels like LOOKUP without MANY should be deprecated..
            */
            const void *row = rows[pos];
            unsigned h = rightHash->hash(row)%htSize;
            // NB: 'pos' and 'count' won't be used if dedup variety
            addEntry(row, h, pos, count);
            pos = pos2;
        }
    }
};

class CLookupJoinSlaveActivity : public CLookupJoinActivityBase<CLookupHT>
{
public:
    CLookupJoinSlaveActivity(CGraphElementBase *_container) : CLookupJoinActivityBase<CLookupHT>(_container)
    {
        dedup = true;
    }
};

class CLookupManyJoinSlaveActivity : public CLookupJoinActivityBase<CLookupManyHT>
{
public:
    CLookupManyJoinSlaveActivity(CGraphElementBase *_container) : CLookupJoinActivityBase<CLookupManyHT>(_container)
    {
        // NB: could not dedup if JFtransformMaySkip or JFmatchrequired, but returnMany not necessarily true
        returnMany = 0 != (JFmanylookup & flags);
    }
};

class CAllTable
{
    const void **rows;
    rowidx_t htSize;
    rowidx_t nextRhsRow;

public:
    CAllTable()
    {
        reset();
    }
    void reset()
    {
        htSize = 0;
        rows = NULL;
        nextRhsRow = 0;
    }
    inline const void *getNextRHS()
    {
        if (++nextRhsRow<htSize)
            return rows[nextRhsRow];
        return NULL;
    }
    inline const void *getFirstRHSMatch(const void *leftRow, const void *&failRow)
    {
        nextRhsRow = 0;
        failRow = NULL;
        return rows[0]; // guaranteed to be at least one row
    }
    void addRows(CThorExpandingRowArray &_rows)
    {
        htSize = _rows.ordinality();
        rows = _rows.getRowArray();
        nextRhsRow = 0;
    }
};

class CAllJoinSlaveActivity : public CInMemJoinBase<CAllTable, IHThorAllJoinArg>
{
    typedef CInMemJoinBase<CAllTable, IHThorAllJoinArg> PARENT;

protected:
    void getRHS(bool stopping)
    {
        if (gotRHS)
            return;
        gotRHS = true;

        // ALL join must fit into memory
        if (needGlobal)
        {
            doBroadcastRHS(stopping);
            if (stopping) // broadcast done and no-one spilt, this node can now stop
                return;

            rhsTableLen = getGlobalRHSTotal();
            rhs.resize(rhsTableLen);
            ForEachItemIn(a, rhsNodeRows)
            {
                CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                rhs.appendRows(rows, true);
                rows.kill(); // free up ptr table asap
            }
        }
        else
        {
            if (stopping) // if local can stop now
                return;
            // if input counts known, use to presize RHS table
            ThorDataLinkMetaInfo rightMeta;
            rightITDL->getMetaInfo(rightMeta);
            rowcount_t rhsTotalCount = RCUNSET;
            if (rightMeta.totalRowsMin == rightMeta.totalRowsMax)
            {
                rhsTotalCount = rightMeta.totalRowsMax;
                if (rhsTotalCount > RIMAX)
                    throw MakeActivityException(this, 0, "Too many rows on RHS for ALL join: %" RCPF "d", rhsTotalCount);
                rhs.resize((rowidx_t)rhsTotalCount);
            }
            while (!abortSoon)
            {
                OwnedConstThorRow row = right->ungroupedNextRow();
                if (!row)
                    break;
                rhs.append(row.getClear());
            }
            rhsTableLen = rhs.ordinality();
            if (rhsTotalCount != RCUNSET) // verify matches meta
                assertex(rhsTableLen == rhsTotalCount);
        }
        table.addRows(rhs);
        ActPrintLog("rhs table: %d elements", rhsTableLen);
    }
public:
    CAllJoinSlaveActivity(CGraphElementBase *_container) : PARENT(_container)
    {
        returnMany = true;
    }
// IThorSlaveActivity overloaded methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        PARENT::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!gotRHS)
            getRHS(false);
        OwnedConstThorRow row = lookupNextRow();
        if (!row.get())
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }
    virtual void stop()
    {
        if (!gotRHS && needGlobal)
            getRHS(true); // If global, need to handle RHS until all are slaves stop
        PARENT::stop();
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
// IBCastReceive
    virtual void bCastReceive(CSendItem *sendItem)
    {
        if (bcast_stop == sendItem->queryCode())
        {
            sendItem->Release();
            sendItem = NULL; // NB: NULL indicates end
        }
        rowProcessor.addBlock(sendItem);
    }
};


CActivityBase *createLookupJoinSlave(CGraphElementBase *container) 
{ 
    IHThorHashJoinArg *helper = (IHThorHashJoinArg *)container->queryHelper();
    if (CLookupManyJoinSlaveActivity::needDedup(helper))
        return new CLookupJoinSlaveActivity(container);
    else
        return new CLookupManyJoinSlaveActivity(container);
}

CActivityBase *createAllJoinSlave(CGraphElementBase *container)
{
    return new CAllJoinSlaveActivity(container);
}


