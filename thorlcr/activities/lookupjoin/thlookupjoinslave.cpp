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
#include "thlookupjoincommon.hpp"

#ifdef _DEBUG
#define _TRACEBROADCAST
#endif


enum join_t { JT_Undefined, JT_Inner, JT_LeftOuter, JT_RightOuter, JT_LeftOnly, JT_RightOnly, JT_LeftOnlyTransform };


#define MAX_SEND_SIZE 0x100000 // 1MB
#define MAX_QUEUE_BLOCKS 5

enum broadcast_code { bcast_none, bcast_send, bcast_sendStopping, bcast_stop };
enum broadcast_flags { bcastflag_null=0, bcastflag_spilt=0x100 };
#define BROADCAST_CODE_MASK 0x00FF
#define BROADCAST_FLAG_MASK 0xFF00
class CSendItem : public CSimpleInterface
{
    CMessageBuffer msg;
    unsigned info, node, slave, headerLen;
public:
    CSendItem(broadcast_code _code, unsigned _node, unsigned _slave) : info((unsigned)_code), node(_node), slave(_slave)
    {
        msg.append(info);
        msg.append(node);
        msg.append(slave);
        headerLen = msg.length();
    }
    CSendItem(CMessageBuffer &_msg)
    {
        msg.swapWith(_msg);
        msg.read(info);
        msg.read(node);
        msg.read(slave);
        headerLen = msg.getPos();
    }
    unsigned length() const { return msg.length(); }
    void reset() { msg.setLength(headerLen); }
    CMessageBuffer &queryMsg() { return msg; }
    broadcast_code queryCode() const { return (broadcast_code)(info & BROADCAST_CODE_MASK); }
    unsigned queryNode() const { return node; } // 0 based
    unsigned querySlave() const { return slave; } // 0 based
    broadcast_flags queryFlags() const { return (broadcast_flags)(info & BROADCAST_FLAG_MASK); }
    void setFlag(broadcast_flags _flag)
    {
        info = (info & ~BROADCAST_FLAG_MASK) | ((short)_flag);
        msg.writeDirect(0, sizeof(info), &info); // update
    }
};


interface IBCastReceive
{
    virtual void bCastReceive(CSendItem *sendItem, bool stop) = 0;
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
    unsigned myNode, nodes, mySlave, slaves, senders, mySender;
    IBCastReceive *recvInterface;
    InterruptableSemaphore allDoneSem;
    CriticalSection allDoneLock, stopCrit;
    CriticalSection *broadcastLock;
    bool allRequestStop, stopping, stopRecv, receiving, nodeBroadcast;
    unsigned waitingAtAllDoneCount;
    broadcast_flags stopFlag;
    Owned<IBitSet> sendersDone, broadcastersStopping;

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
                threaded.join(INFINITE);
        }
        void wait()
        {
            threaded.join(INFINITE);
        }
    // IThreaded
        virtual void threadmain() override
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
            for (;;)
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
                threaded.join(INFINITE);
        }
        void wait()
        {
            ActPrintLog(&broadcaster.activity, "CSend::wait(), messages to send: %d", broadcastQueue.ordinality());
            addBlock(NULL);
            threaded.join(INFINITE);
        }
    // IThreaded
        virtual void threadmain() override
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

    // NB: returns true if all done. Sets allDoneExceptSelf if all except this sender are done
    bool senderStop(unsigned sender)
    {
        bool done = sendersDone->testSet(sender, true);
        assertex(false == done);
        unsigned which = sendersDone->scan(0, false);
        if (which != senders)
            return false;
        // all have signalled stop
        activity.ActPrintLog("CBroadcaster::senderStop() all done - waitingAtAllDoneCount=%u", waitingAtAllDoneCount);
        if (waitingAtAllDoneCount)
        {
            allDoneSem.signal(waitingAtAllDoneCount);
            waitingAtAllDoneCount = 0;
        }
        return true;
    }
    bool senderStop(CSendItem &sendItem)
    {
        unsigned sender = nodeBroadcast?sendItem.queryNode():sendItem.querySlave();
        return senderStop(sender);
    }
    unsigned target(unsigned i, unsigned node)
    {
        // For a tree broadcast, calculate the next node to send the data to. i represents the ith copy sent from this node.
        // node is a 0 based node number.
        // It returns a 0 based node number of the next node to send the data to.
        unsigned n = node;
        unsigned j=0;
        while (n)
        {
            j++;
            n /= 2;
        }
        return ((1<<(i+j))+node);
    }
    void broadcastToOthers(CSendItem *sendItem)
    {
        dbgassertex(broadcastLock);
        mptag_t rt = ::createReplyTag();
        unsigned origin = sendItem->queryNode();
        unsigned pseudoNode = (myNode<origin) ? nodes-origin+myNode : myNode-origin;
        CMessageBuffer replyMsg;
        // sends to all in 1st pass, then waits for ack from all
        for (unsigned sendRecv=0; sendRecv<2 && !activity.queryAbortSoon(); sendRecv++)
        {
            unsigned i = 0;
            while (!activity.queryAbortSoon())
            {
                unsigned t = target(i++, pseudoNode);
                if (t>=nodes)
                    break;
                t += origin;
                if (t>=nodes)
                    t -= nodes;
                t += 1; // adjust 0 based to 1 based, i.e. excluding master at 0
#ifdef _TRACEBROADCAST
                unsigned sendLen = sendItem->length();
#endif
                if (0 == sendRecv) // send
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sending to node %d, origin node %d, origin slave %d, size %d, code=%d", myNode+1, t, origin+1, sendItem->querySlave()+1, sendLen, (unsigned)sendItem->queryCode());
#endif
                    CMessageBuffer &msg = sendItem->queryMsg();
                    msg.setReplyTag(rt); // simulate sendRecv
                    CriticalBlock b(*broadcastLock); // prevent other channels overlapping, otherwise causes queue ordering issues with MP multi packet messages to same dst.
                    comm.send(msg, t, mpTag);
                }
                else // recv reply
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Waiting for reply from node %d, origin node %d, origin slave %d, size %d, code=%d, replyTag=%d", myNode+1, t, origin+1, sendItem->querySlave()+1, sendLen, (unsigned)sendItem->queryCode(), (unsigned)rt);
#endif
                    if (!activity.receiveMsg(comm, replyMsg, t, rt))
                        break;
#ifdef _TRACEBROADCAST
                    ActPrintLog(&activity, "Broadcast node %d Sent to node %d, origin node %d, origin slave %d, size %d, code=%d - received ack", myNode+1, t, origin+1, sendItem->querySlave()+1, sendLen, (unsigned)sendItem->queryCode());
#endif
                }
            }
        }
    }
    void cancelReceive()
    {
        stopRecv = true;
        if (receiving)
            comm.cancel(RANK_ALL, mpTag);
    }
    bool receiveMsg(CMessageBuffer &mb, rank_t *sender)
    {
        BooleanOnOff onOff(receiving);
        // check 'cancelledReceive' every 10 secs
        while (!stopRecv)
        {
            if (comm.recv(mb, RANK_ALL, mpTag, sender, 10000))
                return true;
        }
        return false;
    }
    void recvLoop()
    {
        // my sender is implicitly stopped (never sends to self)
        if (senderStop(mySender))
        {
            recvInterface->bCastReceive(NULL, true);
            return;
        }
        ActPrintLog(&activity, "Start of recvLoop()");
        CMessageBuffer msg;
        while (!stopRecv && !activity.queryAbortSoon())
        {
            rank_t sendRank;
            if (!receiveMsg(msg, &sendRank))
            {
                ActPrintLog(&activity, "recvLoop() - receiveMsg cancelled");
                break;
            }
            mptag_t replyTag = msg.getReplyTag();
            CMessageBuffer ackMsg;
            Owned<CSendItem> sendItem = new CSendItem(msg);
#ifdef _TRACEBROADCAST
            ActPrintLog(&activity, "Broadcast node %d received from node %d, origin node %d, origin slave %d, size %d, code=%d", myNode+1, (unsigned)sendRank, sendItem->queryNode()+1, sendItem->querySlave()+1, sendItem->length(), (unsigned)sendItem->queryCode());
#endif
            comm.send(ackMsg, sendRank, replyTag); // send ack
#ifdef _TRACEBROADCAST
            ActPrintLog(&activity, "Broadcast node %d, sent ack to node %d, replyTag=%d", myNode+1, (unsigned)sendRank, (unsigned)replyTag);
#endif
            sender.addBlock(sendItem.getLink());
            assertex(myNode != sendItem->queryNode());
            switch (sendItem->queryCode())
            {
                case bcast_stop:
                {
                    CriticalBlock b(allDoneLock);
                    ActPrintLog(&activity, "recvLoop - received bcast_stop, from : node=%u, slave=%u", sendItem->queryNode()+1, sendItem->querySlave()+1);
                    bool stop = senderStop(*sendItem);
                    recvInterface->bCastReceive(sendItem.getLink(), stop);
                    if (stop)
                    {
                        ActPrintLog(&activity, "recvLoop, received last senderStop, node=%u, slave=%u", sendItem->queryNode()+1, sendItem->querySlave()+1);
                        // NB: this slave has nothing more to receive.
                        // However the sender will still be re-broadcasting some packets, including these stop packets
                        return;
                    }
                    break;
                }
                case bcast_sendStopping:
                    setStopping(sendItem->queryNode());
                // fall through
                case bcast_send:
                {
                    if (!allRequestStop) // don't care if all stopping
                        recvInterface->bCastReceive(sendItem.getClear(), false);
                    break;
                }
                default:
                    throwUnexpected();
            }
        }
        ActPrintLog(&activity, "End of recvLoop()");
    }
    inline void _setStopping(unsigned node)
    {
        broadcastersStopping->set(node, true);
        // allRequestStop=true, if I'm stopping and all others have requested also
        allRequestStop = broadcastersStopping->scan(0, false) == nodes;
    }
public:
    CBroadcaster(CActivityBase &_activity) : activity(_activity), receiver(*this), sender(*this), comm(_activity.queryJob().queryNodeComm())
    {
        allRequestStop = stopping = stopRecv = false;
        waitingAtAllDoneCount = 0;
        myNode = activity.queryJob().queryMyNodeRank()-1; // 0 based
        mySlave = activity.queryJobChannel().queryMyRank()-1; // 0 based
        nodes = activity.queryJob().queryNodes();
        slaves = activity.queryJob().querySlaves();
        mySender = mySlave;
        senders = slaves;
        sendersDone.setown(createThreadSafeBitSet());
        broadcastersStopping.setown(createThreadSafeBitSet());
        mpTag = TAG_NULL;
        recvInterface = NULL;
        stopFlag = bcastflag_null;
        broadcastLock = NULL;
        receiving = false;
        nodeBroadcast = false;
    }
    void start(IBCastReceive *_recvInterface, mptag_t _mpTag, bool _stopping, bool _nodeBroadcast)
    {
        nodeBroadcast = _nodeBroadcast;
        if (nodeBroadcast)
        {
            mySender = myNode;
            senders = nodes;
        }
        else
        {
            mySender = mySlave;
            senders = slaves;
        }
        stopping = _stopping;
        recvInterface = _recvInterface;
        stopRecv = false;
        mpTag = _mpTag;
        if (recvInterface)
        {
            receiver.start();
            sender.start();
        }
    }
    void reset()
    {
        allRequestStop = stopping = false;
        waitingAtAllDoneCount = 0;
        stopFlag = bcastflag_null;
        sendersDone->reset();
        broadcastersStopping->reset();
    }
    CSendItem *newSendItem(broadcast_code code)
    {
        if (stopping && (bcast_send==code))
            code = bcast_sendStopping;
        return new CSendItem(code, myNode, mySlave);
    }
    void setBroadcastLock(CriticalSection *_broadcastLock)
    {
        broadcastLock = _broadcastLock;
    }
    void resetSendItem(CSendItem *sendItem)
    {
        sendItem->reset();
    }
    void waitReceiverDone(unsigned sender)
    {
        {
            CriticalBlock b(allDoneLock);
            if (senderStop(sender))
            {
                receiver.abort(false);
                recvInterface->bCastReceive(NULL, true);
                return;
            }
            waitingAtAllDoneCount++;
        }
        allDoneSem.wait();
    }
    void end()
    {
        receiver.wait(); // terminates when received stop from all others
        sender.wait(); // terminates when any remaining packets, including final stop packets have been re-broadcast
    }
    void cancel(IException *e=NULL)
    {
        receiver.abort(true);
        sender.abort(true);
        if (e)
        {
            allDoneSem.interrupt(LINK(e), waitingAtAllDoneCount);
            activity.fireException(e);
        }
        else
            allDoneSem.signal(waitingAtAllDoneCount);
        waitingAtAllDoneCount = 0;
    }
    bool send(CSendItem *sendItem)
    {
        broadcastToOthers(sendItem);
        return !allRequestStop;
    }
    bool isStopping()
    {
        return broadcastersStopping->test(myNode);
    }
    broadcast_flags queryStopFlag() { return stopFlag; }
    bool stopRequested()
    {
        if (bcastflag_null != queryStopFlag()) // if this node has requested to stop immediately
            return true;
        return allRequestStop; // if not, if all have request to stop
    }
    void setStopping(unsigned node)
    {
        CriticalBlock b(stopCrit); // multiple channels could call
        _setStopping(node);
    }
    void stop(unsigned node, broadcast_flags flag)
    {
        CriticalBlock b(stopCrit); // multiple channels could call
        _setStopping(node);
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
        virtual void threadmain() override
        {
            chunkUnique = parent.run(startRow, endRow);
        }
    };

    rowidx_t getMore(rowidx_t &startRow) // NB: returns end row #
    {
        //NOTE: If we could guarantee that nextChunkStartRow could not overflow then the spin lock could be replaced
        //with a atomic fetch_add().
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
        for (;;)
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
        threadCount = activity.getOptInt(THOROPT_JOINHELPER_THREADS, 0);
        if (0 == threadCount)
            threadCount = activity.queryMaxCores();
    }
    bool init(rowidx_t rowCount, roxiemem::IRowManager *rowManager)
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
            void *pBitSetMem = rowManager->allocate(bitSetMemSz, activity.queryContainer().queryId(), SPILL_PRIORITY_LOW);
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

#ifdef _TRACEBROADCAST
#define InterChannelBarrier() interChannelBarrier(__LINE__)
#else
#define InterChannelBarrier() interChannelBarrier();
#endif

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
    inline size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned numRows, const void **rows, unsigned flags)
    {
        return helper->transform(rowBuilder, left, right, numRows, rows, flags);
    }
    inline size32_t joinTransform(ARowBuilder &rowBuilder, const void *left, const void *right, unsigned count, unsigned flags)
    {
        return helper->transform(rowBuilder, left, right, count, flags);
    }
};


class CThorRowArrayWithFlushMarker : public CThorSpillableRowArray
{
public:
    CThorRowArrayWithFlushMarker(CActivityBase &activity) : CThorSpillableRowArray(activity)
    {
    }
    CThorRowArrayWithFlushMarker(CActivityBase &activity, IThorRowInterfaces *rowIf, EmptyRowSemantics emptyRowSemantics=ers_forbidden, StableSortFlag stableSort=stableSort_none, rowidx_t initialSize=InitialSortElements, size32_t commitDelta=CommitStep)
        : CThorSpillableRowArray(activity, rowIf, emptyRowSemantics, stableSort, initialSize, commitDelta)
    {
    }
    rowidx_t flushMarker = 0;
};


struct HtEntry { rowidx_t index, count; };

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
class CInMemJoinBase : public CSlaveActivity, public CAllOrLookupHelper<HELPER>, implements ILookAheadStopNotify, implements IBCastReceive
{
    typedef CSlaveActivity PARENT;

    Owned<IException> leftexception;

    bool eos, eog, someSinceEog;

protected:
    typedef CAllOrLookupHelper<HELPER> HELPERBASE;

    using HELPERBASE::helper;

    static int sortBySize(void * const *_i1, void * const *_i2)
    {
        CThorSpillableRowArray *i1 = (CThorSpillableRowArray *)*_i1;
        CThorSpillableRowArray *i2 = (CThorSpillableRowArray *)*_i2;
        return i2->numCommitted()-i1->numCommitted();
    }

    /* Utility class, that is called from the broadcaster to queue up received blocks
     * It will block if it has > MAX_QUEUE_BLOCKS to process (on the queue)
     * Processing will decompress the incoming blocks and add them to a row array per slave
     */
    class CRowProcessor : public CSimpleInterfaceOf<IThreaded>
    {
        CThreadedPersistent threaded;
        CInMemJoinBase &owner;
        bool stopped;
        SimpleInterThreadQueueOf<CSendItem, true> blockQueue;
        Owned<IException> exception;

        void clearQueue()
        {
            for (;;)
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
            threaded.join(INFINITE);
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
        virtual void threadmain() override
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
                    owner.processRHSRows(sendItem->querySlave(), expandedMb);
                }
            }
            catch (IException *e)
            {
                exception.setown(e);
                EXCLOG(e, "CRowProcessor");
            }
        }
    } *rowProcessor;

    CriticalSection rhsRowLock;
    Owned<CBroadcaster> broadcaster;
    CBroadcaster *channel0Broadcaster;
    CriticalSection *broadcastLock;
    rowidx_t rhsTableLen;
    Owned<HTHELPER> table; // NB: only channel 0 uses table, unless failing over to local lookup join
    Linked<HTHELPER> tableProxy; // Channels >1 will reference channel 0 table unless failed over
    HtEntry currentHashEntry; // Used for lookup,many only
    OwnedConstThorRow leftRow;

    IThorDataLink *leftITDL, *rightITDL;
    Owned<IRowStream> left;
    IRowStream *right = nullptr;
    IThorAllocator *rightThorAllocator;
    roxiemem::IRowManager *rightRowManager;
    Owned<IThorRowInterfaces> sharedRightRowInterfaces;
    Owned<IEngineRowAllocator> rightAllocator;
    Owned<IEngineRowAllocator> leftAllocator;
    Owned<IEngineRowAllocator> allocator; // allocator for output transform
    Owned<IOutputRowSerializer> rightSerializer;
    Owned<IOutputRowDeserializer> rightDeserializer;
    bool gotRHS;
    join_t joinType;
    OwnedConstThorRow defaultRight;
    bool local;
    unsigned flags;
    bool exclude;
    const void *rhsNext;
    CThorExpandingRowArray rhs;
    Owned<IOutputMetaData> outputMeta;
    IOutputMetaData *rightOutputMeta;
    PointerArrayOf<CThorRowArrayWithFlushMarker> rhsSlaveRows;
    IArrayOf<IRowStream> gatheredRHSNodeStreams;
    bool rhsConstant = false;
    bool rhsStartedBefore = false;

    unsigned keepLimit;
    unsigned joined;
    unsigned joinCounter;
    OwnedConstThorRow defaultLeft;

    bool leftMatch, grouped;
    bool fuzzyMatch, returnMany;
    rank_t myNodeNum, mySlaveNum;
    unsigned numNodes, numSlaves;
    OwnedMalloc<CInMemJoinBase *> channels;

    std::atomic<unsigned> interChannelToNotifyCount{0}; // only used on channel 0
    InterruptableSemaphore interChannelBarrierSem;
    bool channelActivitiesAssigned;

    inline bool isLocal() const { return local; }
    inline bool isGlobal() const { return !local; }
    inline void signalInterChannelBarrier()
    {
        interChannelBarrierSem.signal();
    }
    inline bool incNotifyCountAndCheck()
    {
        if (++interChannelToNotifyCount == queryJob().queryJobChannels())
        {
            interChannelToNotifyCount = 0; // reset for next barrier
            return true;
        }
        return false;
    }
    inline void interChannelBarrier()
    {
        if (queryJob().queryJobChannels()>1)
        {
            if (channels[0]->incNotifyCountAndCheck())
            {
                for (unsigned ch=0; ch<queryJob().queryJobChannels(); ch++)
                {
                    if (channels[ch] != this)
                        channels[ch]->signalInterChannelBarrier();
                }
            }
            else
                interChannelBarrierSem.wait();
        }
    }
#ifdef _TRACEBROADCAST
    void interChannelBarrier(unsigned lineNo)
    {
        ActPrintLog("waiting on interChannelBarrier, lineNo = %d", lineNo);
        interChannelBarrier();
        ActPrintLog("past interChannelBarrier, lineNo = %d", lineNo);
    }
#endif
    inline bool isGroupOp() const
    {
        switch (container.getKind())
        {
            case TAKlookupdenormalizegroup:
            case TAKsmartdenormalizegroup:
            case TAKalldenormalizegroup:
                return true;
            default:
                break;
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
            default:
                break;
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
            default:
                break;
        }
        return str.append("---> Unknown Join Type <---");
    }
    void clearRHS()
    {
        ForEachItemIn(a, rhsSlaveRows)
        {
            CThorSpillableRowArray *rows = rhsSlaveRows.item(a);
            if (rows)
                rows->kill();
        }
        rhs.kill();
    }
    void clearHT()
    {
        rhsTableLen = 0;
        table->reset();
        tableProxy.clear();
    }
    void processRHSRows(unsigned slave, MemoryBuffer &mb)
    {
        /* JCSMORE - I wonder if this should be done asynchronously on a few threads (<=1 per target)
         * It also might be better to use hash the rows now and assign to rhsSlaveRows arrays, it's a waste of hash() calls
         * if it never spills, but will make flushing non-locals simpler if spilling occurs.
         */
        CThorSpillableRowArray &rows = *rhsSlaveRows.item(slave);
        CThorExpandingRowArray rhsInRowsTemp(*this, sharedRightRowInterfaces);
        CThorExpandingRowArray pending(*this, sharedRightRowInterfaces);
        RtlDynamicRowBuilder rowBuilder(rightAllocator); // NB: rightAllocator is the shared allocator
        CThorStreamDeserializerSource memDeserializer(mb.length(), mb.toByteArray());
        while (!memDeserializer.eos())
        {
            size32_t sz = rightDeserializer->deserialize(rowBuilder, memDeserializer);
            pending.append(rowBuilder.finalizeRowClear(sz));
            if (pending.ordinality() >= 100)
            {
                // NB: If spilt, addRHSRows will filter out non-locals
                if (!addRHSRows(rows, pending, rhsInRowsTemp)) // NB: in SMART case, must succeed
                    throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
            }
        }
        if (pending.ordinality())
        {
            // NB: If spilt, addRHSRows will filter out non-locals
            if (!addRHSRows(rows, pending, rhsInRowsTemp)) // NB: in SMART case, must succeed
                throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
        }
    }
    void broadcastRHS() // broadcasting local rhs
    {
        Owned<CSendItem> sendItem = broadcaster->newSendItem(bcast_send);
        MemoryBuffer mb;
        CThorExpandingRowArray rhsInRowsTemp(*this, sharedRightRowInterfaces);
        CThorExpandingRowArray pending(*this, sharedRightRowInterfaces);
        try
        {
            CMemoryRowSerializer mbser(mb);
            while (!abortSoon)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;

                    /* Add all locally read right rows to channel0 directly
                     * NB: these rows remain on their channel allocator.
                     */
                    if (numNodes>1)
                    {
                        rightSerializer->serialize(mbser, (const byte *)row.get());
                        pending.append(row.getClear());
                        if (mb.length() >= MAX_SEND_SIZE || channel0Broadcaster->stopRequested())
                            break;
                    }
                    else
                        pending.append(row.getClear());
                    if (pending.ordinality() >= 100)
                    {
                        if (!channels[0]->addRHSRows(mySlaveNum, pending, rhsInRowsTemp)) // may cause broadcaster to be told to stop (for isStopping() to become true)
                            throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
                    }
                    if (channel0Broadcaster->stopRequested())
                        break;
                }
                if (pending.ordinality())
                {
                    if (!channels[0]->addRHSRows(mySlaveNum, pending, rhsInRowsTemp)) // may cause broadcaster to be told to stop (for isStopping() to become true)
                        throw MakeActivityException(this, 0, "Out of memory: Unable to add any more rows to RHS");
                }
                if (0 == mb.length()) // will always be true if numNodes = 1
                    break;
                if (channel0Broadcaster->stopRequested())
                    sendItem->setFlag(channel0Broadcaster->queryStopFlag());
                ThorCompress(mb, sendItem->queryMsg());
                if (!broadcaster->send(sendItem))
                    break;
                if (channel0Broadcaster->stopRequested())
                    break;
                mb.clear();
                broadcaster->resetSendItem(sendItem);
            }
        }
        catch (IException *e)
        {
            ActPrintLog(e, "CInMemJoinBase::broadcastRHS: exception");
            throw;
        }

        sendItem.setown(broadcaster->newSendItem(bcast_stop));
        if (channel0Broadcaster->stopRequested())
            sendItem->setFlag(channel0Broadcaster->queryStopFlag());
        ActPrintLog("Sending final RHS broadcast packet");
        broadcaster->send(sendItem); // signals stop to others
    }
    inline void resetRhsNext()
    {
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
            rhsNext = tableProxy->getNextRHS(currentHashEntry); // NB: currentHashEntry only used for Lookup,Many case
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
                size32_t sz = HELPERBASE::joinTransform(rowBuilder, leftRow, rightRow, numRows, filteredRhs.getArray(), JTFmatchedleft|(numRows ? JTFmatchedright : 0));
                if (sz)
                    ret.setown(rowBuilder.finalizeRowClear(sz));
            }
            else
            {
                ret.set(leftRow);
                if (filteredRhs.ordinality())
                {
                    size32_t rowSize = 0;
                    for (;;)
                    {
                        const void *rightRow = filteredRhs.item(rcCount);
                        size32_t sz = HELPERBASE::joinTransform(rowBuilder, ret, rightRow, ++rcCount, JTFmatchedleft|JTFmatchedright);
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
            for (;;)
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
                            // NB: currentHashEntry used for Lookup,Many or All cases
                            rhsNext = tableProxy->getFirstRHSMatch(leftRow, failRow, currentHashEntry); // also checks abortLimit/atMost
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
                                size32_t sz = HELPERBASE::joinTransform(rowBuilder, leftRow, rhsNext, ++joinCounter, JTFmatchedleft|JTFmatchedright);
                                if (sz)
                                {
                                    OwnedConstThorRow row = rowBuilder.finalizeRowClear(sz);
                                    someSinceEog = true;
                                    if (++joined == keepLimit)
                                        rhsNext = NULL;
                                    else if (!returnMany)
                                        rhsNext = NULL;
                                    else
                                        rhsNext = tableProxy->getNextRHS(currentHashEntry); // NB: currentHashEntry only used for Lookup,Many case
                                    return row.getClear();
                                }
                            }
                        }
                        rhsNext = tableProxy->getNextRHS(currentHashEntry); // NB: currentHashEntry used for Lookup,Many or All cases
                    }
                    if (!leftMatch && NULL == rhsNext && 0!=(flags & JFleftouter))
                    {
                        size32_t sz = HELPERBASE::joinTransform(rowBuilder, leftRow, defaultRight, 0, JTFmatchedleft);
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
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    CInMemJoinBase(CGraphElementBase *_container, const StatisticsMapping &statsMapping = basicActivityStatistics)
        : CSlaveActivity(_container, statsMapping), HELPERBASE((HELPER *)queryHelper()), rhs(*this)
    {
        gotRHS = false;
        rhsNext = NULL;
        myNodeNum = queryJob().queryMyNodeRank()-1; // 0 based
        mySlaveNum = queryJobChannel().queryMyRank()-1; // 0 based
        numNodes = queryJob().queryNodes();
        numSlaves = queryJob().querySlaves();
        local = container.queryLocal() || (1 == numSlaves);
        rowProcessor = NULL;
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
        channel0Broadcaster = NULL;
        channelActivitiesAssigned = false;
        table.setown(new HTHELPER);
        rightOutputMeta = NULL;
        if (getOptBool("lkjoinUseSharedAllocator", true))
        {
            ActPrintLog("Using shared row manager for RHS");
            rightThorAllocator = queryJob().querySharedAllocator();
        }
        else
            rightThorAllocator = queryJobChannel().queryThorAllocator();
        rightRowManager = rightThorAllocator->queryRowManager();
        broadcastLock = NULL;
        if (!isGlobal())
            setRequireInitData(false);
        rhsConstant = getOptBool("lookupRhsConstant", false); // for testing purposes only
        appendOutputLinked(this);
    }
    ~CInMemJoinBase()
    {
        if (isGlobal())
        {
            ::Release(rowProcessor);
            ForEachItemIn(a, rhsSlaveRows)
            {
                CThorSpillableRowArray *rows = rhsSlaveRows.item(a);
                if (rows)
                    delete rows;
            }
            if (broadcastLock)
                delete broadcastLock;
        }
    }
    CriticalSection *queryBroadcastLock()
    {
        return broadcastLock;
    }
    HTHELPER *queryTable() { return table; }
    void startLeftInput()
    {
        try
        {
            startInput(0);
            if (ensureStartFTLookAhead(0))
                setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), LOOKUPJOINL_SMART_BUFFER_SIZE, ::canStall(input), grouped, RCUNBOUND, this, &container.queryJob().queryIDiskUsage()), false);
            left.set(inputStream); // can be replaced by loader stream
        }
        catch(IException *e)
        {
            leftexception.setown(e);
        }
    }
    virtual bool isRhsConstant() const { return rhsConstant; }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        StringBuffer str;
        ActPrintLog("Join type is %s", getJoinTypeStr(str).str());

        assertex(inputs.ordinality() == 2);

        leftITDL = queryInput(0);
        rightITDL = queryInput(1);
        rightOutputMeta = rightITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
        rightAllocator.setown(rightThorAllocator->getRowAllocator(rightOutputMeta, container.queryId(), (roxiemem::RHFpacked|roxiemem::RHFunique)));

        allocator.set(queryRowAllocator());
        leftAllocator.set(::queryRowAllocator(leftITDL));
        outputMeta.set(leftITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta());

        rightSerializer.set(::queryRowSerializer(rightITDL));
        rightDeserializer.set(::queryRowDeserializer(rightITDL));

        if ((flags & JFonfail) || (flags & JFleftouter))
        {
            RtlDynamicRowBuilder rr(rightAllocator);
            rr.ensureRow();
            size32_t rrsz = helper->createDefaultRight(rr);
            defaultRight.setown(rr.finalizeRowClear(rrsz));
        }

        if (isGlobal())
        {
            mpTag = container.queryJobChannel().deserializeMPTag(data);

            unsigned slaves = container.queryJob().querySlaves();
            rhsSlaveRows.ensure(slaves);
            for (unsigned s=0; s<container.queryJob().querySlaves(); s++)
                rhsSlaveRows.append(new CThorRowArrayWithFlushMarker(*this));
            channels.allocateN(queryJob().queryJobChannels());
            broadcaster.setown(new CBroadcaster(*this));
            if (0 == queryJobChannelNumber())
            {
                rowProcessor = new CRowProcessor(*this);
                broadcastLock = new CriticalSection;
            }
            sharedRightRowInterfaces.setown(createThorRowInterfaces(rightRowManager, rightOutputMeta, queryId(), queryHeapFlags(), &queryJobChannel().querySharedMemCodeContext()));
            rhs.setup(sharedRightRowInterfaces);
            // NB: use sharedRightRowInterfaces, so that expanding ptr array is using shared allocator
            for (unsigned s=0; s<container.queryJob().querySlaves(); s++)
                rhsSlaveRows.item(s)->setup(sharedRightRowInterfaces, ers_forbidden, stableSort_none, true);
        }
    }
    virtual void setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered) override
    {
        PARENT::setInputStream(index, _input, consumerOrdered);
        if (1 == index)
            right = queryInputStream(1);
    }
    virtual void reset() override
    {
        PARENT::reset();
        if (!isRhsConstant())
        {
            gotRHS = false;
            rhsTableLen = 0;
        }
    }
    virtual void start() override
    {
        joined = 0;
        joinCounter = 0;
        leftMatch = false;
        rhsNext = NULL;

        if (isGlobal())
        {
            // It is not until here, that it is guaranteed all channel slave activities have been initialized.
            if (!channelActivitiesAssigned)
            {
                channelActivitiesAssigned = true;
                for (unsigned c=0; c<queryJob().queryJobChannels(); c++)
                {
                    CInMemJoinBase &channel = (CInMemJoinBase &)queryChannelActivity(c);
                    channels[c] = &channel;
                }
                broadcaster->setBroadcastLock(channels[0]->queryBroadcastLock());
                channel0Broadcaster = channels[0]->broadcaster;
            }
        }

        eos = eog = someSinceEog = false;
        currentHashEntry.index = 0;
        currentHashEntry.count = 0;

        if (rhsStartedBefore && isRhsConstant()) // if this is the 2nd+ iteration and the RHS is constant, don't bother restarting right, it will not be used.
        {
            startLeftInput();
        }
        else
        {
            CAsyncCallStart asyncLeftStart(std::bind(&CInMemJoinBase::startLeftInput, this));
            try
            {
                startInput(1);
                rhsStartedBefore = true;
            }
            catch (CATCHALL)
            {
                asyncLeftStart.wait();
                left->stop();
                throw;
            }
            asyncLeftStart.wait();
        }
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
        if (isGlobal())
        {
            cancelReceiveMsg(queryJob().queryNodeComm(), RANK_ALL, mpTag);
            interChannelBarrierSem.interrupt(NULL);
            if (broadcaster)
                broadcaster->cancel();
            if (rowProcessor)
                rowProcessor->abort();
        }
    }
    virtual void stop()
    {
        if (!isRhsConstant()) // if rhs is a constant dataset, or if failed over
        {
            clearRHS();
            clearHT();
        }
        if (right)
            stopInput(1, "(R)");
        if (broadcaster)
            broadcaster->reset();
        stopInput(0, "(L)");
        left.clear();
        dataLinkStop();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canStall = true;
    }

// The methods below are only used if activity is global
    void doBroadcastRHS(bool stopping)
    {
        if (stopping)
            channel0Broadcaster->setStopping(myNodeNum);
        if (0 == queryJobChannelNumber())
        {
            rowProcessor->start();
            broadcaster->start(this, mpTag, stopping, false); // slaves broadcasting
            broadcastRHS();
            broadcaster->end();
            rowProcessor->wait();
        }
        else
        {
            broadcaster->start(NULL, mpTag, stopping, false); // pass NULL for IBCastReceive, since only channel 0 receives
            broadcastRHS();
            channel0Broadcaster->waitReceiverDone(mySlaveNum);
        }
    }
    void doBroadcastStop(mptag_t tag, broadcast_flags flag) // only called on channel 0
    {
        broadcaster->reset();
        broadcaster->start(this, tag, false, true); // nodes broadcasting
        Owned<CSendItem> sendItem = broadcaster->newSendItem(bcast_stop);
        if (flag)
            sendItem->setFlag(flag);
        broadcaster->send(sendItem); // signals stop to others
        broadcaster->end();
    }
    rowidx_t getGlobalRHSTotal()
    {
        rowcount_t rhsRows = 0;
        ForEachItemIn(a, rhsSlaveRows)
        {
            CThorSpillableRowArray &rows = *rhsSlaveRows.item(a);
            rows.flush();
            rhsRows += rows.numCommitted();
            if (rhsRows > RIMAX)
                throw MakeActivityException(this, 0, "Too many RHS rows: %" RCPF "d", rhsRows);
        }
        return (rowidx_t)rhsRows;
    }
    bool addRHSRows(unsigned slave, CThorExpandingRowArray &inRows, CThorExpandingRowArray &rhsInRowsTemp)
    {
        CThorSpillableRowArray &rows = *rhsSlaveRows.item(slave);
        return addRHSRows(rows, inRows, rhsInRowsTemp);
    }
    virtual bool addRHSRows(CThorSpillableRowArray &rhsRows, CThorExpandingRowArray &inRows, CThorExpandingRowArray &rhsInRowsTemp)
    {
        CriticalBlock b(rhsRowLock);
        return rhsRows.appendRows(inRows, true);
    }

// IBCastReceive (only used if global)
    virtual void bCastReceive(CSendItem *sendItem, bool stop)
    {
        if (sendItem && (bcast_stop == sendItem->queryCode()))
        {
            sendItem->Release();
            if (!stop)
                return;
            sendItem = NULL; // fall through, base signals stop to rowProcessor
        }
        dbgassertex((sendItem==NULL) == stop); // if sendItem==NULL stop must = true, if sendItem != NULL stop must = false;
        rowProcessor->addBlock(sendItem);
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

interface IChannelDistributor
{
    virtual void putRow(const void *row) = 0;
    virtual bool spill(bool critical) = 0;
    virtual roxiemem::IBufferedRowCallback *queryCallback() = 0;
};

template <class HTHELPER>
class CLookupJoinActivityBase : public CInMemJoinBase<HTHELPER, IHThorHashJoinArg>, implements roxiemem::IBufferedRowCallback
{
    typedef CInMemJoinBase<HTHELPER, IHThorHashJoinArg> PARENT;
protected:
    using PARENT::container;
    using PARENT::queryJob;
    using PARENT::queryJobChannel;
    using PARENT::queryJobChannelNumber;
    using PARENT::myNodeNum;
    using PARENT::numNodes;
    using PARENT::numSlaves;
    using PARENT::ActPrintLog;
    using PARENT::right;
    using PARENT::left;
    using PARENT::rightITDL;
    using PARENT::leftITDL;
    using PARENT::rhsSlaveRows;
    using PARENT::rhsTableLen;
    using PARENT::table;
    using PARENT::flags;
    using PARENT::outputMeta;
    using PARENT::leftMatch;
    using PARENT::gotRHS;
    using PARENT::isLocal;
    using PARENT::isGlobal;
    using PARENT::leftRow;
    using PARENT::allocator;
    using PARENT::defaultRight;
    using PARENT::grouped;
    using PARENT::abortSoon;
    using PARENT::rightRowManager;
    using PARENT::rightAllocator;
    using PARENT::sharedRightRowInterfaces;
    using PARENT::leftAllocator;
    using PARENT::returnMany;
    using PARENT::fuzzyMatch;
    using PARENT::keepLimit;
    using PARENT::doBroadcastRHS;
    using PARENT::doBroadcastStop;
    using PARENT::getGlobalRHSTotal;
    using PARENT::getOptBool;
    using PARENT::getOpt;
    using PARENT::broadcaster;
    using PARENT::inputs;
    using PARENT::queryHelper;
    using PARENT::slaveTimerStats;
    using PARENT::timeActivities;
    using PARENT::fireException;
    using PARENT::lookupNextRow;
    using PARENT::rowProcessor;
    using PARENT::dataLinkIncrement;
    using PARENT::helper;
    using PARENT::clearHT;
    using PARENT::rhs;
    using PARENT::queryRowManager;
    using PARENT::channels;
    using PARENT::interChannelBarrier;
    using PARENT::sortBySize;
    using PARENT::channel0Broadcaster;
    using PARENT::mySlaveNum;
    using PARENT::tableProxy;
    using PARENT::gatheredRHSNodeStreams;
    using PARENT::queryInput;
    using PARENT::rhsRowLock;
    using PARENT::hasStarted;
    using PARENT::stats;

    IHash *leftHash, *rightHash;
    ICompare *compareRight, *compareLeftRight;

    unsigned abortLimit, atMost;
    bool dedup, stable;

    mptag_t lhsDistributeTag, rhsDistributeTag, broadcast2MpTag, broadcast3MpTag;

    // Handling failover to a) hashed local lookupjoin b) hash distributed standard join
    bool smart;
    bool rhsCollated, rhsCompacted;
    unsigned spillCompInfo;
    Owned<IHashDistributor> lhsDistributor, rhsDistributor;
    ICompare *compareLeft;
    std::atomic<bool> failedOverToLocal{false}, failedOverToStandard{false};
    CriticalSection broadcastSpillingLock;
    Owned<IJoinHelper> joinHelper;
    std::atomic<unsigned> aggregateFailoversToLocal{0}; // total number of times this activity has failed over to local smart join (0/1 unless in loop)
    std::atomic<unsigned> aggregateFailoversToStandard{0}; // total number of times this activity has failed over to standard hash join (0/1 unless in loop)

    // NB: Only used by channel 0
    Owned<CFileOwner> overflowWriteFile;
    Owned<IRowWriter> overflowWriteStream;
    rowcount_t overflowWriteCount;
    OwnedMalloc<IChannelDistributor *> channelDistributors;
    unsigned nextRhsToSpill = 0;

    inline bool isSmart() const { return smart; }
    inline void setFailoverToLocal()
    {
        bool expectedState = false;
        if (failedOverToLocal.compare_exchange_strong(expectedState, true))
            ++aggregateFailoversToLocal;
    }
    inline void setFailoverToStandard()
    {
        bool expectedState = false;
        if (failedOverToStandard.compare_exchange_strong(expectedState, true))
            ++aggregateFailoversToStandard;
    }
    inline bool hasFailedOverToLocal() const { return failedOverToLocal; }
    inline bool hasFailedOverToStandard() const { return failedOverToStandard; }
    inline bool isRhsCollated() const { return rhsCollated; }
    rowidx_t clearNonLocalRows(CThorRowArrayWithFlushMarker &rows, unsigned slave)
    {
        rowidx_t clearedRows = 0;
        rowidx_t committedRows = rows.numCommitted();
        ActPrintLog("clearNonLocalRows[slave=%u], numCommitted=%" RIPF "u, totalRows(inc uncommitted)=%" RIPF "u, flushMarker=%" RIPF "u", slave, committedRows, rows.queryTotalRows(), rows.flushMarker);

        const void **_rows = rows.getBlock(committedRows);
        for (rowidx_t r=rows.flushMarker; r<committedRows; r++)
        {
            const void *row = _rows[r];
            if (row) // NB: rows can be null if OOM event flushed and saved row arrays, in which case flushMarker will have been reset
            {
                unsigned hv = rightHash->hash(row);
                if (myNodeNum != (hv % numNodes))
                {
                    ReleaseThorRow(row);
                    _rows[r] = nullptr;
                    ++clearedRows;
                }
            }
        }
        /* Record point that clearNonLocalRows reached,
         * so that can resume from that point when recalled.
         */
        rows.flushMarker = committedRows;
        return clearedRows;
    }
    rowidx_t clearNonLocalRowsProtected(CThorRowArrayWithFlushMarker &rows, unsigned slave)
    {
        CThorArrayLockBlock block(rows);
        return clearNonLocalRows(rows, slave);
    }
    // Annoyingly similar to above, used post broadcast when rhsSlaveRows collated into 'rhs'
    rowidx_t clearNonLocalRows(CThorExpandingRowArray &rows)
    {
        rowidx_t clearedRows = 0;
        rowidx_t numRows = rows.ordinality();
        for (rowidx_t r=0; r<numRows; r++)
        {
            unsigned hv = rightHash->hash(rows.query(r));
            if (myNodeNum != (hv % numNodes))
            {
                OwnedConstThorRow row = rows.getClear(r); // dispose of
                ++clearedRows;
            }
        }
        return clearedRows;
    }
    bool clearAllNonLocalRows(const char *msg, bool spillRowArrays=false)
    {
        // This is likely to free memory, so block others (threads) until done
        // NB: This will not block appends
        CriticalBlock b(broadcastSpillingLock);
        if (!hasFailedOverToLocal())
        {
            setFailoverToLocal();
            ActPrintLog("Clearing non-local rows - cause: %s", msg);

            broadcaster->stop(myNodeNum, bcastflag_spilt); // signals to broadcast to start stopping immediately and to signal spilt to others

            rowidx_t clearedRows = 0;
            if (rhsCollated)
            {
                // This only needs to be done once, no rows will be added after collated
                clearedRows += clearNonLocalRows(rhs);
                rhs.compact();
                rhsCompacted = true;
            }
            else
            {
                /* NB: It is likely that there will be unflushed rows in the rhsSlaveRows arrays after we are done here.
                 * These will need flushing when all is done and clearNonLocalRows will need recalling to process rest
                 */
                ForEachItemIn(slave, rhsSlaveRows)
                {
                    CThorRowArrayWithFlushMarker &rows = *rhsSlaveRows.item(slave);
                    clearedRows += clearNonLocalRowsProtected(rows, slave);
                }
            }
            ActPrintLog("handleLowMem: clearedRows = %" RIPF "d", clearedRows);
            if (clearedRows)
                return true;
        }
        if (spillRowArrays) // only do if have to due to memory pressure. Not via foreign node notification.
        {
            // NB: round robin through row-arrays, to avoid spilling the same row arrays that have had a few new rows added
            unsigned startRhsToSpill = nextRhsToSpill;
            do
            {
                unsigned curRhsToSpill = nextRhsToSpill;
                if (++nextRhsToSpill == rhsSlaveRows.ordinality())
                    nextRhsToSpill = 0;

                CThorRowArrayWithFlushMarker &rows = *rhsSlaveRows.item(curRhsToSpill);
                if (rows.numCommitted())
                {
                    Owned<CFileOwner> file;
                    {
                        // NB: rows may still be added to the row arrays, so protect array whilst saving
                        CThorArrayLockBlock block(rows);
                        if (rows.numCommitted() == clearNonLocalRows(rows, curRhsToSpill)) // this is to clear out any stragglers that can get added whilst initial fail over to local was happening.
                            continue; // all rows were cleared, no local rows remain, so nothing to save. Skip to next row set

                        VStringBuffer tempPrefix("spill_%d", container.queryId());
                        StringBuffer tempName;
                        GetTempName(tempName, tempPrefix.str(), true);
                        file.setown(new CFileOwner(createIFile(tempName.str())));
                        VStringBuffer spillPrefixStr("clearAllNonLocalRows(%d)", SPILL_PRIORITY_SPILLABLE_STREAM);
                        // 3rd param. is skipNulls = true, the row arrays may have had the non-local rows delete already.
                        rows.save(file->queryIFile(), spillCompInfo, true, spillPrefixStr.str()); // saves committed rows
                        rows.flushMarker = 0; // reset because array will be moved as a consequence of further adds, so next scan must be from start
                    }

                    unsigned rwFlags = DEFAULT_RWFLAGS;
                    if (spillCompInfo)
                    {
                        rwFlags |= rw_compress;
                        rwFlags |= spillCompInfo;
                    }
                    gatheredRHSNodeStreams.append(* createRowStream(&file->queryIFile(), queryRowInterfaces(rightITDL), rwFlags));
                    return true;
                }
            }
            while (nextRhsToSpill != startRhsToSpill);
        }
        return false;
    }
    void checkSmartMemException(IException *e)
    {
        if (!isSmart())
            throw e;
        switch (e->errorCode())
        {
        case ROXIEMM_MEMORY_POOL_EXHAUSTED:
        case ROXIEMM_MEMORY_LIMIT_EXCEEDED:
            break;
        default:
            throw e;
        }
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
            table->setup(this, rightRowManager, size, leftHash, rightHash, compareLeftRight);
        }
        catch (IException *e)
        {
            checkSmartMemException(e);
            e->Release();
            return false;
        }
        rhsTableLen = size;
        return true;
    }
    /*
     * getGatheredRHSStream() returns stream of global rows that have been gathered locally
     * Used by handleFailoverToLocalRHS() only.
     */
    IRowStream *getGatheredRHSStream()
    {
        if (rhsCollated) // if global rows are already collated, just move into spillable stream.
        {
            // NB: If spilt after rhsCollated set, callback will have cleared and compacted, rows will still be sorted
            if (rhs.ordinality())
            {
                CThorSpillableRowArray spillableRHS(*this, sharedRightRowInterfaces);
                spillableRHS.transferFrom(rhs);

                /* Set priority higher than std. lookup priority, because any spill will indicate need to
                 * fail over to standard join and it is better to 1st spill a smaller channel collection
                 * that this will feed, than this larger stream.
                 */
                return spillableRHS.createRowStream(SPILL_PRIORITY_LOOKUPJOIN+10, spillCompInfo);
            }
        }
        else
        {
            rhsSlaveRows.sort(sortBySize); // because want biggest consumed 1st

            // NB: Some streams may have already been added to gatheredRHSNodeStreams, as a result of previous spilling
            for (unsigned a=0; a<rhsSlaveRows.ordinality(); a++)
            {
                CThorSpillableRowArray &rows = *rhsSlaveRows.item(a);
                if (rows.numCommitted())
                {
                    /* NB: will kill array when stream is exhausted or if spilt
                     * Set priority higher than std. lookup priority, because any spill will indicate need to
                     * fail over to standard join and it is better to 1st spill a smaller channel collection
                     * that this will feed, than these larger stream.
                     */
                    gatheredRHSNodeStreams.append(* rows.createRowStream(SPILL_PRIORITY_LOOKUPJOIN+10, spillCompInfo)); // NB: default SPILL_PRIORITY_SPILLABLE_STREAM is lower than SPILL_PRIORITY_LOOKUPJOIN
                }
            }
            if (overflowWriteFile)
            {
                unsigned rwFlags = DEFAULT_RWFLAGS;
                if (spillCompInfo)
                {
                    rwFlags |= rw_compress;
                    rwFlags |= spillCompInfo;
                }
                ActPrintLog("Reading overflow RHS broadcast rows : %" RCPF "d", overflowWriteCount);
                Owned<IRowStream> overflowStream = createRowStream(&overflowWriteFile->queryIFile(), queryRowInterfaces(rightITDL), rwFlags);
                gatheredRHSNodeStreams.append(* overflowStream.getClear());
            }
            if (gatheredRHSNodeStreams.ordinality())
                return createConcatRowStream(gatheredRHSNodeStreams.ordinality(), gatheredRHSNodeStreams.getArray());
        }
        return NULL;
    }
    bool prepareLocalHT(CMarker &marker, IThorRowCollector &rightCollector)
    {
        try
        {
            if (!marker.init(rightCollector.numRows(), queryRowManager()))
                return false;
        }
        catch (IException *e)
        {
            checkSmartMemException(e);
            e->Release();
            return false;
        }

        rowidx_t uniqueKeys = 0;
        {
            CThorArrayLockBlock b(rightCollector);
            if (rightCollector.hasSpilt())
                return false;
            /* transfer rows out of collector to perform calc, but we'll keep lock,
             * so that a request to spill, will block delay, but can still proceed after calculate is done
             */
            CThorExpandingRowArray temp(*this);
            rightCollector.transferRowsOut(temp);
            uniqueKeys = marker.calculate(temp, compareRight, false);
            rightCollector.transferRowsIn(temp);
        }
        if (!setupHT(uniqueKeys)) // could cause spilling
        {
            if (!isSmart())
                throw MakeActivityException(this, 0, "Failed to allocate [LOCAL] hash table");
            return false;
        }
        return true;
    }
    /*
     * handleGlobalRHS() attempts to broadcast and gather RHS rows and setup HT on channel 0
     * Checks at various stages if spilt and bails out.
     * Side effect of setting 'rhsCollated' based on ch0 value on all channels
     * and setting setFailoverToLocal() if fails over.
     */
    bool handleGlobalRHS(CMarker &marker, bool globallySorted, bool stopping)
    {
        if (0 == queryJobChannelNumber()) // All channels broadcast, but only channel 0 receives
        {
            if (isSmart())
            {
                overflowWriteCount = 0;
                overflowWriteFile.clear();
                overflowWriteStream.clear();
                rightRowManager->addRowBuffer(this);
            }
            doBroadcastRHS(stopping);

            rowidx_t rhsRows = 0;
            {
                CriticalBlock b(broadcastSpillingLock);
                rhsRows = getGlobalRHSTotal(); // flushes all rhsSlaveRows arrays to calculate total.
                if (hasFailedOverToLocal())
                    overflowWriteStream.clear(); // broadcast has finished, no more can be written
            }
            if (!hasFailedOverToLocal())
            {
                if (stable && !globallySorted)
                    rhs.setup(sharedRightRowInterfaces, ers_forbidden, stableSort_earlyAlloc);
                bool success=false;
                try
                {
                    if (marker.init(rhsRows, rightRowManager)) // May fail if insufficient memory available
                    {
                        // NB: If marker.init() returned false, it will have called the MM callbacks and have setup hasFailedOverToLocal() already
                        success = rhs.resize(rhsRows, SPILL_PRIORITY_LOW); // NB: Could OOM, handled by exception handler
                    }
                }
                catch (IException *e)
                {
                    checkSmartMemException(e);
                    e->Release();
                    success = false;
                }
                if (!success)
                {
                    ActPrintLog("Out of memory trying to size the global RHS row table for a SMART join, will now attempt a distributed local lookup join");
                    if (!hasFailedOverToLocal())
                    {
                        // NB: someone else could have provoked callback already
                        clearAllNonLocalRows("OOM on sizing global row table");
                        dbgassertex(hasFailedOverToLocal());
                    }
                }

                // For testing purposes only
                if (isSmart() && getOptBool(THOROPT_LKJOIN_LOCALFAILOVER, getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)))
                    clearAllNonLocalRows("testing");
            }
            rowidx_t uniqueKeys = 0;
            {
                /* NB: This does not allocate/will not provoke spilling, but spilling callback still active
                 * and need to protect rhsSlaveRows access
                 */
                CriticalBlock b(broadcastSpillingLock);
                if (!hasFailedOverToLocal())
                {
                    // If spilt, don't size ht table now, ht will be sized later if local rhs fits
                    ForEachItemIn(a, rhsSlaveRows)
                    {
                        CThorSpillableRowArray &rows = *rhsSlaveRows.item(a);
                        rhs.appendRows(rows, true); // NB: This should not cause spilling, rhs is already sized and we are only copying ptrs in
                        rows.kill(); // free up ptr table asap
                    }
                    // Have to keep broadcastSpillingLock locked until sort and calculate are done
                    uniqueKeys = marker.calculate(rhs, compareRight, !globallySorted);
                    rhsCollated = true;
                    ActPrintLog("Collated all RHS rows");

                    if (stable && !globallySorted)
                    {
                        ActPrintLog("Clearing rhs stable ptr table");
                        rhs.setup(sharedRightRowInterfaces, ers_forbidden, stableSort_none); // don't need stable ptr table anymore
                    }
                }
            }
            if (!hasFailedOverToLocal()) // check again after processing above
            {
                if (!setupHT(uniqueKeys)) // NB: Sizing can cause spilling callback to be triggered or OOM in case of !smart
                {
                    ActPrintLog("Out of memory trying to size the global hash table for a SMART join, will now attempt a distributed local lookup join");
                    clearAllNonLocalRows("OOM on sizing global hash table"); // NB: setupHT should have provoked callback already
                    dbgassertex(hasFailedOverToLocal());
                }
            }
            if (isSmart())
            {
                /* NB: Potentially one of the slaves spilt late after broadcast and rowprocessor finished
                 * NB2: This is also to let others know state of this slave's spill state.
                 * Need to remove spill callback and broadcast one last message to know.
                 */

                rightRowManager->removeRowBuffer(this);

                ActPrintLog("Broadcasting final spilt status: %s", hasFailedOverToLocal() ? "spilt" : "did not spill");
                // NB: Will cause other slaves to flush non-local if any have and failedOverToLocal will be set on all
                doBroadcastStop(broadcast2MpTag, hasFailedOverToLocal() ? bcastflag_spilt : bcastflag_null);

                if (hasFailedOverToLocal())
                {
                    // If HT sized already and now spilt, it's too big. Clear for re-use by handleLocalRHS()
                    clearHT();
                    marker.reset();
                }

                if (!rhsCollated) // NB: could have spilt after collated
                {
                    // Can now clean/prepare remaining row arrays for next stages
                    ForEachItemIn(a, rhsSlaveRows)
                    {
                        CThorRowArrayWithFlushMarker &rows = *rhsSlaveRows.item(a);
                        rows.flush(true); // If the row array was spilt, force to relocate so it can be compacted

                        /* NB: need to clear non-locals that were added after spill
                         * There should not be many, as broadcast starts to stop as soon as a slave notifies it is spilling
                         * and ignores all non-locals.
                         */
                        clearNonLocalRows(rows, a);

                        ActPrintLog("Compacting rhsSlaveRows[%u], has %" RIPF "u rows", a, rows.numCommitted());
                        rows.compact();
                    }
                }
            }
            InterChannelBarrier();
            if (!REJECTLOG(MCthorDetailedDebugInfo))
            {
                ::ActPrintLog(this, thorDetailedLogLevel, "Shared memory manager memory report");
                rightRowManager->reportMemoryUsage(false);
                ::ActPrintLog(this, thorDetailedLogLevel, "End of shared manager memory report");
            }
        }
        else
        {
            CLookupJoinActivityBase *lkJoinCh0 = (CLookupJoinActivityBase *)channels[0];
            if (isSmart())
            {
                /* Add IBufferedRowCallback to all channels, because memory pressure can come on any IRowManager
                 * However, all invoked callbacks need to be handled by ch0
                 */
                rightRowManager->addRowBuffer(lkJoinCh0);
            }
            doBroadcastRHS(stopping);
            InterChannelBarrier(); // wait for channel 0, which will have marked rhsCollated and broadcast spilt status to all others
            if (isSmart())
                rightRowManager->removeRowBuffer(lkJoinCh0);
            if (lkJoinCh0->hasFailedOverToLocal())
                setFailoverToLocal();
            rhsCollated = lkJoinCh0->isRhsCollated();
        }
        if (!REJECTLOG(MCthorDetailedDebugInfo))
        {
            ::ActPrintLog(this, thorDetailedLogLevel, "Channel memory manager report");
            queryRowManager()->reportMemoryUsage(false);
            ::ActPrintLog(this, thorDetailedLogLevel, "End of channel memory manager report");
        }
        return !hasFailedOverToLocal();
    }
    /*
     * NB: returned stream or rhs will be sorted
     */
    IThorRowCollector *handleLocalRHS(IRowStream *right, ICompare *cmp)
    {
        Owned<IThorRowCollector> channelCollector;
        if (isSmart())
        {
            dbgassertex(!stable);
            if (getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)) // for testing only (force to disk, as if spilt)
                channelCollector.setown(createThorRowCollector(*this, queryRowInterfaces(rightITDL), cmp, stableSort_none, rc_allDisk, SPILL_PRIORITY_LOOKUPJOIN));
            else
                channelCollector.setown(createThorRowCollector(*this, queryRowInterfaces(rightITDL), cmp, stableSort_none, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN));
        }
        else
        {
            // i.e. will fire OOM if runs out of memory loading local right
            channelCollector.setown(createThorRowCollector(*this, queryRowInterfaces(rightITDL), cmp, stable ? stableSort_lateAlloc : stableSort_none, rc_allMem, SPILL_PRIORITY_DISABLE));
        }
        channelCollector->setTracingPrefix("Join right");
        Owned<IRowWriter> writer = channelCollector->getWriter();
        while (!abortSoon)
        {
            const void *next = right->nextRow();
            if (!next)
            {
                next = right->nextRow();
                if (!next)
                    break;
            }
            writer->putRow(next);
        }
        return channelCollector.getClear();
    }
    /*
     * NB: if global attempt fails.
     * Returnes stream or rhs will be sorted
     */
    IThorRowCollector *handleFailoverToLocalRHS(ICompare *cmp)
    {
        class CChannelDistributor : public CSimpleInterfaceOf<IChannelDistributor>, implements roxiemem::IBufferedRowCallback
        {
            CLookupJoinActivityBase &owner;
            Owned<IThorRowCollector> channelCollector;
            Owned<IRowWriter> channelCollectorWriter;
            IChannelDistributor **channelDistributors;
            unsigned nextSpillChannel;
            CriticalSection crit;
            atomic_t spilt;
        public:
            CChannelDistributor(CLookupJoinActivityBase &_owner, ICompare *cmp) : owner(_owner)
            {
                channelCollector.setown(createThorRowCollector(owner, queryRowInterfaces(owner.rightITDL), cmp, stableSort_none, rc_mixed, SPILL_PRIORITY_DISABLE));
                channelCollector->setTracingPrefix("Join right");
                channelCollectorWriter.setown(channelCollector->getWriter());
                channelDistributors = ((CLookupJoinActivityBase *)owner.channels[0])->channelDistributors;
                channelDistributors[owner.queryJobChannelNumber()] = this;
                nextSpillChannel = 0;
                atomic_set(&spilt, 0);
                //NB: all channels will have done this, before rows are added
            }
#define HPCC_17331 // Whilst under investigation. Should be solved by fix for HPCC-21091
            void process(IRowStream *right)
            {
#ifdef HPCC_17331
                unsigned skippedNonLocalRows = 0;
#endif
                for (;;)
                {
                    OwnedConstThorRow row = right->nextRow();
                    if (!row)
                        break;
                    unsigned hv = owner.rightHash->hash(row);
                    unsigned slave = hv % owner.numSlaves;
                    unsigned channelDst = owner.queryJob().queryJobSlaveChannelNum(slave+1);
#ifdef HPCC_17331
                    if (NotFound == channelDst)
                    {
                        if (0 == skippedNonLocalRows++)
                        {
                            VStringBuffer errMsg("1st unexpected non-local row detected, slave: %u", slave+1);
                            IOutputMetaData *inputOutputMeta = owner.rightITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
                            if (inputOutputMeta->hasXML())
                            {
                                CommonXmlWriter xmlWriter(0);
                                errMsg.append(" - row: ");
                                inputOutputMeta->toXML((byte *) row.get(), xmlWriter);
                                errMsg.append(xmlWriter.str());
                            }
                            if (owner.queryJob().getOptBool("smartjoin_failonnonlocal", true))
                            {
                                errMsg.newline().append("Please see JIRA issue HPCC-17331 and report incident");
                                throw MakeActivityException(&owner, 0, "%s", errMsg.str());
                            }
                            else
                                owner.ActPrintLog("%s", errMsg.str());
                        }
                        continue; // skip row
                    }
#else
                    dbgassertex(NotFound != channelDst); // if NotFound, slave is not a slave of this process
#endif
                    dbgassertex(channelDst < owner.queryJob().queryJobChannels());
                    channelDistributors[channelDst]->putRow(row.getClear());
                }
#ifdef HPCC_17331
                if (skippedNonLocalRows)
                    owner.ActPrintLog("WARNING: %u unexpected non-local row(s) detected", skippedNonLocalRows);
#endif
            }
            void processDistRight(IRowStream *right)
            {
                for (;;)
                {
                    OwnedConstThorRow row = right->nextRow();
                    if (!row)
                        break;
                    putRow(row.getClear());
                }
            }
        // roxiemem::IBufferedRowCallback impl.
            virtual bool freeBufferedRows(bool critical)
            {
                CriticalBlock b(crit); // JCSMORE no idea why this crit is here, looks like should be deleted.

                owner.ActPrintLog("CChannelDistributor free memory callback called");
                owner.setFailoverToStandard(); // mark early so statistics / eclwatch can see that activity has failed over to hash join asap.
                unsigned startSpillChannel = nextSpillChannel;
                for (;;)
                {
                    bool res = channelDistributors[nextSpillChannel]->spill(critical);
                    ++nextSpillChannel;
                    if (nextSpillChannel == owner.queryJob().queryJobChannels())
                        nextSpillChannel = 0;
                    if (res)
                        return true;
                    if (nextSpillChannel == startSpillChannel)
                        break;
                }
                return false;
            }
            virtual unsigned getSpillCost() const
            {
                return SPILL_PRIORITY_LOOKUPJOIN;
            }
            virtual unsigned getActivityId() const
            {
                return owner.queryActivityId();
            }
            virtual IThorRowCollector *getCollector() { return channelCollector.getLink(); }
        // IChannelDistributor impl.
            virtual void putRow(const void *row)
            {
                if (atomic_cas(&spilt, 0, 1))
                {
                    StringBuffer traceInfo;
                    if (channelCollector->shrink(&traceInfo)) // grab back some valuable table array space
                        owner.ActPrintLog("CChannelDistributor %s", traceInfo.str());
                }
                channelCollectorWriter->putRow(row);
            }
            virtual bool spill(bool critical) // called from OOM callback
            {
                if (!channelCollector->spill(critical))
                    return false;
                atomic_set(&spilt, 1);
                return true;
            }
            virtual roxiemem::IBufferedRowCallback *queryCallback() { return this; }
        } channelDistributor(*this, cmp);

        // Wait for channelDistributor on all channels to have initialized channelDistributors on channel 0, before beginning process()
        InterChannelBarrier();

        // NB: clearAllNonLocalRows (called on channel 0) will have already cleared all rows that are not local to this *node*

        /* Add IBufferedRowCallback to all channels, because memory pressure can come on any IRowManager
         * However, all invoked callbacks are handled by ch0 and round-robin freeing channel collectors
         */
        roxiemem::IBufferedRowCallback *callback = ((CLookupJoinActivityBase *)channels[0])->channelDistributors[0]->queryCallback();
        queryRowManager()->addRowBuffer(callback);
        Owned<IException> exception;
        Owned<IThorRowCollector> channelCollector;
        try
        {
            if (0 == queryJobChannelNumber())
            {
                // Pass all gathered RHS rows through channel distributor, in order to hash them into their own channel.
                Owned<IRowStream> gatheredRHSStream = getGatheredRHSStream();
                if (gatheredRHSStream)
                    channelDistributor.process(gatheredRHSStream);
            }
            InterChannelBarrier(); // wait for channel[0] to process in mem rows 1st

            if (getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)) // for testing only (force to disk, as if spilt)
                channelDistributor.spill(false);

            Owned<IRowStream> distChannelStream = rhsDistributor->connect(queryRowInterfaces(rightITDL), right, rightHash, nullptr, nullptr);
            channelDistributor.processDistRight(distChannelStream);
        }
        catch (IException *e)
        {
            EXCLOG(e, "During channel distribution");
            exception.setown(e);
        }

        /* Now that channel distribution done, remove its roxiemem memory callback
         * but allow collector return to continue to spill if there's memory pressure.
         */
        channelCollector.setown(channelDistributor.getCollector());
        channelCollector->setup(cmp, stableSort_none, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN);
        queryRowManager()->removeRowBuffer(callback);
        InterChannelBarrier(); // need barrier point to ensure all have removed callback before channelDistributor is destroyed
        if (exception)
            throw exception.getClear();
        return channelCollector.getClear();
    }
    void setupStandardJoin(IRowStream *right)
    {
        // NB: lhs ordering and grouping lost from here on.. (will have been caught earlier if global)
        if (grouped)
            throw MakeActivityException(this, 0, "Degraded to standard join, LHS order cannot be preserved");

        Owned<IThorRowLoader> rowLoader = createThorRowLoader(*this, queryRowInterfaces(leftITDL), helper->isLeftAlreadyLocallySorted() ? NULL : compareLeft);
        rowLoader->setTracingPrefix("Join left");
        left.setown(rowLoader->load(left, abortSoon, false));
        leftITDL = queryInput(0); // reset
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
        joinHelper->init(left, right, leftAllocator, rightAllocator, ::queryRowMetaData(leftITDL));
    }
    void getRHS(bool stopping)
    {
        if (gotRHS)
            return;
        gotRHS = true;

        /* Global Lookup:
         * ==============
         * See : handleGlobalRHS()
         * Distributes RHS (using broadcaster) all channels broadcast, but only channel 0 receives/collates.
         * Whilst broadcasting, rows are kept in separate row arrays for each slave.
         * The separate slave row arrays are combined into a single RHS table and a HT is built.
         * If an OOM occurs, it fails over to local lookup 'Failover Local Lookup' (see below)
         * If succeeds, shares HT between channels and each channel works independently, streaming LHS to match RHS HT table.
         *
         * Failover Local Lookup:
         * ======================
         * On OOM event, all non-local (that don't hash partition to local node) are purged (see clearNonLocalRows())
         * See : handleGlobalRHS()
         * Row arrays are compacted.
         * A spillable stream of all local row arrays is formed (see getGatheredRHSStream())
         * The collected local RHS rows are partitioned with channel distributor - splitting them into channels.
         * Remaining non-local nodes are read from global RHS distributor and partitioned by channel distributor also.
         * LHS is co-distributed, i.e. to channels.
         * If an OOM occurs during this partitioning, it fails over to 'Failover Standard Join' (see below)
         * If succeeds, falls through to Local Lookup Handling
         *
         * Local Lookup
         * ============
         * Gathers local dataset into RHS table.
         * If succeeds proceeds to 'Local Lookup Handling'
         *
         * Local Lookup Handling
         * =====================
         * Prepares HT based on local channel rows
         * IF OOM's fails over to 'Failover Standard Join' (see below)
         * If succeeds, adds channel rows to HT (in global case channels >0 share table and HT)
         *
         * Failover Standard Join:
         * =======================
         * The LHS side is loaded and spilt and sorted if necessary
         * A regular join helper is created to perform a local join against the two hash distributed sorted sides.
         */

        Owned<IThorRowCollector> rightCollector;
        try
        {
            CMarker marker(*this);
            Owned<IRowStream> rightStream;
            if (isGlobal())
            {
                /* All slaves on all channels now know whether any one spilt or not, i.e. whether to perform local hash join or not
                 * If any have spilt, still need to distribute rest of RHS..
                 */
                bool ok = handleGlobalRHS(marker, helper->isRightAlreadySorted(), stopping);
                if (stopping)
                {
                    ActPrintLog("Global getRHS stopped");
                    return;
                }
                if (ok)
                    ActPrintLog("RHS global rows fitted in memory in this channel, count: %" RIPF "d", rhs.ordinality());
                else
                {
                    ActPrintLog("Spilt whilst broadcasting, will attempt distributed local lookup join");

                    // NB: lhs ordering and grouping lost from here on..
                    if (grouped)
                        throw MakeActivityException(this, 0, "Degraded to Distributed Local Lookup, but input is marked as grouped and cannot preserve LHS order");

                    ICompare *cmp = rhsCollated ? NULL : compareRight; // if rhsCollated=true, then sorted, otherwise can't rely on any previous order.
                    rightCollector.setown(handleFailoverToLocalRHS(cmp));
                    if (rightCollector->hasSpilt())
                    {
                        ActPrintLog("Global SMART JOIN spilt to disk during Distributed Local Lookup handling. Failing over to Standard Join");
                        rightStream.setown(rightCollector->getStream());
                        setFailoverToStandard();
                    }

                    // start LHS distributor, needed by local lookup or full join
                    left.setown(lhsDistributor->connect(queryRowInterfaces(leftITDL), left, leftHash, nullptr, nullptr));

                    // NB: Some channels in this or other slave processes may have fallen over to hash join
                }
            }
            else
            {
                if (stopping)
                {
                    ActPrintLog("Local getRHS stopped");
                    return;
                }
                ICompare *cmp = helper->isRightAlreadyLocallySorted() ? NULL : compareRight;
                rightCollector.setown(handleLocalRHS(right, cmp));
                if (rightCollector->hasSpilt())
                {
                    rightStream.setown(rightCollector->getStream());
                    ActPrintLog("Local SMART JOIN spilt to disk. Failing over to regular local join");
                    setFailoverToStandard();
                }
                else
                    ActPrintLog("RHS local rows fitted in memory in this channel, count: %" RIPF "d", rhs.ordinality());
            }
            if (!rightStream)
            {
                // All RHS rows fitted in memory, rows were transferred out back into 'rhs' and sorted

                if (isLocal() || hasFailedOverToLocal())
                {
                    if (hasFailedOverToLocal())
                        marker.reset();
                    if (!prepareLocalHT(marker, *rightCollector)) // can cause others to spill, but must not be allowed to spill channel rows I'm working on.
                    {
                        setFailoverToStandard();
                        ActPrintLog("Out of memory trying to prepare [LOCAL] hashtable for a SMART join (%" RIPF "d rows), will now failover to a std hash join", rhs.ordinality());
                    }
                    rightStream.setown(rightCollector->getStream(false, &rhs));
                }
            }
            if (rightStream)
            {
                ActPrintLog("Performing STANDARD JOIN");
                setFailoverToStandard();
                setupStandardJoin(rightStream); // NB: rightStream is sorted
            }
            else
            {
                // NB: No spilling here on in
                if (isLocal() || hasFailedOverToLocal())
                {
                    ActPrintLog("Performing LOCAL LOOKUP JOIN: rhs size=%u, lookup table size = %" RIPF "u", rhs.ordinality(), rhsTableLen);
                    table->addRows(rhs, marker);
                    tableProxy.set(table);
                }
                else
                {
                    if (0 == queryJobChannelNumber()) // only ch0 has table, ch>0 will share ch0's table.
                    {
                        ActPrintLog("Performing GLOBAL LOOKUP JOIN: rhs size=%u, lookup table size = %" RIPF "u", rhs.ordinality(), rhsTableLen);
                        table->addRows(rhs, marker);
                        tableProxy.set(table);
                        InterChannelBarrier();
                    }
                    else
                    {
                        InterChannelBarrier();
                        tableProxy.set(channels[0]->queryTable());
                        rhsTableLen = tableProxy->queryTableSize();
                    }
                }
            }
        }
        catch (IException *e)
        {
            if (!isOOMException(e))
                throw e;
            IOutputMetaData *inputOutputMeta = rightITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
            // rows may either be in separate slave row arrays or in single rhs array, or split.
            rowcount_t total = rightCollector ? rightCollector->numRows() : (getGlobalRHSTotal() + rhs.ordinality());
            throw checkAndCreateOOMContextException(this, e, "gathering RHS rows for lookup join", total, inputOutputMeta, NULL);
        }
    }
public:
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
    CLookupJoinActivityBase(CGraphElementBase *_container) : PARENT(_container, lookupJoinActivityStatistics)
    {
        rhsCollated = rhsCompacted = false;
        broadcast2MpTag = broadcast3MpTag = lhsDistributeTag = rhsDistributeTag = TAG_NULL;
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
        smart = isSmartJoin(*this);
        overflowWriteCount = 0;
        spillCompInfo = 0x0;
        if (getOptBool(THOROPT_COMPRESS_SPILLS, true))
        {
            StringBuffer compType;
            getOpt(THOROPT_COMPRESS_SPILL_TYPE, compType);
            setCompFlag(compType, spillCompInfo);
        }
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
                    size32_t transformedSize = helper->onFailTransform(ret, leftRow, defaultRight, e.get(), JTFmatchedleft);
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
    virtual bool isRhsConstant() const { return PARENT::isRhsConstant() && !hasFailedOverToStandard(); }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        PARENT::init(data, slaveData);

        if (isGlobal())
        {
            if (0 == queryJobChannelNumber())
                channelDistributors.allocateN(queryJob().queryJobChannels(), true);
            broadcast2MpTag = queryJobChannel().deserializeMPTag(data);
            broadcast3MpTag = queryJobChannel().deserializeMPTag(data);
            lhsDistributeTag = queryJobChannel().deserializeMPTag(data);
            rhsDistributeTag = queryJobChannel().deserializeMPTag(data);
            rhsDistributor.setown(createHashDistributor(this, queryJobChannel().queryJobComm(), rhsDistributeTag, false, false, NULL, "RHS"));
            lhsDistributor.setown(createHashDistributor(this, queryJobChannel().queryJobComm(), lhsDistributeTag, false, false, NULL, "LHS"));
        }
        if (isSmart())
        {
            if (isGlobal())
            {
                for (unsigned s=0; s<container.queryJob().querySlaves(); s++)
                    rhsSlaveRows.item(s)->setup(sharedRightRowInterfaces, ers_forbidden, stableSort_none, false);
            }
        }
    }
    virtual void start() override
    {
        PARENT::start();
        dbgassertex(isSmart() || (leftITDL->isGrouped() == grouped)); // std. lookup join expects these to match
    }
    virtual void reset() override
    {
        PARENT::reset();
        if (isSmart())
        {
            if (!isRhsConstant())
            {
                if (isGlobal())
                {
                    failedOverToLocal = false;
                    rhsCollated = rhsCompacted = false;
                }
            }
            failedOverToStandard = false;
            nextRhsToSpill = 0;
        }
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!gotRHS)
        {
            getRHS(false);
            StringBuffer msg;
            if (isSmart())
            {
                msg.append("SmartJoin - ");
                if (hasFailedOverToStandard())
                    msg.append("Failed over to standard join");
                else if (isGlobal() && hasFailedOverToLocal())
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
    virtual void abort() override
    {
        PARENT::abort();
        if (rhsDistributor)
            rhsDistributor->abort();
        if (lhsDistributor)
            lhsDistributor->abort();
        if (joinHelper)
            joinHelper->stop();
    }
    virtual void stop() override
    {
        if (hasStarted())
        {
            if (isGlobal())
            {
                if (gotRHS)
                {
                    // Other channels sharing HT. So do not reset until all here
                    // NB: See handleGlobalRHS, if any channel failed over to local, all will have been marked failed over to local.
                    if (!hasFailedOverToLocal() && queryJob().queryJobChannels()>1)
                        InterChannelBarrier();
                }
                else
                    getRHS(true); // If global, need to handle RHS until all are slaves stop
            }

            if (rhsDistributor)
            {
                // NB: if rhsDistributor is in use, its stream is already consumed by processDistRight()
                rhsDistributor->disconnect(true);
                rhsDistributor->join();
            }
            if (lhsDistributor)
            {
                // If LHS distributor is in use, must signal stop to ensure distributor doesn't stall in receiver
                if (left)
                    left->stop();
                lhsDistributor->disconnect(true);
                lhsDistributor->join();
            }
            joinHelper.clear();
        }
        PARENT::stop();
    }
    virtual bool isGrouped() const override
    {
        return isSmart() ? false : queryInput(0)->isGrouped();
    }
    virtual void bCastReceive(CSendItem *sendItem, bool stop) // NB: only called on channel 0
    {
        if (sendItem)
        {
            if (0 != (sendItem->queryFlags() & bcastflag_spilt))
            {
                VStringBuffer msg("Notification that node %d spilt", sendItem->queryNode()+1);
                clearAllNonLocalRows(msg.str());
            }
        }
        PARENT::bCastReceive(sendItem, stop);
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
        return clearAllNonLocalRows("Out of memory callback", true);
    }
    rowidx_t keepLocal(CThorExpandingRowArray &rows, CThorExpandingRowArray &localRows)
    {
        ForEachItemIn(r, rows)
        {
            unsigned hv = rightHash->hash(rows.query(r));
            if (myNodeNum == (hv % numNodes))
                localRows.append(rows.getClear(r));
        }
        rows.clearRows();
        return localRows.ordinality();
    }
    virtual bool addRHSRows(CThorSpillableRowArray &rhsRows, CThorExpandingRowArray &inRows, CThorExpandingRowArray &rhsInRowsTemp) override
    {
        dbgassertex(0 == rhsInRowsTemp.ordinality());
        if (hasFailedOverToLocal())
        {
            if (0 == keepLocal(inRows, rhsInRowsTemp))
                return true;
        }
        CriticalBlock b(rhsRowLock);
        if (overflowWriteFile)
        {
            /* Tried to do outside crit above, but if empty, and now overflow, need to inside
             * Will be one off if at all
             */
            if (0 == rhsInRowsTemp.ordinality())
            {
                if (0 == keepLocal(inRows, rhsInRowsTemp))
                    return true;
            }
            overflowWriteCount += rhsInRowsTemp.ordinality();
            ForEachItemIn(r, rhsInRowsTemp)
                overflowWriteStream->putRow(rhsInRowsTemp.getClear(r));
            return true;
        }
        if (hasFailedOverToLocal())
        {
            /* Tried to do outside crit above, but hasFailedOverToLocal() could be true, since gaining lock
             * Will be one off if at all
             */
            if (0 == rhsInRowsTemp.ordinality())
            {
                if (0 == keepLocal(inRows, rhsInRowsTemp))
                    return true;
            }
            if (rhsRows.appendRows(rhsInRowsTemp, true))
                return true;
        }
        else
        {
            if (rhsRows.appendRows(inRows, true))
                return true;
            dbgassertex(hasFailedOverToLocal());

            if (0 == keepLocal(inRows, rhsInRowsTemp))
                return true;

            // keep it only if it hashes to my node
            if (rhsRows.appendRows(rhsInRowsTemp, true))
                return true;
        }
        /* Could OOM whilst still failing over to local lookup again, dealing with last row, or trailing
         * few rows being received. Unlikely since all local rows will have been cleared, but possible,
         * particularly if last rows end up causing row ptr table expansion here.
         *
         * Need to stash away somewhere to allow it to continue.
         */
        unsigned rwFlags = DEFAULT_RWFLAGS;
        if (spillCompInfo)
        {
            rwFlags |= rw_compress;
            rwFlags |= spillCompInfo;
        }
        StringBuffer tempFilename;
        GetTempName(tempFilename, "lookup_local", true);
        ActPrintLog("Overflowing RHS broadcast rows to spill file: %s", tempFilename.str());
        OwnedIFile iFile = createIFile(tempFilename.str());
        overflowWriteFile.setown(new CFileOwner(iFile.getLink()));
        overflowWriteStream.setown(createRowWriter(iFile, queryRowInterfaces(rightITDL), rwFlags));

        overflowWriteCount += rhsInRowsTemp.ordinality();
        ForEachItemIn(r, rhsInRowsTemp)
            overflowWriteStream->putRow(rhsInRowsTemp.getClear(r));
        return true;
    }
    virtual void serializeStats(MemoryBuffer &mb) override
    {
        if (isSmart())
        {
            if (isGlobal())
                stats.setStatistic(StNumSmartJoinDegradedToLocal, aggregateFailoversToLocal); // NB: is going to be same for all slaves.
            stats.setStatistic(StNumSmartJoinSlavesDegradedToStd, aggregateFailoversToStandard);
        }
        PARENT::serializeStats(mb);
    }
};

class CTableCommon : public CInterfaceOf<IInterface>
{
protected:
    rowidx_t tableSize;
public:
    rowidx_t queryTableSize() const { return tableSize; }
    void reset()
    {
        tableSize = 0;
    }
};

class CHTBase : public CTableCommon
{
protected:
    OwnedConstThorRow htMemory;
    IHash *leftHash, *rightHash;
    ICompare *compareLeftRight;

public:
    CHTBase()
    {
        reset();
    }
    void setup(CSlaveActivity *activity, roxiemem::IRowManager *rowManager, rowidx_t size, IHash *_leftHash, IHash *_rightHash, ICompare *_compareLeftRight)
    {
        unsigned __int64 _sz = sizeof(const void *) * ((unsigned __int64)size);
        memsize_t sz = (memsize_t)_sz;
        if (sz != _sz) // treat as OOM exception for handling purposes.
            throw MakeStringException(ROXIEMM_MEMORY_LIMIT_EXCEEDED, "Unsigned overflow, trying to allocate hash table of size: %" I64F "d ", _sz);
        void *ht = rowManager->allocate(sz, activity->queryContainer().queryId(), SPILL_PRIORITY_LOW);
        memset(ht, 0, sz);
        htMemory.setown(ht);
        tableSize = size;
        leftHash = _leftHash;
        rightHash = _rightHash;
        compareLeftRight = _compareLeftRight;
    }
    void reset()
    {
        CTableCommon::reset();
        htMemory.clear();
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
        unsigned h = leftHash->hash(left)%tableSize;
        for (;;)
        {
            const void *right = ht[h];
            if (!right)
                break;
            if (0 == compareLeftRight->docompare(left, right))
                return right;
            h++;
            if (h>=tableSize)
                h = 0;
        }
        return NULL;
    }
    void releaseHTRows()
    {
        roxiemem::ReleaseRoxieRowArray(tableSize, ht);
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
    void setup(CLookupJoinActivityBase<CLookupHT> *_activity, roxiemem::IRowManager *rowManager, rowidx_t size, IHash *leftHash, IHash *rightHash, ICompare *compareLeftRight)
    {
        activity = _activity;
        CHTBase::setup(activity, rowManager, size, leftHash, rightHash, compareLeftRight);
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
        for (;;)
        {
            const void *&htRow = ht[hash];
            if (!htRow)
            {
                LinkThorRow(row);
                htRow = row;
                break;
            }
            hash++;
            if (hash>=tableSize)
                hash = 0;
        }
    }
    inline const void *getNextRHS(HtEntry &currentHashEntry __attribute__((unused)))
    {
        return NULL; // no next in LOOKUP without MANY
    }
    inline const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry __attribute__((unused)))
    {
        failRow = NULL;
        return findFirst(leftRow);
    }
    virtual void addRows(CThorExpandingRowArray &_rows, CMarker &marker)
    {
        const void **rows = _rows.getRowArray();
        rowidx_t pos=0;
        for (;;)
        {
            rowidx_t nextPos = marker.findNextBoundary(pos);
            if (0 == nextPos)
                break;
            const void *row = rows[pos];
            unsigned h = rightHash->hash(row)%tableSize;
            addEntry(row, h);
            pos = nextPos;
        }
        // Rows now in hash table, rhs arrays no longer needed
        _rows.kill();
        marker.reset();
    }
};

class CLookupManyHT : public CHTBase
{
    CLookupJoinActivityBase<CLookupManyHT> *activity;
    HtEntry *ht;
    const void **rows;

    inline HtEntry *lookup(unsigned hash)
    {
        HtEntry *e = ht+hash;
        if (0 == e->count)
            return NULL;
        return e;
    }
    const void *findFirst(const void *left, HtEntry &currentHashEntry)
    {
        unsigned h = leftHash->hash(left)%tableSize;
        for (;;)
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
            if (h>=tableSize)
                h = 0;
        }
        return NULL;
    }
public:
    CLookupManyHT()
    {
        reset();
    }
    void setup(CLookupJoinActivityBase<CLookupManyHT> *_activity, roxiemem::IRowManager *rowManager, rowidx_t size, IHash *leftHash, IHash *rightHash, ICompare *compareLeftRight)
    {
        activity = _activity;
        CHTBase::setup(activity, rowManager, size, leftHash, rightHash, compareLeftRight);
        ht = (HtEntry *)htMemory.get();
    }
    inline void addEntry(const void *row, unsigned hash, rowidx_t index, rowidx_t count)
    {
        for (;;)
        {
            HtEntry &e = ht[hash];
            if (!e.count)
            {
                e.index = index;
                e.count = count;
                break;
            }
            hash++;
            if (hash>=tableSize)
                hash = 0;
        }
    }
    void reset()
    {
        CHTBase::reset();
        ht = NULL;
        rows = NULL;
    }
    inline const void *getNextRHS(HtEntry &currentHashEntry)
    {
        if (1 == currentHashEntry.count)
            return NULL;
        --currentHashEntry.count;
        return rows[++currentHashEntry.index];
    }
    inline const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentHashEntry)
    {
        const void *right = findFirst(leftRow, currentHashEntry);
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
        for (;;)
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
            unsigned h = rightHash->hash(row)%tableSize;
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

class CAllTable : public CTableCommon
{
    const void **rows;
public:
    CAllTable()
    {
        reset();
    }
    void reset()
    {
        CTableCommon::reset();
        rows = NULL;
    }
    inline const void *getNextRHS(HtEntry &currentEntry)
    {
        if (++currentEntry.index<tableSize)
            return rows[currentEntry.index];
        return NULL;
    }
    inline const void *getFirstRHSMatch(const void *leftRow, const void *&failRow, HtEntry &currentEntry)
    {
        currentEntry.index = 0;
        failRow = NULL;
        return rows[0]; // guaranteed to be at least one row
    }
    void addRows(CThorExpandingRowArray &_rows)
    {
        tableSize = _rows.ordinality();
        rows = _rows.getRowArray();
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

        try
        {
            // ALL join must fit into memory
            if (isGlobal())
            {
                doBroadcastRHS(stopping);
                if (stopping) // broadcast done and no-one spilt, this node can now stop
                    return;

                if (0 == queryJobChannelNumber()) // All channels broadcast, but only channel 0 receives
                {
                    rhsTableLen = getGlobalRHSTotal();
                    rhs.resize(rhsTableLen);
                    ForEachItemIn(a, rhsSlaveRows)
                    {
                        CThorSpillableRowArray &rows = *rhsSlaveRows.item(a);
                        rhs.appendRows(rows, true);
                        rows.kill(); // free up ptr table asap
                    }
                    table->addRows(rhs);
                    tableProxy.set(table);
                    InterChannelBarrier();
                }
                else
                {
                    InterChannelBarrier();
                    tableProxy.set(channels[0]->queryTable());
                    rhsTableLen = tableProxy->queryTableSize();
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
                table->addRows(rhs);
                tableProxy.set(table);
            }
            ActPrintLog("rhs table: %d elements", rhsTableLen);
        }
        catch (IException *e)
        {
            if (!isOOMException(e))
                throw e;
            IOutputMetaData *inputOutputMeta = rightITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
            // rows may either be in separate slave row arrays or in single rhs array, or split.
            rowidx_t total = getGlobalRHSTotal() + rhs.ordinality();
            throw checkAndCreateOOMContextException(this, e, "gathering RHS rows for lookup join", total, inputOutputMeta, NULL);
        }
    }
public:
    CAllJoinSlaveActivity(CGraphElementBase *_container) : PARENT(_container)
    {
        returnMany = true;
    }
// IThorSlaveActivity overloaded methods
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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
        if (hasStarted())
        {
            if (isGlobal())
            {
                if (gotRHS)
                {
                    // Other channels sharing HT. So do not reset until all here
                    if (queryJob().queryJobChannels()>1)
                        InterChannelBarrier();
                }
                else
                    getRHS(true); // If global, need to handle RHS until all are slaves stop
            }
        }
        PARENT::stop();
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
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


