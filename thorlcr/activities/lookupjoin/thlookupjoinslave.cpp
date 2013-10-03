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

#include "thactivityutil.ipp"
#include "javahash.hpp"
#include "javahash.tpp"
#include "thorport.hpp"
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
enum broadcast_flags { bcastflag_spilt=0x100 };
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
        mptag_t rt = createReplyTag();
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
                        recvInterface->bCastReceive(NULL); // signal last
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
    CBroadcaster(CActivityBase &_activity) : activity(_activity), receiver(*this), sender(*this), comm(_activity.queryJob().queryJobComm())
    {
        allDone = allDoneWaiting = allRequestStop = stopping = stopRecv = false;
        myNode = activity.queryJob().queryMyRank();
        slaves = activity.queryJob().querySlaves();
        slavesDone.setown(createBitSet());
        slavesStopping.setown(createBitSet());
        mpTag = TAG_NULL;
        recvInterface = NULL;
    }
    void start(IBCastReceive *_recvInterface, mptag_t _mpTag, bool _stopping)
    {
        stopping = _stopping;
        if (stopping)
            slavesStopping->set(myNode-1, true);
        recvInterface = _recvInterface;
        stopRecv = false;
        mpTag = _mpTag;
        receiver.start();
        sender.start();
    }
    void reset()
    {
        allDone = allDoneWaiting = allRequestStop = stopping = false;
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
};


/* 
    This activity loads the RIGHT hand stream into the hash table, therefore
    the right hand stream -should- contain the fewer records

    Inner, left outer and left only joins supported

*/


/*
 * The main activity class
 * It's intended to be used when the RHS globally, is small enough to fit within the memory of a single node.
 * It performs the join, by 1st ensuring all RHS data is on all nodes, creating a hash table of this gathered set
 * then it streams the LHS through, matching against the RHS hash table entries.
 * It also handles match conditions where there is no hard match (, ALL), in those cases no hash table is needed.
 * TODO: right outer/only joins
 */
class CLookupJoinActivity : public CSlaveActivity, public CThorDataLink, implements ISmartBufferNotify, implements IBCastReceive, implements roxiemem::IBufferedRowCallback
{
    IHThorHashJoinArg *hashJoinHelper;
    IHThorAllJoinArg *allJoinHelper;
    const void **rhsTable;
    rowidx_t rhsTableLen, htCount, htDedupCount;
    rowidx_t rhsRows;
    IHash *leftHash, *rightHash;
    ICompare *compareRight, *compareLeftRight;

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
    OwnedConstThorRow leftRow;
    Owned<IException> leftexception;
    Semaphore leftstartsem;
    CThorExpandingRowArray rhs, ht;
    bool eos, needGlobal;
    unsigned flags;
    bool exclude;
    unsigned candidateMatches, abortLimit, atMost;
    ConstPointerArray candidates;
    unsigned candidateIndex;
    const void *rhsNext;
    Owned<IOutputMetaData> outputMeta;
    rowcount_t rhsTotalCount;
    mptag_t lhsDistributeTag, rhsDistributeTag, broadcast2MpTag;

    PointerArrayOf<CThorSpillableRowArray> rhsNodeRows;
    CBroadcaster broadcaster;

    // AllJoin only
    rowidx_t nextRhsRow;
    unsigned keepLimit;
    unsigned joined;
    OwnedConstThorRow defaultLeft;
    Owned<IBitSet> rightMatchSet;

    unsigned lastRightOuter;
    bool doRightOuter;
    bool eog, someSinceEog, leftMatch, grouped;
    Semaphore gotOtherROs;
    bool waitForOtherRO, fuzzyMatch, returnMany, dedup;

    // Handling failover to a) hashed local lookupjoin b) hash distributed standard join
    atomic_t spiltBroadcastingRHS;
    CriticalSection broadcastSpillingLock;
    bool localLookupJoin;
    bool failoverToLocalLookupJoin, failoverToStdJoin;
    rank_t myNode;
    unsigned numNodes;
    Owned<IHashDistributor> lhsDistributor, rhsDistributor;
    ICompare *compareLeft;
    Owned<IJoinHelper> joinHelper;
    UnsignedArray flushedRowMarkers;

    inline bool isLookup() const
    {
        switch (container.getKind())
        {
            case TAKlookupjoin:
            case TAKlookupdenormalize:
            case TAKlookupdenormalizegroup:
            case TAKsmartjoin:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                return true;
        }
        return false;
    }
    inline bool isAll() const
    {
        switch (container.getKind())
        {
            case TAKalljoin:
            case TAKalldenormalize:
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
    inline bool isSmart() const
    {
        switch (container.getKind())
        {
            case TAKsmartjoin:
            case TAKsmartdenormalize:
            case TAKsmartdenormalizegroup:
                return true;
        }
        return false;
    }
    inline bool hasBroadcastSpilt() const { return 0 != atomic_read(&spiltBroadcastingRHS); }
    inline void setBroadcastingSpilt(bool tf) { atomic_set(&spiltBroadcastingRHS, (int)tf); }

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
    rowidx_t getHTSize(rowcount_t rows)
    {
        if (rows < 10) return 16;
        rowcount_t res = rows/3*4; // make HT 1/3 bigger than # rows
        if ((res < rows) || (res > RIMAX)) // check for overflow, or result bigger than rowidx_t size
            throw MakeActivityException(this, 0, "Too many rows on RHS for hash table: %"RCPF"d", rows);
        return (rowidx_t)res;
    }
    /* Utility class, that is called from the broadcaster to queue up received blocks
     * It will block if it has > MAX_QUEUE_BLOCKS to process (on the queue)
     * Processing will decompress the incoming blocks and add them to a row array per slave
     */
    class CRowProcessor : implements IThreaded
    {
        CThreadedPersistent threaded;
        CLookupJoinActivity &owner;
        bool stopped;
        SimpleInterThreadQueueOf<CSendItem, true> blockQueue;
        Owned<IException> exception;
    public:
        CRowProcessor(CLookupJoinActivity &_owner) : threaded("CRowProcessor", this), owner(_owner)
        {
            stopped = false;
            blockQueue.setLimit(MAX_QUEUE_BLOCKS);
        }
        ~CRowProcessor()
        {
            blockQueue.stop();
            loop
            {
                Owned<CSendItem> sendItem = blockQueue.dequeueNow();
                if (NULL == sendItem)
                    break;
            }
            wait();
        }
        void start()
        {
            stopped = false;
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
        void wait() { threaded.join(); }
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

    void clearRHS()
    {
        ht.kill();
        rhs.kill();
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                rows->kill();
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLookupJoinActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this), broadcaster(*this), rhs(*this, NULL), ht(*this, NULL, true),
          rowProcessor(*this)
    {
        gotRHS = false;
        nextRhsRow = 0;
        rhsNext = NULL;
        atMost = 0;
        setBroadcastingSpilt(false);
        localLookupJoin = false;
        myNode = queryJob().queryMyRank();
        numNodes = queryJob().querySlaves();
        needGlobal = !container.queryLocal() && (container.queryJob().querySlaves() > 1);
        broadcast2MpTag = lhsDistributeTag = rhsDistributeTag = TAG_NULL;

        rhsTable = NULL;
        rhsTableLen = htCount = htDedupCount = 0;
        rhsRows = RIUNSET;
        rhsTotalCount = RCUNSET;
        leftITDL = rightITDL = NULL;
        candidateIndex = 0;
        candidateMatches = 0;

        eos = false;
        eog = someSinceEog = false;
        joined = 0;
        doRightOuter = false;
        leftMatch = false;
        grouped = false;
        lastRightOuter = 0;
        fuzzyMatch = returnMany = dedup = false;
        waitForOtherRO = true;

        hashJoinHelper = NULL;
        allJoinHelper = NULL;
        exclude = false;
        abortLimit = keepLimit = 0;
        allJoinHelper = NULL;
        hashJoinHelper = NULL;
        leftHash = rightHash = NULL;
        hashJoinHelper = NULL;
        compareLeft = compareRight = compareLeftRight = NULL;

        if (isAll())
        {
            allJoinHelper = (IHThorAllJoinArg *)queryHelper();
            flags = allJoinHelper->getJoinFlags();
            returnMany = true;
            keepLimit = allJoinHelper->getKeepLimit();
            fuzzyMatch = 0 != (JFmatchrequired & flags);
        }
        else
        {
            dbgassertex(isLookup());

            hashJoinHelper = (IHThorHashJoinArg *)queryHelper();
            flags = hashJoinHelper->getJoinFlags();
            leftHash = hashJoinHelper->queryHashLeft();
            rightHash = hashJoinHelper->queryHashRight();
            compareRight = hashJoinHelper->queryCompareRight();
            compareLeft = hashJoinHelper->queryCompareLeft();
            compareLeftRight = hashJoinHelper->queryCompareLeftRight();
            if (JFmanylookup & flags)
                returnMany = true;
            keepLimit = hashJoinHelper->getKeepLimit();
            abortLimit = hashJoinHelper->getMatchAbortLimit();
            atMost = hashJoinHelper->getJoinLimit();

            fuzzyMatch = 0 != (JFmatchrequired & flags);
            bool maySkip = 0 != (flags & JFtransformMaySkip);
            dedup = compareRight && !maySkip && !fuzzyMatch && !returnMany;

            // code gen should spot invalid constants on KEEP with LOOKUP (without MANY)
        }
        exclude = 0 != (flags & JFexclude);
        if(0 == keepLimit)
            keepLimit = (unsigned)-1;
        if (0 == abortLimit)
            abortLimit = (unsigned)-1;
        if (0 == atMost)
            atMost = (unsigned)-1;
        if (abortLimit < atMost)
            atMost = abortLimit;

        if (flags & JFleftouter)
            joinType = exclude ? JT_LeftOnly : JT_LeftOuter;
        else if (flags & JFrightouter)
        {
            UNIMPLEMENTED;
            rightMatchSet.setown(createBitSet());
            joinType = exclude ? JT_RightOnly : JT_RightOuter;
        }
        else
            joinType = JT_Inner;
        StringBuffer str;

        failoverToLocalLookupJoin = failoverToStdJoin = isSmart();
        ActPrintLog("Join type is %s, failoverToLocalLookupJoin=%s, failoverToStdJoin=%s",
                getJoinTypeStr(str).str(), failoverToLocalLookupJoin?"true":"false", failoverToStdJoin?"true":"false");
    }
    ~CLookupJoinActivity()
    {
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                delete rows;
        }
    }
    rowidx_t clearNonLocalRows(CThorSpillableRowArray &rows, rowidx_t startPos)
    {
        rowidx_t clearedRows = 0;
        rowidx_t numRows = rows.numCommitted();
        for (unsigned r=startPos; r<numRows; r++)
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
        if (hasBroadcastSpilt())
            return false;

        rowidx_t clearedRows = 0;
        /* NB: It is likely that there will be unflushed rows in the rhsNodeRows arrays after we are done here.
        /* These will need flushing when all is done and clearNonLocalRows will need recalling to process rest
         */

        setBroadcastingSpilt(true);
        ActPrintLog("Clearing non-local rows - cause: %s", msg);
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorSpillableRowArray &rows = *rhsNodeRows.item(a);

            /* Record point to which clearNonLocalRows will reach
             * so that can resume from that point, when done adding/flushing
             */
            flushedRowMarkers.append(rows.numCommitted());
            clearedRows += clearNonLocalRows(rows, 0);
        }
        ActPrintLog("handleLowMem: clearedRows = %"RIPF"d", clearedRows);
        return 0 != clearedRows;
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);

        if (!container.queryLocal())
        {
            mpTag = container.queryJob().deserializeMPTag(data);
            broadcast2MpTag = container.queryJob().deserializeMPTag(data);
            lhsDistributeTag = container.queryJob().deserializeMPTag(data);
            rhsDistributeTag = container.queryJob().deserializeMPTag(data);
        }

        unsigned slaves = container.queryJob().querySlaves();
        rhsNodeRows.ensure(slaves);
        while (slaves--)
            rhsNodeRows.append(new CThorSpillableRowArray(*this, NULL));
    }
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
        ActPrintLog("LHS input finished, %"RCPF"d rows read", count);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        assertex(inputs.ordinality() == 2);
        gotRHS = false;
        nextRhsRow = 0;
        joined = 0;
        leftMatch = false;
        rhsNext = NULL;
        candidateMatches = 0;
        rhsTotalCount = RCUNSET;
        htCount = htDedupCount = 0;
        rhsTableLen = 0;
        rhsRows = RIUNSET;
        eos = eog = someSinceEog = false;
        leftITDL = inputs.item(0);
        rightITDL = inputs.item(1);
        allocator.set(queryRowAllocator());
        leftAllocator.set(::queryRowAllocator(leftITDL));
        outputMeta.set(leftITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta());

        right.set(rightITDL);
        rightAllocator.set(::queryRowAllocator(rightITDL));
        rightSerializer.set(::queryRowSerializer(rightITDL));
        rightDeserializer.set(::queryRowDeserializer(rightITDL));

        setBroadcastingSpilt(false);
        localLookupJoin = false;
        flushedRowMarkers.kill();

        if (failoverToLocalLookupJoin && hashJoinHelper) // only for LOOKUP not ALL
        {
            if (needGlobal)
                queryJob().queryRowManager()->addRowBuffer(this);
        }

        RtlDynamicRowBuilder rr(rightAllocator);
        if ((flags & JFonfail) || (flags & JFleftouter))
            rr.ensureRow();
        RtlDynamicRowBuilder rl(leftAllocator);
        if (flags & JFrightouter)
            rl.ensureRow();
        size32_t rrsz=0;
        size32_t rlsz=0;

        if (isAll())
        {
            if (rr.exists())
                rrsz = allJoinHelper->createDefaultRight(rr);
            if (rl.exists())
                rlsz = allJoinHelper->createDefaultLeft(rl);
            grouped = allJoinHelper->queryOutputMeta()->isGrouped();
        }
        else
        {
            dbgassertex(isLookup());
            if (rr.exists())
                rrsz = hashJoinHelper->createDefaultRight(rr);
            if (rl.exists())
                rlsz = hashJoinHelper->createDefaultLeft(rl);
            grouped = hashJoinHelper->queryOutputMeta()->isGrouped();
        }

        leftITDL = createDataLinkSmartBuffer(this,leftITDL,LOOKUPJOINL_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(leftITDL->queryFromActivity()),grouped,RCUNBOUND,this,false,&container.queryJob().queryIDiskUsage());
        left.setown(leftITDL);
        startInput(leftITDL);

        try
        {
            if (isLookup() && !isSmart())
            {
                bool inputGrouped = leftITDL->isGrouped();
                dbgassertex(inputGrouped == grouped); // std. lookup join expects these to match
            }

            if (rrsz)
                defaultRight.setown(rr.finalizeRowClear(rrsz));
            if (rlsz)
                defaultLeft.setown(rl.finalizeRowClear(rlsz));

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
        gotOtherROs.signal();
        cancelReceiveMsg(RANK_ALL, mpTag);
        broadcaster.cancel();
        rowProcessor.abort();
        if (rhsDistributor)
            rhsDistributor->abort();
        if (lhsDistributor)
            lhsDistributor->abort();
    }
    virtual void stop()
    {
        if (!gotRHS && needGlobal)
            getRHS(true); // If global, need to handle RHS until all are slaves stop

        // JCS->GH - if in a child query, it would be good to preserve RHS.. would need tip/flag from codegen that constant
        clearRHS();

        if (right)
        {
            stopInput(right, "(R)");
            right.clear();
        }
        broadcaster.reset();
        stopInput(left, "(L)");
        left.clear();
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
        dataLinkStop();
    }
    inline bool match(const void *lhs, const void *rhsrow)
    {
        if (isAll())
            return allJoinHelper->match(lhs, rhsrow);
        else
        {
            dbgassertex(isLookup());
            return hashJoinHelper->match(lhs, rhsrow);
        }
    }
    inline const void *joinTransform(const void *lhs, const void *rhsrow)
    {
        RtlDynamicRowBuilder row(allocator);
        size32_t thisSize;
        switch (container.getKind())
        {
            case TAKalljoin:
            case TAKalldenormalize:
                thisSize = allJoinHelper->transform(row, lhs, rhsrow);
                break;
            case TAKlookupjoin:
            case TAKlookupdenormalize:
            case TAKsmartjoin:
            case TAKsmartdenormalize:
                thisSize = hashJoinHelper->transform(row, lhs, rhsrow);
                break;
            default:
                throwUnexpected();
        }
        if (thisSize)
            return row.finalizeRowClear(thisSize);
        return NULL;
    }
    // find routines for lookup only (compareRight!=NULL), ALL will not call
    const void *find(const void *r, unsigned &h) 
    {
        loop
        {
            const void *e = rhsTable[h];
            if (!e) 
                break;
            if (0 == compareLeftRight->docompare(r,e))
                return e;
            h++;
            if (h>=rhsTableLen)
                h = 0;
        }
        return NULL;
    }
    const void *findFirst(const void *r, unsigned &h) 
    {
        h = leftHash->hash(r)%rhsTableLen;
        return find(r, h);
    }
    const void *findNext(const void *r, unsigned &h) 
    {
        h++;
        if (h>=rhsTableLen)
            h = 0;
        return find(r, h);
    }
    void prepareRightOnly()
    {
        assertex(!doRightOuter);

        // will have to merge rhsMatchSets in a tree as there can be a large number of outer records per node.
        // topn does something similar, reuse.
        // when all merged, have complete merged set on node 0 only.
        // either output all from node 0, or partition set and distribute partition info to each node for output.

        // If right only, no need to broadcast rhs record set, can look for matches in local input and merge match etc. set as above.

        doRightOuter = true;
        gotOtherROs.signal();
    }
    const void *handleRightOnly()
    {
        if (waitForOtherRO)
        {
            gotOtherROs.wait();         
            waitForOtherRO = false;
        }       
        if (eog)
        {
            loop
            {
                if (nextRhsRow >= rhsRows)
                    break;
                bool setNext = (nextRhsRow < lastRightOuter);
                if (setNext)
                {
                    nextRhsRow = rightMatchSet->scan(nextRhsRow, false);
                    if (nextRhsRow >= rhsRows) break;
                }
                OwnedConstThorRow row = joinTransform(defaultLeft, rhsTable[nextRhsRow]);
                if (!setNext)
                    nextRhsRow++;
                if (row)
                {
                    eog = false;
                    return row.getClear();
                }
            }
            eos = true;
        }
        eog = true;
        return NULL;
    }
    inline void nextRhs()
    {
        if ((unsigned)-1 != atMost)
            rhsNext = candidateIndex < candidates.ordinality() ? candidates.item(candidateIndex++) : NULL;
        else if (isLookup())
            rhsNext = findNext(leftRow, nextRhsRow);
        else if (++nextRhsRow<rhsRows)
            rhsNext = rhsTable[nextRhsRow];
        else
            rhsNext = NULL;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!gotRHS)
            getRHS(false);
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
    const void *lookupNextRow()
    {
        if (!abortSoon && !eos)
        {
            if (doRightOuter)
            {
                OwnedConstThorRow row = handleRightOnly();
                if (row)
                    return row.getClear();
                return NULL;
            }
            loop
            {
                if (NULL == rhsNext)
                {
                    nextRhsRow = 0;
                    joined = 0;
                    candidateMatches = 0;
                    leftMatch = false;
                    if ((unsigned)-1 != atMost)
                    {
                        candidates.kill();
                        candidateIndex = 1;
                    }
                    leftRow.setown(left->nextRow());
                    if (leftRow)
                    {
                        eog = false;
                        if (rhsRows)
                        {
                            if (isAll())
                                rhsNext = rhsTable[nextRhsRow];
                            else
                            {
                                rhsNext = findFirst(leftRow, nextRhsRow);
                                if ((unsigned)-1 != atMost) // have to build candidates to know
                                {
                                    while (rhsNext)
                                    {
                                        ++candidateMatches;
                                        if (candidateMatches>abortLimit)
                                        {
                                            if (0 == (JFmatchAbortLimitSkips & flags))
                                            {
                                                Owned<IException> e;
                                                try
                                                {
                                                    if (hashJoinHelper)
                                                        hashJoinHelper->onMatchAbortLimitExceeded();
                                                    CommonXmlWriter xmlwrite(0);
                                                    if (outputMeta && outputMeta->hasXML())
                                                    {
                                                        outputMeta->toXML((const byte *) leftRow.get(), xmlwrite);
                                                    }
                                                    throw MakeActivityException(this, 0, "More than %d match candidates in join for row %s", abortLimit, xmlwrite.str());
                                                }
                                                catch (IException *_e)
                                                {
                                                    if (0 == (JFonfail & flags))
                                                        throw;
                                                    e.setown(_e);
                                                }
                                                RtlDynamicRowBuilder ret(allocator);
                                                size32_t transformedSize = hashJoinHelper->onFailTransform(ret, leftRow, defaultRight, e.get());
                                                rhsNext = NULL;
                                                if (transformedSize)
                                                {
                                                    candidateMatches = 0;
                                                    return ret.finalizeRowClear(transformedSize);
                                                }
                                            }
                                            else
                                                leftMatch = true; // there was a lhs match, even though rhs group exceeded limit. Therefore this lhs will not be considered left only/left outer
                                            candidateMatches = 0;
                                            break;
                                        }
                                        else if (candidateMatches>atMost)
                                        {
                                            candidateMatches = 0;
                                            break;
                                        }
                                        candidates.append(rhsNext);
                                        rhsNext = findNext(leftRow, nextRhsRow);
                                    }                               
                                    if (0 == candidateMatches)
                                        rhsNext = NULL;
                                    else if (candidates.ordinality())
                                        rhsNext = candidates.item(0);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (eog)
                        {
                            if (flags & JFrightouter)
                            {
                                prepareRightOnly();
                                OwnedConstThorRow row = handleRightOnly();
                                if (row)
                                    return row.getClear();
                            }
                            else
                                eos = true;
                        }
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
                if (isDenormalize())
                {
                    OwnedConstThorRow ret;
                    RtlDynamicRowBuilder tmpbuf(allocator);
                    unsigned rcCount = 0;
                    ConstPointerArray filteredRhs;
                    while (rhsNext)
                    {
                        if (abortSoon) 
                            return NULL;
                        if (!fuzzyMatch || (isAll()?allJoinHelper->match(leftRow, rhsNext):hashJoinHelper->match(leftRow, rhsNext)))
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
                        nextRhs();
                    }
                    if (filteredRhs.ordinality() || (!leftMatch && 0!=(flags & JFleftouter)))
                    {
                        unsigned numRows = filteredRhs.ordinality();
                        const void *rightRow = numRows ? filteredRhs.item(0) : defaultRight.get();
                        if (isGroupOp())
                        {
                            size32_t sz = isAll()?allJoinHelper->transform(tmpbuf, leftRow, rightRow, numRows, filteredRhs.getArray()):hashJoinHelper->transform(tmpbuf, leftRow, rightRow, numRows, filteredRhs.getArray());
                            if (sz)
                                ret.setown(tmpbuf.finalizeRowClear(sz));
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
                                    size32_t sz = isAll()?allJoinHelper->transform(tmpbuf, ret, rightRow, ++rcCount):hashJoinHelper->transform(tmpbuf, ret, rightRow, ++rcCount);
                                    if (sz)
                                    {
                                        rowSize = sz;
                                        ret.setown(tmpbuf.finalizeRowClear(sz));
                                    }
                                    if (rcCount == filteredRhs.ordinality())
                                        break;
                                    tmpbuf.ensureRow();
                                }
                                if (!rowSize)
                                    ret.clear();
                            }
                        }
                    }
                    if (ret)
                    {
                        someSinceEog = true;
                        return ret.getClear();
                    }
                }
                else
                {
                    while (rhsNext)
                    {
                        if (!fuzzyMatch || match(leftRow, rhsNext))
                        {
                            leftMatch = true;
                            if (!exclude)
                            {
                                OwnedConstThorRow row = joinTransform(leftRow, rhsNext);
                                if (row)
                                {
                                    someSinceEog = true;
                                    if (++joined == keepLimit)
                                        rhsNext = NULL;
                                    else if (!returnMany)
                                        rhsNext = NULL;
                                    else
                                        nextRhs();
                                    return row.getClear();
                                }
                            }
                        }
                        else if (flags & JFrightouter)
                            rightMatchSet->set(nextRhsRow);
                        nextRhs();
                    }
                    if (!leftMatch && NULL == rhsNext && 0!=(flags & JFleftouter))
                    {
                        OwnedConstThorRow row = joinTransform(leftRow, defaultRight);
                        if (row)
                        {
                            someSinceEog = true;
                            return row.getClear();
                        }
                    }
                }
            }
        }
        return NULL;
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canStall = true;
    }
    void addRowHt(const void *p)
    {
        OwnedConstThorRow _p = p;
        unsigned h = rightHash->hash(p)%rhsTableLen;
        loop
        {
            const void *e = ht.query(h);
            if (!e)
            {
                ht.setRow(h, _p.getClear());
                htCount++;
                break;
            }
            if (dedup && 0 == compareRight->docompare(e,p))
            {
                htDedupCount++;
                break; // implicit dedup
            }
            h++;
            if (h>=rhsTableLen)
                h = 0;
        }
    }
    void broadcastRHS() // broadcasting local rhs
    {
        bool stopRHSBroadcast = false;
        Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_send);
        MemoryBuffer mb;
        try
        {
            CThorSpillableRowArray &localRhsRows = *rhsNodeRows.item(queryJob().queryMyRank()-1);
            CMemoryRowSerializer mbser(mb);
            while (!abortSoon)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;

                    {
                        if (hasBroadcastSpilt())
                        {
                            // keep it only if it hashes to my node
                            unsigned hv = rightHash->hash(row.get());
                            if ((myNode-1) == (hv % numNodes))
                                localRhsRows.append(row.getLink());

                            // ok so switch tactics.
                            // clearAllNonLocalRows() will have cleared out non-locals by now
                            // but I may be half way through serializing rows here, which are mixed and this row
                            // may still need to be sent.
                            // The destination rowProcessor will take care of any that need post-filtering,
                            // so ensure last buffer is sent, below before exiting broadcastRHS broadcast

                            stopRHSBroadcast = true;
                        }
                        else
                        {
                            /* NB: It could still spill here, i.e. before appending a non-local row
                             * When all is done, a last pass is needed to clear out non-locals
                             */
                            localRhsRows.append(row.getLink());
                        }
                    }

                    rightSerializer->serialize(mbser, (const byte *)row.get());
                    if (mb.length() >= MAX_SEND_SIZE || stopRHSBroadcast)
                        break;
                }
                if (0 == mb.length())
                    break;
                if (stopRHSBroadcast)
                    sendItem->setFlag(bcastflag_spilt);
                ThorCompress(mb, sendItem->queryMsg());
                if (!broadcaster.send(sendItem))
                    break;
                if (stopRHSBroadcast)
                    break;
                mb.clear();
                broadcaster.resetSendItem(sendItem);
            }
        }
        catch (IException *e)
        {
            ActPrintLog(e, "CLookupJoinActivity::broadcastRHS: exception");
            throw;
        }

        sendItem.setown(broadcaster.newSendItem(bcast_stop));
        if (stopRHSBroadcast)
            sendItem->setFlag(bcastflag_spilt);
        ActPrintLog("Sending final RHS broadcast packet");
        broadcaster.send(sendItem); // signals stop to others
    }
    void processRHSRows(unsigned slave, MemoryBuffer &mb)
    {
        CThorSpillableRowArray &rows = *rhsNodeRows.item(slave-1);
        RtlDynamicRowBuilder rowBuilder(rightAllocator);
        CThorStreamDeserializerSource memDeserializer(mb.length(), mb.toByteArray());
        while (!memDeserializer.eos())
        {
            size32_t sz = rightDeserializer->deserialize(rowBuilder, memDeserializer);
            OwnedConstThorRow fRow = rowBuilder.finalizeRowClear(sz);
            if (hasBroadcastSpilt())
            {
                /* NB: recvLoop should be winding down, a slave signal spilt and communicated to all
                 * So these will be the last few broadcast rows, when broadcaster is complete, the rest will be hash distributed
                 */

                // hash row and discard unless for this node
                unsigned hv = rightHash->hash(fRow.get());
                if ((myNode-1) == (hv % numNodes))
                    rows.append(fRow.getClear());
            }
            else
            {
                /* NB: It could still spill here, i.e. before appending a non-local row
                 * When all is done, a last pass is needed to clear out non-locals
                 */
                rows.append(fRow.getClear());
            }
        }
    }
    void setupDistributors()
    {
        if (!rhsDistributor)
        {
            rhsDistributor.setown(createHashDistributor(this, queryJob().queryJobComm(), rhsDistributeTag, false, NULL));
            right.setown(rhsDistributor->connect(queryRowInterfaces(rightITDL), right.getClear(), rightHash, NULL));
            lhsDistributor.setown(createHashDistributor(this, queryJob().queryJobComm(), lhsDistributeTag, false, NULL));
            left.setown(lhsDistributor->connect(queryRowInterfaces(leftITDL), left.getClear(), leftHash, NULL));
        }
    }
    void getRHS(bool stopping)
    {
/*
 * This handles LOOKUP and ALL, but most of the complexity is for LOOKUP handling OOM
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

        if (gotRHS)
            return;
        gotRHS = true;
        // if input counts known, get global aggregate and pre-allocate HT
        ThorDataLinkMetaInfo rightMeta;
        rightITDL->getMetaInfo(rightMeta);
        if (rightMeta.totalRowsMin == rightMeta.totalRowsMax)
            rhsTotalCount = rightMeta.totalRowsMax;
        if (needGlobal)
        {
            CMessageBuffer msg;
            msg.append(rhsTotalCount);
            container.queryJob().queryJobComm().send(msg, 0, mpTag);
            if (!receiveMsg(msg, 0, mpTag))
                return;
            msg.read(rhsTotalCount);

            rowProcessor.start();
            broadcaster.start(this, mpTag, stopping);
            broadcastRHS();
            broadcaster.end();
            rowProcessor.wait();

            if (stopping)
            {
                queryJob().queryRowManager()->removeRowBuffer(this);
                return;
            }

            // NB: no more rows can be added to rhsNodeRows at this point, but they could stil be flushed

            rhsRows = 0;
            ForEachItemIn(a, rhsNodeRows)
            {
                CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                rows.flush();
                rhsRows += rows.numCommitted();
            }

            if (isSmart() && getOptBool(THOROPT_LKJOIN_LOCALFAILOVER, getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER))) // For testing purposes only
                clearAllNonLocalRows("testing");

            if (isLookup())
            {
                // If spilt, don't size ht table now, if local rhs fits, ht will be sized later
                if (!hasBroadcastSpilt())
                {
                    rhsTableLen = getHTSize(rhsRows);
                    // NB: This sizing could cause spilling callback to be triggered
                    ht.ensure(rhsTableLen); // Pessimistic if LOOKUP,KEEP(1)
                    /* JCSMORE - failure to size should not be failure condition
                     * It will mark spiltBroadcastingRHS and try to degrade
                     * JCS->GH: However, need to catch OOM somehow..
                     */
                    ht.clearUnused();
                }
            }
            else
                rhs.ensure(rhsRows);

            /* NB: Potentially one of the slaves spilt late after broadcast and rowprocessor finished
             * Need to remove spill callback and broadcast one last message to know.
             */

            queryJob().queryRowManager()->removeRowBuffer(this);

            ActPrintLog("Broadcasting final split status");
            broadcaster.reset();
            // NB: using a different tag from 1st broadcast, as 2nd on other nodes can start sending before 1st on this has quit receiving
            broadcaster.start(this, broadcast2MpTag, false);
            Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_stop);
            if (hasBroadcastSpilt())
                sendItem->setFlag(bcastflag_spilt);
            broadcaster.send(sendItem); // signals stop to others
            broadcaster.end();

            /* All slaves now know whether any one spilt or not, i.e. whether to perform local hash join or not
             * If any have, still need to distribute rest of RHS..
             */

            // flush spillable row arrays, and clear any non-locals if spiltBroadcastingRHS and compact
            if (hasBroadcastSpilt())
            {
                ForEachItemIn(a, rhsNodeRows)
                {
                    CThorSpillableRowArray &rows = *rhsNodeRows.item(a);
                    clearNonLocalRows(rows, flushedRowMarkers.item(a));
                    rows.compact();
                }
            }

            if (hasBroadcastSpilt()) // NB: Can only be active for LOOKUP (not ALL)
            {
                ActPrintLog("Spilt whilst broadcasting, will attempt distribute local lookup join");
                localLookupJoin = true;

                // NB: lhs ordering and grouping lost from here on..
                if (grouped)
                    throw MakeActivityException(this, 0, "Degraded to distributed lookup join, LHS order cannot be preserved");

                // If HT sized already (due to total from meta) and now spilt, too big clear and size later
                ht.kill();

                setupDistributors();

                /* NB: The collected broadcast rows thus far (in rhsNodeRows) were ordered/deterministic.
                 * However, the rest of the rows received via the distributor and non-deterministic.
                 * Therefore the order overall is non-deterministic from this point on.
                 * For that reason, the rest of the RHS (distributed) rows will be processed ahead of the
                 * collected [broadcast] rows in the code below for efficiency reasons.
                 */
                IArrayOf<IRowStream> streams;
                streams.append(*right.getLink()); // what remains of 'right' will be read through distributor
                ForEachItemIn(a, rhsNodeRows)
                {
                    CThorSpillableRowArray &sRowArray = *rhsNodeRows.item(a);
                    CThorExpandingRowArray rowArray(*this, NULL);
                    rowArray.transferFrom(sRowArray);
                    streams.append(*rowArray.createRowStream(0, (rowidx_t)-1, true)); // NB: will kill array when stream exhausted
                }
                right.setown(createConcatRowStream(streams.ordinality(), streams.getArray()));
            }
            else
            {
                if (rhsTotalCount != RCUNSET) // verify matches meta if set/calculated (and haven't spilt)
                    assertex(rhsRows == rhsTotalCount);
            }
        }
        else
        {
            if (isLookup())
                localLookupJoin = true;
            else
            {
                if (RCUNSET != rhsTotalCount)
                {
                    rhsRows = (rowidx_t)rhsTotalCount;
                    rhs.ensure(rhsRows);
                }
                // local ALL join, must fit into memory
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;
                    rhs.append(row.getClear());
                }
                if (RIUNSET == rhsRows)
                    rhsRows = rhs.ordinality();
            }
        }

        if (localLookupJoin) // NB: Can only be active for LOOKUP (not ALL)
        {
            Owned<IThorRowLoader> rowLoader;
            if (failoverToStdJoin)
            {
                if (getOptBool(THOROPT_LKJOIN_HASHJOINFAILOVER)) // for testing only (force to disk, as if spilt)
                    rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), compareRight, false, rc_allDisk, SPILL_PRIORITY_LOOKUPJOIN));
                else
                {
                    rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), compareRight, false, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN));
                    rowLoader->setOptions(rcflag_noAllInMemSort); // If fits into memory, don't want it sorted
                }
            }
            else
            {
                // i.e. will fire OOM if runs out of memory loading local right
                rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(rightITDL), compareRight, false, rc_allMem, SPILL_PRIORITY_DISABLE));
            }
            Owned<IRowStream> rightStream = rowLoader->load(right, abortSoon, false, &rhs);

            if (!rightStream)
            {
                ActPrintLog("RHS local rows fitted in memory, count: %"RIPF"d", rhs.ordinality());
                // all fitted in memory, rows were transferred out back into 'rhs'
                // Will be unsorted because of rcflag_noAllInMemSort

                /* Now need to size HT.
                 * transfer rows back into a spillable container
                 * If HT sizing DOESN'T cause spill, then, row will be transferred back into 'rhs'
                 * If HT sizing DOES cause spill, sorted rightStream will be created.
                 */

                rowLoader.clear();
                Owned<IThorRowCollector> collector = createThorRowCollector(*this, queryRowInterfaces(rightITDL), compareRight,false, rc_mixed, SPILL_PRIORITY_LOOKUPJOIN);
                collector->setOptions(rcflag_noAllInMemSort); // If fits into memory, don't want it sorted
                rhsRows = rhs.ordinality();
                collector->transferRowsIn(rhs);
                rhsTableLen = getHTSize(rhsRows);

                // could cause spilling of 'rhs'
                ht.ensure(rhsTableLen); // Pessimistic if LOOKUP,KEEP(1)
                /* JCSMORE - failure to size should not be failure condition
                 * If it failed, the 'collector' will have spilt and it will not need HT
                 * JCS->GH: However, need to catch OOM somehow..
                 */

                ht.clearUnused();
                rightStream.setown(collector->getStream(false, &rhs));
            }
            if (rightStream) // NB: returned stream, implies spilt AND sorted, if not, 'rhs' is filled
            {
                ht.kill(); // no longer needed

                ActPrintLog("RHS spilt to disk. Standard Join will be used");

                // NB: lhs ordering and grouping lost from here on.. (will have been caught earlier if global)
                if (grouped)
                    throw MakeActivityException(this, 0, "Degraded to standard join, LHS order cannot be preserved");

                rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(leftITDL), compareLeft));
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
                        joinHelper.setown(createJoinHelper(*this, hashJoinHelper, this, hintparallelmatch, hintunsortedoutput));
                        break;
                    }
                    case TAKlookupdenormalize:
                    case TAKlookupdenormalizegroup:
                    case TAKsmartdenormalize:
                    case TAKsmartdenormalizegroup:
                        joinHelper.setown(createDenormalizeHelper(*this, hashJoinHelper, this));
                        break;
                    default:
                        throwUnexpected();
                }
                joinHelper->init(left, rightStream, leftAllocator, rightAllocator, ::queryRowMetaData(leftITDL), &abortSoon);
                return;
            }
            else
                ActPrintLog("Local RHS loaded to memory, performing local lookup join");
        }
        if (!stopping)
            prepareRHS();
    }
    void prepareRHS()
    {
        // NB: this method is not used if we've failed over to a regular join in getRHS()

        // JCSMORE - would be nice to make this multi-core, clashes and compares can be expensive
        if (localLookupJoin)
        {
            // If got this far, without turning into a standard fully distributed join, then all rows are in rhs
            if (isLookup()) // if isAll(), want to leave them in rhs as is.
            {
                rowidx_t r=0;
                for (; r<rhs.ordinality(); r++)
                    addRowHt(rhs.getClear(r));
                rhs.kill(); // free up ptr table asap
            }
        }
        else if (needGlobal)
        {
            // If global and !localLookupJoin, then rows are in 'rhsNodeRows' arrays
            if (isAll())
            {
                ForEachItemIn(a2, rhsNodeRows)
                {
                    CThorSpillableRowArray &rows = *rhsNodeRows.item(a2);
                    rowidx_t r=0;
                    for (; r<rows.numCommitted(); r++)
                        rhs.append(rows.getClear(r));
                    rows.kill(); // free up ptr table asap
                }
            }
            else
            {
                ForEachItemIn(a2, rhsNodeRows)
                {
                    CThorSpillableRowArray &rows = *rhsNodeRows.item(a2);
                    rowidx_t r=0;
                    for (; r<rows.numCommitted(); r++)
                        addRowHt(rows.getClear(r));
                    rows.kill(); // free up ptr table asap
                }
            }
        }
        ActPrintLog("rhs table: %d elements", rhsRows);
        if (isLookup())
            rhsTable = ht.getRowArray();
        else
        {
            assertex(isAll());
            rhsTable = rhs.getRowArray();
        }
    }
// IBCastReceive
    virtual void bCastReceive(CSendItem *sendItem)
    {
        if (sendItem)
        {
            if (0 != (sendItem->queryFlags() & bcastflag_spilt))
            {
                VStringBuffer msg("Notification that slave %d spilt", sendItem->queryOrigin());
                clearAllNonLocalRows(msg.str());
            }
        }
        rowProcessor.addBlock(sendItem); // NB: NULL indicates end
    }
// IBufferedRowCallback
    virtual unsigned getPriority() const
    {
        return SPILL_PRIORITY_LOOKUPJOIN;
    }
    virtual bool freeBufferedRows(bool critical)
    {
        // NB: only installed if lookup join and global
        return clearAllNonLocalRows("Out of memory callback");
    }
};

CActivityBase *createLookupJoinSlave(CGraphElementBase *container) 
{ 
    return new CLookupJoinActivity(container);
}
