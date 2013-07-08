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

/*
 * 1) Check lookup join varieties in child queries, i.e. restarability
 * 2) Shink size of pre-sized HT if switching to distributed local lookup
 */

#include "thlookupjoinslave.ipp"
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
enum joinkind_t { join_lookup, join_all, denormalize_lookup, denormalize_all };


#define MAX_SEND_SIZE 0x100000 // 1MB
#define MAX_QUEUE_BLOCKS 5

enum broadcast_code { bcast_none, bcast_send, bcast_sendStopping, bcast_stop };
enum broadcast_flags { bcastflag_spilt=0x01 };
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
        msg.read((unsigned &)info);
        msg.read(origin);
    }
    unsigned length() const { return msg.length(); }
    void reset() { msg.setLength(headerLen); }
    CMessageBuffer &queryMsg() { return msg; }
    broadcast_code queryCode() const { return (broadcast_code)(info & BROADCAST_CODE_MASK); }
    unsigned queryOrigin() const { return origin; }
    broadcast_flags queryFlags() const { return (broadcast_flags)((info & BROADCAST_FLAG_MASK)>>8); }
    void setFlag(broadcast_flags _flag)
    {
        info = (info & ~BROADCAST_FLAG_MASK) | ((byte)_flag << 8);
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

    PointerArrayOf<CThorExpandingRowArray> rhsNodeRows;
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

    // Handling OOM
    CriticalSection localHashCrit;
    bool spiltBroadcastingRHS, localLookupJoin;
    rank_t myNode;
    unsigned numNodes;
    Owned<IHashDistributor> lhsDistributor, rhsDistributor;
    ICompare *compareLeft;
    Owned<IJoinHelper> joinHelper;

    inline bool isLookup() { return (joinKind==join_lookup)||(joinKind==denormalize_lookup); }
    inline bool isAll() { return (joinKind==join_all)||(joinKind==denormalize_all); }
    inline bool isDenormalize() { return (joinKind==denormalize_all)||(joinKind==denormalize_lookup); }
    inline bool isGroupOp() { return (TAKlookupdenormalizegroup == container.getKind() || TAKalldenormalizegroup == container.getKind()); }
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
            CThorExpandingRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                rows->kill();
        }
    }
protected:
    joinkind_t joinKind;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLookupJoinActivity(CGraphElementBase *_container, joinkind_t _joinKind) 
        : CSlaveActivity(_container), CThorDataLink(this), joinKind(_joinKind), broadcaster(*this), rhs(*this, NULL), ht(*this, NULL, true),
          rowProcessor(*this)
    {
        gotRHS = false;
        joinType = JT_Undefined;
        nextRhsRow = 0;
        rhsNext = NULL;
        returnMany = false;
        candidateMatches = 0;
        atMost = 0;
        spiltBroadcastingRHS = localLookupJoin = false;
        myNode = queryJob().queryMyRank();
        numNodes = queryJob().querySlaves();
        needGlobal = !container.queryLocal() && (container.queryJob().querySlaves() > 1);
    }
    ~CLookupJoinActivity()
    {
        ForEachItemIn(a, rhsNodeRows)
        {
            CThorExpandingRowArray *rows = rhsNodeRows.item(a);
            if (rows)
                rows->Release();
        }
    }
    bool clearNonLocalRows(const char *msg)
    {
        rowidx_t clearedRows = 0;
        {
            CriticalBlock b(localHashCrit);
            if (spiltBroadcastingRHS)
                return false;
            ActPrintLog("Clearing non-local rows - cause: %s", msg);
            spiltBroadcastingRHS = true;
            ForEachItemIn(a, rhsNodeRows)
            {
                CThorExpandingRowArray &rows = *rhsNodeRows.item(a);
                rowidx_t numRows = rows.ordinality();
                for (unsigned r=0; r<numRows; r++)
                {
                    unsigned hv = rightHash->hash(rows.query(r));
                    if (myNode != (hv % numNodes))
                    {
                        OwnedConstThorRow row = rows.getClear(r); // dispose of
                        ++clearedRows;
                    }
                }
                rows.compact();
            }
        }

        ActPrintLog("handleLowMem: clearedRows = %"RIPF"d", clearedRows);
        return 0 != clearedRows;
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        ActPrintLog("init");
        appendOutputLinked(this);

        eos = false;
        someSinceEog = false;
        eog = false;
        joined = 0;
        doRightOuter = false;
        leftMatch = false;
        grouped = false;
        lastRightOuter = 0;
        waitForOtherRO = true;
        hashJoinHelper = NULL;
        allJoinHelper = NULL;
        abortLimit = 0;
        compareRight = NULL;
        leftHash = NULL;
        rightHash = NULL;
        compareLeftRight = NULL;
        keepLimit = 0;
        switch (joinKind)
        {
            case join_all:
            case denormalize_all:
            {
                allJoinHelper = (IHThorAllJoinArg *)queryHelper();
                flags = allJoinHelper->getJoinFlags();
                returnMany = true;
                keepLimit = allJoinHelper->getKeepLimit();
                fuzzyMatch = 0 != (JFmatchrequired & flags);
                break;
            }
            case join_lookup:
            case denormalize_lookup:
            {
                hashJoinHelper = (IHThorHashJoinArg *)queryHelper();
                leftHash = hashJoinHelper->queryHashLeft();
                rightHash = hashJoinHelper->queryHashRight();
                compareRight = hashJoinHelper->queryCompareRight();
                compareLeft = hashJoinHelper->queryCompareLeft();
                compareLeftRight = hashJoinHelper->queryCompareLeftRight();
                flags = hashJoinHelper->getJoinFlags();
                if (JFmanylookup & flags)
                    returnMany = true;
                keepLimit = hashJoinHelper->getKeepLimit();
                abortLimit = hashJoinHelper->getMatchAbortLimit();
                atMost = hashJoinHelper->getJoinLimit();

                fuzzyMatch = 0 != (JFmatchrequired & flags);
                bool maySkip = 0 != (flags & JFtransformMaySkip);
                dedup = compareRight && !maySkip && !fuzzyMatch && (!returnMany || 1==keepLimit);

                // code gen should spot invalid constants on KEEP with LOOKUP (without MANY)
                break;
            }
            default:
                assertex(!"Unexpected join kind");
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

        if (!container.queryLocal())
            mpTag = container.queryJob().deserializeMPTag(data);

        unsigned slaves = container.queryJob().querySlaves();
        rhsNodeRows.ensure(slaves);
        while (slaves--)
            rhsNodeRows.append(new CThorExpandingRowArray(*this, NULL, true)); // true, nulls not needed?
        StringBuffer str;
        ActPrintLog("Join type is %s", getJoinTypeStr(str).str());
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
        rhsNext = NULL;
        candidateMatches = 0;
        rhsTotalCount = RCUNSET;
        htCount = htDedupCount = 0;
        eos = false;
        leftITDL = inputs.item(0);
        rightITDL = inputs.item(1);
        grouped = leftITDL->isGrouped();
        allocator.set(queryRowAllocator());
        leftAllocator.set(::queryRowAllocator(leftITDL));
        outputMeta.set(leftITDL->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta());
        leftITDL = createDataLinkSmartBuffer(this,leftITDL,LOOKUPJOINL_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(leftITDL->queryFromActivity()),grouped,RCUNBOUND,this,false,&container.queryJob().queryIDiskUsage());
        left.setown(leftITDL);
        startInput(leftITDL);
        right.set(rightITDL);
        rightAllocator.set(::queryRowAllocator(rightITDL));
        rightSerializer.set(::queryRowSerializer(rightITDL));
        rightDeserializer.set(::queryRowDeserializer(rightITDL));

        spiltBroadcastingRHS = localLookupJoin = false;

        if (hashJoinHelper) // only for LOOKUP not ALL
        {
        	if (needGlobal)
        		queryJob().queryRowManager()->addRowBuffer(this);
        }

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
            IException *e = leftexception.getClear();
            right->stop();
            throw e;
        }
        RtlDynamicRowBuilder rr(rightAllocator);
        if ((flags & JFonfail) || (flags & JFleftouter))
            rr.ensureRow();
        RtlDynamicRowBuilder rl(leftAllocator);
        if (flags & JFrightouter)
            rl.ensureRow();
        size32_t rrsz=0;
        size32_t rlsz=0;
        switch(joinKind)
        {
            case join_all:
            case denormalize_all:
            {
                if (rr.exists()) 
                    rrsz = allJoinHelper->createDefaultRight(rr);
                if (rl.exists()) 
                    rlsz = allJoinHelper->createDefaultLeft(rl);
                break;
            }
            case join_lookup:
            case denormalize_lookup:
            {
                if (rr.exists()) 
                    rrsz = hashJoinHelper->createDefaultRight(rr);
                if (rl.exists()) 
                    rlsz = hashJoinHelper->createDefaultLeft(rl);
                break;
            }
        };
        if (rrsz) 
            defaultRight.setown(rr.finalizeRowClear(rrsz));
        if (rlsz)
            defaultLeft.setown(rl.finalizeRowClear(rlsz));
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
        if (!gotRHS)
            getRHS(true);
        clearRHS();
        if (right)
        {
            stopInput(right, "(R)");
            right.clear();
            if (rhsDistributor)
            {
                rhsDistributor->disconnect(true);
                rhsDistributor->join();
                rhsDistributor.clear();
            }
        }
        broadcaster.reset();
        stopInput(left, "(L)");
        left.clear();
        if (lhsDistributor)
        {
            lhsDistributor->disconnect(true);
            lhsDistributor->join();
            lhsDistributor.clear();
        }
        joinHelper.clear();
        dataLinkStop();
    }
    inline bool match(const void *lhs, const void *rhsrow)
    {
        switch (joinKind)
        {
            case join_all:
            case denormalize_all:
                return allJoinHelper->match(lhs, rhsrow);
            case join_lookup:
            case denormalize_lookup:
                return hashJoinHelper->match(lhs, rhsrow);
            default:
                throwUnexpected();
        }
    }
    inline const void *joinTransform(const void *lhs, const void *rhsrow)
    {
        RtlDynamicRowBuilder row(allocator);
        size32_t thisSize;
        switch (joinKind)
        {
            case join_all:
            case denormalize_all:
                thisSize = allJoinHelper->transform(row, lhs, rhsrow);
                break;
            case join_lookup:
            case denormalize_lookup:
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
        rowidx_t sent = 0;
        try
        {
            CThorExpandingRowArray &localRhsRows = *rhsNodeRows.item(queryJob().queryMyRank()-1);
            CMemoryRowSerializer mbser(mb);
            while (!abortSoon)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;

                    {
                        CriticalBlock b(localHashCrit);
                        if (spiltBroadcastingRHS)
                        {
                            // keep it only if it hashes to my node
                            unsigned hv = rightHash->hash(row.get());
                            if (myNode == (hv % numNodes))
                                localRhsRows.append(row.getLink());

                            // ok so switch tactics.
                            // clearNonLocalRows() will have cleared out non-locals by now
                            // but I may be half way through serializing rows here, which are mixed and this row
                            // may still need to be sent.
                            // The destination rowProcessor will take care of any that need post-filtering,
                            // so ensure last buffer is sent, below before exiting broadcastRHS broadcast

                            stopRHSBroadcast = true;
                            ActPrintLog("Spill interrupted broadcast, %"RIPF"d rows were sent", sent);
                        }
                        else
                            localRhsRows.append(row.getLink());
                    }

                    ++sent;
                    rightSerializer->serialize(mbser, (const byte *)row.get());
                    if (mb.length() >= MAX_SEND_SIZE)
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
        CThorExpandingRowArray &rows = *rhsNodeRows.item(slave-1);
        RtlDynamicRowBuilder rowBuilder(rightAllocator);
        CThorStreamDeserializerSource memDeserializer(mb.length(), mb.toByteArray());
        while (!memDeserializer.eos())
        {
            size32_t sz = rightDeserializer->deserialize(rowBuilder, memDeserializer);
            OwnedConstThorRow fRow = rowBuilder.finalizeRowClear(sz);
            CriticalBlock b(localHashCrit);
            if (spiltBroadcastingRHS)
            {
                /* NB: recvLoop should be winding down, a slave signal spilt and communicated to all
                 * So these will be the last few broadcast rows, when broadcaster is complete, the rest will be hash distributed
                 */

                // hash row and discard unless for this node
                unsigned hv = rightHash->hash(fRow.get());
                if (myNode == (hv % numNodes)) // JCSMORE - I'm slightly assuming that IHashDistributor will do same modulus later (it does but..)
                    rows.append(fRow.getClear());
            }
            else
                rows.append(fRow.getClear());
        }
    }
    void setupDistributors()
    {
        rhsDistributor.setown(createHashDistributor(this, queryJob().queryJobComm(), mpTag, false, NULL));
        right.setown(rhsDistributor->connect(queryRowInterfaces(rightITDL), right.getClear(), rightHash, NULL));

        lhsDistributor.setown(createHashDistributor(this, queryJob().queryJobComm(), mpTag, false, NULL));
        left.setown(lhsDistributor->connect(queryRowInterfaces(leftITDL), left.getClear(), leftHash, NULL));
    }
    void getRHS(bool stopping)
    {
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
        }
        if (RCUNSET==rhsTotalCount)
            rhsTableLen = 0; // set later after gather
        else
        {
            if (isLookup())
            {
                rhsTableLen = getHTSize(rhsTotalCount);
                ht.ensure(rhsTableLen);
                ht.clearUnused();
                // NB: 'rhs' row array will not be used
            }
            else
            {
                rhsTableLen = 0;
                rhs.ensure((rowidx_t)rhsTotalCount);
            }
        }
		if (needGlobal)
		{
		    /* This is for isLookup(), what about ,ALL ? */

			rowProcessor.start();
			broadcaster.start(this, mpTag, stopping);
			broadcastRHS();
			broadcaster.end();
			rowProcessor.wait();

			/* NB: Potentially one of the slave spilt late after broadcast and rowprocessor finished
			 * Need to remove spill callback and broadcast one last message to know.
			 */

			queryJob().queryRowManager()->removeRowBuffer(this);

			broadcaster.reset();
			broadcaster.start(this, mpTag, stopping);
	        Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_stop);
	        if (spiltBroadcastingRHS)
	            sendItem->setFlag(bcastflag_spilt);
	        ActPrintLog("Sending final RHS broadcast packet");
	        broadcaster.send(sendItem); // signals stop to others
			broadcaster.end();

			/* All slaves now know whether any one spilt or not, i.e. whether to perform local hash join or not
			 * If any have, still need to distribute rest of RHS..
			 */

			if (spiltBroadcastingRHS)
			{
			    localLookupJoin = true;
                ActPrintLog("Spilt whilst broadcasting, will attempt distribute local lookup join");
				setupDistributors();

				// NB: At this point, there are still slaves*arrays of rows

				class CWrappedRight : public CSimpleInterface, implements IRowStream
				{
				    Linked<IRowStream> right;
				public:
                    rowidx_t count;

                    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);
				    CWrappedRight(IRowStream *_right) : right(_right)
				    {
				        count = 0;
				    }
				    ~CWrappedRight()
				    {
				        PROGLOG("CWrappedRight : read : %"RIPF"d rows", count);
				    }
				// IRowStream
				    // IRowStream
                    virtual const void *nextRow()
                    {
                        const void *row = right->nextRow();
                        if (!row)
                            return NULL;
                        ++count;
                        return row;
                    }
                    virtual void stop() { right->stop(); }
				};
				IRowStream *_right = new CWrappedRight(right);
                IArrayOf<IRowStream> streams;
//                streams.append(*right.getLink()); // what remains of 'right' will be read through distributor
                streams.append(*_right);
                ForEachItemIn(a, rhsNodeRows)
                {
                    CThorExpandingRowArray &rowArray = *rhsNodeRows.item(a);

                    ActPrintLog("Post clear, rowArray[%d] has %"RIPF"d rows", a, rowArray.ordinality());
                    streams.append(*rowArray.createRowStream());
    				// JCSMORE - would be good to dispose of the row ptr arrays as these 'rowArray's are consumed..
                }
                right.setown(createConcatRowStream(streams.ordinality(), streams.getArray()));
			}
		}
		else
		{
		    if (isLookup())
		        localLookupJoin = true;
		    else
		    {   // local ALL join, must fit into memory
	            while (!abortSoon)
	            {
	                OwnedConstThorRow row = right->ungroupedNextRow();
	                if (!row)
	                    break;
	                rhs.append(row.getClear());
	            }
		    }
		}

		if (localLookupJoin)
		{
		    Owned<IThorRowLoader> rowLoader = createThorRowLoader(*this, queryRowInterfaces(rightITDL), compareRight);
            rowLoader->setOptions(rcflag_noAllInMemSort); // If fits into memory, don't want it sorted
            Owned<IRowStream> rightStream = rowLoader->load(right, abortSoon, false, &rhs);

            if (rightStream) // NB: returned stream, implies spilt AND sorted, if not 'rhs' is filled
            {
                ActPrintLog("RHS spilt to disk. Standard Join will be used.");
                ActPrintLog("Loading/Sorting LHS");

                // NB: lhs ordering and grouping lost from here on..

                // JCS->GH - I hope it never hits this.. i.e. you prevent such a form being generated..
                if (grouped)
                    throw MakeActivityException(this, 0, "Degraded to standard join, LHS is grouped but cannot preserve grouping");

                rowLoader.setown(createThorRowLoader(*this, queryRowInterfaces(leftITDL), compareLeft));
                left.setown(rowLoader->load(left, abortSoon, false));
                leftITDL = inputs.item(0); // reset
                ActPrintLog("LHS loaded/sorted");

                // rightStream is sorted
                // so now going to do a std. join on distributed sorted streams
                switch(container.getKind())
                {
                    case TAKlookupjoin:
                    {
                        bool hintunsortedoutput = getOptBool(THOROPT_UNSORTED_OUTPUT, JFreorderable & flags); // JCS->GH - are you going to generate this flag?
                        bool hintparallelmatch = getOptBool(THOROPT_PARALLEL_MATCH, hintunsortedoutput); // i.e. unsorted, implies use parallel by default, otherwise no point
                        joinHelper.setown(createJoinHelper(*this, hashJoinHelper, this, hintparallelmatch, hintunsortedoutput));
                        break;
                    }
                    case TAKlookupdenormalize:
                    case TAKlookupdenormalizegroup:
                        joinHelper.setown(createDenormalizeHelper(*this, hashJoinHelper, this));
                        break;
                    default:
                        throwUnexpected();
                }
                joinHelper->init(left, rightStream, leftAllocator, rightAllocator, ::queryRowMetaData(leftITDL), &abortSoon);
                return;
            }
            else
            {
                ActPrintLog("RHS hash distributed rows : %"RIPF"d", rhs.ordinality());
                // all fitted in memory, rows were transferred out back into 'rhs'
                // Will be unsorted because of rcflag_noAllInMemSort
            }
		}
		if (!stopping)
			prepareRHS();
    }
    void prepareRHS()
    {
        if (needGlobal)
        {
            rowidx_t maxRows = 0;
            if (localLookupJoin)
                maxRows = rhs.ordinality();
            else
            {
                ForEachItemIn(a, rhsNodeRows)
                {
                    CThorExpandingRowArray &rows = *rhsNodeRows.item(a);
                    maxRows += rows.ordinality();
                }
                if (rhsTotalCount != RCUNSET)
                { // ht pre-expanded already
                    assertex(maxRows == rhsTotalCount);
                }
            }
            rhsRows = maxRows;
        }
        else
        {
            if (RCUNSET != rhsTotalCount)
                rhsRows = rhsTotalCount;
            else
                rhsRows = rhs.ordinality(); // total wasn't known
        }
        if (RCUNSET == rhsTotalCount) //NB: if rhsTotalCount known, HT will have been sized earlier
        {
            if (isLookup())
            {
                rhsTableLen = getHTSize(rhsRows);
                ht.ensure(rhsTableLen); // Pessimistic if LOOKUP,KEEP(1)
                ht.clearUnused();
            }
        }

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
                    CThorExpandingRowArray &rows = *rhsNodeRows.item(a2);
                    rowidx_t r=0;
                    for (; r<rows.ordinality(); r++)
                        rhs.append(rows.getClear(r));
                    rows.kill(); // free up ptr table asap
                }
            }
            else
            {
                ForEachItemIn(a2, rhsNodeRows)
                {
                    CThorExpandingRowArray &rows = *rhsNodeRows.item(a2);
                    rowidx_t r=0;
                    for (; r<rows.ordinality(); r++)
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
                clearNonLocalRows(msg.str());
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
        return clearNonLocalRows("Out of memory callback");
    }
};

CActivityBase *createLookupJoinSlave(CGraphElementBase *container) 
{ 
    return new CLookupJoinActivity(container, join_lookup); 
}

CActivityBase *createAllJoinSlave(CGraphElementBase *container) 
{ 
    return new CLookupJoinActivity(container, join_all); 
}

CActivityBase *createLookupDenormalizeSlave(CGraphElementBase *container) 
{ 
    return new CLookupJoinActivity(container, denormalize_lookup); 
}

CActivityBase *createAllDenormalizeSlave(CGraphElementBase *container) 
{ 
    return new CLookupJoinActivity(container, denormalize_all); 
}

