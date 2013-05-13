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

#ifdef _DEBUG
#define _TRACEBROADCAST
#endif

enum join_t { JT_Undefined, JT_Inner, JT_LeftOuter, JT_RightOuter, JT_LeftOnly, JT_RightOnly, JT_LeftOnlyTransform };
enum joinkind_t { join_lookup, join_all, denormalize_lookup, denormalize_all };
const char *joinActName[4] = { "LOOKUPJOIN", "ALLJOIN", "LOOKUPDENORMALIZE", "ALLDENORMALIZE" };


#define MAX_SEND_SIZE 0x100000 // 1MB
#define MAX_QUEUE_BLOCKS 5

enum broadcast_code { bcast_none, bcast_send, bcast_sendStopping, bcast_stop };
class CSendItem : public CSimpleInterface
{
    CMessageBuffer msg;
    broadcast_code code;
    unsigned origin, headerLen;
public:
    CSendItem(broadcast_code _code, unsigned _origin) : code(_code), origin(_origin)
    {
        msg.append((unsigned)code);
        msg.append(origin);
        headerLen = msg.length();
    }
    CSendItem(CMessageBuffer &_msg)
    {
        msg.swapWith(_msg);
        msg.read((unsigned &)code);
        msg.read(origin);
    }
    unsigned length() const { return msg.length(); }
    void reset() { msg.setLength(headerLen); }
    CMessageBuffer &queryMsg() { return msg; }
    broadcast_code queryCode() const { return code; }
    unsigned queryOrigin() const { return origin; }
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
            activity.fireException(LINK(e));
        }
        else
            allDoneSem.signal();
    }
    bool send(CSendItem *sendItem)
    {
        broadcastToOthers(sendItem);
        return !allRequestStop;
    }
    void final()
    {
        ActPrintLog(&activity, "CBroadcaster::final()");
        Owned<CSendItem> sendItem = newSendItem(bcast_stop);
        send(sendItem);
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
class CLookupJoinActivity : public CSlaveActivity, public CThorDataLink, implements ISmartBufferNotify, implements IBCastReceive
{
    IHThorHashJoinArg *hashJoinHelper;
    IHThorAllJoinArg *allJoinHelper;
    const void **rhsTable;
    rowidx_t rhsTableLen, htCount, htDedupCount;
    rowidx_t rhsRows;
    IHash *leftHash, *rightHash;
    ICompare *compareRight, *compareLeftRight;

    Owned<IThorDataLink> right;
    Owned<IThorDataLink> left;
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
    StringAttr joinStr;
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
    void stopRightInput()
    {
        if (right)
        {
            stopInput(right, "(R)");
            right.clear();
        }
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        StringBuffer js(joinActName[(int)joinKind]);
        js.append("(").append(container.queryId()).append(")");
        joinStr.set(js.str());
        ActPrintLog("%s: init",joinStr.get());
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
        ActPrintLog("%s: Join type is %s", joinStr.get(), getJoinTypeStr(str).str());
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
        ActPrintLog("%s: LHS input finished, %"RCPF"d rows read", joinStr.get(), count);
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
        grouped = inputs.item(0)->isGrouped();
        left.set(inputs.item(0));
        allocator.set(queryRowAllocator());
        leftAllocator.set(::queryRowAllocator(left));
        outputMeta.set(left->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta());
        left.setown(createDataLinkSmartBuffer(this,left,LOOKUPJOINL_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(left->queryFromActivity()),grouped,RCUNBOUND,this,false,&container.queryJob().queryIDiskUsage()));       
        StringBuffer str(joinStr);
        startInput(left);
        right.set(inputs.item(1));
        rightAllocator.set(::queryRowAllocator(right));
        rightSerializer.set(::queryRowSerializer(right));
        rightDeserializer.set(::queryRowDeserializer(right));

        try
        {
            startInput(right); 
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
        dataLinkStart(joinStr, container.queryId());
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        gotOtherROs.signal();
        cancelReceiveMsg(RANK_ALL, mpTag);
        broadcaster.cancel();
        rowProcessor.abort();
    }
    virtual void stop()
    {
        if (!gotRHS)
            getRHS(true);
        clearRHS();
        stopRightInput();
        stopInput(left);
        dataLinkStop();
        left.clear();
        right.clear();
        broadcaster.reset();
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
        if (!abortSoon && !eos)
        {
            if (doRightOuter)
            {
                OwnedConstThorRow row = handleRightOnly();
                if (row)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
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
                                                    dataLinkIncrement();
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
                                {
                                    dataLinkIncrement();
                                    return row.getClear();
                                }
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
                        dataLinkIncrement();
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
                                    dataLinkIncrement();
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
                            dataLinkIncrement();
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
    // Add to HT if one has been created, otherwise to row array and HT will be created later
    void addRow(const void *p)
    {
        if (rhsTableLen)
            addRowHt(p);
        else
            rhs.append(p);
    }
    void sendRHS() // broadcasting local rhs
    {
        try
        {
            CThorExpandingRowArray &localRhsRows = *rhsNodeRows.item(queryJob().queryMyRank()-1);
            Owned<CSendItem> sendItem = broadcaster.newSendItem(bcast_send);
            MemoryBuffer mb;
            CMemoryRowSerializer mbs(mb);
            while (!abortSoon)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;
                    localRhsRows.append(row.getLink());
                    rightSerializer->serialize(mbs, (const byte *)row.get());
                    if (mb.length() >= MAX_SEND_SIZE)
                        break;
                }
                if (0 == mb.length())
                    break;
                ThorCompress(mb, sendItem->queryMsg());
                if (!broadcaster.send(sendItem))
                    break;
                mb.clear();
                sendItem->reset();
            }
        }
        catch (IException *e)
        {
            ActPrintLog(e, "CLookupJoinActivity::sendRHS: exception");
            throw;
        }
        broadcaster.final(); // signal stop to others
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
            rows.append(fRow.getClear());
        }
    }
    void getRHS(bool stopping)
    {
        if (gotRHS)
            return;
        gotRHS = true;
        // if input counts known, get global aggregate and pre-allocate HT
        ThorDataLinkMetaInfo rightMeta;
        right->getMetaInfo(rightMeta);
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
        Owned<IException> exception;
        try
        {
            if (needGlobal)
            {
                rowProcessor.start();
                broadcaster.start(this, mpTag, stopping);
                sendRHS();
                broadcaster.end();
                rowProcessor.wait();
            }
            else if (!stopping)
            {
                while (!abortSoon)
                {
                    OwnedConstThorRow row = right->ungroupedNextRow();
                    if (!row)
                        break;
                    addRow(row.getClear());
                }
            }
            if (!stopping)
                prepareRHS();
        }
        catch (IOutOfMemException *e) { exception.setown(e); }
        if (exception.get())
        {
            StringBuffer errStr(joinStr);
            errStr.append("(").append(container.queryId()).appendf(") right-hand side is too large (%"I64F"u bytes in %"RIPF"d rows) for %s : (",(unsigned __int64) rhs.serializedSize(),rhs.ordinality(),joinStr.get());
            errStr.append(exception->errorCode()).append(", ");
            exception->errorMessage(errStr);
            errStr.append(")");
            IException *e2 = MakeActivityException(this, TE_TooMuchData, "%s", errStr.str());
            ActPrintLog(e2);
            throw e2;
        }
    }
    void prepareRHS()
    {
        if (needGlobal)
        {
            rowidx_t maxRows = 0;
            ForEachItemIn(a, rhsNodeRows)
            {
                CThorExpandingRowArray &rows = *rhsNodeRows.item(a);
                maxRows += rows.ordinality();
            }
            if (rhsTotalCount != RCUNSET)
            { // ht pre-expanded already
                assertex(maxRows == rhsTotalCount);
            }
            rhsRows = maxRows;
        }
        else // local
        {
            if (RCUNSET != rhsTotalCount)
                rhsRows = (rowidx_t)rhsTotalCount;
            else // all join, or lookup if total count unkown
                rhsRows = rhs.ordinality();
        }
        if (RCUNSET == rhsTotalCount) //NB: if rhsTotalCount known, will have been sized earlier
        {
            if (isAll())
            {
                if (needGlobal) // otherwise (local), it expanded as rows added
                    rhs.ensure(rhsRows);
            }
            else
            {
                rhsTableLen = getHTSize(rhsRows);
                ht.ensure(rhsTableLen); // Pessimistic if LOOKUP,KEEP(1)
                ht.clearUnused();
                if (!needGlobal)
                {
                    rowidx_t r=0;
                    for (; r<rhs.ordinality(); r++)
                        addRowHt(rhs.getClear(r));
                    rhs.kill(); // free up ptr table asap
                }
                // else built up from rhsNodeRows
            }
        }
        if (needGlobal)
        {
            // JCSMORE - would be nice to make this multi-core, clashes and compares can be expensive
            ForEachItemIn(a2, rhsNodeRows)
            {
                CThorExpandingRowArray &rows = *rhsNodeRows.item(a2);
                rowidx_t r=0;
                for (; r<rows.ordinality(); r++)
                    addRow(rows.getClear(r));
                rows.kill(); // free up ptr table asap
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
        rowProcessor.addBlock(sendItem);
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
