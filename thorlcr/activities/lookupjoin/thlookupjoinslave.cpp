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

#include "thorxmlwrite.hpp"

// #define STOPRIGHT_ASAP
#ifdef _DEBUG
#define _TRACEBROADCAST
#endif

enum join_t { JT_Undefined, JT_Inner, JT_LeftOuter, JT_RightOuter, JT_LeftOnly, JT_RightOnly, JT_LeftOnlyTransform };
enum joinkind_t { join_lookup, join_all, denormalize_lookup, denormalize_all };
const char *joinActName[4] = { "LOOKUPJOIN", "ALLJOIN", "LOOKUPDENORMALIZE", "ALLDENORMALIZE" };

class CBroadcaster
{
    CActivityBase *activity;
public:
    CMessageBuffer broadcasting;
    unsigned nodeindex, broadcastSlave;
    unsigned numnodes;
    mptag_t mpTag;
    bool &aborted, receiving;
    ICommunicator *comm;

    CBroadcaster(CActivityBase *_activity, bool &_aborted) : activity(_activity), aborted(_aborted)
    {
        numnodes = 0;
        comm = NULL;
        receiving = false;
    }
    ~CBroadcaster()
    {
        clear();
    }
    unsigned target(unsigned i, unsigned node)
    {
        unsigned n = node;
        unsigned j=0;
        while (n) {
            j++;
            n /= 2;
        }
        unsigned res = ((1<<(i+j))+node)+1;
        if (res == broadcastSlave)
            res = 1;
        return res;
    }
    void init(unsigned _self, unsigned _numnodes, ICommunicator *_comm, mptag_t _mpTag, unsigned _broadcastSlave)
    {
        nodeindex = _self;
        numnodes = _numnodes;
        comm = _comm;
        mpTag = _mpTag;
        broadcastSlave = _broadcastSlave;
    }
    bool receive(MemoryBuffer &mb)
    {
#ifdef _TRACEBROADCAST
        ActPrintLog(activity, "Broadcast node %d Receiving on tag %d",nodeindex,(int)mpTag);
#endif
        CMessageBuffer msg;
        rank_t sender;
        BooleanOnOff onOff(receiving);
        if (comm->recv(msg, RANK_ALL, mpTag, &sender))
        {
#ifdef _TRACEBROADCAST
            ActPrintLog(activity, "Broadcast node %d Received %d from %d",nodeindex, msg.length(), sender);
#endif
            try
            {
                mb.swapWith(msg);
                msg.clear(); // send empty reply
#ifdef _TRACEBROADCAST
                ActPrintLog(activity, "Broadcast node %d reply to %d",nodeindex, sender);
#endif
                comm->reply(msg);
                if (aborted) 
                    return false;
#ifdef _TRACEBROADCAST
                ActPrintLog(activity, "Broadcast node %d Received %d",nodeindex, mb.length());
#endif
            }
            catch (IException *e)
            {
                ActPrintLog(activity, e, "CBroadcaster::recv(2): exception");
                throw;
            }
        }
#ifdef _TRACEBROADCAST
        ActPrintLog(activity, "receive done");
#endif
        return (0 != mb.length());
    }
    void cancelReceive()
    {
        ActPrintLog(activity, "CBroadcaster::cancelReceive");
        if (comm && receiving)
            comm->cancel(RANK_ALL, mpTag);
    }
    void broadcast(MemoryBuffer &buffer)
    {
        clear();
        broadcasting.swapWith(buffer);
        doBroadcast();
    }
    void doBroadcast()
    {
        try
        {
            unsigned i = 0;
            unsigned n;
            if (1 == nodeindex)
                n = broadcastSlave-1;
            else if (broadcastSlave==nodeindex)
                n = 0;
            else
                n = nodeindex-1;
            loop {
                unsigned t = target(i++,n);
                if (t>numnodes)
                    break;
                if (t != broadcastSlave)
                {
#ifdef _TRACEBROADCAST
                    ActPrintLog(activity, "Broadcast node %d Sending to node %d size %d",nodeindex,t,broadcasting.length());
#endif
                    mptag_t rt = createReplyTag();
                    broadcasting.setReplyTag(rt); // simulate sendRecv
                    comm->send(broadcasting, t, mpTag);     
                    CMessageBuffer rMsg;
                    comm->recv(rMsg, t, rt);                    
#ifdef _TRACEBROADCAST
                    ActPrintLog(activity, "Broadcast node %d Sent to node %d size %d received back %d",nodeindex,t,broadcasting.length(),rMsg.length());
#endif
                }
            }
        }
        catch (IException *e)
        {
            ActPrintLog(activity, e, "CBroadcaster::broadcast exception");
            throw;
        }
#ifdef _TRACEBROADCAST
        ActPrintLog(activity, "do broadcast done done");
#endif
    }
    void endBroadcast()
    {
        clear();
        doBroadcast();
    }
    void clear()
    {
        broadcasting.clear();
    }   
};

/* 
    This activity loads the RIGHT hand stream into the hash table, therefore
    the right hand stream -should- contain the fewer records

    Inner, left outer and left only joins supported

*/

class CLookupJoinActivity : public CSlaveActivity, public CThorDataLink, implements ISmartBufferNotify
{
    IHThorHashJoinArg *hashJoinHelper;
    IHThorAllJoinArg *allJoinHelper;
    const void **rhsTable;
    unsigned rhsTableLen;
    unsigned rhsRows;
    IHash *leftHash, *rightHash;
    ICompare *compareRight, *compareLeftRight;

    Owned<IThorDataLink> right;
    Owned<IThorDataLink> left;
    Owned<IEngineRowAllocator> rightAllocator;
    Owned<IEngineRowAllocator> leftAllocator;
    Owned<IEngineRowAllocator> allocator;
    Owned<IOutputRowSerializer> rightSerializer;
    bool gotRHS;
    join_t joinType;
    OwnedConstThorRow defaultRight;
    OwnedConstThorRow leftRow;
    CBroadcaster broadcaster;
    Owned<IException> leftexception;
    Semaphore leftstartsem;
    CThorRowArray rhs;
    bool eos;
    unsigned flags;
    bool exclude;
    unsigned candidateMatches, abortLimit, atMost;
    unsigned broadcastSlave;
    ConstPointerArray candidates;
    unsigned candidateIndex;
    const void *rhsNext;
    Owned<IOutputMetaData> outputMeta;

    // AllJoin only
    unsigned nextRhsRow;
    unsigned keepLimit;
    unsigned joined;
    OwnedConstThorRow defaultLeft;
    Owned<IBitSet> rightMatchSet;

    unsigned lastRightOuter;
    bool doRightOuter;
    bool eog, someSinceEog, leftMatch, grouped;
    Semaphore gotOtherROs;
    bool waitForOtherRO, fuzzyMatch, returnMany;

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

protected:
    joinkind_t joinKind;
    StringAttr joinStr;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLookupJoinActivity(CGraphElementBase *_container, joinkind_t _joinKind) 
        : CSlaveActivity(_container), CThorDataLink(this), joinKind(_joinKind), broadcaster(this, abortSoon)
    {
        gotRHS = false;
        joinType = JT_Undefined;
        nextRhsRow = 0;
        rhsNext = NULL;
        returnMany = false;
        candidateMatches = 0;
        atMost = 0;
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
                // code gen should spot invalid constants on KEEP with LOOKUP (without MANY)
                break;
            }
            default:
                assertex(!"Unexpected join kind");
        }
        fuzzyMatch = 0 != (JFmatchrequired & flags);
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
        broadcastSlave = 1; // first node collects local, then nodes 2->n ordered

        StringBuffer str;
        ActPrintLog("%s: Join type is %s, broadcastSlave=%d", joinStr.get(), getJoinTypeStr(str).str(), broadcastSlave);
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
        rhs.setSizing(true, true);
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
        broadcaster.cancelReceive();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
    virtual void stop()
    {
        rhs.reset(false);
        stopRightInput();
        stopInput(left);
        dataLinkStop();
        left.clear();
        right.clear();
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
        {
            getRHS();
            gotRHS = true;
        }
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
    void sendToBroadcastSlave()
    {
#ifdef _TRACEBROADCAST
        ActPrintLog("%s: sendToBroadcastSlave sending to slave %d", joinStr.get(), broadcastSlave);
#endif
        try
        {
            MemoryBuffer mb;
            CMemoryRowSerializer mbs(mb);

            unsigned r=0;
            while (!abortSoon)
            {
                while (r!=rhs.ordinality())
                {
                    const byte *row = (const byte *)rhs.item(r++);
                    rightSerializer->serialize(mbs, row);
                    if (mb.length() > 0x80000)
                        break;
                }
                CMessageBuffer msg;
                if (!receiveMsg(msg, broadcastSlave, mpTag))
                    return;
                if (0 != mb.length())
                {
                    ThorCompress(mb.toByteArray(), mb.length(), msg.clear());
#ifdef _TRACEBROADCAST
                    ActPrintLog("sendToBroadcastSlave Compressing buf from %d to %d",mb.length(),msg.length());
#endif
#ifdef _TRACEBROADCAST
                    ActPrintLog("sendToBroadcastSlave sending reply to %d on tag %d",(int)broadcastSlave,(int)mpTag);
#endif
                }
                if (!container.queryJob().queryJobComm().reply(msg))
                    return;
                if (0 == mb.length())
                    break;
                mb.clear();
            }
#ifdef _TRACEBROADCAST
            ActPrintLog("%s: sendToBroadcastSlave sent", joinStr.get());
#endif
        }
        catch (IException *e)
        {
            ActPrintLog(e, "CLookupJoinActivity::sendToBroadcastSlave: exception");
            throw;
        }
    }
    void processRows(MemoryBuffer &mb)
    {
        Linked<IEngineRowAllocator> allocator = ::queryRowAllocator(right);
        Linked<IOutputRowDeserializer> deserializer = ::queryRowDeserializer(right);
        CThorStreamDeserializerSource memDeserializer(mb.length(), mb.toByteArray());
        while (!memDeserializer.eos())
        {
            RtlDynamicRowBuilder rowBuilder(allocator);
            size32_t sz = deserializer->deserialize(rowBuilder, memDeserializer);
            OwnedConstThorRow fRow = rowBuilder.finalizeRowClear(sz);
            rhs.append(fRow.getClear()); // will throw IThorRowArrayException if full
        }
    }
    void gatherLocal()
    {
        while (!abortSoon)
        {
            OwnedConstThorRow rhsRow = right->ungroupedNextRow();
            if (!rhsRow)
                break;
            rhs.append(rhsRow.getClear()); // will throw IThorRowArrayException if full
        }
#ifdef STOPRIGHT_ASAP
        stopRightInput();
#endif
    }
    void getRHS()
    {
        Owned<IException> exception;
        try
        {
            gatherLocal();
            if (!container.queryLocal() && container.queryJob().querySlaves() > 1)
            {
                broadcaster.init(container.queryJob().queryMyRank(), container.queryJob().querySlaves(), &container.queryJob().queryJobComm(), mpTag, broadcastSlave);
                if (container.queryJob().queryMyRank()==broadcastSlave)
                {
                    unsigned fromNode = 1;
                    Owned<IBitSet> slavesDone = createBitSet();

                    MemoryBuffer tmp;
                    CMessageBuffer msg;
                    bool allDone = false;
                    while (!abortSoon)
                    {
#ifdef _TRACEBROADCAST
                        ActPrintLog("getRHS Receiving");
#endif
                        if (fromNode == broadcastSlave)
                        {
                            slavesDone->testSet(broadcastSlave-1, true);
                            ++fromNode;
                        }
                        {
                            BooleanOnOff onOff(receiving);
                            container.queryJob().queryJobComm().sendRecv(msg, fromNode, mpTag);
                        }
#ifdef _TRACEBROADCAST
                        ActPrintLog("getRHS got %d from %d",msg.length(),(int)fromNode);
#endif
                        if (0 == msg.length())
                        {
                            bool done = slavesDone->testSet(fromNode-1, true);
                            assertex(false == done);
                            if (slavesDone->scan(0, false) == container.queryJob().querySlaves()) // i.e. got all
                                allDone = true;
                            ++fromNode;
                        }
                        else
                        {
                            ThorExpand(msg, tmp);
#ifdef _TRACEBROADCAST
                            ActPrintLog("getRHS expanding.1 %d to %d",msg.length(), tmp.length());
#endif
                            msg.clear();
                            processRows(tmp);
                            tmp.clear();
                        }
                        if (allDone)
                            break;
                    }

                    // now all (global) rows in this (broadcast node) rhs HT.
                    CMemoryRowSerializer mbs(tmp.clear());
                    allDone = false;
                    unsigned r=0;
                    while (!abortSoon)
                    {
                        loop
                        {
                            if (r == rhs.ordinality())
                            {
                                allDone = true;
                                break;
                            }
                            const byte *row = (const byte *)rhs.item(r++);
                            rightSerializer->serialize(mbs, row);
                            if (tmp.length() > 0x80000)
                                break;
                        }
                        if (0 != tmp.length())
                        {
                            ThorCompress(tmp, msg);
#ifdef _TRACEBROADCAST
                            ActPrintLog("getRHS compress.1 %d to %d",tmp.length(), msg.length());
#endif
                            tmp.clear();

                            broadcaster.broadcast(msg);
                            msg.clear();
                        }
                        if (allDone)
                            break;
                    }
                }
                else
                {
                    sendToBroadcastSlave();
                    rhs.clear();
                    MemoryBuffer buf;
                    MemoryBuffer expBuf;
                    while (broadcaster.receive(buf))
                    {
                        ThorExpand(buf, expBuf);
#ifdef _TRACEBROADCAST
                        ActPrintLog("Expanding received buf from %d to %d",buf.length(),expBuf.length());
#endif
                        processRows(expBuf);
                        expBuf.clear();
                        broadcaster.broadcast(buf); // will swap buf
                        buf.clear();
                    }
                }
                broadcaster.endBroadcast();     // send final
                broadcaster.clear();
            }
            prepareRHS();
        }
        catch (IOutOfMemException *e) { exception.setown(e); }
        catch (IThorRowArrayException *e) { exception.setown(e); }
        if (exception.get())
        {
            StringBuffer errStr(joinStr);
            errStr.append("(").append(container.queryId()).appendf(") right-hand side is too large (%"I64F"u bytes in %d rows) for %s : (",(unsigned __int64) rhs.totalSize(),rhs.ordinality(),joinStr.get());
            errStr.append(exception->errorCode()).append(", ");
            exception->errorMessage(errStr);
            errStr.append(")");
            IException *e2 = MakeActivityException(this, TE_TooMuchData, errStr.str());
            ActPrintLog(e2, NULL);
            throw e2;
        }
    }
    void prepareRHS()
    {
        // first count records (a bit slow for variable)
        rhsRows = rhs.ordinality();
        rhsTableLen = rhsRows*4/3+16;  // could go bigger if room (or smaller if not)
        if (isAll())
        {
            rhsTable = (const void **)rhs.base();
            ActPrintLog("ALLJOIN rhs table: %d elements", rhsRows);
        }
        else // lookup, or all join with some hard matching.
        {
            unsigned htTable = rhsRows;
            rhs.reserve(rhsTableLen); // will throw IThorRowArrayException if full

            unsigned count = 0;
            unsigned dup = 0;

            bool maySkip = 0 != (flags & JFtransformMaySkip);
            bool dedup = compareRight && !maySkip && !fuzzyMatch && (!returnMany || 1==keepLimit);
            for (unsigned i=0;i<rhsRows;i++)
            {
                OwnedConstThorRow p = rhs.itemClear(i);
                unsigned h = htTable+rightHash->hash(p.get())%rhsTableLen;
                loop
                {
                    const byte *e = rhs.item(h);
                    if (!e)
                    {
                        rhs.setRow(h, p.getClear());
                        count++;
                        break;
                    }
                    if (dedup && 0 == compareRight->docompare(e,p))
                    {
                        dup++;
                        break; // implicit dedup
                    }
                    h++;
                    if (h>=htTable+rhsTableLen)
                        h = htTable;
                }
            }
            rhsTable = (const void **)rhs.base()+htTable;
            ActPrintLog("LOOKUPJOIN hash table created: %d elements %d duplicates",count,dup);
        }
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
