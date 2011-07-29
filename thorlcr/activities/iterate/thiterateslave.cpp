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


#include "thmem.hpp"
#include "jiface.hpp"
#include "slave.hpp"
#include "thbufdef.hpp"

#include "thiterateslave.ipp"

class IterateSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
    OwnedConstThorRow first;
protected:
    Owned<IThorDataLink> input;
    Owned<IRowInterfaces> inrowif;
    bool global;
    bool eof, nextPut;
    rowcount_t count;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    IterateSlaveActivityBase(CGraphElementBase *_container, bool _global) : CSlaveActivity(_container), CThorDataLink(this)
    {
        global = _global;
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        if (global)
            mpTag = container.queryJob().deserializeMPTag(data);
    }
    const void *getFirst() // for global, not called on 1st slave
    {
        CMessageBuffer msg;
        if (!container.queryJob().queryJobComm().recv(msg, container.queryJob().queryMyRank()-1, mpTag)) // previous node
            return NULL;
        msg.read(count);
        size32_t r = msg.remaining();
        OwnedConstThorRow firstRow;
        if (r)
            firstRow.deserialize(inrowif, r, msg.readDirect(r));
        return firstRow.getClear();
    }
    void putNext(const void *prev)
    {
        if (nextPut) return;
        nextPut = true;
        if (global && container.queryJob().queryMyRank()!=container.queryJob().querySlaves()) // is not last
        {
            CMessageBuffer msg;
            msg.append(count);
            if (prev)
            {
                CMemoryRowSerializer msz(msg);
                ::queryRowSerializer(input)->serialize(msz, (const byte *)prev);
            }
            if (!container.queryJob().queryJobComm().send(msg, container.queryJob().queryMyRank()+1, mpTag)) // to next
                return;
        }
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        count = 0;
        eof = nextPut = false;
        inrowif.set(::queryRowInterfaces(inputs.item(0)));
        if (global) // only want lookahead if global (hence serial)
            input.setown(createDataLinkSmartBuffer(this, inputs.item(0),ITERATE_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(this),false,RCUNBOUND,NULL,false,&container.queryJob().queryIDiskUsage())); // only allow spill if input can stall
        else
            input.set(inputs.item(0));
        try
        { 
            startInput(input); 
        }
        catch (IException *e)
        {
            ActPrintLog(e,"ITERATE");
            throw;
        }
        dataLinkStart("ITERATOR", container.queryId());

    }
    void stop()
    {
        if (global)
            putNext(NULL);
        stopInput(input);
        input.clear();
        dataLinkStop();
    }
    bool isGrouped() { return false; }
};

class IterateSlaveActivity : public IterateSlaveActivityBase
{
    IHThorIterateArg *helper;
    OwnedConstThorRow prev;

public:

    IterateSlaveActivity(CGraphElementBase *_container, bool _global) 
        : IterateSlaveActivityBase(_container,_global)
    {
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorIterateArg *> (queryHelper());
        IterateSlaveActivityBase::init(data,slaveData);
    }
    virtual void start()
    {
        IterateSlaveActivityBase::start();
        prev.clear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        loop {
            if (eof || abortSoon)
                break;
            
            if (!prev) {
                if (!global || (1==container.queryJob().queryMyRank())) {
                    // construct first row
                    RtlDynamicRowBuilder r(queryRowAllocator());
                    size32_t sz = helper->createDefault(r);
                    prev.setown(r.finalizeRowClear(sz));
                }
                else {
                    prev.setown(getFirst());
                    if (!prev) {
                        putNext(NULL); // send to next node (even though prev not got)
                        eof = true;
                        break;
                    }
                }
            }
            OwnedConstThorRow next = input->ungroupedNextRow();
            if (!next) {
                putNext(prev); // send to next node if applicable
                eof = true;
                break;
            }
            RtlDynamicRowBuilder ret(queryRowAllocator());
            size32_t sz = helper->transform(ret, prev, next, ++count);
            if (sz != 0) {
                dataLinkIncrement();
                prev.setown(ret.finalizeRowClear(sz));
                return prev.getLink();
            }
        }
        return NULL;
    }

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canBufferInput = true;
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
};


CActivityBase *createIterateSlave(CGraphElementBase *container)
{
    return new IterateSlaveActivity(container, true);
}

CActivityBase *createLocalIterateSlave(CGraphElementBase *container)
{
    return new IterateSlaveActivity(container, false);
}

class CProcessSlaveActivity : public IterateSlaveActivityBase
{
    IHThorProcessArg * helper;
    OwnedConstThorRow left;
    OwnedConstThorRow right;
    OwnedConstThorRow nextright;
    Owned<IEngineRowAllocator> rightRowAllocator;
public:

    CProcessSlaveActivity(CGraphElementBase *_container, bool _global) 
        : IterateSlaveActivityBase(_container,_global)
    {
    }

    ~CProcessSlaveActivity()
    {
    }

    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorProcessArg *> (queryHelper());
        rightRowAllocator.setown(createThorRowAllocator(helper->queryRightRecordSize(),queryActivityId()));
        IterateSlaveActivityBase::init(data,slaveData);
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        loop {
            if (eof || abortSoon)
                break;
            if (!left) {
                if (!global || (1==container.queryJob().queryMyRank())) {
                    // construct first row
                    RtlDynamicRowBuilder r(rightRowAllocator);  
                    size32_t sz = helper->createInitialRight(r);  
                    right.setown(r.finalizeRowClear(sz));
                }
                else {
                    right.setown(getFirst());
                    if (!right) { 
                        putNext(NULL); // send to right node (even though right not got)
                        eof = true;
                        break;
                    }
                }
            }
            left.setown(input->ungroupedNextRow());
            if (!left) {
                putNext(right); // send to next node 
                eof = true;
                break;
            }
            RtlDynamicRowBuilder nextl(queryRowAllocator());
            RtlDynamicRowBuilder nextr(rightRowAllocator);  
            size32_t thisSize = helper->transform(nextl, nextr, left, right, ++count);
            if (thisSize != 0) {
                nextright.setown(right.getClear());
                size32_t szr = helper->queryRightRecordSize()->getRecordSize(nextr.getSelf());  // better TBD?
                right.setown(nextr.finalizeRowClear(szr));
                dataLinkIncrement();
                size32_t szl = queryRowAllocator()->queryOutputMeta()->getRecordSize(nextl.getSelf());  // better TBD?
                left.setown(nextl.finalizeRowClear(szl));
                return left.getLink();
            }
        }
        return NULL;
    }

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canBufferInput = true;
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
};



CActivityBase *createProcessSlave(CGraphElementBase *container)
{
    return new CProcessSlaveActivity(container, true);
}

CActivityBase *createLocalProcessSlave(CGraphElementBase *container)
{
    return new CProcessSlaveActivity(container, false);
}


class CChildIteratorSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    // only for toplevel activity 
    IHThorChildIteratorArg * helper;
    bool eof;
    rowcount_t count;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CChildIteratorSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        helper = static_cast <IHThorChildIteratorArg *> (queryHelper());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eof = !container.queryLocalOrGrouped() && (container.queryJob().queryMyRank()>1);
        count = 0;
        dataLinkStart("CHILDITERATOR", container.queryId());
    }
    void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!eof) {
            if (count==0)
                eof = !helper->first();
            else
                eof = !helper->next();
            if (!eof) {
                RtlDynamicRowBuilder r(queryRowAllocator());
                size32_t sz = helper->transform(r);
                if (sz) {
                    count++;
                    dataLinkIncrement();
                    return r.finalizeRowClear(sz);
                }
            }
        }
        return NULL;
    }

    bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

//=====================================================================================================

class CRawIteratorSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorRawIteratorArg *helper;
    MemoryBuffer resultBuffer;
    Owned<ISerialStream> bufferStream;
    CThorStreamDeserializerSource rowSource;
    bool eogPending, grouped;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRawIteratorSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        bufferStream.setown(createMemoryBufferSerialStream(resultBuffer));
        rowSource.setStream(bufferStream);
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        helper = (IHThorRawIteratorArg *)queryHelper();
        grouped = helper->queryOutputMeta()->isGrouped(); // JCSMORE->GH - shouldn't this match graph info (it doesn't in groupchild.ecl)
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("RAWITERATOR", container.queryId());
        eogPending = false;
        size32_t len;
        const void *data;
        helper->queryDataset(len, data);
        resultBuffer.setBuffer(len, const_cast<void *>(data), false);
    }
    virtual void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);

        if (rowSource.eos())
            return NULL;
        if (eogPending)
        {
            eogPending = false;
            return NULL;
        }
        RtlDynamicRowBuilder rowBuilder(queryRowAllocator());
        size32_t sz = queryRowDeserializer()->deserialize(rowBuilder, rowSource);   
        if (grouped)
            rowSource.read(sizeof(bool), &eogPending);
        dataLinkIncrement();
        return rowBuilder.finalizeRowClear(sz);
    }
    bool isGrouped() { return grouped; } // JCSMORE - see note above
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

CActivityBase *createRawIteratorSlave(CGraphElementBase *container)
{
    return new CRawIteratorSlaveActivity(container);
}


class CLinkedRawIteratorSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    // only for toplevel activity 
    IHThorLinkedRawIteratorArg * helper;
    bool dohere, grouped;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLinkedRawIteratorSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        helper = static_cast <IHThorLinkedRawIteratorArg *> (queryHelper());
        grouped = helper->queryOutputMeta()->isGrouped();
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("LINKEDRAWITERATOR", container.queryId());
        dohere = container.queryLocalOrGrouped() || (1 == container.queryJob().queryMyRank());
    }
    void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (dohere) {
            OwnedConstThorRow row;
            row.set(helper->next()); // needs linking alledgedly
            if (row.get()) {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }
    bool isGrouped() { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
    }
};

CActivityBase *createLinkedRawIteratorSlave(CGraphElementBase *container)
{
    return new CLinkedRawIteratorSlaveActivity(container);
}


CActivityBase *createChildIteratorSlave(CGraphElementBase *container)
{
    return new CChildIteratorSlaveActivity(container);
}


class CStreamedIteratorSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorStreamedIteratorArg *helper;
    Owned<IRowStream> rows;
    bool eof, isLocal, isLocalCache;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CStreamedIteratorSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        helper = static_cast <IHThorStreamedIteratorArg *> (queryHelper());
        isLocalCache = isLocal = false;
    }
    virtual void start()
    {
        if (!isLocalCache)
        {
            isLocalCache = true;
            isLocal = container.queryOwnerId() && container.queryOwner().isLocalOnly();
        }
        eof = isLocal ? false : (container.queryJob().queryMyRank()>1);
        if (!eof)
            rows.setown(helper->createInput());
        dataLinkStart("STREAMEDITERATOR", container.queryId());
    }
    void stop()
    {
        if (rows)
        {
            rows->stop();
            rows.clear();
        }
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eof || abortSoon)
            return NULL;
        assertex(rows);
        const void * next = rows->nextRow();
        if (next)
            dataLinkIncrement();
        return next;
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        info.unknownRowsOutput = true;
    }
};

CActivityBase *createStreamedIteratorSlave(CGraphElementBase *container)
{
    return new CStreamedIteratorSlaveActivity(container);
}
