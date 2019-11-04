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


#include "jiface.hpp"
#include "slave.hpp"
#include "thbufdef.hpp"

#include "thiterateslave.ipp"

class IterateSlaveActivityBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    OwnedConstThorRow first;
protected:
    IThorRowInterfaces *inrowif = nullptr;
    bool global;
    bool eof, nextPut;
    rowcount_t count;

public:
    IterateSlaveActivityBase(CGraphElementBase *_container, bool _global) : CSlaveActivity(_container)
    {
        global = _global;
        if (!global)
            setRequireInitData(false);
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (global)
            mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    const void *getFirst() // for global, not called on 1st slave
    {
        CMessageBuffer msg;
        if (!queryJobChannel().queryJobComm().recv(msg, queryJobChannel().queryMyRank()-1, mpTag)) // previous node
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
        if (global && !lastNode())
        {
            CMessageBuffer msg;
            msg.append(count);
            if (prev)
            {
                CMemoryRowSerializer msz(msg);
                inrowif->queryRowSerializer()->serialize(msz, (const byte *)prev);
            }
            if (!queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag)) // to next
                return;
        }
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        if (global) // only want lookahead if global (hence serial)
        {
            if (ensureStartFTLookAhead(0))
                setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), ENTH_SMART_BUFFER_SIZE, true, false, RCUNBOUND, NULL, &container.queryJob().queryIDiskUsage()), false);
        }
        count = 0;
        eof = nextPut = false;
    }
    virtual void stop() override
    {
        if (hasStarted())
        {
            if (global)
                putNext(NULL);
        }
        PARENT::stop();
    }
    virtual bool isGrouped() const override { return false; }
};

class IterateSlaveActivity : public IterateSlaveActivityBase
{
    IHThorIterateArg *helper;
    OwnedConstThorRow prev;

public:

    IterateSlaveActivity(CGraphElementBase *_container, bool _global) 
        : IterateSlaveActivityBase(_container,_global)
    {
        helper = static_cast <IHThorIterateArg *> (queryHelper());
        inrowif = this;
    }
    virtual void start() override
    {
        IterateSlaveActivityBase::start();
        prev.clear();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;) {
            if (eof || abortSoon)
                break;
            
            if (!prev) {
                if (!global || firstNode()) {
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
            OwnedConstThorRow next = inputStream->ungroupedNextRow();
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

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canBufferInput = true;
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
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
    Owned<IThorRowInterfaces> rightOutputRowIf;
    IEngineRowAllocator *rightRowAllocator = nullptr;

public:

    CProcessSlaveActivity(CGraphElementBase *_container, bool _global) 
        : IterateSlaveActivityBase(_container,_global)
    {
        helper = static_cast <IHThorProcessArg *> (queryHelper());
        rightOutputRowIf.setown(createRowInterfaces(helper->queryRightRecordSize()));
        inrowif = rightOutputRowIf;
        rightRowAllocator = rightOutputRowIf->queryRowAllocator();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;) {
            if (eof || abortSoon)
                break;
            if (!left) {
                if (!global || firstNode()) {
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
            left.setown(inputStream->ungroupedNextRow());
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

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canBufferInput = true;
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
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


class CChildIteratorSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    // only for toplevel activity 
    IHThorChildIteratorArg * helper;
    bool eof;
    rowcount_t count;

public:
    CChildIteratorSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorChildIteratorArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eof = !container.queryLocalOrGrouped() && !firstNode();
        count = 0;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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

    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
    }
};

class CLinkedRawIteratorSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    // only for toplevel activity 
    IHThorLinkedRawIteratorArg * helper;
    bool dohere, grouped;

public:
    CLinkedRawIteratorSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorLinkedRawIteratorArg *> (queryHelper());
        grouped = helper->queryOutputMeta()->isGrouped();
        setRequireInitData(false);
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        dohere = container.queryLocalOrGrouped() || firstNode();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (dohere)
        {
            OwnedConstThorRow row;
            row.set(helper->next()); // needs linking allegedly
            if (row.get())
            {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;
    }
    virtual bool isGrouped() const override { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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


class CStreamedIteratorSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorStreamedIteratorArg *helper;
    Owned<IRowStream> rows;
    bool eof;

public:
    CStreamedIteratorSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorStreamedIteratorArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start() override
    {
        PARENT::start();
        bool isLocal = container.queryLocalData() || container.queryOwner().isLocalChild();
        eof = isLocal ? false : !firstNode();
        if (!eof)
            rows.setown(helper->createInput());
    }
    virtual void stop() override
    {
        if (rows)
        {
            rows->stop();
            rows.clear();
        }
        PARENT::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eof || abortSoon)
            return NULL;
        assertex(rows);
        const void * next = rows->nextRow();
        if (next)
            dataLinkIncrement();
        return next;
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
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
