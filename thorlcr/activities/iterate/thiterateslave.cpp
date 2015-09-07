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
                ::queryRowSerializer(input)->serialize(msz, (const byte *)prev);
            }
            if (!queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag)) // to next
                return;
        }
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
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
        dataLinkStart();
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
        ActivityTimer t(totalCycles, timeActivities);
        loop {
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
        rightRowAllocator.setown(queryJob().getRowAllocator(helper->queryRightRecordSize(),queryActivityId()));
        IterateSlaveActivityBase::init(data,slaveData);
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        loop {
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
        ActivityTimer s(totalCycles, timeActivities);
        eof = !container.queryLocalOrGrouped() && !firstNode();
        count = 0;
        dataLinkStart();
    }
    void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
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
        ActivityTimer s(totalCycles, timeActivities);
        dataLinkStart();
        dohere = container.queryLocalOrGrouped() || firstNode();
    }
    void stop()
    {
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (dohere) {
            OwnedConstThorRow row;
            row.set(helper->next()); // needs linking allegedly
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
    bool eof;

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
    }
    virtual void start()
    {
        bool isLocal = container.queryLocalData() || container.queryOwner().isLocalChild();
        eof = isLocal ? false : !firstNode();
        if (!eof)
            rows.setown(helper->createInput());
        dataLinkStart();
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
        ActivityTimer t(totalCycles, timeActivities);
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
