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

// Handling Rollup and Dedup

#include "throllupslave.ipp"
#include "thactivityutil.ipp"
#include "thorport.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"

class CDedupAllHelper : public CSimpleInterface, implements IRowStream
{
    CActivityBase *activity;

    unsigned dedupCount;
    const void ** dedupArray;
    unsigned dedupIdx;
    IThorDataLink * in;
    IHThorDedupArg * helper;
    bool keepLeft;
    bool * abort;
    IStopInput *iStopInput;

    Owned<IThorRowLoader> rowLoader;
    CThorExpandingRowArray rows;

    void remove(unsigned idx)
    {
        OwnedConstThorRow row = rows.getClear(idx); // discard
    }
    void dedupAll()
    {
        unsigned idxL, idxR;
        for(idxL = 0; idxL < dedupCount; idxL++)
        {
            const void * left = dedupArray[idxL];
            if(left)
            {
                for(idxR = 0; idxR < dedupCount; idxR++)
                {
                    if((*abort))
                    {
                        dedupIdx = dedupCount = 0;                          
                        return;
                    }
                    const void * right = dedupArray[idxR];
                    if((idxL != idxR) && right)
                    {
                        if(helper->matches(left, right))
                        {   
                            if(keepLeft)
                            {                               
                                remove(idxR);
                            }
                            else
                            {
                                remove(idxL);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDedupAllHelper(CActivityBase *_activity) : activity(_activity), rows(*_activity, _activity)
    {
        in = NULL;
        helper = NULL;
        abort = NULL;
        dedupIdx = dedupCount = 0;
        dedupArray = NULL;
        iStopInput = NULL;
        keepLeft = true;
        rowLoader.setown(createThorRowLoader(*activity, NULL, stableSort_none, rc_allMem));
    }

    void init(IThorDataLink * _in, IHThorDedupArg * _helper, bool _keepLeft, bool * _abort, IStopInput *_iStopInput)
    {
        in = _in;
        helper = _helper;
        keepLeft = _keepLeft;
        abort = _abort;
        iStopInput = _iStopInput;

        assertex(in);
        assertex(helper);
        assertex(abort);

        dedupIdx = dedupCount = 0;
    }
    bool calcNextDedupAll(bool groupOp)
    {
#if THOR_TRACE_LEVEL >=5
        ActPrintLog(activity, "DedupAllHelper::calcNextDedupAll");
#endif
        dedupIdx = 0;
        rows.kill();

        // JCSMORE - could do in chunks and merge if > mem
        Owned<IRowStream> rowStream = groupOp ? rowLoader->loadGroup(in, activity->queryAbortSoon(), &rows) : rowLoader->load(in, activity->queryAbortSoon(), false, &rows);
        dedupCount = rows.ordinality();
        ActPrintLog(activity, "DEDUP: rows loaded = %d",dedupCount);

        if (iStopInput)
            iStopInput->stopInput();

        if (0 == dedupCount)
        {
            dedupArray = NULL;
            return false;
        }
        dedupArray = rows.getRowArray();
        dedupAll();
        return true;
    }
// IRowStream
    virtual const void *nextRow()
    {
        while(dedupIdx < dedupCount)
        {
            OwnedConstThorRow r = rows.getClear(dedupIdx++);
            if(r)
                return r.getClear();
        }
        return NULL;
    }
    virtual void stop() { }
};

class CDedupRollupBaseActivity : public CSlaveActivity, implements IStopInput
{
    bool rollup;
    CriticalSection stopsect;
    Linked<IRowInterfaces> rowif;

protected:
    bool eogNext, eos;
    bool global;
    bool groupOp;
    OwnedConstThorRow kept;
    OwnedConstThorRow keptTransformed; // only used by rollup
    Owned<IThorDataLink> input;
    bool needFirstRow;

    unsigned numKept; // not used by rollup
public:
    CDedupRollupBaseActivity(CGraphElementBase *container, bool _rollup, bool _global, bool _groupOp) 
        : CSlaveActivity(container)
    {
        rollup = _rollup;
        global = _global;
        groupOp = _groupOp;
    }
    virtual void stopInput()
    {
        CriticalBlock block(stopsect);  // can be called async by distribute
        if (input)
        {
            CSlaveActivity::stopInput(input);
            input.clear();
        }
    }
    void start()
    {
        needFirstRow = true;
        input.set(inputs.item(0));
        rowif.set(queryRowInterfaces(input));
        eogNext = eos = false;
        numKept = 0;
        if (global)
            input.setown(createDataLinkSmartBuffer(this, input,rollup?ROLLUP_SMART_BUFFER_SIZE:DEDUP_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(this),false,RCUNBOUND,NULL,false,&container.queryJob().queryIDiskUsage())); // only allow spill if input can stall
        startInput(input); 
    }   
    void stop()
    {
        stopInput();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        if (global) {
            info.canBufferInput = true;
            info.isSequential = true;
        }
        info.canReduceNumRows = true;
    }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        if (global)
            mpTag = container.queryJobChannel().deserializeMPTag(data); // only used for global acts
    }
    inline void checkFirstRow()
    {
        if (needFirstRow)
        {
            needFirstRow = false;
            if (global && !firstNode())
            {
                CMessageBuffer msg;
                if (!receiveMsg(msg, queryJobChannel().queryMyRank()-1, mpTag)) // from previous node
                    return;
                msg.read(numKept);
                size32_t rowLen;
                msg.read(rowLen);
                if (rowLen)
                    kept.deserialize(rowif, rowLen, msg.readDirect(rowLen));
                if (rollup)
                {
                    msg.read(rowLen);
                    if (rowLen)
                        keptTransformed.deserialize(rowif, rowLen, msg.readDirect(rowLen));
                    else
                        keptTransformed.set(kept);
                }
                if (kept)
                    return;
            }
            kept.setown(input->nextRow());
            if (!kept && global)
                putNextKept(); // pass on now
            if (rollup)
                keptTransformed.set(kept);
        }
    }
    bool putNextKept()
    {
        assertex(global);
        eos = true;
        if (lastNode())
            return false;
        CMessageBuffer msg;
        msg.append(numKept);
        DelayedSizeMarker sizeMark(msg);
        if (kept.get())
        {
            CMemoryRowSerializer msz(msg);
            rowif->queryRowSerializer()->serialize(msz,(const byte *)kept.get());
            sizeMark.write();
            if (rollup)
            {
                sizeMark.restart();
                if (kept.get()!=keptTransformed.get())
                {
                    rowif->queryRowSerializer()->serialize(msz,(const byte *)keptTransformed.get());
                    sizeMark.write();
                }
            }
        }
        else if (rollup)
            sizeMark.restart(); // write (0 size) for keptTransformed row
        queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag); // to next node
        return true;
    }
    virtual void abort()
    {
        if (global)
            cancelReceiveMsg(queryJobChannel().queryMyRank(), mpTag);
    }
};

class CDedupBaseSlaveActivity : public CDedupRollupBaseActivity, public CThorDataLink
{
protected:
    IHThorDedupArg *ddhelper;
    bool keepLeft;
    unsigned numToKeep;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDedupBaseSlaveActivity(CGraphElementBase *_container, bool global, bool groupOp)
        : CDedupRollupBaseActivity(_container, false, global, groupOp), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDedupRollupBaseActivity::init(data, slaveData);
        appendOutputLinked(this);   // adding 'me' to outputs array
        ddhelper = static_cast <IHThorDedupArg *>(queryHelper());
        keepLeft = ddhelper->keepLeft();
        numToKeep = ddhelper->numToKeep();
        assertex(keepLeft || numToKeep == 1);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        CDedupRollupBaseActivity::start();
        dataLinkStart();
    }
    virtual void stop()
    {
        CDedupRollupBaseActivity::stop();
        dataLinkStop();
    }
    virtual bool isGrouped() { return groupOp; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        CDedupRollupBaseActivity::getMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
    }
};

class CDedupSlaveActivity : public CDedupBaseSlaveActivity
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDedupSlaveActivity(CGraphElementBase *_container, bool global, bool groupOp)
        : CDedupBaseSlaveActivity(_container, global, groupOp)
    {
    }
    inline bool eog()
    {
        if (abortSoon)
            return true;
        else if (groupOp)
        {
            if (eogNext)
            {
                eogNext = false;
                return true;
            }
        }
        return false;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eos)
            return NULL;
        checkFirstRow();

        if (eog())
            return NULL;

        if (!kept && !groupOp)
            return NULL;
        OwnedConstThorRow next;
        loop
        {
            next.setown(input->nextRow());
            if (!next)
            {
                if (!groupOp)
                    next.setown(input->nextRow());
                if (!next)
                {
                    if (global&&putNextKept()) // send kept to next node
                        return NULL;
                }
                numKept = 0;
                break; // return kept
            }
            if (!kept)           // NH was && groupOp but seemed wrong
            {
                numKept = 0;
                break;
            }
            if (!ddhelper->matches(kept, next))
            {
                numKept = 0;
                break;
            }
            if(numKept < numToKeep - 1)
            {
                numKept++;
                break;
            }
            if (!keepLeft) 
                kept.setown(next.getClear());
        }
        OwnedConstThorRow ret = kept.getClear();
        kept.setown(next.getClear());
        if (ret)
        {
            dataLinkIncrement();
            return ret.getClear();
        }
        return NULL;
    }
};

class CDedupAllSlaveActivity : public CDedupBaseSlaveActivity
{
    Owned<CDedupAllHelper> dedupHelper;
    bool lastEog;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDedupAllSlaveActivity(CGraphElementBase *_container, bool groupOp)
        : CDedupBaseSlaveActivity(_container, false, groupOp)
    {
        lastEog = false;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDedupBaseSlaveActivity::init(data, slaveData);
        assertex(1 == numToKeep);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        CDedupBaseSlaveActivity::start();

        lastEog = false;
        assertex(!global);      // dedup(),local,all only supported
        dedupHelper.setown(new CDedupAllHelper(this));
        dedupHelper->init(input, ddhelper, keepLeft, &abortSoon, groupOp?NULL:this);
        dedupHelper->calcNextDedupAll(groupOp);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eos)
            return NULL;

        loop
        {
            OwnedConstThorRow row = dedupHelper->nextRow();
            if (row)
            {
                lastEog = false;
                dataLinkIncrement();
                return row.getClear();
            }
            else
            {
                if (!groupOp || !dedupHelper->calcNextDedupAll(true))
                    eos = true;
                return NULL;
            }
        }
    }
};

class CRollupSlaveActivity : public CDedupRollupBaseActivity, public CThorDataLink
{
private:
    IHThorRollupArg * ruhelper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRollupSlaveActivity(CGraphElementBase *_container, bool global, bool groupOp) 
        : CDedupRollupBaseActivity(_container, true, global, groupOp), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDedupRollupBaseActivity::init(data, slaveData);
        appendOutputLinked(this);   // adding 'me' to outputs array
        ruhelper = static_cast <IHThorRollupArg *>  (queryHelper());
    }
    inline bool eog()
    {
        if (abortSoon)
            return true;
        else if (groupOp)
        {
            if (eogNext)
            {
                eogNext = false;
                return true;
            }
        }
        else if (!kept)
            return true;
        return false;
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        CDedupRollupBaseActivity::start();
        dataLinkStart();
    }
    virtual void stop()
    {
        if (global && !eos) // if stopped early, ensure next informed
        {
            kept.clear();
            keptTransformed.clear();
            putNextKept();
        }
        CDedupRollupBaseActivity::stop();
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (eos) return NULL;
        checkFirstRow();
        if (eog())
            return NULL;
        OwnedConstThorRow next;
        loop
        {
            next.setown(input->nextRow());
            if (!next)
            {
                if (!groupOp)
                    next.setown(input->nextRow());
                if (!next)
                {
                    if (global&&putNextKept()) // send kept to next node
                        return NULL;
                    break;
                }
            }
            else if (!kept)  // NH was && groupOp but seemed wrong
                break;
            if (!ruhelper->matches(kept, next))
                break;
            RtlDynamicRowBuilder ret(queryRowAllocator());
            unsigned thisSize = ruhelper->transform(ret, keptTransformed, next);
            if (thisSize)
                keptTransformed.setown(ret.finalizeRowClear(thisSize));

            if (ruhelper->getFlags() & RFrolledismatchleft)
                kept.set(keptTransformed);
            else
                kept.setown(next.getClear());
        }
        OwnedConstThorRow row = keptTransformed.getClear();
        kept.setown(next.getClear());
        keptTransformed.set(kept);
        if (row)
        {
            dataLinkIncrement();
            return row.getClear();
        }
        return NULL;
    }
    virtual bool isGrouped() { return groupOp; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        CDedupRollupBaseActivity::getMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
    }
};

class CRollupGroupSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorRollupGroupArg *helper;
    Owned<IThorRowLoader> groupLoader;
    bool eoi;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRollupGroupSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        eoi = false;
        input = NULL;
        helper = NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorRollupGroupArg *)queryHelper();
        appendOutputLinked(this);   // adding 'me' to outputs array
        groupLoader.setown(createThorRowLoader(*this, NULL, stableSort_none, rc_allMem));
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        input = inputs.item(0);
        eoi = false;
        startInput(input);
        dataLinkStart();
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!eoi)
        {
            CThorExpandingRowArray rows(*this, queryRowInterfaces(input));
            Owned<IRowStream> rowStream = groupLoader->loadGroup(input, abortSoon, &rows);
            unsigned count = rows.ordinality();
            if (count)
            {
                RtlDynamicRowBuilder row(queryRowAllocator());
                size32_t sz = helper->transform(row, count, rows.getRowArray());
                rows.kill();
                if (sz)
                {
                    dataLinkIncrement();
                    return row.finalizeRowClear(sz);
                }
            }
        }
        eoi = true;
        return NULL;
    }
    virtual bool isGrouped() { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
        info.canReduceNumRows = true;
    }
};


CActivityBase *createDedupSlave(CGraphElementBase *container)
{
    return new CDedupSlaveActivity(container, true, false);
}

CActivityBase *createLocalDedupSlave(CGraphElementBase *container)
{
    IHThorDedupArg *ddhelper = static_cast <IHThorDedupArg *>(container->queryHelper());
    if (ddhelper->compareAll())
        return new CDedupAllSlaveActivity(container, false);
    else
        return new CDedupSlaveActivity(container, false, false);
}

CActivityBase *createGroupDedupSlave(CGraphElementBase *container)
{
    IHThorDedupArg *ddhelper = static_cast <IHThorDedupArg *>(container->queryHelper());
    if (ddhelper->compareAll())
        return new CDedupAllSlaveActivity(container, true);
    else
        return new CDedupSlaveActivity(container, false, true);
}

CActivityBase *createRollupSlave(CGraphElementBase *container)
{
    return new CRollupSlaveActivity(container, true, false);
}

CActivityBase *createLocalRollupSlave(CGraphElementBase *container)
{
    return new CRollupSlaveActivity(container, false, false);
}

CActivityBase *createGroupRollupSlave(CGraphElementBase *container)
{
    return new CRollupSlaveActivity(container, false, true);
}

CActivityBase *createRollupGroupSlave(CGraphElementBase *container)
{
    return new CRollupGroupSlaveActivity(container);
}


