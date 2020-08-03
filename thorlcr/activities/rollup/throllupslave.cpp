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

class CDedupAllHelper : implements IRowStream, public CSimpleInterface
{
    CActivityBase *activity;

    unsigned dedupCount = 0;
    const void ** dedupArray = nullptr;
    unsigned dedupIdx = 0;
    IThorDataLink * in = nullptr;
    IEngineRowStream *inputStream = nullptr;
    IHThorDedupArg * helper = nullptr;
    bool keepLeft = true;
    bool * abort = nullptr;
    IStopInput *iStopInput = nullptr;

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
        rowLoader.setown(createThorRowLoader(*activity, NULL, stableSort_none, rc_allMem));
    }

    void init(IThorDataLink * _in, IEngineRowStream *_inputStream, IHThorDedupArg * _helper, bool _keepLeft, bool * _abort, IStopInput *_iStopInput)
    {
        assertex(_in);
        inputStream = _inputStream;
        helper = _helper;
        keepLeft = _keepLeft;
        abort = _abort;
        iStopInput = _iStopInput;

        assertex(helper);
        assertex(abort);

        dedupIdx = dedupCount = 0;
    }
    bool calcNextDedupAll(bool groupOp)
    {
        ActPrintLog(activity, thorDetailedLogLevel, "DedupAllHelper::calcNextDedupAll");
        dedupIdx = 0;
        rows.kill();

        // JCSMORE - could do in chunks and merge if > mem
        try
        {
            groupOp ? rowLoader->loadGroup(inputStream, activity->queryAbortSoon(), &rows) : rowLoader->load(inputStream, activity->queryAbortSoon(), false, &rows);
        }
        catch (IException *e)
        {
            if (!isOOMException(e))
                throw e;
            IOutputMetaData *inputOutputMeta = in->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
            throw checkAndCreateOOMContextException(activity, e, "loading group for dedup all", rowLoader->numRows(), inputOutputMeta, rowLoader->probeRow(0));
        }
        dedupCount = rows.ordinality();
        ActPrintLog(activity, thorDetailedLogLevel, "DEDUP: rows loaded = %d",dedupCount);

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
    typedef CSlaveActivity PARENT;

    bool rollup;
    Linked<IThorRowInterfaces> rowif;

protected:
    bool eogNext;
    bool eos = true; // until started
    bool global;
    bool groupOp;
    OwnedConstThorRow kept;
    OwnedConstThorRow keptTransformed; // only used by rollup
    bool needFirstRow;

    unsigned numKept; // not used by rollup
public:
    CDedupRollupBaseActivity(CGraphElementBase *_container, bool _rollup, bool _global, bool _groupOp)
        : CSlaveActivity(_container)
    {
        rollup = _rollup;
        global = _global;
        groupOp = _groupOp;
        if (!global)
            setRequireInitData(false);
    }
    virtual void start()
    {
        PARENT::start();
        if (global)
        {
            if (ensureStartFTLookAhead(0))
                setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), rollup?ROLLUP_SMART_BUFFER_SIZE:DEDUP_SMART_BUFFER_SIZE, ::canStall(input), false, RCUNBOUND, NULL, &container.queryJob().queryIDiskUsage()), false);
        }
        needFirstRow = true;
        rowif.set(queryRowInterfaces(input));
        eogNext = eos = false;
        numKept = 0;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        if (global)
        {
            info.canBufferInput = true;
            info.isSequential = true;
        }
        info.canReduceNumRows = true;
    }

    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
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
            kept.setown(inputStream->nextRow());
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
// IStopInput
    virtual void stopInput() override
    {
        PARENT::stopInput(0);
    }
};

class CDedupBaseSlaveActivity : public CDedupRollupBaseActivity
{
protected:
    IHThorDedupArg *ddhelper;
    ICompare *compareBest;
    bool keepLeft = 0;
    bool keepBest = 0;
    unsigned numToKeep = 0;

public:
    CDedupBaseSlaveActivity(CGraphElementBase *_container, bool global, bool groupOp)
        : CDedupRollupBaseActivity(_container, false, global, groupOp)
    {
        ddhelper = static_cast <IHThorDedupArg *>(queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDedupRollupBaseActivity::start();
        keepLeft = ddhelper->keepLeft();
        keepBest = ddhelper->keepBest();
        numToKeep = ddhelper->numToKeep();
        compareBest = ddhelper->queryCompareBest();
        assertex( (keepLeft || numToKeep == 1) && (!keepBest || numToKeep==1));
    }
    virtual bool isGrouped() const override { return groupOp; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        CDedupRollupBaseActivity::getMetaInfo(info);
        calcMetaInfoSize(info, queryInput(0));
    }
};

class CDedupSlaveActivity : public CDedupBaseSlaveActivity
{
public:
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
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eos)
            return NULL;
        checkFirstRow();

        if (eog())
            return NULL;

        if (!kept && !groupOp)
            return NULL;
        OwnedConstThorRow next;
        for (;;)
        {
            next.setown(inputStream->nextRow());
            if (!next)
            {
                if (!groupOp)
                    next.setown(inputStream->nextRow());
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
            if (!keepLeft || (keepBest && (compareBest->docompare(kept,next) > 0)))
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
    CDedupAllSlaveActivity(CGraphElementBase *_container, bool groupOp)
        : CDedupBaseSlaveActivity(_container, false, groupOp)
    {
        lastEog = false;
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDedupBaseSlaveActivity::start();
        assertex(1 == numToKeep);

        lastEog = false;
        assertex(!global);      // dedup(),local,all only supported
        dedupHelper.setown(new CDedupAllHelper(this));
        dedupHelper->init(input, inputStream, ddhelper, keepLeft, &abortSoon, groupOp?NULL:this);
        dedupHelper->calcNextDedupAll(groupOp);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eos)
            return NULL;

        for (;;)
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

class CRollupSlaveActivity : public CDedupRollupBaseActivity
{
private:
    IHThorRollupArg * ruhelper;

public:
    CRollupSlaveActivity(CGraphElementBase *_container, bool global, bool groupOp) 
        : CDedupRollupBaseActivity(_container, true, global, groupOp)
    {
        ruhelper = static_cast <IHThorRollupArg *>  (queryHelper());
        appendOutputLinked(this);   // adding 'me' to outputs array
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
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        CDedupRollupBaseActivity::start();
    }
    virtual void stop() override
    {
        if (global && !eos) // if stopped early, ensure next informed
        {
            kept.clear();
            keptTransformed.clear();
            putNextKept();
        }
        CDedupRollupBaseActivity::stop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eos) return NULL;
        checkFirstRow();
        if (eog())
            return NULL;
        OwnedConstThorRow next;
        for (;;)
        {
            next.setown(inputStream->nextRow());
            if (!next)
            {
                if (!groupOp)
                    next.setown(inputStream->nextRow());
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
    virtual bool isGrouped() const override { return groupOp; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        CDedupRollupBaseActivity::getMetaInfo(info);
        calcMetaInfoSize(info, queryInput(0));
    }
};

class CRollupGroupSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorRollupGroupArg *helper;
    Owned<IThorRowLoader> groupLoader;
    bool eoi;
    CThorExpandingRowArray rows;

public:
    CRollupGroupSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), rows(*this, NULL)
    {
        helper = (IHThorRollupGroupArg *)queryHelper();
        eoi = false;
        groupLoader.setown(createThorRowLoader(*this, NULL, stableSort_none, rc_allMem));
        setRequireInitData(false);
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eoi = false;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (eoi)
            return NULL;

        try
        {
            for (;;)
            {
                groupLoader->loadGroup(inputStream, abortSoon, &rows);
                unsigned count = rows.ordinality();
                if (0 == count)
                {
                    eoi = true;
                    return NULL;
                }

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
        catch (IException *e)
        {
            if (!isOOMException(e))
                throw e;
            IOutputMetaData *inputOutputMeta = input->queryFromActivity()->queryContainer().queryHelper()->queryOutputMeta();
            throw checkAndCreateOOMContextException(this, e, "loading group for rollup group", groupLoader->numRows(), inputOutputMeta, groupLoader->probeRow(0));
        }
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        calcMetaInfoSize(info, queryInput(0));
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


