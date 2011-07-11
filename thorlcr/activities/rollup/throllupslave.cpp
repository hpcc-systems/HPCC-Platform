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

// Handling Rollup and Dedup

#include "throllupslave.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/rollup/throllupslave.cpp $ $Id: throllupslave.cpp 63725 2011-04-01 17:40:45Z jsmith $");

#include "thactivityutil.ipp"
#include "thorport.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"

interface IDedupAllHelper : extends IInterface
{
    virtual void init(IThorDataLink * in, IHThorDedupArg * helper, bool keepLeft, bool * abort, IStopInput *iStopInput) = 0;
    virtual bool calcNextDedupAll() = 0;
    virtual const void *nextRow() = 0;
};

class BaseDedupAllHelper : public CSimpleInterface, implements IDedupAllHelper
{
protected:
    CActivityBase *activity;
    unsigned dedupCount;
    const void ** dedupArray;
    unsigned dedupIdx;
    IThorDataLink * in;
    IHThorDedupArg * helper;
    bool keepLeft;
    bool * abort;
    IStopInput *iStopInput;

    virtual void remove(unsigned idx) = 0;
    virtual const void *getclear(unsigned idx) = 0;

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

    BaseDedupAllHelper(CActivityBase *_activity) : activity(_activity)
    {
        in = NULL;
        helper = NULL;
        abort = NULL;
    }

// IDedupAllHelper
    virtual void init(IThorDataLink * _in, IHThorDedupArg * _helper, bool _keepLeft, bool * _abort, IStopInput *_iStopInput)
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

    const void *nextRow()
    {
        while(dedupIdx < dedupCount)
        {
            OwnedConstThorRow r = getclear(dedupIdx++);
            if(r)
                return r.getClear();
        }
        return NULL;
    }
};




class DedupAllHelper : public BaseDedupAllHelper
{
    CThorRowArray rows;
    bool first;

protected:
    virtual void remove(unsigned idx) { rows.setNull(idx); }
    virtual const void *getclear(unsigned idx) { return rows.itemClear(idx); }

public:
    DedupAllHelper(CActivityBase *activity) : BaseDedupAllHelper(activity)
    {
        first = true;
    }

    ~DedupAllHelper()
    {
    }

    virtual bool calcNextDedupAll()
    {
#if THOR_TRACE_LEVEL >=5
        ActPrintLog(activity, "DedupAllHelper::calcNextDedupAll");
#endif
        dedupIdx = 0;
        if(first)           // one call only
        {

            first = false;
            rows.setSizing(true,true);
            dedupCount = rows.load(*in, true, *abort, NULL); // JCSMORE should 'overflowed' be use & a setMaxTotal at least in GroupDedupAllHelper case?
            ActPrintLog(activity, "DEDUP: rows loaded = %d",dedupCount);
            //if (overflowed) // JCSMORE
            //  throw MakeActivityException(activity, TE_TooMuchData, "overflow error?");

            if (iStopInput)
                iStopInput->stopInput();
            dedupArray = (const void **)rows.base();
            dedupAll();
            return true;
        }
        else
        {
            dedupArray = NULL;
            dedupCount = 0;
            return false;
        }
    }
};

class GroupDedupAllHelper : public BaseDedupAllHelper
{
    CThorRowArray rows;
    bool first;

protected:
    virtual void remove(unsigned idx) { rows.setNull(idx); }
    virtual const void *getclear(unsigned idx) 
    { 
        return rows.itemClear(idx); 
    }

public:
    GroupDedupAllHelper(CActivityBase *activity) : BaseDedupAllHelper(activity)
    { 
        first = true;
    }
    virtual bool calcNextDedupAll()
    {
#if THOR_TRACE_LEVEL >= 10
        ActPrintLog(activity, "GroupDedupAllHelper::calcNextDedupAll");
#endif
        dedupIdx = 0;
        if (first) {
            rows.setSizing(true,true);
            first = false;
        }
        rows.clear();
        try {
            dedupCount = rows.load(*in,false);
            ActPrintLog(activity, "DEDUP: rows loaded = %d",dedupCount);
            if (dedupCount) {
                dedupArray = (const void **)rows.base();
                dedupAll();
                return true;
            }
        }
        catch (IException *e)
        {
            IThorException *e2 = MakeActivityException(activity, e, NULL);
            e->Release();
            throw e2;
        }
        dedupArray = NULL;
        return false;
    }
};

IDedupAllHelper * createDedupAllHelper(CActivityBase *activity, bool grouped) 
{ 
    if(grouped)
        return new GroupDedupAllHelper(activity);
    else
        return new DedupAllHelper(activity);
}

class CDedupRollupBaseActivity : public CSlaveActivity, implements IStopInput
{
    bool rollup;
    CriticalSection stopsect;
    Linked<IRowInterfaces> rowif;

protected:
    StringAttr id;
    bool eogNext, eos;
    bool global;
    bool groupOp;
    OwnedConstThorRow kept;
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
        StringBuffer tmp;
        tmp.append(global?"GLOBAL":groupOp?"GROUP":"LOCAL");
        tmp.append(rollup?"ROLLUP":"DEDUP");
        id.set(tmp.str());
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
            mpTag = container.queryJob().deserializeMPTag(data); // only used for global acts
    }
    inline void checkFirstRow()
    {
        if (needFirstRow)
        {
            needFirstRow = false;
            if (global && container.queryJob().queryMyRank()>1)
            {
                CMessageBuffer msg;
                if (!receiveMsg(msg, container.queryJob().queryMyRank()-1, mpTag)) // from previous node
                    return;
                msg.read(numKept);
                bool isKept;
                msg.read(isKept);
                if (isKept)
                {
                    size32_t r = msg.remaining();
                    kept.deserialize(rowif, r, msg.readDirect(r));
                    if (kept)
                        return;
                }
            }
            kept.setown(input->nextRow());
            if (!kept && global)
                putNextKept(); // pass on now
        }
    }
    bool putNextKept()
    {
        assertex(global);
        eos = true;
        if (container.queryJob().queryMyRank()==container.queryJob().querySlaves()) // is last
            return false;
        CMessageBuffer msg;
        msg.append(numKept);
        msg.append(NULL != kept.get());
        if (kept.get())
        {
            CMemoryRowSerializer msz(msg);
            rowif->queryRowSerializer()->serialize(msz,(const byte *)kept.get());
        }
        container.queryJob().queryJobComm().send(msg, container.queryJob().queryMyRank()+1, mpTag); // to next node
        return true;
    }
    virtual void abort()
    {
        if (global)
            cancelReceiveMsg(container.queryJob().queryMyRank(), mpTag);
    }
};

class CDedupSlaveActivity : public CDedupRollupBaseActivity, public CThorDataLink
{
    IHThorDedupArg * ddhelper;
    bool keepLeft;
    unsigned numToKeep;
    bool compareAll;
    Linked<IDedupAllHelper> dedupHelper;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CDedupSlaveActivity(CGraphElementBase *_container, bool global, bool groupOp) 
        : CDedupRollupBaseActivity(_container, false, global, groupOp), CThorDataLink(this)
    {
    }
    ~CDedupSlaveActivity()
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CDedupRollupBaseActivity::init(data, slaveData);
        appendOutputLinked(this);   // adding 'me' to outputs array
        ddhelper = static_cast <IHThorDedupArg *>   (queryHelper());
        keepLeft = ddhelper->keepLeft();
        numToKeep = ddhelper->numToKeep();
        compareAll = ddhelper->compareAll();
        if (compareAll)
            assertex(1 == numToKeep);
        assertex(keepLeft || numToKeep == 1);
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
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        CDedupRollupBaseActivity::start();
        if(compareAll)
        {           
            assertex(!global);      // dedup(),local,all only supported
            dedupHelper.setown(createDedupAllHelper(this, groupOp));
            dedupHelper->init(input, ddhelper, keepLeft, &abortSoon, this);
            dedupHelper->calcNextDedupAll();
        }
        dataLinkStart(id, container.queryId());
    }
    virtual void stop()
    {
        CDedupRollupBaseActivity::stop();
        dataLinkStop();
    }
    
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eos) return NULL;
        if (!compareAll)
            checkFirstRow();

        if (eog())
            return NULL;

        if(compareAll)
        {
            loop
            {
                OwnedConstThorRow row = dedupHelper->nextRow();
                if (row) {
                    dataLinkIncrement();
                    return row.getClear();
                }
                if(!dedupHelper->calcNextDedupAll())
                    return NULL;
            }
        }

        if (!kept && !groupOp)
            return NULL;
        OwnedConstThorRow next;
        loop {
            next.setown(input->nextRow());
            if (!next)
            {
                if (!groupOp)
                    next.setown(input->nextRow());
                if (!next) {
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
    virtual bool isGrouped() { return groupOp; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        CDedupRollupBaseActivity::getMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
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

    ~CRollupSlaveActivity()
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
        ActivityTimer s(totalCycles, timeActivities, NULL);
        CDedupRollupBaseActivity::start();
        dataLinkStart(id, container.queryId());
    }

    virtual void stop()
    {
        CDedupRollupBaseActivity::stop();
        dataLinkStop();
    }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eos) return NULL;
        checkFirstRow();
        if (eog())
            return NULL;
        OwnedConstThorRow next;
        loop  {
            next.setown(input->nextRow());
            if (!next) {
                if (!groupOp)
                    next.setown(input->nextRow());
                if (!next) {
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
            unsigned thisSize = ruhelper->transform(ret, kept, next);
            if (thisSize)
                kept.setown(ret.finalizeRowClear(thisSize));
        }
        OwnedConstThorRow row = kept.getClear();
        kept.setown(next.getClear());
        if (row) {
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
    CThorRowArray group;
    bool eoi;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CRollupGroupSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorRollupGroupArg *)queryHelper();
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        eoi = false;
        startInput(input);
        dataLinkStart("ROLLUPGROUP", container.queryId());
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (!eoi)
        {
            unsigned count = group.load(*input, false);
            if (count)
            {
                RtlDynamicRowBuilder row(queryRowAllocator());
                size32_t sz = helper->transform(row, count, (const void **)group.base());
                group.clear();
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
    return new CDedupSlaveActivity(container, false, false);
}

CActivityBase *createGroupDedupSlave(CGraphElementBase *container)
{
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


