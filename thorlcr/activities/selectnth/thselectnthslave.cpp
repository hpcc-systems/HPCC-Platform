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

#include "thselectnthslave.ipp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"

class CSelectNthSlaveActivity : public CSlaveActivity, implements ILookAheadStopNotify
{
    typedef CSlaveActivity PARENT;

    bool first, isLocal, seenNth;
    rowcount_t lookaheadN, N, startN;
    bool createDefaultIfFail;
    IHThorSelectNArg *helper;
    SpinLock spin; // MORE: Remove this and use an atomic variable for lookaheadN

    void initN()
    {
        // in n<0 before start of dataset (so output blank row)
        // n==0 means output nothing and return 0 (get returns eos)
        if (isLocal || firstNode())
        {
            N = (rowcount_t)helper->getRowToSelect();
            if (0==N)
                N=RCMAX;
        }
        else
        {
            CMessageBuffer msg;
            if (!receiveMsg(msg, queryJobChannel().queryMyRank()-1, mpTag))
                return;
            msg.read(N);
            msg.read(seenNth);
            if (!seenNth && lastNode())
                createDefaultIfFail = true;
        }
        startN = N;
        ActPrintLog("SELECTNTH: Selecting row %" I64F "d", N);
    }
    void sendN()
    {
        if (isLocal || lastNode())
            return;
        CMessageBuffer msg;
        msg.append(N);
        msg.append(seenNth); // used by last node to trigger fail if not seen
        queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag);
    }

public:
    CSelectNthSlaveActivity(CGraphElementBase *_container, bool _isLocal) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorSelectNArg *> (queryHelper());
        isLocal = _isLocal;
        createDefaultIfFail = isLocal || lastNode();
        if (container.queryLocalOrGrouped())
            setRequireInitData(false);
        appendOutputLinked(this);
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);

        lookaheadN = RCMAX;
        startN = 0; // set by initN()
        try
        {
            PARENT::start();
        }
        catch (IException *e)
        {
            ActPrintLog(e);
            N=0;
            sendN();
            throw;
        }

        rowcount_t rowN = (rowcount_t)helper->getRowToSelect();
        if (!isLocal && rowN)
        {
            if (ensureStartFTLookAhead(0))
                setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), SELECTN_SMART_BUFFER_SIZE, ::canStall(input), false, rowN, this, &container.queryJob().queryIDiskUsage()), false);
        }

        seenNth = false;
        if (0 == rowN)
        {
            ThorDataLinkMetaInfo info;
            queryInput(0)->getMetaInfo(info);
            StringBuffer meta;
            meta.appendf("META(totalRowsMin=%" I64F "d,totalRowsMax=%" I64F "d, spilled=%" I64F "d,byteTotal=%" I64F "d)",
                info.totalRowsMin,info.totalRowsMax,info.spilled,info.byteTotal);
#if 0                 
            Owned<IThorException> e = MakeActivityWarning(this, -1, "%s", meta.str());
            fireException(e);
#else
            ActPrintLog("%s",meta.str());
#endif
        }
        first = true;
    }
    virtual void abort() override
    {
        CSlaveActivity::abort();
        if (!firstNode())
            cancelReceiveMsg(RANK_ALL, mpTag);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        OwnedConstThorRow ret;
        Owned<IException> exception;
        if (first) // only return 1!
        {
            try
            {
                first = false;
                initN();
                if (RCMAX==N) // indicates before start of dataset e.g. ds[0]
                {
                    RtlDynamicRowBuilder row(queryRowAllocator());
                    size32_t sz = helper->createDefault(row);
                    ret.setown(row.finalizeRowClear(sz));
                    N = 0; // return that processed all
                }
                else if (N)
                {
                    while (!abortSoon)
                    {
                        ret.setown(inputStream->ungroupedNextRow());
                        if (!ret)
                            break;
                        N--;
                        {
                            SpinBlock block(spin);
                            if (lookaheadN<startN) // will not reach N==0, so don't bother continuing to read
                            {
                                N = startN-lookaheadN;
                                ret.clear();
                                break;
                            }
                        }
                        if (0==N)
                            break;
                    }
                    if ((N!=0)&&createDefaultIfFail)
                    {
                        N = 0; // return that processed all (i.e. none left)
                        RtlDynamicRowBuilder row(queryRowAllocator());
                        size32_t sz = helper->createDefault(row);
                        ret.setown(row.finalizeRowClear(sz));
                    }
                }
                if (startN && 0 == N)
                    seenNth = true;
            }
            catch (IException *e)
            {
                N=0;
                exception.setown(e);
            }
            sendN();
            if (exception.get())
                throw exception.getClear();
        }
        if (ret) 
            dataLinkIncrement();
        return ret.getClear();
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSequential = true; 
        info.canReduceNumRows = true; // not sure what selectNth is doing
        calcMetaInfoSize(info, queryInput(0));
    }
// IStartableEngineRowStream methods used for global selectn only
    virtual void onInputFinished(rowcount_t count) override
    {
        SpinBlock b(spin);
        lookaheadN = count;
    }
};


CActivityBase *createLocalSelectNthSlave(CGraphElementBase *container)
{
    return new CSelectNthSlaveActivity(container, true);
}

CActivityBase *createSelectNthSlave(CGraphElementBase *container)
{
    return new CSelectNthSlaveActivity(container, false);
}

