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

class CSelectNthSlaveActivity : public CSlaveActivity, public CThorDataLink, implements ISmartBufferNotify
{
    bool first, isLocal, seenNth;
    rowcount_t lookaheadN, N, startN;
    bool createDefaultIfFail;
    Owned<IThorDataLink> input;
    IHThorSelectNArg *helper;
    SpinLock spin;

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
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSelectNthSlaveActivity(CGraphElementBase *_container, bool _isLocal) : CSlaveActivity(_container), CThorDataLink(this)
    {
        isLocal = _isLocal;
        createDefaultIfFail = isLocal || lastNode();
    }
    ~CSelectNthSlaveActivity()
    {
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJobChannel().deserializeMPTag(data);
        appendOutputLinked(this);
        helper = static_cast <IHThorSelectNArg *> (queryHelper());
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities);

        lookaheadN = RCMAX;
        startN = 0; // set by initN()
        rowcount_t rowN = (rowcount_t)helper->getRowToSelect();
        if (!isLocal && rowN)
            input.setown(createDataLinkSmartBuffer(this, inputs.item(0), SELECTN_SMART_BUFFER_SIZE, isSmartBufferSpillNeeded(this), false, rowN, this, false, &container.queryJob().queryIDiskUsage()));
        else
            input.set(inputs.item(0));
        try
        {
            startInput(input);
        }
        catch (IException *e)
        {
            ActPrintLog(e);
            N=0;
            sendN();
            throw;
        }
        dataLinkStart();

        seenNth = false;
        if (0==helper->getRowToSelect())
        {
            ThorDataLinkMetaInfo info;
            inputs.item(0)->getMetaInfo(info);
            StringBuffer meta;
            meta.appendf("META(totalRowsMin=%" I64F "d,totalRowsMax=%" I64F "d,rowsOutput=%" RCPF "d,spilled=%" I64F "d,byteTotal=%" I64F "d)",
                info.totalRowsMin,info.totalRowsMax,info.rowsOutput,info.spilled,info.byteTotal);
#if 0                 
            Owned<IThorException> e = MakeActivityWarning(this, -1, "%s", meta.str());
            fireException(e);
#else
            ActPrintLog("%s",meta.str());
#endif
        }
        first = true;
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        if (!firstNode())
            cancelReceiveMsg(RANK_ALL, mpTag);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
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
                        ret.setown(input->ungroupedNextRow());
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
    virtual bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSequential = true; 
        info.canReduceNumRows = true; // not sure what selectNth is doing
        calcMetaInfoSize(info,inputs.item(0));
    }
// ISmartBufferNotify methods used for global selectn only
    virtual void onInputStarted(IException *) { } // not needed
    virtual bool startAsync() { return false; }
    virtual void onInputFinished(rowcount_t count)
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

