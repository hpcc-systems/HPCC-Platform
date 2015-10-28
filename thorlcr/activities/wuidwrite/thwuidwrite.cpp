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

#include "jlib.hpp"

#include "eclhelper.hpp"

#include "thexception.hpp"

#include "thwuidwrite.ipp"
#include "thbufdef.hpp"

#define INVALID_SEQUENCE_VALUE  -1
#define DEFAULT_WUIDWRITE_LIMIT 10

class CWorkUnitWriteMasterBase : public CMasterActivity
{
protected:
    CMessageBuffer resultData;
    rowcount_t numResults;
    int flushThreshold;
    unsigned workunitWriteLimit, totalSize, activityMaxSize;
    bool appendOutput;
    StringAttr resultName;
    unsigned resultSeq;

public:
    CWorkUnitWriteMasterBase(CMasterGraphElement * info) : CMasterActivity(info)
    {
        numResults = 0;
        totalSize = 0;
        resultSeq = 0;
        appendOutput = false;
        flushThreshold = -1;
        workunitWriteLimit = 0;
        mpTag = container.queryJob().allocateMPTag(); // used by local too
        activityMaxSize = 0;
    }
    virtual void init()
    {
        CMasterActivity::init();
        // In absense of OPT_OUTPUTLIMIT check pre 5.2 legacy name OPT_OUTPUTLIMIT_LEGACY
        workunitWriteLimit = activityMaxSize ? activityMaxSize : getOptInt(OPT_OUTPUTLIMIT, getOptInt(OPT_OUTPUTLIMIT_LEGACY, defaultDaliResultLimit));
        if (workunitWriteLimit>defaultDaliResultOutputMax)
            throw MakeActivityException(this, 0, "Configured max result size, %d MB, exceeds absolute max limit of %d MB. A huge Dali result usually indicates the ECL needs altering.", workunitWriteLimit, defaultDaliResultOutputMax);
        assertex(workunitWriteLimit<=0x1000); // 32bit limit because MemoryBuffer/CMessageBuffers involved etc.
        workunitWriteLimit *= 0x100000;
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
    void addResult(rowcount_t resultCount, MemoryBuffer &resultData, bool complete)
    {
        Owned<IWorkUnit> wu = &container.queryJob().queryWorkUnit().lock();
        Owned<IWUResult> result = updateWorkUnitResult(wu, resultName, resultSeq);
        if (appendOutput)
            result->addResultRaw(resultData.length(), resultData.toByteArray(), ResultFormatRaw);
        else
            result->setResultRaw(resultData.length(), resultData.toByteArray(), ResultFormatRaw);
        result->setResultRowCount(resultCount);
        result->setResultTotalRowCount(resultCount);
        resultData.clear();
        if (complete)
            result->setResultStatus(ResultStatusCalculated);
        appendOutput = true;
    }
    virtual void flushResults(bool complete=false)
    {
        if (resultData.length() || complete)
        {
            ActPrintLog("flushing result");
            addResult(numResults, resultData, complete);
            resultData.clear();
            ActPrintLog("result flushed");
        }
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

class CWorkUnitWriteGlobalMasterBase : public CWorkUnitWriteMasterBase
{
public:
    CWorkUnitWriteGlobalMasterBase(CMasterGraphElement * info) : CWorkUnitWriteMasterBase(info)
    {
    }
    void process()
    {
        CWorkUnitWriteMasterBase::process();

        unsigned nslaves = container.queryJob().querySlaves();

        CMessageBuffer mb;
        unsigned s=0;
        for (; s<nslaves; s++)
        {
            loop
            {
                if (!queryJobChannel().queryJobComm().send(mb, s+1, mpTag)) return;
                if (!receiveMsg(mb, s+1, mpTag)) return;
                if (0 == mb.length())
                    break;
                unsigned numGot;
                mb.read(numGot);
                unsigned l=mb.remaining();
                if (workunitWriteLimit && totalSize+resultData.length()+l > workunitWriteLimit)
                {
                    StringBuffer errMsg("Dataset too large to output to workunit (limit is set to ");
                    errMsg.append(workunitWriteLimit/0x100000).append(") megabytes, in result (");
                    if (resultName.length())
                        errMsg.append("name=").append(resultName);
                    else
                        errMsg.append("sequence=").append(resultSeq);
                    errMsg.append(")");
                    throw MakeThorException(TE_WorkUnitWriteLimitExceeded, "%s", errMsg.str());
                }
                resultData.append(l, mb.readDirect(l));
                mb.clear();
                numResults += numGot;

                if (-1 != flushThreshold && resultData.length() >= (unsigned)flushThreshold)
                    flushResults();
            }
        }
        flushResults(true);
    }
};

class CWorkUnitWriteActivityMaster : public CWorkUnitWriteGlobalMasterBase
{
    IHThorWorkUnitWriteArg *helper;
public:
    CWorkUnitWriteActivityMaster(CMasterGraphElement * info) : CWorkUnitWriteGlobalMasterBase(info)
    {
        helper = (IHThorWorkUnitWriteArg *)queryHelper();
        appendOutput = 0 != (POFextend & helper->getFlags());
        flushThreshold = getOptInt(THOROPT_OUTPUT_FLUSH_THRESHOLD, -1);
        resultName.set(helper->queryName());
        resultSeq = helper->getSequence();
        if (POFmaxsize & helper->getFlags())
            activityMaxSize = helper->getMaxSize();
    }
    virtual void init()
    {
        CWorkUnitWriteGlobalMasterBase::init();
        if (appendOutput)
        {
            Owned<IWorkUnit> wu = &container.queryJob().queryWorkUnit().lock();
            Owned<IWUResult> result = updateWorkUnitResult(wu, resultName, resultSeq);
            numResults = validRC(result->getResultRowCount());
        }
    }
};

class CWorkUnitWriteLocalActivityMaster : public CWorkUnitWriteMasterBase
{
    class CMessageHandler : public CSimpleInterface, implements IThreaded
    {
    public:
        CWorkUnitWriteLocalActivityMaster &act;
        CThreaded threaded;
        UnsignedArray senders;
        bool stopped, waiting, started;
        CriticalSection crit;
        Semaphore sem;

    public:
        CMessageHandler(CWorkUnitWriteLocalActivityMaster &_act) : act(_act), threaded("CWorkUnitWriteLocalActivityMaster::CMessageHandler")
        {
            started = waiting = stopped = false;
            threaded.init(this);
        }
        ~CMessageHandler()
        {
            stop();
            threaded.join();
        }
        void stop()
        {
            stopped = true;
            sem.signal();
        }
        void add(unsigned sender)
        {
            CriticalBlock b(crit);
            assertex(NotFound == senders.find(sender)); // sanity check
            senders.append(sender);
            if (waiting)
            {
                waiting = false;
                sem.signal();
            }
        }
        virtual void main()
        {
            started = true;
            loop
            {
                CriticalBlock b(crit);
                if (0 == senders.ordinality())
                {
                    waiting = true;
                    CriticalUnblock ub(crit);
                    sem.wait();
                }
                if (stopped) break;

                unsigned sender = senders.popGet();
                act.getData(sender);
            }
        }
    };
    Owned<CMessageHandler> messageHandler;
    unsigned sent;
    IHThorWorkUnitWriteArg *helper;

public:
    CWorkUnitWriteLocalActivityMaster(CMasterGraphElement * info) : CWorkUnitWriteMasterBase(info)
    {
        helper = (IHThorWorkUnitWriteArg *)queryHelper();
        appendOutput = 0 != (POFextend & helper->getFlags());
        flushThreshold = globals->getPropInt("@output_flush_threshold", -1);
        resultName.set(helper->queryName());
        resultSeq = helper->getSequence();
        sent = 0;
        if (POFmaxsize & helper->getFlags())
            activityMaxSize = helper->getMaxSize();
    }
    void getData(unsigned sender)
    {
        CMessageBuffer msg, replyMsg;
        if (!receiveMsg(replyMsg, sender, mpTag, NULL, 5*60000))
            throwUnexpected();
        replyMsg.swapWith(msg);
        queryJobChannel().queryJobComm().reply(replyMsg); // ack
        
        unsigned numGot;
        msg.read(numGot);
        unsigned l=msg.remaining();
        if (workunitWriteLimit && totalSize+resultData.length()+l > workunitWriteLimit)
            throw MakeThorException(TE_WorkUnitWriteLimitExceeded, "Dataset too large to output to workunit (limit %d megabytes)", workunitWriteLimit/0x100000);
        resultData.append(l, msg.readDirect(l));
        numResults += numGot;

        // NB if 0 == numGot - then final packet from sender
        if (0 == numGot || (-1 != flushThreshold && resultData.length() >= (unsigned)flushThreshold))
        {
            totalSize += resultData.length();
            flushResults(0 == numGot);
        }
    }
    virtual void handleSlaveMessage(CMessageBuffer &msg)
    {
        ++sent;
        rank_t sender = container.queryJob().queryJobGroup().rank(msg.getSender());
        if (!messageHandler)
            messageHandler.setown(new CMessageHandler(*this));
        messageHandler->add(sender);
        msg.clear();
        queryJobChannel().queryJobComm().reply(msg);
    }
};

CActivityBase *createWorkUnitWriteActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CWorkUnitWriteLocalActivityMaster(container);
    else
        return new CWorkUnitWriteActivityMaster(container);
}

// ==================================

class CDictionaryWorkunitWriteActivityMaster : public CWorkUnitWriteGlobalMasterBase
{
    IHThorDictionaryWorkUnitWriteArg *helper;
public:
    CDictionaryWorkunitWriteActivityMaster(CMasterGraphElement * info) : CWorkUnitWriteGlobalMasterBase(info)
    {
        helper = (IHThorDictionaryWorkUnitWriteArg *)queryHelper();
        resultName.set(helper->queryName());
        resultSeq = helper->getSequence();
    }
    virtual void flushResults(bool complete=false)
    {
        assertex(complete);
        ActPrintLog("dictionary result");
        Owned<IRowInterfaces> rowIf = createRowInterfaces(container.queryInput(0)->queryHelper()->queryOutputMeta(),queryId(),queryCodeContext());
        IOutputRowDeserializer *deserializer = rowIf->queryRowDeserializer();
        CMessageBuffer mb;
        Owned<ISerialStream> stream = createMemoryBufferSerialStream(resultData);
        CThorStreamDeserializerSource rowSource;
        rowSource.setStream(stream);

        RtlLinkedDictionaryBuilder builder(queryRowAllocator(), helper->queryHashLookupInfo());
        while (!rowSource.eos())
        {
            RtlDynamicRowBuilder rowBuilder(queryRowAllocator());
            size32_t sz = deserializer->deserialize(rowBuilder, rowSource);
            const void *row = rowBuilder.finalizeRowClear(sz);
            builder.appendOwn(row);
        }

        size32_t usedCount = rtlDictionaryCount(builder.getcount(), builder.queryrows());
        MemoryBuffer rowData;
        CThorDemoRowSerializer out(rowData);
        rtlSerializeDictionary(out, rowIf->queryRowSerializer(), builder.getcount(), builder.queryrows());
        addResult(usedCount, rowData, complete);
        resultData.clear();
        ActPrintLog("dictionary flushed");
    }
};



CActivityBase *createDictionaryWorkunitWriteMaster(CMasterGraphElement *container)
{
    return new CDictionaryWorkunitWriteActivityMaster(container);
}
