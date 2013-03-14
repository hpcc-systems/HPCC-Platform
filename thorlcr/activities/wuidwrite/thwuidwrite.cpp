/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
    unsigned workunitWriteLimit, totalSize;
    bool appendOutput;
    IHThorWorkUnitWriteArg *helper;

public:
    CWorkUnitWriteMasterBase(CMasterGraphElement * info) : CMasterActivity(info)
    {
        helper = (IHThorWorkUnitWriteArg *)queryHelper();
        numResults = 0;
        totalSize = 0;
        appendOutput = 0 != (POFextend & helper->getFlags());
        flushThreshold = getOptInt(THOROPT_OUTPUT_FLUSH_THRESHOLD, -1);
        workunitWriteLimit = 0;
        mpTag = container.queryJob().allocateMPTag(); // used by local too
    }
    void init()
    {
        workunitWriteLimit = getOptInt(THOROPT_OUTPUTLIMIT, DEFAULT_WUIDWRITE_LIMIT);
        if (workunitWriteLimit>DALI_RESULT_OUTPUTMAX)
            throw MakeActivityException(this, 0, "Dali result outputs are restricted to a maximum of %d MB, the default limit is %d MB. A huge dali result usually indicates the ECL needs altering.", DALI_RESULT_OUTPUTMAX, DEFAULT_WUIDWRITE_LIMIT);
        workunitWriteLimit *= 0x100000;

        if (appendOutput)
        {
            Owned<IWorkUnit> wu = &container.queryJob().queryWorkUnit().lock();
            Owned<IWUResult> result = updateWorkUnitResult(wu, helper->queryName(), helper->getSequence());
            numResults = validRC(result->getResultRowCount());
        }
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
    void flushResults(bool complete=false)
    {
        if (resultData.length() || complete)
        {
            Owned<IWorkUnit> wu = &container.queryJob().queryWorkUnit().lock();
            Owned<IWUResult> result = updateWorkUnitResult(wu, helper->queryName(), helper->getSequence());
            ActPrintLog("WORKUNITWRITE: flushing result");
            if (appendOutput)
                result->addResultRaw(resultData.length(), resultData.toByteArray(), ResultFormatRaw);
            else
                result->setResultRaw(resultData.length(), resultData.toByteArray(), ResultFormatRaw);
            appendOutput = true;
            result->setResultRowCount(numResults);
            result->setResultTotalRowCount(numResults);
            resultData.clear();
            if (complete)
                result->setResultStatus(ResultStatusCalculated);
            ActPrintLog("WORKUNITWRITE: result flushed");
        }
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

class CWorkUnitWriteActivityMaster : public CWorkUnitWriteMasterBase
{
public:
    CWorkUnitWriteActivityMaster(CMasterGraphElement * info) : CWorkUnitWriteMasterBase(info)
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
                if (!container.queryJob().queryJobComm().send(mb, s+1, mpTag)) return;
                if (!receiveMsg(mb, s+1, mpTag)) return;
                if (0 == mb.length())
                    break;
                unsigned numGot;
                mb.read(numGot);
                unsigned l=mb.remaining();
                if (workunitWriteLimit && resultData.length()+l > workunitWriteLimit)
                {
                    StringBuffer errMsg("Dataset too large to output to workunit (limit ");
                    errMsg.append(workunitWriteLimit/0x100000).append(") megabytes, in result (");
                    const char *name = helper->queryName();
                    if (name)
                        errMsg.append("name=").append(name);
                    else
                        errMsg.append("sequence=").append(helper->getSequence());
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

                unsigned sender = senders.pop();
                act.getData(sender);
            }
        }
    };
    Owned<CMessageHandler> messageHandler;
    unsigned sent;
public:
    CWorkUnitWriteLocalActivityMaster(CMasterGraphElement * info) : CWorkUnitWriteMasterBase(info)
    {
        sent = 0;
    }
    void getData(unsigned sender)
    {
        CMessageBuffer msg, replyMsg;
        if (!receiveMsg(replyMsg, sender, mpTag, NULL, 5*60000))
            throwUnexpected();
        replyMsg.swapWith(msg);
        container.queryJob().queryJobComm().reply(replyMsg); // ack
        
        unsigned numGot;
        msg.read(numGot);
        unsigned l=msg.remaining();
        if (workunitWriteLimit && resultData.length()+l > workunitWriteLimit)
            throw MakeThorException(TE_WorkUnitWriteLimitExceeded, "Dataset too large to output to workunit (limit %d megabytes)", workunitWriteLimit/0x100000);
        resultData.append(l, msg.readDirect(l));
        numResults += numGot;

        // NB if 0 == numGot - then final packet from sender
        if (0 == numGot || (-1 != flushThreshold && resultData.length() >= (unsigned)flushThreshold))
            flushResults(0 == numGot);
    }
    virtual void handleSlaveMessage(CMessageBuffer &msg)
    {
        ++sent;
        rank_t sender = container.queryJob().queryJobGroup().rank(msg.getSender());
        if (!messageHandler)
            messageHandler.setown(new CMessageHandler(*this));
        messageHandler->add(sender);
        msg.clear();
        container.queryJob().queryJobComm().reply(msg);
    }
};

CActivityBase *createWorkUnitWriteActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CWorkUnitWriteLocalActivityMaster(container);
    else
        return new CWorkUnitWriteActivityMaster(container);
}
