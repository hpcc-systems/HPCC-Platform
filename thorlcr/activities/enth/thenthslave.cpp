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

#include "thenthslave.ipp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"

class BaseEnthActivity : public CSlaveActivity, implements ILookAheadStopNotify
{
    typedef CSlaveActivity PARENT;

protected:
    StringBuffer actStr;
    Semaphore finishedSem;
    rowcount_t counter = 0, localRecCount = 0;
    rowcount_t denominator = 0, numerator = 0;

    bool haveLocalCount() { return RCUNBOUND != localRecCount; }
    inline bool wanted()
    {
        counter += numerator;
        if(counter >= denominator)
        {
            counter -= denominator;
            return true;
        }       
        return false;   
    }
    void setInitialCounter(rowcount_t prevRecCount)
    {
        if (numerator)
        {
            IHThorEnthArg * helper = static_cast <IHThorEnthArg *> (queryHelper());
            if(denominator == 0) denominator = 1;

            counter = (rowcount_t)((helper->getSampleNumber()-1) * greatestCommonDivisor(numerator, denominator));
            if (counter >= denominator)
                counter %= denominator;
            rowcount_t maxBatch = (RCMAX - denominator) / numerator;
            while (prevRecCount > 0)
            {
                rowcount_t next = prevRecCount;
                if (next > maxBatch) next = maxBatch;
                counter = (counter + next * numerator) % denominator;
                prevRecCount -= next;
            }
        }
        else
            abortSoon = true;
#if THOR_TRACE_LEVEL >= 5
        ActPrintLog("ENTH: init - Numerator = %" RCPF "d, Denominator = %" RCPF "d", numerator, denominator);   
        ActPrintLog("%s: Initial value of counter %" RCPF "d", actStr.str(), counter);
#endif
    }
    void setLocalCountReq()
    {
        ThorDataLinkMetaInfo info;
        input->getMetaInfo(info);
        // Need lookahead _unless_ row count pre-known.
        if (0 == numerator)
            localRecCount = 0;
        else
        {
            if (info.totalRowsMin == info.totalRowsMax)
            {
                localRecCount = (rowcount_t)info.totalRowsMax;
                ActPrintLog("%s: row count pre-known to be %" RCPF "d", actStr.str(), localRecCount);
            }
            else
            {
                localRecCount = RCUNBOUND;

                // NB: this is post base start()
                if (!hasLookAhead(0))
                    setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), ENTH_SMART_BUFFER_SIZE, true, false, RCUNBOUND, this, &container.queryJob().queryIDiskUsage()), false);
                else
                    startLookAhead(0);
            }
        }
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    BaseEnthActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        appendOutputLinked(this);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        IHThorEnthArg *helper = static_cast <IHThorEnthArg *> (queryHelper());
        counter = 0;
        denominator = (rowcount_t)helper->getProportionDenominator();
        numerator = (rowcount_t)helper->getProportionNumerator();
    }
    virtual bool isGrouped() const override { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.buffersInput = true;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info, input);
    }
};

class CLocalEnthSlaveActivity : public BaseEnthActivity
{
    typedef BaseEnthActivity PARENT;

    bool localCountReq;
public:
    CLocalEnthSlaveActivity(CGraphElementBase *_container) : BaseEnthActivity(_container)
    {
        actStr.append("LOCALENTH");
        localCountReq = false;
        setRequireInitData(false);
    }
    virtual void start()
    {
        PARENT::start();
        if (RCUNBOUND == denominator)
        {
            localCountReq = true;
            setLocalCountReq();
        }
        else
            setInitialCounter(0);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (localCountReq)
        {
            localCountReq = false;
            if (!haveLocalCount())
                finishedSem.wait();
            denominator = localRecCount;
            setInitialCounter(0);
        }
        while (!abortSoon)
        {
            OwnedConstThorRow row(inputStream->ungroupedNextRow());
            if (!row)
                break;
            if (wanted())
            {
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;        
    }
    virtual void abort()
    {
        PARENT::abort();
        localRecCount = 0;
        finishedSem.signal();
    }
    virtual void onInputFinished(rowcount_t count)
    {
        localRecCount = count;
        finishedSem.signal();
    }
};


class CEnthSlaveActivity : public BaseEnthActivity
{
    typedef BaseEnthActivity PARENT;

    Semaphore prevRecCountSem;
    rowcount_t prevRecCount;
    bool first = false; // until start

    void sendCount(rowcount_t count)
    {
        if (lastNode())
            return;
        CMessageBuffer msg;
        msg.append(count);
        queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag);
    }
    bool getPrev()
    {
        if (!firstNode()) // no need if 1st node
        {
            CMessageBuffer msg;
            if (!receiveMsg(msg, queryJobChannel().queryMyRank()-1, mpTag))
                return false;
            msg.read(prevRecCount);
        }
        setInitialCounter(prevRecCount);
        if (haveLocalCount()) // if local total count known, send total now
            sendCount(prevRecCount + localRecCount);
        else
            prevRecCountSem.signal();
        return true;
    }

public:
    CEnthSlaveActivity(CGraphElementBase *container) : BaseEnthActivity(container)
    { 
        actStr.append("ENTH");
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        PARENT::init(data, slaveData);
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void start()
    {
        PARENT::start();
        prevRecCount = 0;
        first = true;
        setLocalCountReq();
    }
    virtual void abort()
    {
        PARENT::abort();
        if (!firstNode())
            cancelReceiveMsg(RANK_ALL, mpTag);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (first)
        {
            first = false;
            if (!getPrev())
                return NULL;
        }
        while (!abortSoon)
        {
            OwnedConstThorRow row(inputStream->ungroupedNextRow());
            if (!row)
                break;
            if (wanted())
            {
                dataLinkIncrement();
                return row.getClear();          
            }
        }
        return NULL;        
    }
    virtual void stop()
    {
        // Need to ensure sequence continues, in nextRow has never been called.
        if (first)
        {
            first = false;
            getPrev();
        }
        PARENT::stop();
    }
    virtual void onInputFinished(rowcount_t localRecCount)
    {
        if (!haveLocalCount())
        {
            ActPrintLog("maximum row count %" RCPF "d", localRecCount);
            prevRecCountSem.wait();
            if (abortSoon)
                return;
            sendCount(prevRecCount + localRecCount);
        }
    }
};


CActivityBase *createLocalEnthSlave(CGraphElementBase *container) 
{ 
    return new CLocalEnthSlaveActivity(container); 
}

CActivityBase *createEnthSlave(CGraphElementBase *container) 
{ 
    return new CEnthSlaveActivity(container);
}


