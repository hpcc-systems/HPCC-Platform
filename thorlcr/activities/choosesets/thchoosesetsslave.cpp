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

#include "thchoosesetsslave.ipp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"

class BaseChooseSetsActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

protected:
    IHThorChooseSetsArg *helper;
    bool done;
    unsigned numSets;
    unsigned *tallies;

public:
    BaseChooseSetsActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorChooseSetsArg *> (queryHelper());
        done = false;
        tallies = NULL;
        appendOutputLinked(this);
    }
    ~BaseChooseSetsActivity()
    {
        if (tallies)
            delete [] tallies;
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void start() override
    {
        CSlaveActivity::start();
        numSets = helper->getNumSets();
        if (tallies)
            delete [] tallies;
        tallies = new unsigned[numSets];

        memset(tallies, 0, sizeof(unsigned)*numSets);
        done = helper->setCounts(tallies);
    }
    virtual bool isGrouped() const override { return false; }
};


class LocalChooseSetsActivity : public BaseChooseSetsActivity
{
    typedef BaseChooseSetsActivity PARENT;

public:
    LocalChooseSetsActivity(CGraphElementBase *container) : BaseChooseSetsActivity(container) { }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        ActPrintLog("CHOOSESETS: Is Local");
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;)
        {
            OwnedConstThorRow row = inputStream->ungroupedNextRow();
            if(!row || done || abortSoon)
                break;

            switch (helper->getRecordAction(row))
            {
            case 2:
                done = true;
                //fall through
            case 1:
                dataLinkIncrement();
                return row.getClear();
            }
        }
        return NULL;        
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.fastThrough = true;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
};


class ChooseSetsActivity : public BaseChooseSetsActivity
{
    typedef BaseChooseSetsActivity PARENT;

    bool first = false;
    bool done = false;

    void getTallies() // NB: not called on first node.
    {
        CMessageBuffer msg;
        if (!receiveMsg(msg, queryJobChannel().queryMyRank()-1, mpTag))
            return;
        memcpy(tallies, msg.readDirect(numSets*sizeof(unsigned)), numSets*sizeof(unsigned));
#if THOR_TRACE_LEVEL >= 5
        StringBuffer s;
        unsigned idx=0;
        for (; idx<numSets; idx++)
            s.append("[").append(tallies[idx]).append("]");
        ActPrintLog("CHOOSESETS: Incoming count = %s", s.str());
#endif
    }
    void sendTallies() // NB: not called on last node.
    {
#if THOR_TRACE_LEVEL >= 5
        StringBuffer s;
        unsigned idx=0;
        for (; idx<numSets; idx++)
            s.append("[").append(tallies[idx]).append("]");
        ActPrintLog("CHOOSESETS: Outgoing count = %s", s.str());
#endif
        CMessageBuffer msg;
        msg.append(numSets * sizeof(unsigned), tallies); 
        queryJobChannel().queryJobComm().send(msg, queryJobChannel().queryMyRank()+1, mpTag);
    }

public:
    ChooseSetsActivity(CGraphElementBase *container) : BaseChooseSetsActivity(container)
    {
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {
        PARENT::init(data, slaveData);
        SocketEndpoint server;
        server.serialize(slaveData);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        ActPrintLog("CHOOSESETS: Is Global");
        PARENT::start();

        if (ensureStartFTLookAhead(0))
            setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), CHOOSESETS_SMART_BUFFER_SIZE, ::canStall(input), false, RCUNBOUND, NULL, &container.queryJob().queryIDiskUsage()), false);

        first = true;
        done = false;
    }
    virtual void abort() override
    {
#if THOR_TRACE_LEVEL >= 5
        ActPrintLog("CHOOSESETS: abort()");
#endif
        CSlaveActivity::abort();
        if (!container.queryLocalOrGrouped() && !firstNode())
            cancelReceiveMsg(RANK_ALL, mpTag);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (first) 
        {
            first = false;
            if (container.queryLocalOrGrouped() || firstNode())
                memset(tallies, 0, sizeof(unsigned)*numSets);
            else
                getTallies();
            done = helper->setCounts(tallies);
        }
        if (!done)
        {
            while (!abortSoon)
            {
                OwnedConstThorRow row = inputStream->ungroupedNextRow();
                if (!row)
                    break;
                switch (helper->getRecordAction(row))
                {
                case 2:
                    done = true;
                    //fall through
                case 1:
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
        }
        if (!container.queryLocalOrGrouped() && !lastNode())
            sendTallies();
        return NULL;
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSequential = true;
        info.canReduceNumRows = true;
        info.canBufferInput = true;
        calcMetaInfoSize(info, queryInput(0));
    }
};


//---------------------------------------------------------------------------

class ChooseSetsPlusActivity;
class CInputCounter : public CSimpleInterfaceOf<IEngineRowStream>
{
    IEngineRowStream *inputStream;
    ChooseSetsPlusActivity &activity;
public:
    CInputCounter(ChooseSetsPlusActivity & _activity) : activity(_activity) { }
    void setInputStream(IEngineRowStream *_inputStream) { inputStream = _inputStream; }

    virtual const void *nextRow() override;
    virtual void stop() override;
    virtual void resetEOF() override { throwUnexpected(); }
};


// A hookling class that counts records as they are read by the smart buffering....
class ChooseSetsPlusActivity : public CSlaveActivity, implements ILookAheadStopNotify
{
    typedef CSlaveActivity PARENT;
    friend class CInputCounter;
protected:
    IHThorChooseSetsExArg * helper;
    bool done;
    bool first;
    unsigned numSets;
    rowcount_t * counts;
    rowcount_t * priorCounts;
    rowcount_t * totalCounts;
    __int64 * limits;
    Owned<CInputCounter> inputCounter;

public:
    IMPLEMENT_IINTERFACE_USING(CSlaveActivity);

    ChooseSetsPlusActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorChooseSetsExArg *> (queryHelper());
        counts = NULL;
        priorCounts = NULL;
        totalCounts = NULL;
        limits = NULL;
        inputCounter.setown(new CInputCounter(*this));
        if (container.queryLocalOrGrouped())
            setRequireInitData(false);
        appendOutputLinked(this);
    }
    ~ChooseSetsPlusActivity()
    {
        free(counts);
        free(priorCounts);
        free(totalCounts);
        free(limits);
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData) override
    {
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void setInputStream(unsigned index, CThorInput &_input, bool consumerOrdered) override
    {
        PARENT::setInputStream(index, _input, consumerOrdered);
        inputCounter->setInputStream(inputStream);
        setLookAhead(0, createRowStreamLookAhead(this, inputCounter.get(), queryRowInterfaces(input), CHOOSESETSPLUS_SMART_BUFFER_SIZE, true, false, RCUNBOUND, this, &container.queryJob().queryIDiskUsage()), true); // read all input
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        ActPrintLog("CHOOSESETS: Is Global");
        if (counts)
        {
            free(counts);
            free(priorCounts);
            free(totalCounts);
            free(limits);
        }
        numSets = helper->getNumSets();
        counts = (rowcount_t *)calloc(sizeof(rowcount_t), numSets);
        priorCounts = (rowcount_t *)calloc(sizeof(rowcount_t), numSets);
        totalCounts = (rowcount_t *)calloc(sizeof(rowcount_t), numSets);
        limits = (__int64 *)calloc(sizeof(__int64), numSets);
        helper->getLimits(limits);
        done = false;
        first = true;
        PARENT::start();
    }
    virtual void abort() override
    {
        CSlaveActivity::abort();
        if (!container.queryLocalOrGrouped())
            cancelReceiveMsg(RANK_ALL, mpTag);
    }
    virtual bool isGrouped() const override { return false; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.buffersInput = true;
        info.isSequential = true;
        info.canReduceNumRows = true;
        info.canBufferInput = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    void getGlobalCounts()
    {
        if (container.queryLocalOrGrouped())
            return;
        CMessageBuffer msg;
        if (!receiveMsg(msg, 0, mpTag))
            return;
        memcpy(totalCounts, msg.readDirect(numSets*sizeof(rowcount_t)), numSets*sizeof(rowcount_t));
        memcpy(priorCounts, msg.readDirect(numSets*sizeof(rowcount_t)), numSets*sizeof(rowcount_t));
    }
    virtual void onInputFinished(rowcount_t count) override
    {
        if (container.queryLocalOrGrouped())
            return;
        CMessageBuffer msg;
        msg.append(numSets*sizeof(rowcount_t), counts);
        queryJobChannel().queryJobComm().send(msg, 0, mpTag);
    }
};

//////////

const void *CInputCounter::nextRow()
{
    OwnedConstThorRow row = inputStream->nextRow();
    if (row)
    {
        unsigned category = activity.helper->getCategory(row);
        if (category)
            activity.counts[category-1]++;
        return row.getClear();
    }
    return NULL;
}

void CInputCounter::stop()
{
    inputStream->stop();
}

//////////

class ChooseSetsLastActivity : public ChooseSetsPlusActivity
{
    unsigned *numToSkip;
    unsigned *numToReturn;

    void calculateSelection()
    {
        bool skipAll = true;
        for (unsigned idx=0; idx < numSets; idx++)
        {
            rowcount_t firstToCopy = 0;
            ActPrintLog("CHOOSESETSLAST: %d P(%" RCPF "d) C(%" RCPF "d) L(%" I64F "d) T(%" RCPF "d)", idx, priorCounts[idx], counts[idx], limits[idx], totalCounts[idx]);
            if (((rowcount_t)limits[idx]) < totalCounts[idx])
                firstToCopy = totalCounts[idx] - (rowcount_t)limits[idx];
            if (priorCounts[idx] + counts[idx] > firstToCopy)
            {
                if (priorCounts[idx] >= firstToCopy)
                    numToSkip[idx] = 0;
                else
                    numToSkip[idx] = (unsigned)(firstToCopy - priorCounts[idx]);
                numToReturn[idx] = (unsigned)(priorCounts[idx] + counts[idx] - firstToCopy);
                skipAll = false;
#if THOR_TRACE_LEVEL >= 5
                ActPrintLog("CHOOSESETSLAST: Selection %d.  Range(%d,%" RCPF "d)", idx, numToSkip[idx], counts[idx]);
#endif
            }
        }
        if (skipAll)
            done = true;
    }
public:
    ChooseSetsLastActivity(CGraphElementBase *container) : ChooseSetsPlusActivity(container)
    {
        numToSkip = numToReturn = NULL; 
    }
    ~ChooseSetsLastActivity()
    {
        free(numToSkip);
        free(numToReturn);
    }
    virtual void start() override
    {
        ChooseSetsPlusActivity::start();
        if (numToSkip)
        {
            free(numToSkip);
            free(numToReturn);
        }
        numToSkip = (unsigned *)calloc(sizeof(unsigned), numSets);
        numToReturn = (unsigned *)calloc(sizeof(unsigned), numSets);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (first) 
        {
            first = false;
            getGlobalCounts();
            if (abortSoon) return NULL;
            calculateSelection();
        }
        if (!done) 
        {
            while (!abortSoon)
            {
                OwnedConstThorRow row = inputStream->ungroupedNextRow();
                if (!row)
                    break;
                unsigned category = helper->getCategory(row);
                if (category && numToReturn[category-1])
                {
                    if (numToSkip[category-1] != 0)
                       numToSkip[category-1]--;
                    else
                    {
                        numToReturn[category-1]--;
                        dataLinkIncrement();
                        return row.getClear();
                    }
                }
            }
        }
        return NULL;        
    }
};


class ChooseSetsEnthActivity : public ChooseSetsPlusActivity
{
    __int64 *numerator;
    rowcount_t *denominator;
    rowcount_t *counter;

    void calculateSelection()
    {
        numerator = limits;
        denominator = totalCounts;
        for (unsigned set=0; set < numSets; set++)
        {
            denominator[set] = totalCounts[set];
            if (numerator[set])
            {
                rowcount_t maxBatch = (rowcount_t)((RCMAX - denominator[set]) / numerator[set]);
                rowcount_t prevRecs = priorCounts[set];
                while (prevRecs > 0)
                {
                    rowcount_t next = prevRecs;
                    if (next > maxBatch) next = maxBatch;
                    counter[set] = (counter[set] + next * numerator[set]) % denominator[set];
                    prevRecs -= next;
                }
            }
        }
    }
public:
    ChooseSetsEnthActivity(CGraphElementBase *container) : ChooseSetsPlusActivity(container)
    {
        counter = NULL; numerator = NULL; denominator = NULL;
    }
    ~ChooseSetsEnthActivity()
    {
        free(counter);
    }
    virtual void start() override
    {
        ChooseSetsPlusActivity::start();
        if (counter)
            free(counter);
        counter = (rowcount_t*)calloc(sizeof(rowcount_t), numSets);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (first) 
        {
            first = false;
            getGlobalCounts();
            if (abortSoon) return NULL;
            calculateSelection();
        }
        if (!done)
        {
            while (!abortSoon)
            {
                OwnedConstThorRow row = inputStream->ungroupedNextRow();
                if (!row)
                    break;
                unsigned category = helper->getCategory(row);
                if (category)
                {
                    counter[category-1] += (rowcount_t)numerator[category-1];
                    if(counter[category-1] >= denominator[category-1])
                    {
                        counter[category-1] -= denominator[category-1];
                        dataLinkIncrement();
                        return row.getClear();
                    }       
                }
            }
        }
        return NULL;        
    }
};



//-----------------------------------------------------------------------------------------------


//---------------------------------------------------------------------------


CActivityBase *createLocalChooseSetsSlave(CGraphElementBase *container) { return new LocalChooseSetsActivity(container); }
CActivityBase *createChooseSetsSlave(CGraphElementBase *container) { return new ChooseSetsActivity(container); }
CActivityBase *createChooseSetsLastSlave(CGraphElementBase *container) { return new ChooseSetsLastActivity(container); }
CActivityBase *createChooseSetsEnthSlave(CGraphElementBase *container) { return new ChooseSetsEnthActivity(container); }


