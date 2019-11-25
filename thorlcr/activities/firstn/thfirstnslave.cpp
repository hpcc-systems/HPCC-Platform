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

#include "platform.h"
#include <limits.h>
#include "eclhelper.hpp"        // for IHThorFirstNArg
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"
#include "thbufdef.hpp"

#include "thfirstnslave.ipp"

class CFirstNSlaveBase : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

protected:
    rowcount_t limit = RCUNBOUND, skipCount = 0;
    bool stopped = true;
    IHThorFirstNArg *helper;

public:
    CFirstNSlaveBase(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        stopped = true;
        helper = (IHThorFirstNArg *)container.queryHelper();
        appendOutputLinked(this);
    }
    void doStopInput()
    {
        stopInput(0);
        abortSoon = true;
    }
    virtual void start() override
    {
        PARENT::start();
        stopped = false;
    }
    virtual void stop() override
    {
        if (!stopped)
        {
            abortSoon = true;
            stopped = true;
        }
        PARENT::stop();
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        info.totalRowsMax = helper->getLimit();
        calcMetaInfoSize(info, queryInput(0));
    }
};

class CFirstNSlaveLocal : public CFirstNSlaveBase
{
    typedef CFirstNSlaveBase PARENT;

    bool firstget = true;
    rowcount_t skipped = 0;
public:
    CFirstNSlaveLocal(CGraphElementBase *_container) : CFirstNSlaveBase(_container)
    {
        setRequireInitData(false);
    }

// IRowStream overrides
    virtual bool isGrouped() const override { return false; }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        skipCount = (rowcount_t)helper->numToSkip();
        limit = (rowcount_t)helper->getLimit();
        firstget = true;
        skipped = 0;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!abortSoon)
        {
            if (firstget)
            {
                firstget = false;
                while (skipped<skipCount)
                {
                    OwnedConstThorRow row = inputStream->ungroupedNextRow();
                    if (!row)
                    {
                        doStopInput();
                        return NULL;
                    }
                    skipped++;
                }
            }
            if (getDataLinkCount() < limit)
            {
                OwnedConstThorRow row = inputStream->ungroupedNextRow();
                if (row)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
            doStopInput(); // NB: really whatever is pulling, should stop asap.
        }
        return NULL;
    }
};

class CFirstNSlaveGrouped : public CFirstNSlaveBase
{
    typedef CFirstNSlaveBase PARENT;

    unsigned countThisGroup = 0;
public:
    CFirstNSlaveGrouped(CGraphElementBase *_container) : CFirstNSlaveBase(_container)
    {
        setRequireInitData(false);
    }

// IRowStream overrides
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void start()
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        skipCount = (rowcount_t)helper->numToSkip();
        limit = (rowcount_t)helper->getLimit();
        countThisGroup = 0;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!abortSoon)
        {
            for (;;)
            {
                if (0 == countThisGroup && skipCount)
                {
                    unsigned skipped = 0;
                    do
                    {
                        OwnedConstThorRow row = inputStream->nextRow();
                        if (row) 
                            skipped++;
                        else
                        {
                            if (0 == skipped)
                            {
                                doStopInput();
                                return NULL;
                            }
                            skipped = 0; // reset, skip group
                        }
                    }
                    while (skipped<skipCount);
                }
                if (countThisGroup < limit)
                {
                    OwnedConstThorRow row = inputStream->nextRow();
                    if (row)
                    {
                        countThisGroup++;
                        dataLinkIncrement();
                        return row.getClear();
                    }
                    else if (0 == countThisGroup && 0==skipCount)
                    {
                        doStopInput();
                        return NULL;
                    }
                }
                else
                { // consume rest of group
                    for (;;)
                    {
                        OwnedConstThorRow row = inputStream->nextRow();
                        if (!row)
                            break;
                    }
                }
                if (countThisGroup)
                    break; // return eog and reset
            }
        }
        countThisGroup = 0;
        return NULL;
    }
};

class CFirstNSlaveGlobal : public CFirstNSlaveBase, implements ILookAheadStopNotify
{
    typedef CFirstNSlaveBase PARENT;

    Semaphore limitgot;
    CriticalSection crit;
    rowcount_t maxres = RCUNBOUND, skipped = 0, totallimit = RCUNBOUND;
    bool firstget = true;
    ThorDataLinkMetaInfo inputMeta;

    rowcount_t lastTotalLimitState = 0;
    rowcount_t lastSkipCountState = RCMAX;

    void sendOnce(rowcount_t count)
    {
        {
            CriticalBlock b(crit);
            if (RCUNBOUND != maxres) // already set and sent
                return;
            maxres = count;
        }
        sendCount();
    }
    void ensureSendCount()
    {
        if (hasStarted() && !isLookAheadActive(0))
            sendOnce(getDataLinkCount() + skipped);
    }
    void doStopInput()
    {
        ensureSendCount();
        PARENT::doStopInput();
    }
public:
    CFirstNSlaveGlobal(CGraphElementBase *container) : CFirstNSlaveBase(container)
    {
    }
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData) override
    {
        PARENT::init(data, slaveData);
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start(); // adds to totalTime (common to local and global firstn)

        limit = maxres = RCUNBOUND;
        skipCount = 0;
        skipped = 0;
        firstget = true;
        input->getMetaInfo(inputMeta);
        totallimit = (rowcount_t)helper->getLimit();

        rowcount_t _skipCount = (rowcount_t)helper->numToSkip(); // max
        if (!isInputFastThrough(0))
        {
            if (hasLookAhead(0) && (lastTotalLimitState == totallimit) && (lastSkipCountState == _skipCount))
                startLookAhead(0);
            else
            {
                rowcount_t maxRead = (totallimit>(RCUNBOUND-_skipCount))?RCUNBOUND:totallimit+_skipCount;
                setLookAhead(0, createRowStreamLookAhead(this, inputStream, queryRowInterfaces(input), FIRSTN_SMART_BUFFER_SIZE, ::canStall(input), false,
                                                                                  maxRead, this, &container.queryJob().queryIDiskUsage()), false); // if a very large limit don't bother truncating
                lastTotalLimitState = totallimit;
                lastSkipCountState = _skipCount;
            }
        }
    }
    virtual void stop() override
    {
        ensureSendCount();
        PARENT::stop();
    }
    virtual void abort()
    {
        PARENT::abort();
        limitgot.signal();
        CriticalBlock b(crit);
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
    bool recvCounts()
    {
        BlockedActivityTimer t(slaveTimerStats, timeActivities);
        CMessageBuffer msgMb;
        if (!receiveMsg(msgMb, 0, mpTag))
            return false; // NB: can be triggered by abort()
        msgMb.read(limit);
        msgMb.read(skipCount);
        skipped = 0;
        limitgot.signal();
        if (inputMeta.totalRowsMin==inputMeta.totalRowsMax)
        {
            ActPrintLog("Row count pre-known to be %" I64F "d", inputMeta.totalRowsMin);
            rowcount_t r = (rowcount_t)inputMeta.totalRowsMin;
            if (limit+skipCount<r)
                r = limit+skipCount;
            // sneaky short circuit
            sendOnce(r);
        }
        ActPrintLog("FIRSTN: Record limit is %" RCPF "d %" RCPF "d", limit, skipCount); 
        return true;
    }
    void sendCount()
    {
        limitgot.wait();

        rowcount_t read = 0;
        rowcount_t skip = skipCount;
        if (limit > 0)
        {
            // maxres includes skipped so can be used to decrement _skip and calculate number read
            // (this may be more than *actually* read if stopped early on this node but think that doesn't matter)
            if (maxres>=skip)
            {
                maxres -= skip;
                skip = 0;
            }
            else
            {
                skip -= maxres;
                maxres = 0;
            }
            if (maxres>0)
            {
                if (maxres>limit)
                    read = limit;
                else
                    read = maxres;
            }
        }
        CMessageBuffer msgMb;
        msgMb.append(read);
        msgMb.append(skip);
        queryJobChannel().queryJobComm().send(msgMb, 0, mpTag);
        ActPrintLog("FIRSTN: Read %" RCPF "d records, left to skip=%" RCPF "d", read, skip);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        if (!abortSoon)
        {
            if (firstget)
            {
                firstget = false;
                if (!recvCounts())
                    return NULL;
                while (skipped<skipCount)
                {
                    OwnedConstThorRow row = inputStream->ungroupedNextRow();
                    if (!row)
                    {
                        doStopInput();
                        return NULL;
                    }
                    skipped++;
                }
            }
            if (getDataLinkCount() < limit)
            {
                OwnedConstThorRow row = inputStream->ungroupedNextRow();
                if (row)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
            doStopInput(); // NB: really whatever is pulling, should stop asap.
        }
        return NULL;
    }

    virtual bool isGrouped() const override { return false; } // need to do different if is!
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        PARENT::getMetaInfo(info);
        info.canBufferInput = true;
    }
// ILookAheadStopNotify
    virtual void onInputFinished(rowcount_t count) override // count is the total read from input (including skipped)
    {
        sendOnce(count);
        ActPrintLog("FIRSTN: maximum row count %" RCPF "d", count);
    }
};

CActivityBase *createFirstNSlave(CGraphElementBase *container)
{
    if (container->queryGrouped())
        return new CFirstNSlaveGrouped(container);
    else if (container->queryLocal())
        return new CFirstNSlaveLocal(container);
    else
        return new CFirstNSlaveGlobal(container);
}
