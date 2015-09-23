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

class CFirstNSlaveBase : public CSlaveActivity, public CThorDataLink
{
protected:
    rowcount_t limit, skipCount;
    Owned<IThorDataLink> input;
    bool stopped;
    IHThorFirstNArg *helper;

    virtual void doStop()
    {
        stopInput(input);
        dataLinkStop();
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CFirstNSlaveBase(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        stopped = true;
        helper = (IHThorFirstNArg *)container.queryHelper();
    }
    ~CFirstNSlaveBase()
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        input.set(inputs.item(0));
        startInput(input);
        stopped = false;
        dataLinkStart();
    }
    void stop()
    {
        if (!stopped)
        {
            abortSoon = true;
            stopped = true;
            doStop();
        }
        input.clear();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        info.totalRowsMax = helper->getLimit();
        calcMetaInfoSize(info,inputs.item(0));
    }
};

class CFirstNSlaveLocal : public CFirstNSlaveBase
{
    bool firstget;
    rowcount_t skipped;
public:
    CFirstNSlaveLocal(CGraphElementBase *container) : CFirstNSlaveBase(container)
    {
    }

// IRowStream overrides
    virtual bool isGrouped() { return false; }
    void start()
    {
        CFirstNSlaveBase::start();
        skipCount = validRC(helper->numToSkip());
        limit = (rowcount_t)helper->getLimit();
        firstget = true;
        skipped = 0;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!abortSoon)
        {
            if (firstget)
            {
                firstget = false;
                while (skipped<skipCount)
                {
                    OwnedConstThorRow row = input->ungroupedNextRow();
                    if (!row)
                    {
                        stop();
                        return NULL;
                    }
                    skipped++;
                }
            }
            if (getDataLinkCount() < limit)
            {
                OwnedConstThorRow row = input->ungroupedNextRow();
                if (row)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
            stop(); // NB: really whatever is pulling, should stop asap.
        }
        return NULL;
    }
};

class CFirstNSlaveGrouped : public CFirstNSlaveBase
{
    unsigned countThisGroup;
public:
    CFirstNSlaveGrouped(CGraphElementBase *container) : CFirstNSlaveBase(container)
    {
    }

// IRowStream overrides
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void start()
    {
        CFirstNSlaveBase::start();
        skipCount = validRC(helper->numToSkip());
        limit = (rowcount_t)helper->getLimit();
        countThisGroup = 0;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        if (!abortSoon)
        {
            loop
            {
                if (0 == countThisGroup && skipCount)
                {
                    unsigned skipped = 0;
                    do
                    {
                        OwnedConstThorRow row = input->nextRow();
                        if (row) 
                            skipped++;
                        else
                        {
                            if (0 == skipped)
                            {
                                stop();
                                return NULL;
                            }
                            skipped = 0; // reset, skip group
                        }
                    }
                    while (skipped<skipCount);
                }
                if (countThisGroup < limit)
                {
                    OwnedConstThorRow row = input->nextRow();
                    if (row)
                    {
                        countThisGroup++;
                        dataLinkIncrement();
                        return row.getClear();
                    }
                    else if (0 == countThisGroup && 0==skipCount)
                    {
                        stop();
                        return NULL;
                    }
                }
                else
                { // consume rest of group
                    loop
                    {
                        OwnedConstThorRow row = input->nextRow();
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

class CFirstNSlaveGlobal : public CFirstNSlaveBase, implements ISmartBufferNotify
{
    Semaphore limitgot;
    CriticalSection crit;
    rowcount_t maxres, skipped, totallimit;
    bool firstget;
    ThorDataLinkMetaInfo inputMeta;

//  ISmartBufferNotify methods used for global firstn only

protected:
    virtual void doStop()
    {
        limitgot.signal(); // JIC not previously signalled by lookahead
        onInputFinished(getDataLinkCount()+skipped);
        CFirstNSlaveBase::doStop();
    }

public:
    CFirstNSlaveGlobal(CGraphElementBase *container) : CFirstNSlaveBase(container)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CFirstNSlaveBase::init(data, slaveData);
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    void start()
    {
        CFirstNSlaveBase::start(); // adds to totalTime (common to local and global firstn)
        ActivityTimer s(totalCycles, timeActivities);
        totallimit = (rowcount_t)helper->getLimit();
        limit = maxres = RCUNBOUND;
        skipCount = 0;
        skipped = 0;
        firstget = true;
        input->getMetaInfo(inputMeta);
        rowcount_t _skipCount = validRC(helper->numToSkip()); // max
        rowcount_t maxRead = (totallimit>(RCUNBOUND-_skipCount))?RCUNBOUND:totallimit+_skipCount;
        input.setown(createDataLinkSmartBuffer(this, input,FIRSTN_SMART_BUFFER_SIZE,isSmartBufferSpillNeeded(this),false,
                                          maxRead,this,true,&container.queryJob().queryIDiskUsage())); // if a very large limit don't bother truncating
        startInput(input);
    }
    void abort()
    {
        CFirstNSlaveBase::abort();
        limitgot.signal();
        CriticalBlock b(crit);
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
    bool recvCounts()
    {
        CMessageBuffer msgMb;
        if (!receiveMsg(msgMb, 0, mpTag))
            return false; // NB: can be triggered by onInputFinished with count==0, or abort()
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
            {
                CriticalBlock b(crit);
                if (RCUNBOUND == maxres)
                {
                    maxres = r;
                    sendCount();
                }
            }
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
        ActivityTimer t(totalCycles, timeActivities);
        if (!abortSoon) {
            if (firstget)
            {
                firstget = false;
                if (!recvCounts())
                    return NULL;
                while (skipped<skipCount)
                {
                    OwnedConstThorRow row = input->ungroupedNextRow();
                    if (!row)
                    {
                        stop();
                        return NULL;
                    }
                    skipped++;
                }
            }
            if (getDataLinkCount() < limit)
            {
                OwnedConstThorRow row = input->ungroupedNextRow();
                if (row)
                {
                    dataLinkIncrement();
                    return row.getClear();
                }
            }
            stop(); // NB: really whatever is pulling, should stop asap.
        }
        return NULL;
    }

    bool isGrouped() { return false; } // need to do different if is!
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        CFirstNSlaveBase::getMetaInfo(info);
        info.canBufferInput = true;
    }
    //  ISmartBufferNotify methods used for global firstn only
    virtual void onInputStarted(IException *) { } // not needed
    virtual bool startAsync() { return false; }
    virtual void onInputFinished(rowcount_t count)  // count is the total read from input (including skipped)
    {
        // sneaky short circuit
        {
            CriticalBlock b(crit);
            if (RCUNBOUND != maxres) return;
            maxres = count;
            sendCount();
        }
        ActPrintLog("FIRSTN: maximum row count %" RCPF "d", count);
        if (0 == count)
        {
            limit = 0;
            CriticalBlock b(crit);
            cancelReceiveMsg(RANK_ALL, mpTag);
        }
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
