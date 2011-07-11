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

#include "thcountprojectslave.ipp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/countproject/thcountprojectslave.cpp $ $Id: thcountprojectslave.cpp 63725 2011-04-01 17:40:45Z jsmith $");


class BaseCountProjectActivity : public CSlaveActivity,  public CThorDataLink, implements ISmartBufferNotify
{
protected:
    IHThorCountProjectArg *helper;
    rowcount_t count;
    Owned<IThorDataLink> input;

    void start()
    {
        count = 0;
        dataLinkStart("COUNTPROJECT", container.queryId());
    }
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    BaseCountProjectActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        helper = NULL;
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorCountProjectArg *> (queryHelper());
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    virtual void onInputStarted(IException *)
    {
        // not needed
    }
    virtual bool startAsync() { return false; }
};


class LocalCountProjectActivity : public BaseCountProjectActivity
{
    bool anyThisGroup;

public:
    LocalCountProjectActivity(CGraphElementBase *container) : BaseCountProjectActivity(container)
    {
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        ActPrintLog("COUNTPROJECT: Is Local");
        input.set(inputs.item(0));
        anyThisGroup = false;
        startInput(input);
        BaseCountProjectActivity::start();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        while (!abortSoon)
        {
            OwnedConstThorRow row(input->nextRow());
            if (!row)
            {
                if (anyThisGroup) 
                    break;
                row.setown(input->nextRow());
                if (!row)
                    break;
                count = 0;
            }
            RtlDynamicRowBuilder ret(queryRowAllocator());
            size32_t sizeGot = helper->transform(ret, row, ++count);
            if (sizeGot)
            {
                dataLinkIncrement();
                anyThisGroup = true;
                return ret.finalizeRowClear(sizeGot);
            }
        }
        count = 0;
        anyThisGroup = false;
        return NULL;        
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
    virtual void onInputFinished(rowcount_t finalCount)
    {
        // no action required
    }
};


class CountProjectActivity : public BaseCountProjectActivity
{
private:
    bool first;
    Semaphore prevRecCountSem;
    rowcount_t prevRecCount, localRecCount;

    bool haveLocalCount() { return RCUNSET != localRecCount; }
    void sendCount(rowcount_t _count)
    {
        // either called by onInputFinished(signaled by nextRow/stop) or by nextRow/stop itself
        if (container.queryJob().queryMyRank() == container.queryJob().querySlaves()) // don't send if last node
            return;
        CMessageBuffer msg;
        msg.append(_count);
        container.queryJob().queryJobComm().send(msg, container.queryJob().queryMyRank()+1, mpTag);
    }
    rowcount_t getPrevCount()
    {
        if (container.queryJob().queryMyRank() > 1)
        {
            CMessageBuffer msg;
            rowcount_t _count;
            if (!receiveMsg(msg, container.queryJob().queryMyRank()-1, mpTag))
                return 0;
            msg.read(_count);
            return _count;
        }
        else
            return 0;
    }
    void signalNext()
    {
        if (haveLocalCount()) // if local total count known, send total now
        {
            ActPrintLog("COUNTPROJECT: row count pre-known to be %"RCPF"d", localRecCount);
            sendCount(prevRecCount + localRecCount);
        }
        else
            prevRecCountSem.signal();
    }
public:
    CountProjectActivity(CGraphElementBase *container) : BaseCountProjectActivity(container)
    {
    }   
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        BaseCountProjectActivity::init(data, slaveData);
        mpTag = container.queryJob().deserializeMPTag(data);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        ActPrintLog( "COUNTPROJECT: Is Global");
        first = true;
        prevRecCount = 0;
        startInput(inputs.item(0));
        ThorDataLinkMetaInfo info;
        inputs.item(0)->getMetaInfo(info);
        localRecCount = (info.totalRowsMin == info.totalRowsMax) ? (rowcount_t)info.totalRowsMax : RCUNSET;
        input.setown(createDataLinkSmartBuffer(this, inputs.item(0), COUNTPROJECT_SMART_BUFFER_SIZE, true, false, RCUNBOUND, this, true, &container.queryJob().queryIDiskUsage())); // could spot disk write output here?
        input->start();
        BaseCountProjectActivity::start();
    }
    virtual void stop()
    {
        BaseCountProjectActivity::stop();
        if (first) // nextRow, therefore getPrevCount()/sendCount() never called
        {
            prevRecCount = count = getPrevCount();
            signalNext();
        }
    }
    virtual void abort()
    {
        CSlaveActivity::abort();
        prevRecCountSem.signal();
        if (container.queryJob().queryMyRank() > 1)
            cancelReceiveMsg(container.queryJob().queryMyRank()-1, mpTag);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (first) 
        {
            first = false;
            prevRecCount = count = getPrevCount();
            signalNext();
        }
        while (!abortSoon)
        {
            OwnedConstThorRow row(input->nextRow()); // NB: lookahead ensures ungrouped
            if (!row) 
                break;
            RtlDynamicRowBuilder ret(queryRowAllocator());
            size32_t sizeGot = helper->transform(ret, row, ++count);
            if (sizeGot)
            {
                dataLinkIncrement();
                return ret.finalizeRowClear(sizeGot);
            }
        }
        return NULL;
    }
    virtual bool isGrouped() { return false; }
    virtual void onInputFinished(rowcount_t localRecCount)
    {
        if (!haveLocalCount())
        {
            prevRecCountSem.wait();
            if (!abortSoon)
            {
                ActPrintLog("count is %"RCPF"d", localRecCount);
                sendCount(prevRecCount + localRecCount);
            }
        }
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        info.isSequential = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
};


CActivityBase *createLocalCountProjectSlave(CGraphElementBase *container) { return new LocalCountProjectActivity(container); }
CActivityBase *createCountProjectSlave(CGraphElementBase *container) { return new CountProjectActivity(container); }

