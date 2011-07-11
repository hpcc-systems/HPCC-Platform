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

#include "thenthslave.ipp"
#include "thactivityutil.ipp"
#include "thbufdef.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/enth/thenthslave.cpp $ $Id: thenthslave.cpp 65251 2011-06-08 08:38:02Z jsmith $");

class BaseEnthActivity : public CSlaveActivity, public CThorDataLink, implements ISmartBufferNotify
{
protected:
    StringBuffer actStr;
    Semaphore finishedSem;
    rowcount_t counter, localRecCount;
    rowcount_t denominator, numerator;
    Owned<IThorDataLink> input;
    bool localCountReq;

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
        ActPrintLog("ENTH: init - Numerator = %"RCPF"d, Denominator = %"RCPF"d", numerator, denominator);   
        ActPrintLog("%s: Inital value of counter %"RCPF"d", actStr.str(), counter);
#endif
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    BaseEnthActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        localCountReq = false;
    }
    ~BaseEnthActivity()
    {
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        IHThorEnthArg *helper = static_cast <IHThorEnthArg *> (queryHelper());
        denominator = validRC(helper->getProportionDenominator());
        numerator = validRC(helper->getProportionNumerator());
        input.set(inputs.item(0));
        startInput(input);
        dataLinkStart("ENTH", container.queryId());

        if (localCountReq)
        {
            ThorDataLinkMetaInfo info;
            input->getMetaInfo(info);
            // Need lookahead _unless_ row count pre-known.
            if (info.totalRowsMin == info.totalRowsMax)
            {
                localRecCount = (rowcount_t)info.totalRowsMax;
                ActPrintLog("%s: row count pre-known to be %"RCPF"d", actStr.str(), localRecCount);
            }
            else
            {
                localRecCount = RCUNBOUND;
                input.setown(createDataLinkSmartBuffer(this, input,ENTH_SMART_BUFFER_SIZE,true,false,RCUNBOUND,this,true,&container.queryJob().queryIDiskUsage()));
                StringBuffer tmpStr(actStr);
                startInput(input);
            }
        }
    }
    virtual void stop()
    {
        stopInput(input);
        input.clear();
        dataLinkStop();
    }
    virtual bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
// ISmartBufferNotify impl.
    virtual void onInputStarted(IException *) { }
    virtual bool startAsync() { return false; }
};

class CLocalEnthSlaveActivity : public BaseEnthActivity
{
public:
    CLocalEnthSlaveActivity(CGraphElementBase *container) : BaseEnthActivity(container)
    {
        actStr.append("LOCALENTH");
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        BaseEnthActivity::init(data, slaveData);
        if (RCUNBOUND == denominator)
            localCountReq = true;
        else
            setInitialCounter(0);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
            OwnedConstThorRow row(input->ungroupedNextRow());
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
        BaseEnthActivity::abort();
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
    Semaphore prevRecCountSem;
    rowcount_t prevRecCount;
    bool first;

    void sendCount(rowcount_t count)
    {
        if (container.queryJob().queryMyRank() == container.queryJob().querySlaves()) // don't send if last node
            return;
        CMessageBuffer msg;
        msg.append(count);
        container.queryJob().queryJobComm().send(msg, container.queryJob().queryMyRank()+1, mpTag);
    }
public:
    CEnthSlaveActivity(CGraphElementBase *container) : BaseEnthActivity(container)
    { 
        localCountReq = true;
        actStr.append("ENTH");
    }
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {
        BaseEnthActivity::init(data, slaveData);
        mpTag = container.queryJob().deserializeMPTag(data);
    }
    virtual void start()
    {
        BaseEnthActivity::start();
        prevRecCount = 0;
        first = true;
    }
    virtual void abort()
    {
        BaseEnthActivity::abort();
        if (1 != container.queryJob().queryMyRank())
            cancelReceiveMsg(RANK_ALL, mpTag);
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (first)
        {
            first = false;
            if (container.queryJob().queryMyRank() > 1) // no need if 1st node
            {
                CMessageBuffer msg;
                if (!receiveMsg(msg, container.queryJob().queryMyRank()-1, mpTag))
                    return NULL;
                msg.read(prevRecCount);
            }
            setInitialCounter(prevRecCount);
            if (haveLocalCount()) // if local total count known, send total now
                sendCount(prevRecCount + localRecCount);
            else
                prevRecCountSem.signal();
        }
        while (!abortSoon)
        {
            OwnedConstThorRow row(input->ungroupedNextRow());
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
    virtual void onInputFinished(rowcount_t localRecCount)
    {
        if (!haveLocalCount())
        {
            ActPrintLog("maximum row count %"RCPF"d", localRecCount);
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


