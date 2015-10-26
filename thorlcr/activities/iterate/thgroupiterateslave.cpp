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

#include "jiface.hpp"
#include "slave.hpp"

#include "thgroupiterateslave.ipp"
#include "thactivityutil.ipp"

class GroupIterateSlaveActivity : public CSlaveActivity, public CThorDataLink
{

private:
    OwnedConstThorRow prev;
    OwnedConstThorRow defaultLeft;
    IHThorGroupIterateArg * helper;
    rowcount_t count;
    bool eogNext;
    bool anyThisGroup;
    IThorDataLink *input;
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);


    GroupIterateSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        helper = static_cast <IHThorGroupIterateArg *> (queryHelper());
    }
    ~GroupIterateSlaveActivity()
    {
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        anyThisGroup = false;
        eogNext = false;    
        count = 0;
        input = inputs.item(0);
        startInput(input);
        dataLinkStart();
        RtlDynamicRowBuilder r(queryRowAllocator());
        size32_t sz = helper->createDefault(r);
        defaultLeft.setown(r.finalizeRowClear(sz));
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        loop {
            if(abortSoon)
                break;          
            if(eogNext) {
                eogNext = false;
                count = 0;
                if (anyThisGroup) { // ignore eogNext if none in group
                    anyThisGroup = false;
                    break;
                }
            }
            
            OwnedConstThorRow row = input->nextRow();
            if (!row)   {
                count = 0;
                if (anyThisGroup) {
                    anyThisGroup = false;
                    break;
                }
                row.setown(input->nextRow());
                if (!row)
                    break;
            }
            RtlDynamicRowBuilder ret(queryRowAllocator());
            size32_t thisSize = helper->transform(ret, anyThisGroup?prev.get():defaultLeft.get(), row, ++count);
            if (thisSize != 0)  {
                const void *r = ret.finalizeRowClear(thisSize);
                prev.set(r);
                dataLinkIncrement();
                anyThisGroup = true;
                return r;
            }
        }
        return NULL;
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
    bool isGrouped() 
    { 
        return true; 
    }
};


class GroupProcessSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorProcessArg * helper;
    rowcount_t count;
    bool eogNext;
    bool anyThisGroup;
    OwnedConstThorRow firstright;
    OwnedConstThorRow nextright;
    IThorDataLink *input;
    Owned<IRowInterfaces> rightrowif;
    Owned<IEngineRowAllocator> rightAllocator;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    GroupProcessSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        helper = static_cast <IHThorProcessArg *> (queryHelper());
        rightrowif.setown(createRowInterfaces(helper->queryRightRecordSize(),queryId(),queryCodeContext()));
        rightAllocator.set(rightrowif->queryRowAllocator());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        RtlDynamicRowBuilder r(rightAllocator);
        size32_t sz = helper->createInitialRight(r);  
        firstright.setown(r.finalizeRowClear(sz));
        anyThisGroup = false;
        count = 0;
        eogNext = false;    
        input = inputs.item(0);
        startInput(input);
        dataLinkStart();
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        loop {
            if(abortSoon)
                break;          
            if(eogNext) {
                eogNext = false;
                count = 0;
                if (anyThisGroup) { // ignore eogNext if none in group
                    anyThisGroup = false;
                    break;
                }
            }
            OwnedConstThorRow row = input->nextRow();
            if (!row) {
                count = 0;
                if (anyThisGroup) {
                    anyThisGroup = false;
                    break;
                }
                row.setown(input->nextRow());
                if (!row)
                    break;
            }
            RtlDynamicRowBuilder ret(queryRowAllocator());
            RtlDynamicRowBuilder right(rightAllocator);
            size32_t thisSize = helper->transform(ret, right, row, anyThisGroup?nextright.get():firstright.get(), ++count);
            if (thisSize != 0) {
                size32_t rsz = rightAllocator->queryOutputMeta()->getRecordSize(right.getSelf());
                nextright.setown(right.finalizeRowClear(rsz));
                dataLinkIncrement();
                anyThisGroup = true;
                return ret.finalizeRowClear(thisSize);
            }
        }
        return NULL;
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
    bool isGrouped() 
    { 
        return true; 
    }
};




CActivityBase *createGroupIterateSlave(CGraphElementBase *container)
{
    return new GroupIterateSlaveActivity(container);
}

CActivityBase *createGroupProcessSlave(CGraphElementBase *container)
{
    return new GroupProcessSlaveActivity(container);
}



