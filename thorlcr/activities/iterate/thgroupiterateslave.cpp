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

class GroupIterateSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    OwnedConstThorRow prev;
    OwnedConstThorRow defaultLeft;
    IHThorGroupIterateArg * helper;
    rowcount_t count;
    bool eogNext;
    bool anyThisGroup;
public:
    GroupIterateSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorGroupIterateArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        anyThisGroup = false;
        eogNext = false;    
        count = 0;
        RtlDynamicRowBuilder r(queryRowAllocator());
        size32_t sz = helper->createDefault(r);
        defaultLeft.setown(r.finalizeRowClear(sz));
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;) {
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
            
            OwnedConstThorRow row = inputStream->nextRow();
            if (!row)   {
                count = 0;
                if (anyThisGroup) {
                    anyThisGroup = false;
                    break;
                }
                row.setown(inputStream->nextRow());
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
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    virtual bool isGrouped() const override
    { 
        return true; 
    }
};


class GroupProcessSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorProcessArg * helper;
    rowcount_t count;
    bool eogNext;
    bool anyThisGroup;
    OwnedConstThorRow firstright;
    OwnedConstThorRow nextright;
    Owned<IThorRowInterfaces> rightrowif;
    Owned<IEngineRowAllocator> rightAllocator;

public:
    GroupProcessSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorProcessArg *> (queryHelper());
        rightrowif.setown(createRowInterfaces(helper->queryRightRecordSize()));
        rightAllocator.set(rightrowif->queryRowAllocator());
        appendOutputLinked(this);   // adding 'me' to outputs array
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        RtlDynamicRowBuilder r(rightAllocator);
        size32_t sz = helper->createInitialRight(r);  
        firstright.setown(r.finalizeRowClear(sz));
        anyThisGroup = false;
        count = 0;
        eogNext = false;    
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;) {
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
            OwnedConstThorRow row = inputStream->nextRow();
            if (!row) {
                count = 0;
                if (anyThisGroup) {
                    anyThisGroup = false;
                    break;
                }
                row.setown(inputStream->nextRow());
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
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        if (helper->canFilter())
            info.canReduceNumRows = true;
        calcMetaInfoSize(info, queryInput(0));
    }
    virtual bool isGrouped() const override
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



