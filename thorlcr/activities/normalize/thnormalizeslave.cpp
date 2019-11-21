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
#include "jiface.hpp"       // IInterface defined in jlib

#include "eclhelper.hpp"        // for IHThorNormalizeArg
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thnormalizeslave.ipp"
#include "thexception.hpp"


class NormalizeSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorNormalizeArg * helper;
    OwnedConstThorRow row;
    unsigned curRow;
    unsigned numThisRow;
    bool anyThisGroup;
    Owned<IEngineRowAllocator> allocator;


public:
    NormalizeSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container)
    {
        helper = static_cast <IHThorNormalizeArg *> (queryHelper());
        allocator.set(queryRowAllocator());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual void start() override
    { 
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        numThisRow = 0;
        curRow = 0;
        anyThisGroup = false;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;)
        {
            while (curRow == numThisRow)
            {
                if (abortSoon) 
                    return NULL;
                row.setown(inputStream->nextRow());
                if (!row&&!anyThisGroup)
                    row.setown(inputStream->nextRow());
                if(!row) {
                    anyThisGroup = false;
                    return NULL;
                }
                curRow = 0;
                numThisRow = helper->numExpandedRows(row);
            }
            if (abortSoon) 
                return NULL;
            RtlDynamicRowBuilder ret(allocator);
            size32_t sz = helper->transform(ret, row, ++curRow);
            if (sz!=0) {
                anyThisGroup = true;
                dataLinkIncrement();
                return ret.finalizeRowClear(sz);
            }
        }
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canIncreaseNumRows = true;
    }
};


////////////////////


class CNormalizeChildSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorNormalizeChildArg *helper;
    INormalizeChildIterator *cursor;
    OwnedConstThorRow childBuf;
    void * curChildRow;
    unsigned curRow;
    bool anyThisGroup;
    Owned<IEngineRowAllocator> allocator;

public:
    CNormalizeChildSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container)
    { 
        helper = static_cast <IHThorNormalizeChildArg *> (queryHelper());
        cursor = helper->queryIterator();
        allocator.set(queryRowAllocator());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        anyThisGroup = false;
        curChildRow = NULL;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;) {
            while(!curChildRow) {
                curRow = 0;
                childBuf.setown(inputStream->nextRow());
                if (!childBuf) {
                    if (anyThisGroup) 
                    {
                        anyThisGroup = false;
                        return NULL;
                    }
                    childBuf.setown(inputStream->nextRow());
                    if (!childBuf) // eos
                        return NULL;
                }
                curChildRow = cursor->first(childBuf);
            }

            RtlDynamicRowBuilder ret(allocator);
            size32_t sz = helper->transform(ret, childBuf, curChildRow, ++curRow);
            curChildRow = cursor->next();

            if (sz) {
                dataLinkIncrement();
                anyThisGroup = true;
                return ret.finalizeRowClear(sz);
            }
        }
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canIncreaseNumRows = true;
    }
};

class CNormalizeLinkedChildSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    IHThorNormalizeLinkedChildArg *helper;
    bool anyThisGroup;

    OwnedConstThorRow curParent;
    OwnedConstThorRow curChild;

    bool advanceInput()
    {
        for (;;)
        {
            curParent.setown(inputStream->nextRow());
            if (!curParent)
            {
                if (anyThisGroup)
                {
                    anyThisGroup = false;
                    return false;
                }
                curParent.setown(inputStream->nextRow());
                if (!curParent)
                    return false;
            }

            curChild.set(helper->first(curParent));
            if (curChild)
                return true;
        }
    }

public:
    CNormalizeLinkedChildSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container)
    { 
        helper = static_cast <IHThorNormalizeLinkedChildArg *> (queryHelper());
        setRequireInitData(false);
        appendOutputLinked(this);
    }
    virtual bool isGrouped() const override { return queryInput(0)->isGrouped(); }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        anyThisGroup = false;
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
        for (;;)
        {
            if (!curParent)
            {
                if (!advanceInput())
                    return NULL;
            }
            OwnedConstThorRow ret = curChild.getClear();
            curChild.set(helper->next());
            if (!curChild)
                curParent.clear();
            if (ret)
            {
                anyThisGroup = true;
                dataLinkIncrement();
                return ret.getClear();
            }
        }
    }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canIncreaseNumRows = true;
    }
};


CActivityBase *createNormalizeChildSlave(CGraphElementBase *container)
{
    return new CNormalizeChildSlaveActivity(container);
}

CActivityBase *createNormalizeLinkedChildSlave(CGraphElementBase *container)
{
    return new CNormalizeLinkedChildSlaveActivity(container);
}


CActivityBase *createNormalizeSlave(CGraphElementBase *container)
{
    return new NormalizeSlaveActivity(container);
}



