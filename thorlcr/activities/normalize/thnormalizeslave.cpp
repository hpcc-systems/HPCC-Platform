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

#include "platform.h"
#include "jiface.hpp"       // IInterface defined in jlib

#include "eclhelper.hpp"        // for IHThorNormalizeArg
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thnormalizeslave.ipp"
#include "thexception.hpp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/normalize/thnormalizeslave.cpp $ $Id: thnormalizeslave.cpp 62376 2011-02-04 21:59:58Z sort $");


class NormalizeSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorNormalizeArg * helper;
    IThorDataLink *input;
    OwnedConstThorRow row;
    unsigned curRow;
    unsigned numThisRow;
    bool anyThisGroup;
    size32_t crcExtraSize;
    Owned<IEngineRowAllocator> allocator;


public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    NormalizeSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorNormalizeArg *> (queryHelper());
        allocator.set(queryRowAllocator());
    }
    void start()
    { 
        ActivityTimer s(totalCycles, timeActivities, NULL);
        numThisRow = 0;
        curRow = 0;
        anyThisGroup = false;
        input = inputs.item(0);
        startInput(input);
        dataLinkStart("NORMALIZE", container.queryId()); 
    }
    void stop()
    { 
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        loop
        {
            while (curRow == numThisRow)
            {
                if (abortSoon) 
                    return NULL;
                row.setown(input->nextRow());
                if (!row&&!anyThisGroup)
                    row.setown(input->nextRow());
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
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canIncreaseNumRows = true;
    }
};


////////////////////


class CNormalizeChildSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorNormalizeChildArg *helper;
    INormalizeChildIterator *cursor;
    IThorDataLink *input;
    OwnedConstThorRow childBuf;
    void * curChildRow;
    unsigned curRow;
    bool anyThisGroup;
    Owned<IEngineRowAllocator> allocator;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNormalizeChildSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    { 
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorNormalizeChildArg *> (queryHelper());

        cursor = helper->queryIterator();
        allocator.set(queryRowAllocator());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        anyThisGroup = false;
        dataLinkStart("NORMALIZECHILD", container.queryId()); 
        curChildRow = NULL;
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        loop {
            while(!curChildRow) {
                curRow = 0;
                childBuf.setown(input->nextRow());
                if (!childBuf) {
                    if (anyThisGroup) 
                    {
                        anyThisGroup = false;
                        return NULL;
                    }
                    childBuf.setown(input->nextRow());
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
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.unknownRowsOutput = true;
        info.canIncreaseNumRows = true;
    }
};

class CNormalizeLinkedChildSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    IHThorNormalizeLinkedChildArg *helper;
    IThorDataLink *input;
    bool anyThisGroup;

    OwnedConstThorRow curParent;
    OwnedConstThorRow curChild;

    bool advanceInput()
    {
        loop
        {
            curParent.setown(input->nextRow());
            if (!curParent)
            {
                if (anyThisGroup)
                {
                    anyThisGroup = false;
                    return false;
                }
                curParent.setown(input->nextRow());
                if (!curParent)
                    return false;
            }

            curChild.set(helper->first(curParent));
            if (curChild)
                return true;
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNormalizeLinkedChildSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    { 
    }
    virtual bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = static_cast <IHThorNormalizeLinkedChildArg *> (queryHelper());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input = inputs.item(0);
        startInput(input);
        anyThisGroup = false;
        dataLinkStart("NORMALIZECHILD", container.queryId()); 
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        loop
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
    void getMetaInfo(ThorDataLinkMetaInfo &info)
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



