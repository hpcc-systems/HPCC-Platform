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

#include "jiface.hpp"
#include "slave.hpp"

#include "thgroupiterateslave.ipp"
#include "thactivityutil.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/iterate/thgroupiterateslave.cpp $ $Id: thgroupiterateslave.cpp 62376 2011-02-04 21:59:58Z sort $");



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
        ActivityTimer s(totalCycles, timeActivities, NULL);
        anyThisGroup = false;
        eogNext = false;    
        count = 0;
        input = inputs.item(0);
        startInput(input);
        dataLinkStart("GROUPITERATE", container.queryId());
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
        ActivityTimer t(totalCycles, timeActivities, NULL);
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
        rightrowif.setown(createRowInterfaces(helper->queryRightRecordSize(),queryActivityId(),queryCodeContext()));
        rightAllocator.set(rightrowif->queryRowAllocator());
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        RtlDynamicRowBuilder r(rightAllocator);
        size32_t sz = helper->createInitialRight(r);  
        firstright.setown(r.finalizeRowClear(sz));
        anyThisGroup = false;
        count = 0;
        eogNext = false;    
        input = inputs.item(0);
        startInput(input);
        dataLinkStart("GROUPPROCESS", container.queryId());
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



