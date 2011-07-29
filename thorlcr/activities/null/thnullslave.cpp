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

#include "thnullslave.ipp"
#include "thactivityutil.ipp"

class CNullSinkSlaveActivity : public ProcessSlaveActivity
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNullSinkSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
    }
// IThorSlaveActivity
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {       
    }
    virtual void process()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        startInput(inputs.item(0));
        stopInput(inputs.item(0));
    }
    virtual void endProcess()
    {
    }
};


class CNullSlaveActivity : public CSlaveActivity, public CThorDataLink
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNullSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        appendOutputLinked(this);
    }
// IThorSlaveActivity
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {       
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("NULL", container.queryId());
    }

    virtual void stop()
    {
        dataLinkStop();
    }

    const void * nextRow() 
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        return NULL;
    }

    virtual bool isGrouped()
    {
        return queryHelper()->queryOutputMeta()->isGrouped();
    }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canReduceNumRows = true; // to 0 in fact
        info.totalRowsMax = 0;
    }
};


class CThroughSlaveActivity : public CSlaveActivity, public CThorDataLink
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CThroughSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        appendOutputLinked(this);
    }
// IThorSlaveActivity
    virtual void init(MemoryBuffer & data, MemoryBuffer &slaveData)
    {       
    }

// IThorDataLink
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        startInput(inputs.item(0));
        dataLinkStart("NULL", container.queryId());
    }
    virtual void stop()
    {
        stopInput(inputs.item(0));
        dataLinkStop();
    }
    const void * nextRow() 
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        return inputs.item(0)->nextRow();
    }
    virtual bool isGrouped()
    {
        return inputs.item(0)->isGrouped();
    }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        inputs.item(0)->getMetaInfo(info);
    }
};



CActivityBase *createNullSinkSlave(CGraphElementBase *container) { return new CNullSinkSlaveActivity(container); }
CActivityBase *createNullSlave(CGraphElementBase *container) { return new CNullSlaveActivity(container); }
CActivityBase *createThroughSlave(CGraphElementBase *container) { return new CThroughSlaveActivity(container); }

