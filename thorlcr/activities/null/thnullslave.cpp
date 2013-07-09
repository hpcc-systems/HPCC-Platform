/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
        dataLinkStart();
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
        dataLinkStart();
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

