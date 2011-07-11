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

#include "slave.ipp"
#include "thactivityutil.ipp"

#include "thbufdef.hpp"
#include "thpullslave.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/pull/thpullslave.cpp $ $Id: thpullslave.cpp 65251 2011-06-08 08:38:02Z jsmith $");




class PullSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    Owned<IThorDataLink> input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    PullSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    ~PullSlaveActivity() 
    {
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    }

// IThorDataLink methods
    virtual void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        input.setown(createDataLinkSmartBuffer(this,inputs.item(0),PULL_SMART_BUFFER_SIZE,true,false,RCUNBOUND,NULL,false,&container.queryJob().queryIDiskUsage()));
        startInput(input);
        dataLinkStart("PULL", container.queryId());
    }
    virtual void stop()
    {
        stopInput(input);
        dataLinkStop();
    }

    const void * nextRow()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow row = input->nextRow();
        if (!row)
            return NULL;
        dataLinkIncrement();
        return row.getClear();
    }

    virtual bool isGrouped() { return false; } // or input->isGrouped?
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
};


CActivityBase *createPullSlave(CGraphElementBase *container)
{
    return new PullSlaveActivity(container);
}

