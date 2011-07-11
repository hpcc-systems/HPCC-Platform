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

#include "thapplyslave.ipp"
#include "thactivityutil.ipp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/apply/thapplyslave.cpp $ $Id: thapplyslave.cpp 62376 2011-02-04 21:59:58Z sort $");

class CApplySlaveActivity : public ProcessSlaveActivity
{
    IHThorApplyArg *helper;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CApplySlaveActivity(CGraphElementBase *container) 
        : ProcessSlaveActivity(container)
    { 
        helper = NULL;
        input = NULL;
    }

// IThorSlaveActivity overloaded methods
    virtual void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = static_cast <IHThorApplyArg *> (queryHelper());
    }
// IThorSlaveProcess overloaded methods
    virtual void process()
    {
        processed = 0;
        input = inputs.item(0);
        startInput(input);
        processed = THORDATALINK_STARTED;
        try
        {
            if (container.queryLocalOrGrouped() || 1 == container.queryJob().queryMyRank())
                helper->start();
            while(!abortSoon)
            {
                ActivityTimer t(totalCycles, timeActivities, NULL);
                OwnedConstThorRow r = input->ungroupedNextRow();
                if (!r)
                    break;
                helper->apply(r);
                processed++;
            }
            if (container.queryLocalOrGrouped() || 1 == container.queryJob().queryMyRank())
                helper->end();
        }
        catch(CATCHALL)
        {
            ActPrintLog("APPLY: exception");
            throw;
        }
    }
    virtual void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }
    }
};

CActivityBase *createApplySlave(CGraphElementBase *container)
{
    return new CApplySlaveActivity(container);
}
