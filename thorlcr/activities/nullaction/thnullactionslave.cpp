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

#include "platform.h"
#include <limits.h>
#include "eclhelper.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"

#include "thnullactionslave.ipp"

class CNullActionSlaveActivity : public CSlaveActivity, public CThorDataLink
{
public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CNullActionSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this) { }
    ~CNullActionSlaveActivity()
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
    } 
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart();
    }
    void stop() { dataLinkStop(); }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        return NULL;
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.totalRowsMin = info.totalRowsMax = 0;
        info.canReduceNumRows = true; // to 0 in fact
    }
};

CActivityBase *createNullActionSlave(CGraphElementBase *container)
{
    return new CNullActionSlaveActivity(container);
}



