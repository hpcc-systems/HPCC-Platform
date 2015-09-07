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


#include "jlib.hpp"
#include "mpbase.hpp"
#include "mputil.hpp"

#include "thresultslave.ipp"

class CResultSlaveActivity : public ProcessSlaveActivity
{
    mptag_t masterMpTag;
    IThorDataLink *input;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CResultSlaveActivity(CGraphElementBase *container) : ProcessSlaveActivity(container) { }

    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        masterMpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    void process()
    {
        processed = 0;

        input = inputs.item(0);
        startInput(input);

        processed = THORDATALINK_STARTED;

        OwnedConstThorRow row = input->ungroupedNextRow();
        CMessageBuffer mb;
        DelayedSizeMarker sizeMark(mb);
        if (row)
        {
            CMemoryRowSerializer msz(mb);
            ::queryRowSerializer(input)->serialize(msz,(const byte *)row.get());
            sizeMark.write();
            processed++;
        }
        queryJobChannel().queryJobComm().send(mb, 0, masterMpTag);
    }

    void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }
    }
};

CActivityBase *createResultSlave(CGraphElementBase *container)
{
    return new CResultSlaveActivity(container);
}

