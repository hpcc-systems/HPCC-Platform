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
        masterMpTag = container.queryJob().deserializeMPTag(data);
    }
    void process()
    {
        processed = 0;

        input = inputs.item(0);
        startInput(input);

        processed = THORDATALINK_STARTED;

        OwnedConstThorRow row = input->ungroupedNextRow();
        CMessageBuffer mb;
        size32_t lenpos = mb.length(); // its 0 really
        mb.append((size32_t)0);
        if (row) {
            CMemoryRowSerializer msz(mb);
            ::queryRowSerializer(input)->serialize(msz,(const byte *)row.get());
            size32_t sz = mb.length()-lenpos-sizeof(size32_t);
            mb.writeDirect(lenpos,sizeof(size32_t),&sz);
            processed++;
        }
        container.queryJob().queryJobComm().send(mb, 0, masterMpTag);
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

