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
#include <limits.h>
#include "eclhelper.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"

#include "thwuidreadslave.ipp"

class CWuidReadSlaveActivity : public CSlaveActivity, public CThorDataLink
{
    Owned<ISerialStream> replyStream;
    CThorStreamDeserializerSource rowSource;
    IHThorWorkunitReadArg *helper;
    bool grouped;
    bool eogPending;
    mptag_t replyTag;
    CMessageBuffer masterReplyMsg;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CWuidReadSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container), CThorDataLink(this)
    {
        replyTag = createReplyTag();
        replyStream.setown(createMemoryBufferSerialStream(masterReplyMsg));
        rowSource.setStream(replyStream);
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = (IHThorWorkunitReadArg *)queryHelper();
        grouped = helper->queryOutputMeta()->isGrouped();
    } 
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("WUIDREAD", container.queryId());

        eogPending = false;
        if (container.queryLocal() || firstNode())
        {
            CMessageBuffer reqMsg;
            reqMsg.setReplyTag(replyTag);
            reqMsg.append(smt_actMsg);
            reqMsg.append(container.queryOwner().queryGraphId());
            reqMsg.append(container.queryId());

            if (!container.queryJob().queryJobComm().sendRecv(reqMsg, 0, container.queryJob().querySlaveMpTag(), LONGTIMEOUT))
                throwUnexpected();

            masterReplyMsg.swapWith(reqMsg);
        }
    }
    void stop() { dataLinkStop(); }

    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon || rowSource.eos())
            return NULL;

        if (eogPending)
        {
            eogPending = false;
            return NULL;
        }

        RtlDynamicRowBuilder rowBuilder(queryRowAllocator());
        size32_t sz = queryRowDeserializer()->deserialize(rowBuilder,rowSource);
        if (grouped)
            rowSource.read(sizeof(bool), &eogPending);
        dataLinkIncrement();
        return rowBuilder.finalizeRowClear(sz);
    }
    bool isGrouped() { return grouped; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.isSource = true;
        if (firstNode())
            info.unknownRowsOutput = true;
        else
        {
            info.unknownRowsOutput = false;
            info.totalRowsMin = 0;
            info.totalRowsMax = 0;
        }
    }
};

CActivityBase *createWuidReadSlave(CGraphElementBase *container)
{
    return new CWuidReadSlaveActivity(container);
}


