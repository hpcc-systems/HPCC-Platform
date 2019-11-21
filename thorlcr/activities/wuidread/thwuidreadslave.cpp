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


#include "platform.h"
#include <limits.h>
#include "eclhelper.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"
#include "thormisc.hpp"

#include "thwuidreadslave.ipp"

class CWuidReadSlaveActivity : public CSlaveActivity
{
    typedef CSlaveActivity PARENT;

    Owned<ISerialStream> replyStream;
    CThorStreamDeserializerSource rowSource;
    IHThorWorkunitReadArg *helper;
    bool grouped;
    bool eogPending;
    mptag_t replyTag;
    CMessageBuffer masterReplyMsg;
    bool resultFetched;

    void getWuidResult()
    {
        if (!resultFetched)
        {
            resultFetched = true;
            CMessageBuffer reqMsg;
            reqMsg.setReplyTag(replyTag);
            reqMsg.append(smt_actMsg);
            reqMsg.append(container.queryOwner().queryGraphId());
            reqMsg.append(container.queryId());

            if (!queryJobChannel().queryJobComm().sendRecv(reqMsg, 0, container.queryJob().querySlaveMpTag(), LONGTIMEOUT))
                throwUnexpected();

            masterReplyMsg.swapWith(reqMsg);
        }
        masterReplyMsg.reset();
    }
public:
    CWuidReadSlaveActivity(CGraphElementBase *_container) 
        : CSlaveActivity(_container)
    {
        helper = (IHThorWorkunitReadArg *)queryHelper();
        replyTag = queryMPServer().createReplyTag();
        replyStream.setown(createMemoryBufferSerialStream(masterReplyMsg));
        rowSource.setStream(replyStream);
        grouped = helper->queryOutputMeta()->isGrouped();
        setRequireInitData(false);
        appendOutputLinked(this);
        resultFetched = false;
    }
    virtual void start() override
    {
        ActivityTimer s(slaveTimerStats, timeActivities);
        PARENT::start();
        eogPending = false;
        if (container.queryLocal() || firstNode())
            getWuidResult();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(slaveTimerStats, timeActivities);
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
    virtual bool isGrouped() const override { return grouped; }
    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) const override
    {
        initMetaInfo(info);
        info.isSource = true;
        if (container.queryLocal() || firstNode())
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


