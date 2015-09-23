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
#include "jlib.hpp"

#include "thormisc.hpp"
#include "thwuidwriteslave.ipp"

#include "thactivityutil.ipp"
#include "thexception.hpp"
#include "thbufdef.hpp"

#include "eclhelper.hpp"        // for IHThorWorkUnitWriteArg
#include "slave.ipp"

#define PIPE_BUFFER_SIZE         0x20000

class CWorkUnitWriteSlaveBase : public ProcessSlaveActivity
{
protected:
    Owned<IThorDataLink> input;
    bool grouped;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CWorkUnitWriteSlaveBase(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
        grouped = false;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        mpTag = container.queryJobChannel().deserializeMPTag(data);
    }
    void processBlock(unsigned &numGot, MemoryBuffer &mb)
    {
        Linked<IOutputRowSerializer> serializer = ::queryRowSerializer(input);
        CMemoryRowSerializer mbs(mb);
        bool first = true;
        do
        {
            if (abortSoon) break;
            OwnedConstThorRow row = input->nextRow();
            if (grouped && !first)
                mb.append(NULL == row.get());
            if (!row)
            {
                row.setown(input->nextRow());
                if (!row)
                    break;
            }
            first = false;
            ++numGot;
            processed++;
            serializer->serialize(mbs,(const byte *)row.get());
        } while (mb.length() < PIPE_BUFFER_SIZE); // NB: allows at least 1
    }
    void endProcess()
    {
        if (processed & THORDATALINK_STARTED)
        {
            stopInput(input);
            processed |= THORDATALINK_STOPPED;
        }
        input.clear();
    }
};

class CWorkUnitWriteGlobalSlaveBaseActivity : public CWorkUnitWriteSlaveBase
{
public:
    CWorkUnitWriteGlobalSlaveBaseActivity(CGraphElementBase *container) : CWorkUnitWriteSlaveBase(container)
    {
    }
    void process()
    {
        input.setown(createDataLinkSmartBuffer(this, inputs.item(0), WORKUNITWRITE_SMART_BUFFER_SIZE, isSmartBufferSpillNeeded(this), grouped, RCUNBOUND, NULL, false, &container.queryJob().queryIDiskUsage()));
        startInput(input);

        processed = THORDATALINK_STARTED;

        ActPrintLog("WORKUNITWRITE: processing first block");

        CMessageBuffer replyMb;
        replyMb.append((unsigned)0);
        unsigned numGot;
        do
        {
            numGot = 0;
            try
            {
                processBlock(numGot, replyMb);
                replyMb.writeDirect(0, sizeof(numGot), &numGot);
            }
            catch(CATCHALL)
            {
                ActPrintLog("WORKUNITWRITE: exception");
                throw;
            }
            CMessageBuffer msgMb;
            if (!receiveMsg(msgMb, 0, mpTag))
                break;
            if (numGot)
            {
                msgMb.swapWith(replyMb);
                replyMb.append((unsigned)0);
            }
            queryJobChannel().queryJobComm().send(msgMb, 0, mpTag);
        } while (!abortSoon && numGot);
    }
    void abort()
    {
        CWorkUnitWriteSlaveBase::abort();
        cancelReceiveMsg(0, mpTag);
    }
};

class CWorkUnitWriteLocalSlaveActivity : public CWorkUnitWriteSlaveBase
{
    IHThorWorkUnitWriteArg *helper;
    mptag_t replyTag;
public:
    CWorkUnitWriteLocalSlaveActivity(CGraphElementBase *container) : CWorkUnitWriteSlaveBase(container)
    {
        helper = static_cast <IHThorWorkUnitWriteArg *> (queryHelper());    // really a dynamic_cast
        grouped = 0 != (POFgrouped & helper->getFlags());
        replyTag = queryMPServer().createReplyTag();
    }
    void process()
    {
        input.set(inputs.item(0));
        startInput(input);
        processed = THORDATALINK_STARTED;

        ActPrintLog("WORKUNITWRITELOCAL: processing first block");

        CMessageBuffer reqMsg;
        reqMsg.setReplyTag(replyTag);
        reqMsg.append(smt_actMsg);
        reqMsg.append(container.queryOwner().queryGraphId());
        reqMsg.append(container.queryId());

        unsigned totalNum = 0;
        CMessageBuffer msg;
        msg.append((unsigned)0);
        unsigned numGot;
        do
        {
            numGot = 0;
            try
            {
                msg.rewrite(sizeof(unsigned));
                processBlock(numGot, msg);
                totalNum += numGot;
                msg.writeDirect(0, sizeof(numGot), &numGot);
            }
            catch(CATCHALL)
            {
                ActPrintLog("WORKUNITWRITE: exception");
                throw;
            }
            if (numGot || totalNum)
            {
                if (!queryJobChannel().queryJobComm().send(reqMsg, 0, container.queryJob().querySlaveMpTag(), MEDIUMTIMEOUT))
                    throwUnexpected();
                bool got = false;
                loop
                {
                    CMessageBuffer replyMsg;
                    if (receiveMsg(replyMsg, 0, replyTag, NULL, MEDIUMTIMEOUT))
                    {
                        msg.setReplyTag(replyTag);
                        if (!queryJobChannel().queryJobComm().send(msg, 0, mpTag, LONGTIMEOUT))
                            throwUnexpected();
                        if (!receiveMsg(replyMsg, 0, replyTag, NULL, LONGTIMEOUT))
                            throwUnexpected();
                        got = true;
                    }
                    if (got || abortSoon)
                        break;
                    ActPrintLog("Blocked (child workunitwrite) sending request to master");
                }
            }
        } while (!abortSoon && numGot);
    }
    void abort()
    {
        CWorkUnitWriteSlaveBase::abort();
        cancelReceiveMsg(0, replyTag);
    }
};


class CWorkUnitWriteGlobalSlaveActivity : public CWorkUnitWriteGlobalSlaveBaseActivity
{
    IHThorWorkUnitWriteArg *helper;
public:
    CWorkUnitWriteGlobalSlaveActivity(CGraphElementBase *container) : CWorkUnitWriteGlobalSlaveBaseActivity(container)
    {
        helper = static_cast <IHThorWorkUnitWriteArg *> (queryHelper());    // really a dynamic_cast
        grouped = 0 != (POFgrouped & helper->getFlags());
    }
};

CActivityBase *createWorkUnitWriteSlave(CGraphElementBase *container)
{
    if (container->queryLocalOrGrouped())
        return new CWorkUnitWriteLocalSlaveActivity(container);
    else
        return new CWorkUnitWriteGlobalSlaveActivity(container);
}


class CDictionaryWorkunitWriteActivity : public CWorkUnitWriteGlobalSlaveActivity
{
public:
    CDictionaryWorkunitWriteActivity(CGraphElementBase *container) : CWorkUnitWriteGlobalSlaveActivity(container)
    {
    }
};

CActivityBase *createDictionaryWorkunitWriteSlave(CGraphElementBase *container)
{
    return new CDictionaryWorkunitWriteActivity(container);
}
