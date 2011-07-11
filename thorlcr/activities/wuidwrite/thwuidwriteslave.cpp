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
#include "jlib.hpp"

#include "thormisc.hpp"
#include "thwuidwriteslave.ipp"

#include "thactivityutil.ipp"
#include "thexception.hpp"
#include "thbufdef.hpp"

#include "eclhelper.hpp"        // for IHThorWorkUnitWriteArg
#include "slave.ipp"


static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/wuidwrite/thwuidwriteslave.cpp $ $Id: thwuidwriteslave.cpp 63725 2011-04-01 17:40:45Z jsmith $");

#define PIPE_BUFFER_SIZE         0x20000

class CWorkUnitWriteSlaveBase : public ProcessSlaveActivity
{
protected:
    IHThorWorkUnitWriteArg *helper;
    Owned<IThorDataLink> input;
    bool grouped;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CWorkUnitWriteSlaveBase(CGraphElementBase *container) : ProcessSlaveActivity(container)
    {
        helper = static_cast <IHThorWorkUnitWriteArg *> (queryHelper());    // really a dynamic_cast
        grouped = 0 != (POFgrouped & helper->getFlags());
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        mpTag = container.queryJob().deserializeMPTag(data);
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

class CWorkUnitWriteGlobalSlaveActivity : public CWorkUnitWriteSlaveBase
{
public:
    CWorkUnitWriteGlobalSlaveActivity(CGraphElementBase *container) : CWorkUnitWriteSlaveBase(container)
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
            container.queryJob().queryJobComm().send(msgMb, 0, mpTag);
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
    mptag_t replyTag;
public:
    CWorkUnitWriteLocalSlaveActivity(CGraphElementBase *container) : CWorkUnitWriteSlaveBase(container)
    {
        replyTag = createReplyTag();
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
                if (!container.queryJob().queryJobComm().send(reqMsg, 0, container.queryJob().querySlaveMpTag(), MEDIUMTIMEOUT))
                    throwUnexpected();
                bool got = false;
                loop
                {
                    CMessageBuffer replyMsg;
                    if (receiveMsg(replyMsg, 0, replyTag, NULL, MEDIUMTIMEOUT))
                    {
                        msg.setReplyTag(replyTag);
                        if (!container.queryJob().queryJobComm().send(msg, 0, mpTag, LONGTIMEOUT))
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


CActivityBase *createWorkUnitWriteSlave(CGraphElementBase *container)
{
    if (container->queryLocalOrGrouped())
        return new CWorkUnitWriteLocalSlaveActivity(container);
    else
        return new CWorkUnitWriteGlobalSlaveActivity(container);
}
