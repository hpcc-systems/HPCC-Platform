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

#include "dasess.hpp"
#include "dadfs.hpp"
#include "thbufdef.hpp"
#include "thexception.hpp"
#include "thfunnel.ipp"

//
// CNonEmptyActivityMaster
//
class CNonEmptyActivityMaster : public CMasterActivity
{
    mptag_t replyTag;
public:
    CNonEmptyActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        replyTag = queryMPServer().createReplyTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(replyTag);
    }
    void process()
    {
        CMessageBuffer msg;
        unsigned inputs = container.getInputs();
        unsigned slaves = container.queryJob().querySlaves();
        unsigned s;
        bool readSome=false, slaveReadSome;

        IntArray replyTags;
        for (s=0; s<slaves; s++)
            replyTags.append(0);
        while (inputs>1)
        {
            inputs--;
            for (s=0; s<slaves; s++)
            {
                rank_t sender;
                if (!receiveMsg(msg, RANK_ALL, replyTag, &sender)) return;

                replyTags.replace(msg.getReplyTag(), ((int)sender)-1);
                msg.read(slaveReadSome);
                if (slaveReadSome) readSome = true;
            }
            msg.clear().append(readSome);
            for (s=0; s<slaves; s++)
            {
                if (!queryJobChannel().queryJobComm().send(msg, ((rank_t) s+1), (mptag_t) replyTags.item(s), LONGTIMEOUT))
                    throw MakeActivityException(this, 0, "Failed to give result to slave");
            }
            if (readSome) // got some, have told slaves to ignore rest, so finish
                break;
        }
    }
    void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, replyTag);
    }
};

CActivityBase *createNonEmptyActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CNonEmptyActivityMaster(container);
}

