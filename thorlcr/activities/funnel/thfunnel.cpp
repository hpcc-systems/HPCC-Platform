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
        replyTag = createReplyTag();
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
                if (!container.queryJob().queryJobComm().send(msg, ((rank_t) s+1), (mptag_t) replyTags.item(s), LONGTIMEOUT))
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

