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
#include "eclhelper.hpp"        // for IHThorFirstNArg

#include "thfirstn.ipp"
#include "thbufdef.hpp"

class CFirstNActivityMaster : public CMasterActivity
{
    static CriticalSection singlefirstnterm;
public:
    CFirstNActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
    void process()
    {
        CMasterActivity::process();

        IHThorFirstNArg *helper = (IHThorFirstNArg *)queryHelper();

        rowcount_t limit = (rowcount_t)helper->getLimit();
        rowcount_t skip = validRC(helper->numToSkip());
        unsigned nslaves = container.queryJob().querySlaves();
        unsigned s = 1;
        loop
        {
            CMessageBuffer msgMb;
            msgMb.append(limit);
            msgMb.append(skip);
            queryJobChannel().queryJobComm().send(msgMb, s, mpTag);
            msgMb.clear();
            if (!receiveMsg(msgMb, s, mpTag))
                return;
            rowcount_t read;
            msgMb.read(read);
            msgMb.read(skip);

            assertex(read <= limit);
            limit -= read;
            if (s == nslaves)
                break;
            ++s;
            if (0 == limit)
            {
                for (; s<=nslaves; s++)
                {
                    if (abortSoon) return;
                    CMessageBuffer msgMb;
                    msgMb.append((rowcount_t)0);
                    msgMb.append((rowcount_t)0);
                    queryJobChannel().queryJobComm().send(msgMb, s, mpTag);
                    msgMb.clear();
                    receiveMsg(msgMb, s, mpTag);
                }
                break;
            }
        }
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
};

CriticalSection CFirstNActivityMaster::singlefirstnterm;

CActivityBase *createFirstNActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CFirstNActivityMaster(container);
}
