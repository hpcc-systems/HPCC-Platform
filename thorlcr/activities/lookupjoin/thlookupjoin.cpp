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

#include "thlookupjoin.ipp"

#include "thexception.hpp"
#include "thbufdef.hpp"

class CLookupJoinActivityMaster : public CMasterActivity
{
public:
    CLookupJoinActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
    void process()
    {
        if (!container.queryLocal() && container.queryJob().querySlaves() > 1)
        {
            CMessageBuffer msg;
            unsigned nslaves = container.queryJob().querySlaves();
            unsigned s = 1;
            rowcount_t totalCount = 0, slaveCount;
            for (; s<=nslaves; s++)
            {
                if (!receiveMsg(msg, s, mpTag))
                    return;
                msg.read(slaveCount);
                if (RCUNSET == slaveCount)
                {
                    totalCount = RCUNSET;
                    break; // unknown
                }
                totalCount += slaveCount;
            }
            s=1;
            msg.clear().append(totalCount);
            for (; s<=nslaves; s++)
                container.queryJob().queryJobComm().send(msg, s, mpTag);
        }
    }
};

CActivityBase *createLookupJoinActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocal())
        return new CMasterActivity(container);
    else
        return new CLookupJoinActivityMaster(container);
}
