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
#include "eclhelper.hpp"
#include "thlimit.ipp"

#define NUMINPARALLEL 16

class CLimitActivityMaster : public CMasterActivity
{
public:
    CLimitActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(mpTag);
    }
    void process()
    {
        unsigned slaves = container.queryJob().querySlaves();
        IHThorLimitArg *helper = (IHThorLimitArg *)queryHelper();

        rowcount_t rowLimit = (rowcount_t)helper->getRowLimit();
        rowcount_t total = 0;
        while (slaves--)
        {
            CMessageBuffer mb;
            if (!receiveMsg(mb, RANK_ALL, mpTag, NULL))
                return;
            if (abortSoon)
                return;
            rowcount_t count;
            mb.read(count);
            total += count;
            if (total > rowLimit)
                break;
        }
        switch (container.getKind())
        {
            case TAKcreaterowlimit: 
            case TAKskiplimit: 
            {
                unsigned slaves = container.queryJob().querySlaves();
                CMessageBuffer mb;
                mb.append(total);
                queryJobChannel().queryJobComm().send(mb, RANK_ALL_OTHER, mpTag);
                break;
            }
            case TAKlimit:
            {
                if (total > rowLimit)
                    helper->onLimitExceeded();
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


CActivityBase *createLimitActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocal())
        return new CMasterActivity(container);
    else
        return new CLimitActivityMaster(container);
}
