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

#include "thcatch.ipp"

class CSkipCatchActivity : public CMasterActivity
{
    Owned<IBarrier> barrier;
public:
    CSkipCatchActivity(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
        barrier.setown(container.queryJobChannel().createBarrier(mpTag));
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
    virtual void process()
    {
        CMasterActivity::process();
        barrier->wait(false);
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        barrier->cancel();
    }
};

CActivityBase *createSkipCatchActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CSkipCatchActivity(container);
}

