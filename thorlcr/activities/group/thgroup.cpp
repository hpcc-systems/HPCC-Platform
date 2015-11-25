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


#include "thactivitymaster.ipp"


class CGroupBaseActivityMaster : public CMasterActivity
{
    Owned<CThorStats> statNumGroups;
    Owned<CThorStats> statNumGroupMax;
public:
    CGroupBaseActivityMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
    }
    virtual void init()
    {
        CMasterActivity::init();
        statNumGroups.setown(new CThorStats(StNumGroups));
        statNumGroupMax.setown(new CThorStats(StNumGroupMax));
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);

        rowcount_t numGroups;
        mb.read(numGroups);
        statNumGroups->set(node, numGroups);

        rowcount_t numGroupMax;
        mb.read(numGroupMax);
        statNumGroupMax->set(node, numGroupMax);
    }
    virtual void getActivityStats(IStatisticGatherer & stats)
    {
        CMasterActivity::getActivityStats(stats);
        statNumGroups->getStats(stats, false);
        statNumGroupMax->getStats(stats, false);
    }
};

class CGroupActivityMaster : public CGroupBaseActivityMaster
{
public:
    CGroupActivityMaster(CMasterGraphElement *info) : CGroupBaseActivityMaster(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append((int)mpTag);
    }
};

CActivityBase *createGroupActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CGroupBaseActivityMaster(container);
    else
        return new CGroupActivityMaster(container);
}

