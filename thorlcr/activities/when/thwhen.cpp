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

#include "thwhen.ipp"

class CWhenActivityMaster : public CMasterActivity
{
    Owned<IBarrier> barrier;
    size32_t savedParentExtractSz;
    const byte *savedParentExtract;
    bool global;

public:
    CWhenActivityMaster(CMasterGraphElement * info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
        barrier.setown(container.queryJobChannel().createBarrier(mpTag));
        global = !container.queryOwner().queryOwner() || container.queryOwner().isGlobal();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (global)
            dst.append((int)mpTag);
    }
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        savedParentExtractSz = parentExtractSz;
        savedParentExtract = parentExtract;
    }
    virtual void process()
    {
        CMasterActivity::process();
        if (!global)
            return; // executed in child query
        ActPrintLog("When: executing dependencies");
        container.executeDependencies(savedParentExtractSz, savedParentExtract, abortSoon ? WhenFailureId : WhenSuccessId, true);
        if (!barrier->wait(false))
            return;
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        barrier->cancel();
    }
};

CActivityBase *createWhenActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CWhenActivityMaster(container);
}


class CIfActionMaster : public CMasterActivity
{
    Owned<IBarrier> barrier;
    size32_t savedParentExtractSz;
    const byte *savedParentExtract;
    bool global;

public:
    CIfActionMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
        barrier.setown(container.queryJobChannel().createBarrier(mpTag));
        global = !container.queryOwner().queryOwner() || container.queryOwner().isGlobal();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (global)
            dst.append((int)mpTag);
    }
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract)
    {
        CMasterActivity::preStart(parentExtractSz, parentExtract);
        savedParentExtractSz = parentExtractSz;
        savedParentExtract = parentExtract;
    }
    void process()
    {
        if (!global)
            return;
        if (!barrier->wait(false))
            return;
        IHThorIfArg *helper = (IHThorIfArg *)queryHelper();
        int controlId = helper->getCondition() ? 1 : 2;
        container.executeDependencies(savedParentExtractSz, savedParentExtract, controlId, true);
    }
};

