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

#include "thwhen.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/when/thcatch.cpp $ $Id: thcatch.cpp 62376 2011-02-04 21:59:58Z sort $");

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
        barrier.setown(container.queryJob().createBarrier(mpTag));
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
        barrier.setown(container.queryJob().createBarrier(mpTag));
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

CActivityBase *createIfActionActivityMaster(CMasterGraphElement *container)
{
    if (container->queryLocalOrGrouped())
        return new CMasterActivity(container);
    else
        return new CIfActionMaster(container);
}

