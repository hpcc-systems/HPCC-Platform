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

#include "jlib.hpp"
#include "thexception.hpp"
#include "thorfile.hpp"
#include "thactivitymaster.ipp"
#include "../diskread/thdiskread.ipp"
#include "thwuidread.ipp"


static bool getWorkunitResultFilename(CGraphElementBase &container, StringBuffer & diskFilename, const char * wuid, const char * stepname, int sequence)
{
    try
    {
        Owned<IConstWUResult> result;
        if (wuid)
            result.setown(container.queryCodeContext()->getExternalResult(wuid, stepname, sequence));
        else
            result.setown(container.queryCodeContext()->getResultForGet(stepname, sequence));
        if (!result)
            throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)");

        SCMStringBuffer tempFilename;
        result->getResultFilename(tempFilename);
        if (tempFilename.length() == 0)
            return false;

        diskFilename.append("~").append(tempFilename.str());
        return true;
    }
    catch (IException * e) 
    {
        StringBuffer text; 
        e->errorMessage(text); 
        e->Release();
        throw MakeThorException(TE_FailedToRetrieveWorkunitValue, "Failed to find value %s:%d in workunit %s [%s]", stepname?stepname:"(null)", sequence, wuid?wuid:"(null)", text.str());
    }
    return false;
}

class CWorkUnitReadMaster : public CMasterActivity
{
    Owned<CMasterActivity> surrogateAct;
public:
    CWorkUnitReadMaster(CMasterGraphElement * info) : CMasterActivity(info) { }

    virtual void init() override
    {
        CMasterActivity::init();
        surrogateAct.clear();
        StringBuffer diskFilename;
        IHThorWorkunitReadArg *wuReadHelper = (IHThorWorkunitReadArg *)queryHelper();
        wuReadHelper->onCreate(queryCodeContext(), NULL, NULL);
        OwnedRoxieString fromWuid(wuReadHelper->getWUID());
        if (getWorkunitResultFilename(container, diskFilename, fromWuid, wuReadHelper->queryName(), wuReadHelper->querySequence()))
        {
            Owned<IHThorDiskReadArg> diskReadHelper = createWorkUnitReadArg(diskFilename, LINK(wuReadHelper));
            surrogateAct.setown((CMasterActivity *)createDiskReadActivityMaster((CMasterGraphElement *)&container, diskReadHelper));
            surrogateAct->init();
        }
    }
    virtual void handleSlaveMessage(CMessageBuffer &msg) override
    {
        if (surrogateAct)
        {
            surrogateAct->handleSlaveMessage(msg);
            return;
        }

        IHThorWorkunitReadArg *helper = (IHThorWorkunitReadArg *)queryHelper();
        size32_t lenData;
        void *tempData;
        OwnedRoxieString fromWuid(helper->getWUID());
        if (fromWuid)
            queryCodeContext()->getExternalResultRaw(lenData, tempData, fromWuid, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        else
            queryCodeContext()->getResultRaw(lenData, tempData, helper->queryName(), helper->querySequence(), helper->queryXmlTransformer(), helper->queryCsvTransformer());
        msg.clear();
        msg.setBuffer(lenData, tempData, true);
        queryJobChannel().queryJobComm().reply(msg);
    }
// overloaded for surrogate disk read case
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb) override
    {
        if (surrogateAct)
        {
            surrogateAct->deserializeStats(node, mb);
            return;
        }
        CMasterActivity::deserializeStats(node, mb);
    }
    virtual void getActivityStats(IStatisticGatherer & stats) override
    {
        if (surrogateAct)
        {
            surrogateAct->getActivityStats(stats);
            return;
        }
        CMasterActivity::getActivityStats(stats);
    }
    virtual void getEdgeStats(IStatisticGatherer & stats, unsigned idx) override
    {
        if (surrogateAct)
        {
            surrogateAct->getEdgeStats(stats, idx);
            return;
        }
        CMasterActivity::getEdgeStats(stats, idx);
    }
    virtual void reset() override
    {
        if (surrogateAct)
        {
            surrogateAct->reset();
            return;
        }
        CMasterActivity::reset();
    }
    virtual MemoryBuffer &queryInitializationData(unsigned slave) const override
    {
        if (surrogateAct)
            return surrogateAct->queryInitializationData(slave);
        return CMasterActivity::queryInitializationData(slave);
    }
    virtual MemoryBuffer &getInitializationData(unsigned slave, MemoryBuffer &dst) const override
    {
        if (surrogateAct)
            return surrogateAct->getInitializationData(slave, dst);
        return CMasterActivity::getInitializationData(slave, dst);
    }
    virtual void configure() override
    {
        if (surrogateAct)
        {
            surrogateAct->configure();
            return;
        }
        CMasterActivity::configure();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) override
    {
        if (surrogateAct)
        {
            surrogateAct->serializeSlaveData(dst, slave);
            return;
        }
        CMasterActivity::serializeSlaveData(dst, slave);
    }
    virtual void slaveDone(size32_t slaveIdx, MemoryBuffer &mb) override
    {
        if (surrogateAct)
        {
            surrogateAct->slaveDone(slaveIdx, mb);
            return;
        }
        CMasterActivity::slaveDone(slaveIdx, mb);
    }
    virtual void preStart(size32_t parentExtractSz, const byte *parentExtract) override
    {
        if (surrogateAct)
        {
            surrogateAct->preStart(parentExtractSz, parentExtract);
            return;
        }
        CMasterActivity::preStart(parentExtractSz, parentExtract);
    }
    virtual void startProcess(bool async=true) override
    {
        if (surrogateAct)
        {
            surrogateAct->startProcess(async);
            return;
        }
        CMasterActivity::startProcess(async);
    }
    virtual bool wait(unsigned timeout) override
    {
        if (surrogateAct)
            return surrogateAct->wait(timeout);
        return CMasterActivity::wait(timeout);
    }
    virtual void done() override
    {
        if (surrogateAct)
        {
            surrogateAct->done();
            return;
        }
        CMasterActivity::done();
    }
    virtual void kill() override
    {
        if (surrogateAct)
        {
            surrogateAct->kill();
            return;
        }
        CMasterActivity::kill();
    }
    virtual bool fireException(IException *e) override
    {
        if (surrogateAct)
            return surrogateAct->fireException(e);
        return CMasterActivity::fireException(e);
    }
};

CActivityBase *createWorkUnitActivityMaster(CMasterGraphElement *container)
{
    return new CWorkUnitReadMaster(container);
}
