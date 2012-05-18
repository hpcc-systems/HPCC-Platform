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

#include "jiface.hpp"

#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"
#include "thdemonserver.hpp"
#include "thcompressutil.hpp"
#include "thmem.hpp"

#include "thloop.ipp"

class CLoopActivityMasterBase : public CMasterActivity
{
protected:
    rtlRowBuilder extractBuilder;
    CGraphBase *loopGraph;
    unsigned emptyIterations;
    unsigned maxEmptyLoopIterations;
    Owned<CThorStats> loopCounterProgress;

    bool sync(unsigned loopCounter)
    {
        unsigned loopEnds = 0;
        unsigned nodes = container.queryJob().querySlaves();
        unsigned n = nodes;
        bool allEmptyIterations = true;
        CMessageBuffer msg;
        while (n--) // a barrier really
        {
            rank_t sender;
            if (!receiveMsg(msg, RANK_ALL, mpTag, &sender, LONGTIMEOUT))
                return false;
            unsigned slaveLoopCounterReq, slaveEmptyIterations;
            msg.read(slaveLoopCounterReq);
            msg.read(slaveEmptyIterations);
            if (0 == slaveLoopCounterReq) // signals end
            {
                ++loopEnds;
                if (loopEnds == nodes)
                    break;
            }
            else
                assertex(slaveLoopCounterReq == loopCounter); // sanity check
            if (0 == slaveEmptyIterations) // either 1st or has been reset, i.e. non-empty
                allEmptyIterations = false;
        }
        assertex(loopEnds==0 || loopEnds==nodes); // Not sure possible in global graph, for some to finish and not others 
        bool final = loopEnds == nodes; // final
        msg.clear();
        if (allEmptyIterations)
            emptyIterations++;
        else
            emptyIterations = 0;
        bool ok = emptyIterations <= maxEmptyLoopIterations;
        msg.append(ok);
        n = nodes;
        while (n--) // a barrier really
            container.queryJob().queryJobComm().send(msg, n+1, mpTag, LONGTIMEOUT);

        if (!ok)
            throw MakeActivityException(this, 0, "Executed LOOP with empty input and output %u times", emptyIterations);

        return final;
    }
public:
    CLoopActivityMasterBase(CMasterGraphElement *info) : CMasterActivity(info)
    {
        loopCounterProgress.setown(new CThorStats("loopCounter-"));
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
        loopGraph = NULL;
    }
    bool fireException(IException *e)
    {
        EXCLOG(e, "Loop master passed exception, aborting loop graph(s)");
        try
        {
            loopGraph->abort(e);
        }
        catch (IException *e)
        {
            EXCLOG(e, "Exception whilst aborting loop graphs");
            e->Release();
        }
        return CMasterActivity::fireException(e);
    }
    bool doinit()
    {
        loopGraph = queryContainer().queryLoopGraph()->queryGraph();
        if (container.queryLocalOrGrouped())
            return false;
        bool global = !loopGraph->isLocalOnly();
        maxEmptyLoopIterations = (unsigned)container.queryJob().getWorkUnitValueInt("@maxEmptyLoopIterations", 1000);
        return !global;
    }
    void process()
    {
        CMasterActivity::process();
        emptyIterations = 0;
    }
    void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (!container.queryLocalOrGrouped())
            serializeMPtag(dst, mpTag);
    }
    void slaveDone(size32_t slaveIdx, MemoryBuffer &mb)
    {
        CMasterGraph *graph = (CMasterGraph *)loopGraph;
        graph->handleSlaveDone(slaveIdx, mb);
    }
    virtual void abort()
    {
        CMasterActivity::abort();
        cancelReceiveMsg(RANK_ALL, mpTag);
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        unsigned loopCounter;
        mb.read(loopCounter);
        loopCounterProgress->set(node, loopCounter);
    }
    virtual void getXGMML(IWUGraphProgress *progress, IPropertyTree *node)
    {
        CMasterActivity::getXGMML(progress, node);
        loopCounterProgress->getXGMML(node, false);
    }
};


class CLoopActivityMaster : public CLoopActivityMasterBase
{
    void checkEmpty()
    {
        // similar to sync, but continiously listens for messages from slaves
        // slave only sends if above threashold, or if was at threshold and non empty
        // this routine is here to spot when all are whirling around processing nothing for > threshold
        Owned<IBitSet> emptyIterations = createBitSet();
        unsigned loopEnds = 0;
        unsigned nodes = container.queryJob().querySlaves();
        unsigned n = nodes;
        bool allEmptyIterations = true;
        CMessageBuffer msg;
        loop
        {
            rank_t sender;
            if (!receiveMsg(msg, RANK_ALL, mpTag, &sender, LONGTIMEOUT))
                return;
            unsigned slaveLoopCounterReq, slaveEmptyIterations;
            msg.read(slaveLoopCounterReq);
            msg.read(slaveEmptyIterations);
            if (0 == slaveLoopCounterReq) // signals end
            {
                ++loopEnds;
                if (loopEnds == nodes)
                    break; // all done
            }
            bool overLimit = slaveEmptyIterations > maxEmptyLoopIterations;
            emptyIterations->set(sender-1, overLimit);
            if (emptyIterations->scan(0, 0) >= nodes) // all empty
                throw MakeActivityException(this, 0, "Executed LOOP with empty input and output > %d maxEmptyLoopIterations times on all nodes", maxEmptyLoopIterations);
        }
    }
public:
    CLoopActivityMaster(CMasterGraphElement *info) : CLoopActivityMasterBase(info)
    {
        if (!container.queryLocalOrGrouped())
            mpTag = container.queryJob().allocateMPTag();
    }
    void init()
    {
        if (TAKloopdataset == container.getKind())
            throwUnexpected();
        if (!CLoopActivityMasterBase::doinit())
            return;
        IHThorLoopArg *helper = (IHThorLoopArg *) queryHelper();
        Owned<IThorGraphResults> results = queryGraph().createThorGraphResults(3);
        queryContainer().queryLoopGraph()->prepareLoopResults(*this, results);
        if (helper->getFlags() & IHThorLoopArg::LFcounter)
            queryContainer().queryLoopGraph()->prepareCounterResult(*this, results, 1, 2);
        loopGraph->setResults(results);
    }
    void process()
    {
        CLoopActivityMasterBase::process();
        if (container.queryLocalOrGrouped())
            return;

        IHThorLoopArg *helper = (IHThorLoopArg *) queryHelper();
        bool global = !loopGraph->isLocalOnly();
        if (global)
        {
            helper->createParentExtract(extractBuilder);
            unsigned loopCounter = 1;
            loop
            {
                if (sync(loopCounter))
                    break;
                Owned<IRowStream> curInput = queryContainer().queryLoopGraph()->execute(*this, (helper->getFlags() & IHThorLoopArg::LFcounter)?loopCounter:0, (IRowWriterMultiReader *)NULL, 0, extractBuilder.size(), extractBuilder.getbytes());
                ++loopCounter;
            }
        }
        else
            checkEmpty();
    }
};

CActivityBase *createLoopActivityMaster(CMasterGraphElement *container)
{
    return new CLoopActivityMaster(container);
}

///////////

class CGraphLoopActivityMaster : public CLoopActivityMasterBase
{
public:
    CGraphLoopActivityMaster(CMasterGraphElement *info) : CLoopActivityMasterBase(info)
    {
    }
    void init()
    {
        if (!CLoopActivityMasterBase::doinit())
            return;
        IHThorGraphLoopArg *helper = (IHThorGraphLoopArg *) queryHelper();
        Owned<IThorGraphResults> results = queryGraph().createThorGraphResults(1);
        if (helper->getFlags() & IHThorGraphLoopArg::GLFcounter)
            queryContainer().queryLoopGraph()->prepareCounterResult(*this, results, 1, 0);
        loopGraph->setResults(results);
    }
    void process()
    {
        CLoopActivityMasterBase::process();
        if (container.queryLocalOrGrouped())
            return;

        IHThorGraphLoopArg *helper = (IHThorGraphLoopArg *) queryHelper();
        bool global = !loopGraph->isLocalOnly();
        if (!global)
            return;
        unsigned maxIterations = helper->numIterations();
        if ((int)maxIterations < 0) maxIterations = 0;
        Owned<IThorGraphResults> loopResults = queryGraph().createThorGraphResults(maxIterations);
        IThorResult *result = loopResults->createResult(*this, 0, this, true);

        helper->createParentExtract(extractBuilder);

        unsigned loopCounter = 1;
        loop
        {
            if (sync(loopCounter))
                break;
            queryContainer().queryLoopGraph()->execute(*this, (helper->getFlags() & IHThorGraphLoopArg::GLFcounter)?loopCounter:0, loopResults.get(), extractBuilder.size(), extractBuilder.getbytes());
            ++loopCounter;
        }
    }
};

CActivityBase *createGraphLoopActivityMaster(CMasterGraphElement *container)
{
    return new CGraphLoopActivityMaster(container);
}

///////////

class CLocalResultActivityMasterBase : public CMasterActivity
{
protected:
    Owned<IRowInterfaces> inputRowIf;

public:
    CLocalResultActivityMasterBase(CMasterGraphElement *info) : CMasterActivity(info)
    {
    }
    virtual void init()
    {
        reset();
    }
    virtual void createResult() = 0;
    virtual void process()
    {
        assertex(container.queryResultsGraph());
        Owned<CGraphBase> graph = queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        IHThorLocalResultWriteArg *helper = (IHThorLocalResultWriteArg *)queryHelper();
        inputRowIf.setown(createRowInterfaces(container.queryInput(0)->queryHelper()->queryOutputMeta(),queryActivityId(),queryCodeContext()));
        createResult();
    }
};

class CLocalResultActivityMaster : public CLocalResultActivityMasterBase
{
public:
    CLocalResultActivityMaster(CMasterGraphElement *info) : CLocalResultActivityMasterBase(info)
    {
    }
    virtual void createResult()
    {
        IHThorLocalResultWriteArg *helper = (IHThorLocalResultWriteArg *)queryHelper();
        Owned<CGraphBase> graph = queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        graph->createResult(*this, helper->querySequence(), this, true); // NB graph owns result
    }
};

CActivityBase *createLocalResultActivityMaster(CMasterGraphElement *container)
{
    return new CLocalResultActivityMaster(container);
}

class CGraphLoopResultWriteActivityMaster : public CLocalResultActivityMasterBase
{
public:
    CGraphLoopResultWriteActivityMaster(CMasterGraphElement *info) : CLocalResultActivityMasterBase(info)
    {
    }
    virtual void createResult()
    {
        IHThorGraphLoopResultWriteArg *helper = (IHThorGraphLoopResultWriteArg *)queryHelper();
        Owned<CGraphBase> graph = queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        graph->createGraphLoopResult(*this, inputRowIf, true); // NB graph owns result
    }
};

CActivityBase *createGraphLoopResultActivityMaster(CMasterGraphElement *container)
{
    return new CGraphLoopResultWriteActivityMaster(container);
}
