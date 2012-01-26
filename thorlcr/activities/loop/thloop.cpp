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
                throw MakeActivityException(this, 0, "Executed LOOP with empty input and output > %maxEmptyLoopIterations times on all nodes", maxEmptyLoopIterations);
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
        Owned<IThorGraphResults> results = createThorGraphResults(3);
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
        Owned<IThorGraphResults> results = createThorGraphResults(1);
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
        Owned<IThorGraphResults> loopResults = createThorGraphResults(maxIterations);
        IThorResult *result = loopResults->createResult(*this, 0, this, true);
        Owned<IRowWriter> resultWriter = result->getWriter();

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
    PointerArrayOf<CThorExpandingRowArray> results;

protected:
    Owned<IRowInterfaces> inputRowIf;

public:
    CLocalResultActivityMasterBase(CMasterGraphElement *info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
        for (unsigned n=0; n<container.queryJob().querySlaves(); n++)
            results.append(new CThorExpandingRowArray(*this));
    }
    ~CLocalResultActivityMasterBase()
    {
        ForEachItemIn(r, results)
        {
            CThorExpandingRowArray *result = results.item(r);
            delete result;
        }
        results.kill();
    }
    virtual void init()
    {
        reset();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        if (!container.queryLocalOrGrouped())
            serializeMPtag(dst, mpTag);
    }
    virtual IThorResult *createResult() = 0;
    virtual void process()
    {
        assertex(container.queryResultsGraph());
        Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        IHThorLocalResultWriteArg *helper = (IHThorLocalResultWriteArg *)queryHelper();
        inputRowIf.setown(createRowInterfaces(container.queryInput(0)->queryHelper()->queryOutputMeta(),queryActivityId(),queryCodeContext()));

        IThorResult *result = createResult();
        if (!result->isLocal())
        {
            Owned<IRowWriter> resultWriter = result->getWriter();
            unsigned todo = container.queryJob().querySlaves();
            for (unsigned n=0; n<todo; n++)
                results.item(n)->kill();
            rank_t sender;
            MemoryBuffer mb;
            CMessageBuffer msg;
            Owned<ISerialStream> stream = createMemoryBufferSerialStream(mb);
            CThorStreamDeserializerSource rowSource(stream);
            loop
            {
                loop
                {
                    if (abortSoon)
                        return;
                    msg.clear();
                    if (receiveMsg(msg, RANK_ALL, mpTag, &sender, 60*1000))
                        break;
                    ActPrintLog("WARNING: tag %d timedout, retrying", (unsigned)mpTag);
                }
                sender = sender - 1; // 0 = master
                if (!msg.length())
                {
                    --todo;
                    if (0 == todo)
                        break; // done
                }
                else
                {
                    ThorExpand(msg, mb.clear());

                    CThorExpandingRowArray *slaveResults = results.item(sender);
                    while (!rowSource.eos())
                    {
                        RtlDynamicRowBuilder rowBuilder(inputRowIf->queryRowAllocator());
                        size32_t sz = inputRowIf->queryRowDeserializer()->deserialize(rowBuilder, rowSource);
                        slaveResults->append(rowBuilder.finalizeRowClear(sz));
                    }
                }
            }
            mb.clear();
            CMemoryRowSerializer mbs(mb);
            CThorExpandingRowArray *slaveResult = results.item(0);
            unsigned rowNum=0;
            unsigned resultNum=1;
            loop
            {
                while (resultNum)
                {
                    if (rowNum == slaveResult->ordinality())
                    {
                        loop
                        {
                            if (resultNum == results.ordinality())
                            {
                                resultNum = 0; // eos
                                break;
                            }
                            slaveResult = results.item(resultNum++);
                            if (slaveResult->ordinality())
                            {
                                rowNum = 0;
                                break;
                            }
                        }
                        if (!resultNum) // eos
                            break;
                    }
                    const void *row = slaveResult->query(rowNum++);
                    inputRowIf->queryRowSerializer()->serialize(mbs, (const byte *)row);
                    LinkThorRow(row);
                    resultWriter->putRow(row);
                    if (mb.length() > 0x80000)
                        break;
                }
                msg.clear();
                if (mb.length())
                {
                    ThorCompress(mb.toByteArray(), mb.length(), msg);
                    mb.clear();
                }
                BooleanOnOff onOff(receiving);
                ((CJobMaster &)container.queryJob()).broadcastToSlaves(msg, mpTag, LONGTIMEOUT, NULL, NULL, true);
                if (0 == msg.length())
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

class CLocalResultActivityMaster : public CLocalResultActivityMasterBase
{
public:
    CLocalResultActivityMaster(CMasterGraphElement *info) : CLocalResultActivityMasterBase(info)
    {
    }
    virtual IThorResult *createResult()
    {
        IHThorLocalResultWriteArg *helper = (IHThorLocalResultWriteArg *)queryHelper();
        Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        bool local = container.queryOwner().isLocalOnly();
        IThorResult *result = graph->queryResult(helper->querySequence());
        if (result)
            local = result->isLocal();
        return graph->createResult(*this, helper->querySequence(), this, local); // NB graph owns result
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
    virtual IThorResult *createResult()
    {
        IHThorGraphLoopResultWriteArg *helper = (IHThorGraphLoopResultWriteArg *)queryHelper();
        Owned<CGraphBase> graph = container.queryOwner().queryJob().getGraph(container.queryResultsGraph()->queryGraphId());
        return graph->createGraphLoopResult(*this, inputRowIf); // NB graph owns result
    }
};

CActivityBase *createGraphLoopResultActivityMaster(CMasterGraphElement *container)
{
    return new CGraphLoopResultWriteActivityMaster(container);
}
