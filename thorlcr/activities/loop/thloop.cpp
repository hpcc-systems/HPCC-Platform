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

#include "thloop.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/loop/thloop.cpp $ $Id: thloop.cpp 63725 2011-04-01 17:40:45Z jsmith $");



class CLoopActivityMasterBase : public CMasterActivity
{
protected:
    rtlRowBuilder extractBuilder;
    CGraphBase *loopGraph;

    bool sync(unsigned loopCounter)
    {
        unsigned loopEnds = 0;
        unsigned nodes = container.queryJob().querySlaves();
        unsigned n = nodes;
        CMessageBuffer msg;
        while (n--) // a barrier really
        {
            rank_t sender;
            if (!receiveMsg(msg, RANK_ALL, mpTag, &sender, LONGTIMEOUT))
                return false;
            unsigned slaveLoopCounterReq;
            msg.read(slaveLoopCounterReq);
            if (0 == slaveLoopCounterReq) // signals end
            {
                ++loopEnds;
                if (loopEnds == nodes)
                    break;
            }
            else
                assertex(slaveLoopCounterReq == loopCounter); // sanity check
        }
        assertex(loopEnds==0 || loopEnds==nodes); // Not sure possible in global graph, for some to finish and not others 
        bool final = loopEnds == nodes; // final
        msg.clear();
        n = nodes;
        while (n--) // a barrier really
            container.queryJob().queryJobComm().send(msg, n+1, mpTag, LONGTIMEOUT);
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
        return !global;
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
        CMasterActivity::process();
        if (container.queryLocalOrGrouped())
            return;

        IHThorLoopArg *helper = (IHThorLoopArg *) queryHelper();
        bool global = !loopGraph->isLocalOnly();
        if (!global)
            return;

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
protected:
    Owned<IRowInterfaces> inputRowIf;

public:
    CLocalResultActivityMasterBase(CMasterGraphElement *info) : CMasterActivity(info)
    {
        mpTag = container.queryJob().allocateMPTag();
    }
    virtual void init()
    {
        reset();
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
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
            PointerArray results;
            Owned<IException> e;
            try
            {
                rank_t sender;
                unsigned todo = container.queryJob().querySlaves();
                unsigned n=0;
                for (; n<todo; n++)
                    results.append(new CThorRowArray());
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

                        CThorRowArray *slaveResults = (CThorRowArray *)results.item(sender);
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
                CThorRowArray *slaveResult = (CThorRowArray *)results.item(0);
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
                                slaveResult = (CThorRowArray *)results.item(resultNum++);
                                if (slaveResult->ordinality())
                                {
                                    rowNum = 0;
                                    break;
                                }
                            }
                            if (!resultNum) // eos
                                break;
                        }
                        const byte *row = slaveResult->item(rowNum++);
                        inputRowIf->queryRowSerializer()->serialize(mbs, row);
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
            catch (IException *_e) { e.setown(_e); }
            ForEachItemIn(r, results)
            {
                CThorRowArray *result = (CThorRowArray *)results.item(r);
                delete result;
            }
            if (e)
                throw e.getClear();
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
        bool local = container.queryOwner().isLocalOnly();
        return graph->createGraphLoopResult(*this, inputRowIf, local); // NB graph owns result
    }
};

CActivityBase *createGraphLoopResultActivityMaster(CMasterGraphElement *container)
{
    return new CGraphLoopResultWriteActivityMaster(container);
}
