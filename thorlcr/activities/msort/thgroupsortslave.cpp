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
#include "slave.hpp"
#include "jsort.hpp"
#include "thmem.hpp"
#include "thbufdef.hpp"
#include "tsorta.hpp"

#include "thgroupsortslave.ipp"
#include "thactivityutil.ipp"
//#define TRACE_UNIQUE

#define TRANSFERBLOCKSIZE 0x10000

#include "jsort.hpp"
#include "thactivityutil.ipp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/thorlcr/activities/msort/thgroupsortslave.cpp $ $Id: thgroupsortslave.cpp 63605 2011-03-30 10:42:56Z nhicks $");


class CCGroupSortSlaveActivity : public CSlaveActivity, public CThorDataLink
{
private:
    IHThorSortArg * helper;
    ICompare      * icompare;
    bool eogNext;
    unsigned index;
    bool refill;
    bool eof;
    CThorRowArray group;

    IThorDataLink *input;
    bool unstable;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CCGroupSortSlaveActivity(CGraphElementBase *_container) : CSlaveActivity(_container), CThorDataLink(this)
    {
        unstable = false;
    }
    ~CCGroupSortSlaveActivity()
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);   // adding 'me' to outputs array
        helper = static_cast <IHThorSortArg *> (queryHelper());
        IHThorAlgorithm * algo = helper?(static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1))):NULL;
        unstable = (algo&&algo->getAlgorithmFlags()&TAFunstable);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        eogNext = false;
        refill = true;
        input = inputs.item(0);
        startInput(input);
        dataLinkStart();
        icompare = helper->queryCompare();              
        index = 0;
        eof = false;
        group.setSizing(true,false);
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if(abortSoon || eof || eogNext)
        {
            eogNext = false;
            return NULL;
        }
        if (refill) {
            refill=false;
            index=0;
            try
            {
                group.reset(false);
                loop {
                    OwnedConstThorRow row = input->nextRow();
                    if (!row)
                        break;
                    group.append(row.getClear());
                    if (group.isFull()) {
                        StringBuffer errStr("GROUPSORT");
                        errStr.append("(").append(container.queryId()).append(") ");
                        errStr.append("exceeded available memory. records=").append(group.ordinality()).append(", memory usage=").append((unsigned)(group.totalSize()/1024)).append('k');
                        IException *e = MakeActivityException(this, TE_TooMuchData, errStr.str());
                        EXCLOG(e, NULL);
                        throw e;
                    }
                }
                if (group.ordinality()==0) {
                    eof = true;
                    return NULL;
                }
                group.sort(*icompare,!unstable);   
            }
            catch (IOutOfMemException *e)
            {
                StringBuffer errStr("GROUPSORT");
                errStr.append("(").append(container.queryId()).append(") ");
                errStr.append("exceeded available memory. records=").append(group.ordinality()).append(", memory usage=").append((unsigned)(group.totalSize()/1024)).append('k');
                errStr.append(": ").append(e->errorCode()).append(", ");
                e->errorMessage(errStr);
                e->Release();
                IException *e2 = MakeActivityException(this, TE_TooMuchData, errStr.str());
                EXCLOG(e2, NULL);
                throw e2;
            }
        }
        if(index >= group.ordinality())
        {
            refill = true;
            return NULL;    // eog
        }   
        const void *row = group.itemClear(index++);
        assertex(row);
        dataLinkIncrement();
        return row;
    }

    bool isGrouped() { return true; }

    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.fastThrough = true; // ish
        calcMetaInfoSize(info,inputs.item(0));
    }
};

// Local Sort

class CLocalSortSlaveActivity : public CSlaveActivity, public CThorDataLink 
{
    IThorDataLink *input;
    IHThorSortArg *helper;
    ICompare *icompare;
    Owned<IThorRowSortedLoader> iloader;
    CThorRowArray rows;
    Owned<IRowStream> out;
    bool unstable;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLocalSortSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this)
    {
    }
    ~CLocalSortSlaveActivity()
    {
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        ActPrintLog("CLocalSortSlaveActivity::init");
        helper = (IHThorSortArg *)queryHelper();
        icompare = helper->queryCompare();
        IHThorAlgorithm * algo = helper?(static_cast<IHThorAlgorithm *>(helper->selectInterface(TAIalgorithm_1))):NULL;
        unstable = (algo&&algo->getAlgorithmFlags()&TAFunstable);
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("LOCALSORT", container.queryId());
        iloader.setown(createThorRowSortedLoader(rows));
        input = inputs.item(0);
        startInput(input);
        bool isempty;
        out.setown(iloader->load(input,queryRowInterfaces(input), icompare,false,abortSoon,isempty,"LOCALSORT",!unstable));
    }
    void stop()
    {
        out.clear();
        stopInput(input);
        dataLinkStop();
        iloader.clear();
    }
    void kill()
    {
        ActPrintLog("CLocalSortSlaveActivity::kill");
        CSlaveActivity::kill();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (abortSoon) 
            return NULL;
        OwnedConstThorRow row = out->nextRow();
        if (row) {
            dataLinkIncrement();
            return row.getClear();
        }
        return NULL;
    }
    virtual bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.buffersInput = true;
        calcMetaInfoSize(info,inputs.item(0));
    }
};

// Sorted 

class CSortedSlaveActivity : public CSlaveActivity, public CThorDataLink, public CThorSteppable
{
    IThorDataLink *input;
    IHThorSortedArg *helper;
    ICompare *icompare;

    IRangeCompare *stepCompare;
    OwnedConstThorRow prev; 

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CSortedSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this), CThorSteppable(this)
    {
        helper = (IHThorSortedArg *)queryHelper();
        icompare = helper->queryCompare();
        stepCompare = NULL;
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        helper = (IHThorSortedArg *)queryHelper();
        icompare = helper->queryCompare();
        appendOutputLinked(this);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        dataLinkStart("SORTED", container.queryId());
        input = inputs.item(0);
        startInput(input);
        IInputSteppingMeta *stepMeta = input->querySteppingMeta();
        if (stepMeta)
            stepCompare = stepMeta->queryCompare();
    }
    void stop()
    {
        stopInput(input);
        dataLinkStop();
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow ret = input->nextRow();
        if (ret && prev && icompare->docompare(prev, ret) > 0)
        {
            // MORE - better to give mismatching rows than indexes?
            throw MakeActivityException(this, TE_NotSorted, "detected incorrectly sorted rows (row %"ACTPF"d,  %"ACTPF"d))", getDataLinkCount(), getDataLinkCount()+1);
        }
        prev.set(ret);
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret && prev && stepCompare->docompare(prev, ret, numFields) > 0)
        {
            // MORE - better to give mismatching rows than indexes?
            throw MakeActivityException(this, TE_NotSorted, "detected incorrectly sorted rows (row %"ACTPF"d,  %"ACTPF"d))", getDataLinkCount(), getDataLinkCount()+1);
        }
        prev.set(ret);
        if (ret)
            dataLinkIncrement();
        return ret.getClear();
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector)
    { 
        return input->gatherConjunctions(collector);
    }
    void resetEOF() 
    { 
        input->resetEOF(); 
    }
    bool isGrouped() { return false; }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        calcMetaInfoSize(info,inputs.item(0));
    }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CSlaveActivity::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};


CActivityBase *createGroupSortSlave(CGraphElementBase *container)
{
    return new CCGroupSortSlaveActivity(container);
}


CActivityBase *createLocalSortSlave(CGraphElementBase *container)
{
    return new CLocalSortSlaveActivity(container);
}

CActivityBase *createSortedSlave(CGraphElementBase *container)
{
    return new CSortedSlaveActivity(container);
}


