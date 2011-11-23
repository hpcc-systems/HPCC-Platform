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

#include "slave.ipp"
#include "eclhelper.hpp"
#include "thactivityutil.ipp"
#include "thbuf.hpp"
#include "thbufdef.hpp"
#include "thormisc.hpp"


class CLimitSlaveActivityBase : public CSlaveActivity, public CThorDataLink
{
protected:
    rowcount_t rowLimit;
    bool eos, eogNext, stopped, resultSent, anyThisGroup;
    IThorDataLink *input;
    IHThorLimitArg *helper;
    StringAttr msg;

    void stopInput(rowcount_t c)
    {
        if (!stopped) {
            stopped = true;
            sendResult(c);
            CSlaveActivity::stopInput(input, msg);
        }
    }

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CLimitSlaveActivityBase(CGraphElementBase *_container, const char *_msg) : CSlaveActivity(_container), msg(_msg), CThorDataLink(this)
    {
        input = NULL;       
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        helper = (IHThorLimitArg *)queryHelper();

        if (!container.queryLocal())
            mpTag = container.queryJob().deserializeMPTag(data);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities, NULL);
        resultSent = container.queryLocal(); // i.e. local, so don't send result to master
        eos = stopped = false;
        input = inputs.item(0);
        anyThisGroup = eogNext = false;
        startInput(input);
        rowLimit = (rowcount_t)helper->getRowLimit();
        dataLinkStart(msg, container.queryId());
    }
    void sendResult(rowcount_t r)
    {
        if (resultSent) return;
        resultSent = true;
        CMessageBuffer mb;
        mb.append(r);
        container.queryJob().queryJobComm().send(mb, 0, mpTag);
    }
    void stop()
    {
        stopInput(getDataLinkCount());
        dataLinkStop();
    }
    bool isGrouped() { return inputs.item(0)->isGrouped(); }
    void getMetaInfo(ThorDataLinkMetaInfo &info)
    {
        initMetaInfo(info);
        info.canReduceNumRows = true;
        info.canBufferInput = false;
        info.totalRowsMax = rowLimit;
        calcMetaInfoSize(info,inputs.item(0));
    }
};

class CLimitSlaveActivity : public CLimitSlaveActivityBase, public CThorSteppable
{
public:

    CLimitSlaveActivity(CGraphElementBase *container) : CLimitSlaveActivityBase(container, "LIMIT"), CThorSteppable(this) { }
    
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eos)
            return NULL;
        while (!abortSoon && !eogNext)
        {
            OwnedConstThorRow row = input->nextRow();
            if (!row)
            {
                if(anyThisGroup)
                {
                    anyThisGroup = false;
                    break;
                }
                row.setown(input->nextRow());
                if (!row)
                {
                    eos = true;
                    sendResult(getDataLinkCount());
                    break;
                }
            }
            anyThisGroup = true;
            if (getDataLinkCount() >= rowLimit)
            {
                eos = true;
                sendResult(getDataLinkCount());
                helper->onLimitExceeded();
                return NULL;
            }
            dataLinkIncrement();
            return row.getClear();
        }
        eogNext = false;
        return NULL;
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
        if (ret)
        {
            if (wasCompleteMatch)
                dataLinkIncrement();
            if (getDataLinkCount() > rowLimit)
                helper->onLimitExceeded();
        }
        return ret.getClear();
    }
    bool gatherConjunctions(ISteppedConjunctionCollector &collector) 
    { 
        return input->gatherConjunctions(collector); 
    }
    void resetEOF() 
    { 
        //Do not reset the rowLimit
        input->resetEOF(); 
    }
// steppable
    virtual void setInput(unsigned index, CActivityBase *inputActivity, unsigned inputOutIdx)
    {
        CLimitSlaveActivityBase::setInput(index, inputActivity, inputOutIdx);
        CThorSteppable::setInput(index, inputActivity, inputOutIdx);
    }
    virtual IInputSteppingMeta *querySteppingMeta() { return CThorSteppable::inputStepping; }
};

class CSkipLimitSlaveActivity : public CLimitSlaveActivityBase
{
    UnsignedArray sizeArray;
    size32_t ptrIndex;
    bool limitChecked, eof, rowTransform;
    IHThorLimitTransformExtra *helperex;
    Owned<IRowWriterMultiReader> buf;
    Owned<IRowStream> reader;

    rowcount_t getResult()
    {
        assertex(resultSent);
        CMessageBuffer mb;
        if (!receiveMsg(mb, 0, mpTag))
            return RCUNSET;
        rowcount_t r;
        mb.read(r);
        return r;
    }

    bool gather()
    {
        rowcount_t count = 0;
        while (!abortSoon)
        {
            OwnedConstThorRow row = input->nextRow();
            if (!row)
            {
                row.setown(input->nextRow());
                if (!row)
                    break;
                else
                    buf->putRow(NULL); 
            }
            buf->putRow(row.getClear());
            if (++count > rowLimit)
                break;

            // We used to warn if excessive buffering. I think there should be callback to signal,
            // Alternatively, could do via IDiskUsage whish smart buffer used to take...
            //throw MakeActivityException(this, 0, "SkipLimit(%"ACTPF"d) exceeded activity buffering limit", container.queryId());
        }
        buf->flush();
        stopInput(count);
        rowcount_t total = container.queryLocal() ? count : getResult();
        if (total > rowLimit)
        {
            buf.clear();
            return false;
        }
        else
        {
            reader.setown(buf->getReader());
            buf.clear();
            return true;
        }
    }

public:
    CSkipLimitSlaveActivity(CGraphElementBase *container,bool _rowTransform) : CLimitSlaveActivityBase(container, "SKIPLIMIT")
    {
        ptrIndex = 0;
        limitChecked = eof = false;
        rowTransform = _rowTransform;
        helperex = NULL;
    }
    void stop()
    {
        stopInput(0);
        dataLinkStop();
    }
    void abort()
    {
        if (!container.queryLocal())
            cancelReceiveMsg(0, mpTag);
        CLimitSlaveActivityBase::abort();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        CLimitSlaveActivityBase::init(data,slaveData);
        if (rowTransform)
            helperex = static_cast<IHThorLimitTransformExtra *>(queryHelper()->selectInterface(TAIlimittransformextra_1));
    }
    void start()
    {
        CLimitSlaveActivityBase::start();
        // JSCTBD: used to take IDiskUsage
        buf.setown(createOverflowableBuffer(this, SKIPLIMIT_BUFFER_SIZE)); 
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities, NULL);
        if (eof) 
            return NULL;
        if (!limitChecked)
        {
            limitChecked = true;
            if (!gather())
            {
                eof = true;
                if (rowTransform&&helperex && firstNode())
                {
                    try
                    {
                        RtlDynamicRowBuilder ret(queryRowAllocator());
                        size32_t sizeGot = helperex->transformOnLimitExceeded(ret);
                        if (sizeGot)
                        {
                            dataLinkIncrement();
                            return ret.finalizeRowClear(sizeGot);
                        }
                    }
                    catch (IException *e)
                    {
                        ActPrintLog(e, "In transformOnLimitExceeded");
                        throw; 
                    }
                    catch (CATCHALL)
                    { 
                        ActPrintLog("LIMIT: Unknown exception in transformOnLimitExceeded()"); 
                        throw;
                    }
                }
                return NULL;
            }
        }
        OwnedConstThorRow row = reader->nextRow();
        if (row)
        {
            dataLinkIncrement();
            anyThisGroup = true;
            return row.getClear();
        }
        if (anyThisGroup)
            anyThisGroup = false;
        else
            eof = true;
        return NULL;
    }
};

activityslaves_decl CActivityBase *createLimitSlave(CGraphElementBase *container)
{
    return new CLimitSlaveActivity(container);
}


activityslaves_decl CActivityBase *createSkipLimitSlave(CGraphElementBase *container)
{
    return new CSkipLimitSlaveActivity(container,false);
}

activityslaves_decl CActivityBase *createRowLimitSlave(CGraphElementBase *container)
{
    return new CSkipLimitSlaveActivity(container,true);
}

