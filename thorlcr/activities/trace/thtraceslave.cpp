/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#include "platform.h"
#include "eclhelper.hpp"
#include "slave.ipp"
#include "thactivityutil.ipp"

class CTraceSlaveActivity : public CSlaveActivity, public CThorDataLink, public CThorSteppable
{
    IThorDataLink *input;
    IHThorTraceArg *helper;
    OwnedRoxieString name;
    unsigned keepLimit;
    unsigned skip;
    unsigned sample;
    bool traceEnabled;

public:
    IMPLEMENT_IINTERFACE_USING(CSimpleInterface);

    CTraceSlaveActivity(CGraphElementBase *_container)
        : CSlaveActivity(_container), CThorDataLink(this), CThorSteppable(this),
          keepLimit(0), skip(0), sample(0), traceEnabled(false)
    {
        helper = (IHThorTraceArg *) queryHelper();
    }
    void init(MemoryBuffer &data, MemoryBuffer &slaveData)
    {
        appendOutputLinked(this);
        traceEnabled = getOptBool(THOROPT_TRACE_ENABLED, false);
    }
    void start()
    {
        ActivityTimer s(totalCycles, timeActivities);
        dataLinkStart();
        input = inputs.item(0);
        startInput(input);
        if (traceEnabled && helper->canMatchAny() && queryRowMetaData())
        {
            keepLimit = helper->getKeepLimit();
            if (keepLimit==(unsigned) -1)
                keepLimit = getOptUInt(THOROPT_TRACE_LIMIT, 100);
            skip = helper->getSkip();
            sample = helper->getSample();
            if (sample)
                sample--;
            name.setown(helper->getName());
            if (!name)
                name.set("Row");
        }
        else
            keepLimit = 0;
    }
    void stop()
    {
        name.clear();
        stopInput(input);
        dataLinkStop();
    }
    void onTrace(const void *row)
    {
        if (keepLimit && helper->isValid(row))
        {
            if (skip)
                skip--;
            else if (sample)
                sample--;
            else
            {
                CommonXmlWriter xmlwrite(XWFnoindent);
                queryRowMetaData()->toXML((const byte *) row, xmlwrite);
                ActPrintLog("TRACE: <%s>%s<%s>", name.get(), xmlwrite.str(), name.get());
                keepLimit--;
                sample = helper->getSample();
                if (sample)
                    sample--;
            }
        }
    }
    CATCH_NEXTROW()
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow ret = input->nextRow();
        if (ret)
        {
            onTrace(ret);
            dataLinkIncrement();
        }
        return ret.getClear();
    }
    const void *nextRowGE(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        try { return nextRowGENoCatch(seek, numFields, wasCompleteMatch, stepExtra); }
        CATCH_NEXTROWX_CATCH;
    }
    const void *nextRowGENoCatch(const void *seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        ActivityTimer t(totalCycles, timeActivities);
        OwnedConstThorRow ret = input->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);
        if (ret)
        {
            onTrace(ret);
            dataLinkIncrement();
        }
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
    bool isGrouped() { return input->isGrouped(); }
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


////////////////////

CActivityBase *createTraceSlave(CGraphElementBase *container)
{
    return new CTraceSlaveActivity(container);
}
