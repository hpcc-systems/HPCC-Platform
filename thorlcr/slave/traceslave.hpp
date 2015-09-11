/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifndef TRACESLAVE_HPP
#define TRACESLAVE_HPP

#include "slave.hpp"
#include "jqueue.tpp"
#include "jthread.hpp"
#include "limits.h"


class CTracingThorDataLink :  implements CInterfaceOf<IThorDataLink>
{
private:
    OwnedConstThorRow *rowBuffer;
    unsigned rowHeadp;

    static const unsigned traceRowBufferSize=10;
    static const unsigned dumpRowIntervalMS=600000;
    Owned<IThorDataLink> thorDataLink;

    IHThorArg *helper;
    CCycleTimer cycleTimer;
    cycle_t intervalCycle;
    activity_id activityId;

    inline bool hasTimeElapsed()
    {
        return (cycleTimer.elapsedCycles() > intervalCycle);
    }
    inline void enqueueRowForTrace(const void *row)
    {
        rowBuffer[rowHeadp].set(row);

        ++rowHeadp;
        if (rowHeadp==traceRowBufferSize)
            rowHeadp = 0;

        if (hasTimeElapsed())
        {
            cycleTimer.reset();
            logTraceRows();
        }
    }
    inline void logTraceRows()
    {
        unsigned rowPos = rowHeadp;
        for (unsigned n=0; n<traceRowBufferSize;++n)
        {
            OwnedConstThorRow row = rowBuffer[rowPos].getClear();

            if (++rowPos==traceRowBufferSize) rowPos = 0;
            if (!row) continue;

            CommonXmlWriter xmlwrite(XWFnoindent);
            helper->queryOutputMeta()->toXML((const byte *) row.get(), xmlwrite);
            DBGLOG ("TRACE: (ActivityId: %d) <ROW>%s</ROW>", activityId, xmlwrite.str());
        }
    }

public:
    virtual void start()
    {
        thorDataLink->start();
        activityId = queryFromActivity()->queryActivityId();
        if (!rowBuffer)
            rowBuffer = new OwnedConstThorRow[traceRowBufferSize];
    }
    virtual bool isGrouped() { return thorDataLink->isGrouped();}
    virtual const void *nextRowGE(const void * seek, unsigned numFields, bool &wasCompleteMatch, const SmartStepExtra &stepExtra)
    {
        const void *row = thorDataLink->nextRowGE(seek, numFields, wasCompleteMatch, stepExtra);

        enqueueRowForTrace(row);

        return row;
    }    // can only be called on stepping fields.
    virtual IInputSteppingMeta *querySteppingMeta() { return thorDataLink->querySteppingMeta(); }
    virtual bool gatherConjunctions(ISteppedConjunctionCollector & collector) { return thorDataLink->gatherConjunctions(collector); }
    virtual void resetEOF() { thorDataLink->resetEOF();}

    virtual void getMetaInfo(ThorDataLinkMetaInfo &info) { thorDataLink->getMetaInfo(info);}
    virtual CActivityBase *queryFromActivity() {return thorDataLink->queryFromActivity();} // activity that has this as an output
    virtual void dataLinkSerialize(MemoryBuffer &mb) {thorDataLink->dataLinkSerialize(mb);}
    virtual unsigned __int64 queryTotalCycles() const { return thorDataLink->queryTotalCycles();}

    virtual const void *nextRow()
    {
        const void *row = thorDataLink->nextRow();

        enqueueRowForTrace(row);

        return row;
    }
    virtual void beforeDispose()
    {
        logTraceRows();
        delete [] rowBuffer;
    }
    virtual void stop() {
        thorDataLink->stop();
    }                              // after stop called NULL is returned

    inline const void *ungroupedNextRow() { return thorDataLink->ungroupedNextRow();}

    CTracingThorDataLink(IThorDataLink *_input, IHThorArg *_helper) : thorDataLink(_input), helper(_helper), rowHeadp(0),rowBuffer(NULL)
    {
        intervalCycle = queryOneSecCycles() * dumpRowIntervalMS / 1000;
    }
};

#endif
