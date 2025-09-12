/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

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

#include "eventsaveas.h"
#include "jevent.hpp"

class event_decl CEventSavingVisitor : public CInterfaceOf<IEventVisitor>
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override
    {
        return true;
    }
    virtual bool visitEvent(CEvent& event) override
    {
        queryRecorder().recordEvent(event);
        return true;
    }
    virtual void departFile(uint32_t bytesRead) override
    {
    }

public:
    CEventSavingVisitor(CEventSavingOp& _op)
        : op(_op)
    {
    }

private:
    CEventSavingOp& op;
};

bool CEventSavingOp::ready() const
{
    return CEventConsumingOp::ready() && !outputPath.isEmpty();
}

bool CEventSavingOp::doOp()
{
    EventRecorder& recorder = queryRecorder();
    if (!recorder.startRecording(options, outputPath, false))
        return false;
    Owned<IEventVisitor> terminalVisitor = new CEventSavingVisitor(*this);
    try
    {
        traverseEvents(inputPath, *terminalVisitor);
        recorder.stopRecording(&summary);
    }
    catch (...)
    {
        recorder.stopRecording(&summary);
        throw;
    }

    return true;
}

void CEventSavingOp::setOutputPath(const char* path)
{
    outputPath = path;
}

void CEventSavingOp::setOutputOptions(const char* _options)
{
    options.set(_options);
}

StringBuffer& CEventSavingOp::getResults(StringBuffer& results) const
{
    results.set("Saved ")
           .append(summary.numEvents)
           .append(" events to ")
           .append(summary.filename);
    return results;
}