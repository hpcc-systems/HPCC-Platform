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

#include "eventoperation.h"
#include "eventfilter.h"
#include "eventmodeling.h"

CEventConsumingOp::CEventConsumingOp()
{
    metaState.setown(new CMetaInfoState());
}

bool CEventConsumingOp::ready() const
{
    return !inputPath.isEmpty() && out.get();
}

void CEventConsumingOp::setInputPath(const char* path)
{
    inputPath.set(path);
}

void CEventConsumingOp::setOutput(IBufferedSerialOutputStream& _out)
{
    out.set(&_out);
}

bool CEventConsumingOp::acceptEvents(const char* eventNames)
{
    return ensureFilter()->acceptEvents(eventNames);
}

bool CEventConsumingOp::acceptAttribute(EventAttr attr, const char* values)
{
    return ensureFilter()->acceptAttribute(attr, values);
}

bool CEventConsumingOp::acceptModel(const IPropertyTree& config)
{
    if (model)
        return false; // only one model per operation
    model.setown(createEventModel(config, queryMetaInfoState()));
    return model != nullptr;
}

IEventFilter* CEventConsumingOp::ensureFilter()
{
    if (!filter)
        filter.setown(createEventFilter(queryMetaInfoState()));
    return filter;
}

bool CEventConsumingOp::traverseEvents(const char* path, IEventVisitor& visitor)
{
    IEventVisitor* actual = &visitor;
    if (filter)
    {
        filter->setNextLink(*actual);
        actual = filter;
    }
    if (model)
    {
        model->setNextLink(*actual);
        actual = model;
    }

    // Always include the meta information parser as the first link in the chain
    // to ensure MetaFileInformation and EventQueryStart events are captured
    metaState->setNextLink(*actual);

    return readEvents(path, *metaState);
}
