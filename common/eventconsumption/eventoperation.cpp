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
#include "eventiterator.h"
#include "eventmodeling.h"

CEventConsumingOp::CEventConsumingOp()
{
    metaState.setown(new CMetaInfoState());
}

bool CEventConsumingOp::ready() const
{
    return !inputPaths.empty() && out.get();
}

void CEventConsumingOp::setInputPath(const char* path)
{
    if (!isEmptyString(path))
        inputPaths.insert(path);
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

const EventFileProperties& CEventConsumingOp::queryIteratorProperties()
{
    if (!cachedSource)
    {
        // Lazy initialization - create the iterator on first access
        switch (inputPaths.size())
        {
        case 0:
            throw makeStringExceptionV(0, "No input files specified");
        case 1:
            cachedSource.setown(createEventFileIterator(inputPaths.begin()->c_str()));
            if (!cachedSource)
                throw makeStringExceptionV(0, "Failed to open event file: %s", inputPaths.begin()->c_str());
            break;
        default:
            {
                Owned<IEventMultiplexer> multiplexer = createEventMultiplexer(*metaState);
                cachedSource.set(multiplexer.get());
                for (const std::string& path : inputPaths)
                {
                    Owned<IEventIterator> input = createEventFileIterator(path.c_str());
                    if (!input)
                        throw makeStringExceptionV(0, "Failed to open event file: %s", path.c_str());
                    multiplexer->addSource(*input);
                }
            }
            break;
        }
    }
    return cachedSource->queryFileProperties();
}

bool CEventConsumingOp::traverseEvents(IEventVisitor& visitor)
{
    // Ensure iterator is created and cached
    (void)queryIteratorProperties();

    // Build visitation chain
    IEventVisitor* visitationHead = &visitor;
    if (filter)
    {
        filter->setNextLink(*visitationHead);
        visitationHead = filter;
    }
    if (model)
    {
        model->setNextLink(*visitationHead);
        visitationHead = model;
    }

    // Add meta collector for single-source case
    Owned<IEventVisitationLink> metaCollector;
    if (inputPaths.size() == 1)
    {
        metaCollector.setown(metaState->getCollector());
        metaCollector->setNextLink(*visitationHead);
        visitationHead = metaCollector;
    }

    visitIterableEvents(*cachedSource, *visitationHead);
    return true;
}
