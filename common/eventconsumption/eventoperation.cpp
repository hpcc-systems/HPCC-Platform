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

bool CEventConsumingOp::preScanRequired() const
{
    if (filter && filter->preScanRequired())
        return true;
    if (model && model->preScanRequired())
        return true;
    return false;
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

unsigned CEventConsumingOp::getNumSources() const
{
    return inputPaths.size();
}

Owned<IEventIterator> CEventConsumingOp::createInputIterator()
{
    Owned<IEventIterator> input;
    switch (getNumSources())
    {
    case 0:
        throw makeStringExceptionV(0, "No input files specified");
    case 1:
        input.setown(createEventFileIterator(inputPaths.begin()->c_str()));
        if (!input)
            throw makeStringExceptionV(0, "Failed to open event file: %s", inputPaths.begin()->c_str());
        break;
    default:
        {
            Owned<IEventMultiplexer> multiplexer = createMultiplexer(*metaState, preScanCompleted);
            for (const std::string& path : inputPaths)
            {
                Owned<IEventIterator> child = createEventFileIterator(path.c_str());
                if (!child)
                    throw makeStringExceptionV(0, "Failed to open event file: %s", path.c_str());
                multiplexer->addSource(*child);
            }
            input.setown(multiplexer.getClear());
        }
        break;
    }
    return input;
}

const EventFileProperties& CEventConsumingOp::queryIteratorProperties()
{
    if (!cachedSource)
        cachedSource.setown(createInputIterator().getClear());
    return cachedSource->queryFileProperties();
}

void CEventConsumingOp::postTraversalReset()
{
    metaState->clearAll();
    cachedSource.clear();
    preScanCompleted = false;
}

bool CEventConsumingOp::preScanEvents(IEventVisitor* visitor)
{
    (void)queryIteratorProperties();

    IEventVisitor* visitationHead = visitor;

    Owned<IEventVisitationLink> metaCollector;
    if (getNumSources() == 1)
    {
        metaCollector.setown(metaState->getCollector());
        if (visitationHead)
            metaCollector->setNextLink(*visitationHead);
        visitationHead = metaCollector.get();
    }

    if (visitationHead)
    {
        visitIterableEvents(*cachedSource, *visitationHead);
    }
    else
    {
        // No explicit visitor provided and getNumSources() > 1.
        // The multiplexer intrinsically triggers the metaState collector internally per event.
        // We can simply drain the iterator without any virtual visitation dispatch overhead.
        CEvent event;
        while (cachedSource->nextEvent(event))
        {
        }
    }

    cachedSource.clear();

    return true;
}

bool CEventConsumingOp::traverseEvents(IEventVisitor& visitor)
{
    if (preScanRequired())
    {
        preScanEvents(getPreScanVisitor());
        preScanCompleted = true;
    }

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
    if (!preScanCompleted && getNumSources() == 1)
    {
        metaCollector.setown(metaState->getCollector());
        metaCollector->setNextLink(*visitationHead);
        visitationHead = metaCollector;
    }

    visitIterableEvents(*cachedSource, *visitationHead);
    return true;
}

IEventMultiplexer* CEventConsumingOp::createMultiplexer(CMetaInfoState& metaState, bool bypassMetaCollector)
{
    return createSerialEventMultiplexer(metaState, bypassMetaCollector);
}
