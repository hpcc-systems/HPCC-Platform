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

#pragma once

#include "eventconsumption.h"
#include "eventfilter.h"
#include "eventmodeling.h"
#include "eventmetaparser.hpp"
#include <set>
#include <string>

// Base operation class that provides meta parser functionality and supports streaming output
// and event filtering by event kind and attribute values. This satisfies the requirements
// for the `event_consuming_op_t` template parameter of `TEventConsumingCommand`.
//
// Subclasses are expected to add support for additional options.
class event_decl CEventConsumingOp : public CInterface
{
public: // abstract method(s)
    virtual bool doOp() = 0;
public:
    CEventConsumingOp();
    CMetaInfoState& queryMetaInfoState() { return *metaState; }
    virtual bool ready() const;
    // Add unique and non-empty paths to the collection of input paths.
    void setInputPath(const char* path);
    void setOutput(IBufferedSerialOutputStream& _out);
    bool acceptEvents(const char* eventNames);
    bool acceptAttribute(EventAttr attr, const char* values);
    bool acceptModel(const IPropertyTree& config);
protected:
    IEventFilter* ensureFilter();
    bool traverseEvents(IEventVisitor& visitor);

    // Query the EventFileProperties from the iterator that will be used for traversal.
    // This method is only valid during doOp() execution, after input paths are set.
    // For multiplexed sources, properties reflect aggregated/ambiguous values.
    const EventFileProperties& queryIteratorProperties();

protected:
    Owned<CMetaInfoState> metaState;
    std::set<std::string> inputPaths;
    Linked<IBufferedSerialOutputStream> out;
private:
    Owned<IEventFilter> filter;
    Owned<IEventModel> model;
    Owned<IEventIterator> cachedSource;  // Cached iterator created on first queryIteratorProperties() call
};
