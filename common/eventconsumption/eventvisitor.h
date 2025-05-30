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
// MORE: If jevent.hpp replaces readEvents with a function that returns a reader instance, from
// which events are pulled instead of visisted, then this file will declare the IEventVisisot
// interface and the adapter to convert pulled events into visisted events.
#include "jevent.hpp"

// Abstract extension of IEventVisitor that supports decorating another visitor. Decorators may:
// - prevent the decorated visitor from seeing data
// - change the data seen by a decorated visitor
// - simulate new data for the decorated visitor to process
// - act on data without affecting the decorated visitor
//
// Implementations may use the IMPLEMENT_IEVENTVISITORDECORATOR macro to limit implementation
// to only cisitEvent.
interface IEventVisitorDecorator : extends IEventVisitor
{
    // Establshes the decorated-decorator relationship.
    virtual void decorate(IEventVisitor& visitor) = 0;
};

// Shortcut to stantart implementations of visitFile and departFile in a decorator implementation.
// All decorator implementations must still implement visitEvent.
#define IMPLEMENT_IEVENTVISITORDECORATOR \
protected: \
    Linked<IEventVisitor> decorated; \
public: \
    void decorate(IEventVisitor& visitor) override { decorated.set(&visitor); } \
    bool visitFile(const char* filename, uint32_t version) override { if (!decorated) return false; return decorated->visitFile(filename, version); } \
    void departFile(uint32_t bytesRead) override { if (!decorated) return; decorated->departFile(bytesRead); }

// Abstraction of the visitor pattern for "reading" binary event data files. The `readEvents`
// function will pass each byte of data contained within a file throwgh exactly one method of
// this interface.
//
// For each compatibile file the visitor can expect:
// 1. One call to visitFile
// 2. Zero or more sequences of:
//    a. One call to visitEvent
//    b. Zero or more calls to visitAttribute
//    c. One call to departEvent
// 3. One call to departFile
//
// Implementations may implement limited filtering during visitation. All methods, except
// `departFile`, may abort visitation. Both `visitEvent` and `visitAttribute` (in the context of
// an event) may suppress visitation of the remainder of the current event.
//
// Reasons for aborting a file include:
// - unrecognized file version; or
// - (remaining) events out of a target date range; or
// - trace IDs are required but are not present.
//
// Reasons for suppressing an event include:
// - event type not required by current use case; or
// - attribute value not out of range for current use case.
interface IEventAttributeVisitor : extends IEventVisitor
{
    enum Continuation {
        visitContinue,
        visitSkipEvent,
        visitSkipFile
    };
    using IEventVisitor::visitEvent;
    virtual Continuation visitEvent(EventType id) = 0;
    virtual Continuation visitAttribute(EventAttr id, const char * value) = 0;
    virtual Continuation visitAttribute(EventAttr id, bool value) = 0;
    virtual Continuation visitAttribute(EventAttr id, __uint64 value) = 0;
    virtual bool departEvent() = 0;
};

extern event_decl bool eventDistributor(CEvent& event, IEventAttributeVisitor& visitor);
