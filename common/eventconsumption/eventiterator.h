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
#include "jevent.hpp"
#include "jptree.hpp"

// An abstraction enabling an event pulling model. Consumers control the pace of event
// production by calling nextEvent() as needed.
//
// The interface is more simplistic than other iterators. A binary event file reader is considered
// to be a potential event source.
//
// The query* methods are used to identify a source of events. The queried values coincide with
// `IEventVisitor::visitFile` and `IEventVisitor::departFile` parameters.
interface IEventIterator : extends IInterface
{
    virtual bool nextEvent(CEvent& event) = 0;
    virtual const char* queryFilename() const = 0;
    virtual uint32_t queryVersion() const = 0;
    virtual uint32_t queryBytesRead() const = 0;
};

// Implementation of IEventIterator that extracts event data from a property tree whose contents
// conform to this format (shown here as YAML):
//
// filename: <filename>
// version: <version>
// bytesRead: <bytes read>
// event:
// - type: <event name>
//   <event attribute name>: <value>
//
// In this format...
// - <event name> is any text label associated with an EventType enumerated value; and
// - <event attribute name> is any text label associated with an EventAttr enumerated value; and
// - `event` and `event/@type` are used in place of elements named for the event type because
//   IPropertyTree only preserves element order of elements with the same name; and
// - <value> is a text representation of the value to be assigned to the event attribute, where
//   the text must be directly convertible to the attribute's underlying data type. The only
//   exception is that a timestamp value may be a human readable date/time string instead of a
//   a number of nanoseconds.
// - filename, version, and bytesRead are optional values that will be used to satisfy the query*
//   methods. Default values of nullptr, 0, and 0 are used when omitted.
class CPropertyTreeEvents : public CInterfaceOf<IEventIterator>
{
public:
    virtual bool nextEvent(CEvent& event) override;
    virtual const char* queryFilename() const override;
    virtual uint32_t queryVersion() const override;
    virtual uint32_t queryBytesRead() const override;
public:
    CPropertyTreeEvents(const IPropertyTree& events);
protected:
    Linked<const IPropertyTree> events;
    Owned<IPropertyTreeIterator> eventsIt;
};

// Dispatch the contents of an event iterator to an event visitor.
extern event_decl void visitIterableEvents(IEventIterator& iter, IEventVisitor& visitor);
