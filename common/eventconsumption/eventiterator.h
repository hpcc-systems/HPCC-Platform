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
// to be a potential event source
interface IEventIterator : extends IInterface
{
    virtual bool nextEvent(CEvent& event) = 0;
};

// Implementation of IEventIterator that extracts event data from a property tree whose contents
// conform to this format (shown here as YAML):
//
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
class CPropertyTreeEvents : public CInterfaceOf<IEventIterator>
{
public:
    virtual bool nextEvent(CEvent& event) override;
public:
    CPropertyTreeEvents(const IPropertyTree& events);
protected:
    Owned<IPropertyTreeIterator> eventsIt;
};
