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
#include "eventmetaparser.hpp"
#include "jevent.hpp"
#include "jptree.hpp"

enum PropertyTreeEventFlags : unsigned
{
    PTEFnone = 0x00,           // Emulate an event file iterator as closely as possible
    PTEFlenientParsing = 0x01, // Don't throw on or unknown events/attributes or incomplete events
    PTEFliteralParsing = 0x02  // Literal parsing: what comes in is what comes out (e.g., don't consume RecordingSource)
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
extern event_decl IEventIterator* createPropertyTreeEvents(const IPropertyTree& events, unsigned flags);
inline IEventIterator* createPropertyTreeEvents(const IPropertyTree& events) { return createPropertyTreeEvents(events, PTEFnone); }

// Extension of IEventIterator intended to act on events originating from multiple source
// iterators. Implementations choose how to interleave events from the various sources.
interface IEventMultiplexer : extends IEventIterator
{
    virtual void addSource(IEventIterator& source) = 0;
};

// Creates an IEventMultiplexer that interleaves events based on EventTimestamp values, in
// ascending order. The iterator is fully responsible for managing the provided meta state.
// The iterator should not be given another multiplexer as a source, and use of a separate
// meta state collector in the visitation chain should be avoided.
//
// All sources must be added before any event consumption. The meta state depends on the
// the total number of sources from which it will receive events. Processing an event
// before all sources are known can yield incorrect results.
extern event_decl IEventMultiplexer* createEventMultiplexer(CMetaInfoState& metaState);

// Dispatch the contents of an event iterator to an event visitor.
extern event_decl void visitIterableEvents(IEventIterator& iter, IEventVisitor& visitor);
