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
#include "eventvisitor.h"

// Extension of IEventVisitationLink that supports filtering of visited events. Implementations
// expect to be linked to another visitor, forwarding all visits not blocked by the specified
// constraints to the next linked visitor.
//
// An event blocked by a type or context constraint must not be forwarded to the next chained visitor.
//
// An event blocked by an attribute constraint must not be forwarded to the next chained visitor.
// Events that do not include a constrained attribute are not blocked.
interface IEventFilter : extends IEventVisitationLink
{
    // Filter on a single event type. All events are accepted by default.
    virtual bool acceptEvent(EventType type) = 0;
    // Filter on all events of a given context. All events are accepted by default.
    virtual bool acceptEvents(EventContext context) = 0;
    // Filter on a comma-delimited list of event type names and/or event context names. All events
    // are accepted by default.
    virtual bool acceptEvents(const char* types) = 0;
    // Filter on a comma-delimited list of attribute value tokens. The internal type of the given
    // attribute is used to determine token processing requirements.
    // - Integral token options are:
    //   - #: a single numeric value
    //   - #-#: a range of numeric values, bounded on both ends
    //   - #-: a range of numeric values, bounded only on the lower end
    //   - -#: a range of numeric values, bounded only on the upper end
    // - String token options include exact matches and regular expressions.
    // - Boolean token options are text representations of true and false recognized by strToBool.
    //   Specification of multiple Boolean values is unnecessary - either they will be repetitive
    //   or will cancel each other out.
    //
    // Supported special cases include:
    // - Timestamps may be given using a standard date/time format, such as "2025-01-01T00:00:00".
    // - A filter for the integral EvAttrFileId may include string tokens that will be applied to
    //   a corresponding EvAttrPath attribute previously observed in a MetaFileInformation event.
    virtual bool acceptAttribute(EventAttr attr, const char* values) = 0;
};

// Obtain a new instance of a standard event filter.
extern event_decl IEventFilter* createEventFilter();
