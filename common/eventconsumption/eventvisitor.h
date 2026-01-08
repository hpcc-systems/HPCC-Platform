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
// which events are pulled instead of visisted, then this file will declare the IEventVisitor
// interface and the adapter to convert pulled events into visisted events.
#include "jevent.hpp"

// Abstract extension of IEventVisitor supporting the chain of responsibility pattern. In terms
// of event visitation, the chain of responsibility is defined by a sequence of zero or more
// visitation links terminated by a final visitor that is most likely not a visitation link.
//
// Event propagation is the key distinction between visitation links and other visitors. Links
// are designed to a (possibly empty) set of events to the next visitor in the chain. Non-link
// visitors are meant to consume the data they receive, without forwading it to another visitor.
//
// Implementations may use the IMPLEMENT_IEVENTVISITATIONLINK macro to simplify implementation.
interface IEventVisitationLink : extends IEventVisitor
{
    virtual void configure(const IPropertyTree& config) = 0;
    virtual void setNextLink(IEventVisitor& visitor) = 0;
    virtual void clearNextLink(bool recursive) = 0;
};

// Shortcut to standard implementations of visitFile and departFile in a visitation link
// implementation. All implementations must still implement visitEvent.
#define IMPLEMENT_IEVENTVISITATIONLINK \
protected: \
    Linked<IEventVisitor> nextLink; \
public: \
    void setNextLink(IEventVisitor& visitor) override { nextLink.set(&visitor); } \
    void clearNextLink(bool recursive) override \
    { \
        if (recursive) \
        { \
            IEventVisitationLink* tmp = dynamic_cast<IEventVisitationLink*>(nextLink.get()); \
            if (tmp) \
                tmp->clearNextLink(recursive); \
        } \
        nextLink.clear(); \
    } \
    bool visitFile(const char* filename, uint32_t version) override { if (!nextLink) return false; return nextLink->visitFile(filename, version); } \
    void departFile(uint32_t bytesRead) override { if (!nextLink) return; nextLink->departFile(bytesRead); }
