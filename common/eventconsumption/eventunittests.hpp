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

#ifdef _USE_CPPUNIT

#include "eventconsumption.h"
#include "eventvisitor.h"
#include "eventiterator.h"
#include "unittests.hpp"

// Event visiting endpoint used by unit tests to verify that visitation links processing known
// input produce expected results.
//
// The class is constructed with an expected events iterator. For each event visited, the actual
// event is compared to the next expected event in the iterator. Any deviation results in a test
// failure. The final test comes in `departFile`, where the expected iterator must not reference
// additional events.
class CEventVisitationLinkTester : public CInterfaceOf<IEventVisitor>
{
public:
    virtual bool visitFile(const char* filename, uint32_t version) override;
    virtual bool visitEvent(CEvent& actualEvent) override;
    virtual void departFile(uint32_t bytesRead) override;
public:
    CEventVisitationLinkTester(interface IEventIterator& expect);
private:
    Linked<IEventIterator> expect;
};

// Extracts `input`, `expect`, and `link` property tree sections from `testData`, passing them to
// `testEventVisitationLinks(input, expect, links)` for processing. The markup may be XML, JSON,
// or 'YAML' format.
extern void testEventVisitationLinks(const char* testData, bool strictParsing);
inline void testEventVisitationLinks(const char* testData) { testEventVisitationLinks(testData, true); }

// Uses a `CEventVisitationLinkTester` instance to iterate over the `input` events, verifying that
// the events received by the tester match the `expected` events after modification by visitation
// links configured by the `links` iterator.
//
// Multiple links may be configured. Within the iteration, the first link is closest to the tester
// and the last link receives the input events.
//
// At this time, only model links are supported. Filters may be supported by a future update.
extern void testEventVisitationLinks(const IPropertyTree& input, const IPropertyTree& expect, IPropertyTreeIterator& links, bool strictParsing);

extern IPropertyTree* createTestConfiguration(const char* configText);

#endif
