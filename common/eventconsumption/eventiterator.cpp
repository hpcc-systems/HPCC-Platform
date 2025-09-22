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

#include "eventiterator.h"

bool CPropertyTreeEvents::nextEvent(CEvent& event)
{
    if (!eventsIt->isValid())
        return false;
    const IPropertyTree& node = eventsIt->query();
    const char* typeStr = node.queryProp("@type");
    EventType type = queryEventType(typeStr);
    if (EventNone == type)
    {
        if (strictParsing)
            throw makeStringExceptionV(-1, "unknown event type: %s", typeStr);
        eventsIt->next();
        return nextEvent(event);
    }
    event.reset(type);

    Owned<IAttributeIterator> attrIt = node.getAttributes();
    ForEach(*attrIt)
    {
        const char* name = attrIt->queryName();
        if ('@' == *name)
            name++;
        if (streq(name, "type"))
            continue;
        EventAttr attrId = queryEventAttribute(name);
        if (EvAttrNone == attrId)
        {
            if (strictParsing)
                throw makeStringExceptionV(-1, "unknown attribute: %s", name);
            continue;
        }
        if (!event.isAttribute(attrId))
        {
            if (strictParsing)
                throw makeStringExceptionV(-1, "unused attribute %s/%s", typeStr, name);
            continue;
        }
        const char* valueStr = attrIt->queryValue();
        if (isEmptyString(valueStr))
            continue;
        CEventAttribute& attr = event.queryAttribute(attrId);
        switch (attr.queryTypeClass())
        {
        case EATCtext:
        case EATCtimestamp:
            attr.setValue(valueStr);
            break;
        case EATCnumeric:
            attr.setValue(strtoull(valueStr, nullptr, 0));
            break;
        case EATCboolean:
            attr.setValue(strToBool(valueStr));
            break;
        default:
            if (strictParsing)
                throw makeStringExceptionV(-1, "unknown attribute type class %u for %s/%s", attr.queryTypeClass(), typeStr, name);
            break;
        }
    }
    if (strictParsing && !event.isComplete())
        throw makeStringExceptionV(-1, "incomplete event %s", typeStr);

    // advance to the next matching node
    (void)eventsIt->next();

    // the requested event was found
    return true;
}

const char* CPropertyTreeEvents::queryFilename() const
{
    return events->queryProp("@filename");
}

uint32_t CPropertyTreeEvents::queryVersion() const
{
    return uint32_t(events->getPropInt64("@version"));
}

uint32_t CPropertyTreeEvents::queryBytesRead() const
{
    return uint32_t(events->getPropInt64("@bytesRead"));
}

CPropertyTreeEvents::CPropertyTreeEvents(const IPropertyTree& _events)
    : CPropertyTreeEvents(_events, true)
{
}

CPropertyTreeEvents::CPropertyTreeEvents(const IPropertyTree& _events, bool _strictParsing)
    : events(&_events)
    , eventsIt(_events.getElements("event"))
    , strictParsing(_strictParsing)
{
    // enable the "next" event to populate from the first matching node
    (void)eventsIt->first();
}

void visitIterableEvents(IEventIterator& iter, IEventVisitor& visitor)
{
    CEvent event;
    visitor.visitFile(iter.queryFilename(), iter.queryVersion());
    while (iter.nextEvent(event))
        visitor.visitEvent(event);
    visitor.departFile(iter.queryBytesRead());
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class EventIteratorTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventIteratorTests);
    CPPUNIT_TEST(testStrictEventParsingUnknownEvent);
    CPPUNIT_TEST(testStrictEventParsingUnknownAttribute);
    CPPUNIT_TEST(testStrictEventParsingUnusedAttribute);
    CPPUNIT_TEST(testStrictEventParsingIncompleteEvent);
    CPPUNIT_TEST(testLenientEventParsing);
    CPPUNIT_TEST_SUITE_END();

public:
    void testStrictEventParsingUnknownEvent()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="Unknown"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testStrictEventParsingUnknownAttribute()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="FileInformation" unknown="foo"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testStrictEventParsingUnusedAttribute()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="FileInformation" InMemorySize="0"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testStrictEventParsingIncompleteEvent()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="FileInformation" FileId="1"/>
                </input>
                <expect>
                </expect>
            </test>
        )!!!";
        CPPUNIT_ASSERT_THROW_MESSAGE("expected exception not thrown", testEventVisitationLinks(testData), std::exception);
    }

    void testLenientEventParsing()
    {
        constexpr const char* testData = R"!!!(
            <test>
                <input>
                    <event type="unknown"/>
                    <event type="FileInformation" unknown="foo"/>
                    <event type="FileInformation" InMemorySize="0"/>
                    <event type="FileInformation" FileId="1"/>
                    <event type="FileInformation" FileId="1" Path="foo"/>
                </input>
                <expect>
                    <event type="FileInformation"/>
                    <event type="FileInformation"/>
                    <event type="FileInformation" FileId="1"/>
                    <event type="FileInformation" FileId="1" Path="foo"/>
                </expect>
            </test>
        )!!!";
        testEventVisitationLinks(testData, false);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EventIteratorTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(EventIteratorTests, "eventiterator");

#endif
