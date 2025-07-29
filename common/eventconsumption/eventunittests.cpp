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

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"
#include "eventmodeling.h"

bool CEventVisitationLinkTester::visitFile(const char* filename, uint32_t version)
{
    return true;
}

bool CEventVisitationLinkTester::visitEvent(CEvent& actualEvent)
{
    CEvent expectEvent;
    CPPUNIT_ASSERT(expect->nextEvent(expectEvent));
    CPPUNIT_ASSERT_EQUAL(expectEvent.queryType(), actualEvent.queryType());
    for (CEventAttribute& expectAttr : expectEvent.definedAttributes)
    {
        CEventAttribute& actualAttr = actualEvent.queryAttribute(expectAttr.queryId());
        CPPUNIT_ASSERT_EQUAL(expectAttr.queryState(), actualAttr.queryState());
        if (!expectAttr.isAssigned())
            continue;
        VStringBuffer msg("%s/%s", queryEventName(expectEvent.queryType()), queryEventAttributeName(expectAttr.queryId()));;
        switch (expectAttr.queryTypeClass())
        {
        case EATCtext:
            CPPUNIT_ASSERT_EQUAL_MESSAGE(msg.str(), expectAttr.queryTextValue(), actualAttr.queryTextValue());
            break;
        case EATCnumeric:
        case EATCtimestamp:
            CPPUNIT_ASSERT_EQUAL_MESSAGE(msg.str(), expectAttr.queryNumericValue(), actualAttr.queryNumericValue());
            break;
        case EATCboolean:
            CPPUNIT_ASSERT_EQUAL_MESSAGE(msg.str(), expectAttr.queryBooleanValue(), actualAttr.queryBooleanValue());
            break;
        default:
            CPPUNIT_FAIL("unhandled attribute type class");
        }
    }
    return true;
}

void CEventVisitationLinkTester::departFile(uint32_t bytesRead)
{
    CEvent noEvent;
    CPPUNIT_ASSERT(!expect->nextEvent(noEvent));
}

CEventVisitationLinkTester::CEventVisitationLinkTester(IEventIterator& _expect)
    : expect(&_expect)
{
}

IPropertyTree* createTestConfiguration(const char* testData)
{
    CPPUNIT_ASSERT_MESSAGE("invalid test data", !isEmptyString(testData));
    while (isspace(*testData))
        ++testData;
    CPPUNIT_ASSERT_MESSAGE("empty test data", *testData);
    Owned<IPropertyTree> tree;
    try
    {
        if ('<' == *testData) // looks like XML
            tree.setown(createPTreeFromXMLString(testData));
        else if ('{' == *testData || '\"' == *testData) // looks like JSON
            tree.setown(createPTreeFromJSONString(testData));
        else // assume YAML
            tree.setown(createPTreeFromYAMLString(testData));
    }
    catch (IException* e)
    {
        StringBuffer msg;
        e->errorMessage(msg);
        e->Release();
        CPPUNIT_FAIL(msg.str());
    }
    CPPUNIT_ASSERT_MESSAGE("invalid configuration", tree != nullptr);
    return tree.getClear();
}

void testEventVisitationLinks(const char* testData)
{
    START_TEST
    Owned<IPropertyTree> testTree = createTestConfiguration(testData);
    IPropertyTree* inputTree = testTree->queryBranch("input");
    CPPUNIT_ASSERT_MESSAGE("missing input section", inputTree != nullptr);
    IPropertyTree* expectTree = testTree->queryBranch("expect");
    CPPUNIT_ASSERT_MESSAGE("missing expected results section", expectTree != nullptr);
    Owned<IPropertyTreeIterator> links = testTree->getElements("link");
    CPPUNIT_ASSERT_MESSAGE("missing visitation link section", links->first());
    testEventVisitationLinks(*inputTree, *expectTree, *links);
    END_TEST
}

void testEventVisitationLinks(const IPropertyTree& inputTree, const IPropertyTree& expectTree, IPropertyTreeIterator& visitationLinks)
{
    START_TEST
    CPPUNIT_ASSERT_MESSAGE("missing visitation links", visitationLinks.first());
    Owned<IEventIterator> input = new CPropertyTreeEvents(inputTree);
    Owned<IEventIterator> expect = new CPropertyTreeEvents(expectTree);
    Owned<IEventVisitor> visitor = new CEventVisitationLinkTester(*expect);
    ForEach(visitationLinks)
    {
        IPropertyTree& linkTree = visitationLinks.query();

        // MORE: support filter configurations, as well as any other visitation links, so entire
        // visitation chain can be tested as `input -> (link)+ -> expect`.

        Owned<IEventVisitationLink> link = createEventModel(linkTree);
        CPPUNIT_ASSERT_MESSAGE("failed to create test visitation link", link != nullptr);
        link->setNextLink(*visitor);
        visitor.setown(link.getClear());
    }
    visitIterableEvents(*input, *visitor);
    END_TEST
}

#endif
