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
#include "eventfilter.h"
#include "eventmodeling.h"
#include "eventoperation.h"

static const char* queryEventAttributeStateName(CEventAttribute::State state)
{
    switch (state)
    {
    case CEventAttribute::Unused: return "unused";
    case CEventAttribute::Defined: return "defined";
    case CEventAttribute::Assigned: return "assigned";
    default: return "unknown";
    }
}

bool CEventVisitationLinkTester::visitFile(const char* filename, uint32_t version)
{
    return true;
}

bool CEventVisitationLinkTester::visitEvent(CEvent& actualEvent)
{
    CEvent expectEvent;
    CPPUNIT_ASSERT(expect->nextEvent(expectEvent));
    if (expectEvent.queryType() != actualEvent.queryType())
        CPPUNIT_ASSERT_EQUAL_MESSAGE("EventType mismatch", queryEventName(expectEvent.queryType()), queryEventName(actualEvent.queryType()));
    for (CEventAttribute& expectAttr : expectEvent.definedAttributes)
    {
        CEventAttribute& actualAttr = actualEvent.queryAttribute(expectAttr.queryId());
        VStringBuffer msg("%s/%s", queryEventName(expectEvent.queryType()), queryEventAttributeName(expectAttr.queryId()));
        if (expectAttr.queryState() != actualAttr.queryState())
            CPPUNIT_ASSERT_EQUAL_MESSAGE(VStringBuffer("%s state mismatch", msg.str()).str(), queryEventAttributeStateName(expectAttr.queryState()), queryEventAttributeStateName(actualAttr.queryState()));
        if (!expectAttr.isAssigned())
            continue;
        switch (expectAttr.queryTypeClass())
        {
        case EATCtext:
            CPPUNIT_ASSERT_EQUAL_MESSAGE(msg.str(), std::string(expectAttr.queryTextValue()), std::string(actualAttr.queryTextValue()));
            break;
        case EATCnumeric:
            CPPUNIT_ASSERT_EQUAL_MESSAGE(msg.str(), expectAttr.queryNumericValue(), actualAttr.queryNumericValue());
            break;
        case EATCtimestamp:
            if (expectAttr.queryNumericValue() != actualAttr.queryNumericValue())
                CPPUNIT_ASSERT_EQUAL_MESSAGE(msg.str(), std::string(expectAttr.queryTextValue()), std::string(actualAttr.queryTextValue()));
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
    if (expect->nextEvent(noEvent))
    {
        VStringBuffer tmp("unexpected next event %s", queryEventName(noEvent.queryType()));
        for (CEventAttribute& attr : noEvent.assignedAttributes)
        {
            tmp.appendf(" %s:", queryEventAttributeName(attr.queryId()));
            switch (attr.queryTypeClass())
            {
            case EATCtext:
                tmp.appendf("'%s'", attr.queryTextValue());
                break;
            case EATCnumeric:
                tmp.appendf("%" I64F "d", attr.queryNumericValue());
                break;
            case EATCboolean:
                tmp.appendf("%s", attr.queryBooleanValue() ? "true" : "false");
                break;
            case EATCtimestamp:
                tmp.appendf("'%s'", attr.queryTextValue());
                break;
            default:
                tmp.appendf("<unknown type>");
                break;
            }
        }
        CPPUNIT_FAIL(tmp.str());
    }
    // CPPUNIT_ASSERT(!expect->nextEvent(noEvent));
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

void testEventVisitationLinks(const char* testData, unsigned flags)
{
    START_TEST
    Owned<IPropertyTree> testTree = createTestConfiguration(testData);
    IPropertyTree* inputTree = testTree->queryBranch("input");
    CPPUNIT_ASSERT_MESSAGE("missing input section", inputTree != nullptr);
    IPropertyTree* expectTree = testTree->queryBranch("expect");
    CPPUNIT_ASSERT_MESSAGE("missing expected results section", expectTree != nullptr);
    Owned<IPropertyTreeIterator> links = testTree->getElements("link");
    testEventVisitationLinks(*inputTree, *expectTree, *links, flags);
    END_TEST
}

void testEventVisitationLinks(const IPropertyTree& inputTree, const IPropertyTree& expectTree, IPropertyTreeIterator& visitationLinks, unsigned flags)
{
    class TestOp : public CEventConsumingOp
    {
    public: // CEventConsumingOp
        virtual bool doOp() override
        {
            Owned<IEventIterator> input;
            Owned<IPropertyTreeIterator> sourcesIter = inputTree.getElements("source");
            std::vector<Owned<IEventIterator>> sources;
            ForEach(*sourcesIter)
                sources.emplace_back(createPropertyTreeEvents(sourcesIter->query(), flags));
            switch (sources.size())
            {
            case 0: // implied single source using inputTree
                input.setown(createPropertyTreeEvents(inputTree, flags));
                break;
            case 1:
                input.setown(sources[0].getClear());
                break;
            default:
                {
                    Owned<IEventMultiplexer> multiplexer = createEventMultiplexer(*metaState);
                    for (auto& s : sources)
                        multiplexer->addSource(*s.getClear());
                    input.setown(multiplexer.getClear());
                }
                break;
            }
            Owned<IEventIterator> expect = createPropertyTreeEvents(expectTree, flags);
            Owned<IEventVisitor> visitor = new CEventVisitationLinkTester(*expect);
            Owned<IEventVisitor> currentVisitor = LINK(visitor);

            // Build the visitation chain from the links
            ForEach(visitationLinks)
            {
                IPropertyTree& linkTree = visitationLinks.query();
                const char* kind = linkTree.queryProp("@kind");
                if (isEmptyString(kind))
                    return false; // missing link kind

                Owned<IEventVisitationLink> link;
                if (strieq(kind, "event-filter"))
                    link.setown(createEventFilter(linkTree, *metaState));
                else
                    link.setown(createEventModel(linkTree, *metaState));

                if (!link)
                    return false; // failed to create link

                link->setNextLink(*currentVisitor);
                currentVisitor.setown(link.getClear());
            }

            // Always include the meta information parser as the first link in the chain
            Owned<IEventVisitationLink> metaCollector = metaState->getCollector();
            metaCollector->setNextLink(*currentVisitor);
            visitIterableEvents(*input, *metaCollector);
            return true;
        }

    public:
        TestOp(const IPropertyTree& _inputTree, const IPropertyTree& _expectTree, IPropertyTreeIterator& _visitationLinks, unsigned _flags)
            : inputTree(_inputTree)
            , expectTree(_expectTree)
            , visitationLinks(_visitationLinks)
            , flags(_flags)
        {
        }

    protected:
        const IPropertyTree& inputTree;
        const IPropertyTree& expectTree;
        IPropertyTreeIterator& visitationLinks;
        unsigned flags;
    };

    START_TEST
    TestOp op(inputTree, expectTree, visitationLinks, flags);
    CPPUNIT_ASSERT_MESSAGE("failed to process visitation links", op.doOp());
    END_TEST
}

#endif
