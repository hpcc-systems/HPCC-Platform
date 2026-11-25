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

#include "eventdump.hpp"
#include "eventiterator.h"
#include "eventprotrace.hpp"

CDumpEventsOp::~CDumpEventsOp()
{
    detachVisitationLinks();
}

void CDumpEventsOp::setFormat(OutputFormat _format)
{
    format = _format;
}

bool CDumpEventsOp::preScanRequired() const
{
    if (exporter && exporter->preScanRequired())
        return true;
    // ServiceName metadata is derived from EventTraceId -> ServiceName mappings that
    // may be observed later across input files due to clock skew, so request a pre-scan.
    if (hasMetaFlag(metaFlags, DumpMetaFlag::ServiceName))
        return true;
    return CEventConsumingOp::preScanRequired();
}

bool CDumpEventsOp::doOp()
{
    if (format == OutputFormat::protrace)
    {
        if (getNumSources() > 1)
            throw makeStringException(-1, "Protrace output does not support multiplexed input");
        if (hasModel())
            throw makeStringException(-1, "Protrace output does not support modeled events");
        const EventFileProperties& properties = queryIteratorProperties();
        if (properties.version == AmbiguousVersion ||
            properties.channelId == AmbiguousChannelId ||
            properties.replicaId == AmbiguousReplicaId ||
            properties.instanceId == AmbiguousInstanceId ||
            properties.options.includeTraceIds == EventFileOption::Ambiguous ||
            properties.options.includeThreadIds == EventFileOption::Ambiguous ||
            properties.options.includeStackTraces == EventFileOption::Ambiguous)
        {
            throw makeStringException(-1, "Protrace output does not support input with multiplexed source metadata");
        }
    }

    detachVisitationLinks();
    exporter.clear();
    Owned<IEventVisitor> visitor;
    switch (format)
    {
    case OutputFormat::json:
        visitor.setown(createDumpJSONEventVisitor(*out, queryMetaInfoState(), metaFlags));
        break;
    case OutputFormat::text:
        visitor.setown(createDumpTextEventVisitor(*out, queryMetaInfoState(), metaFlags));
        break;
    case OutputFormat::xml:
        visitor.setown(createDumpXMLEventVisitor(*out, queryMetaInfoState(), metaFlags));
        break;
    case OutputFormat::yaml:
        visitor.setown(createDumpYAMLEventVisitor(*out, queryMetaInfoState(), metaFlags));
        break;
    case OutputFormat::csv:
        visitor.setown(createDumpCSVEventVisitor(*out, queryIteratorProperties(), queryMetaInfoState(), metaFlags));
        break;
    case OutputFormat::protrace:
        exporter.setown(createProtraceEventExporter(*out, queryMetaInfoState()));
        break;
    default:
        throw makeStringExceptionV(-1, "unsupported output format: %d", (int)format);
    }
    if (exporter)
        return traverseEvents(exporter->queryTerminalVisitor());
    return traverseEvents(*visitor);
}

IEventVisitor* CDumpEventsOp::getPreScanVisitor()
{
    if (exporter)
        return &exporter->queryPreScanVisitor();
    return CEventConsumingOp::getPreScanVisitor();
}

IEventMultiplexer* CDumpEventsOp::createMultiplexer(CMetaInfoState& metaState, bool bypassMetaCollector)
{
    return createChronologicalEventMultiplexer(metaState, bypassMetaCollector);
}

void CDumpEventsOp::setMetaFlags(const char* value)
{
    DumpMetaFlag flags = DumpMetaFlag::None;
    if (!value || !*value)
        flags = DumpMetaFlag::All;
    else
    {
        StringArray metaFlagArray;
        metaFlagArray.appendList(value, ",");
        ForEachItemIn(i, metaFlagArray)
        {
            const char* metaFlag = metaFlagArray.item(i);
            if (strieq(metaFlag, "servicename"))
                flags = flags | DumpMetaFlag::ServiceName;
            else if (strieq(metaFlag, "logicalfilename"))
                flags = flags | DumpMetaFlag::LogicalFileName;
            else if (strieq(metaFlag, "path"))
                flags = flags | DumpMetaFlag::Path;
            else if (strieq(metaFlag, "plane"))
                flags = flags | DumpMetaFlag::Plane;
            else
            {
                StringBuffer msg;
                msg.appendf("Unknown meta flag: %s; check usage for valid flags", metaFlag);
                throw makeStringExceptionV(-1, "%s", msg.str());
            }
        }
    }
    metaFlags = flags;
}

#ifdef _USE_CPPUNIT

#include "eventunittests.hpp"

class CTestDumpEventsOp : public CDumpEventsOp
{
public:
    explicit CTestDumpEventsOp(IPropertyTree& _events) : events(&_events)
    {
    }

    CTestDumpEventsOp(IPropertyTree& _events, const EventFileProperties& _properties)
        : events(&_events), properties(_properties), overrideProperties(true)
    {
    }

protected:
    virtual Owned<IEventIterator> createInputIterator() override
    {
        Owned<IEventIterator> input = createPropertyTreeEvents(*events);
        if (!overrideProperties)
            return input;

        class CPropertiesIterator : public CInterfaceOf<IEventIterator>
        {
        public:
            CPropertiesIterator(IEventIterator& _input, const EventFileProperties& _properties)
                : input(&_input), properties(_properties)
            {
            }

            virtual bool nextEvent(CEvent& event) override
            {
                return input->nextEvent(event);
            }

            virtual const EventFileProperties& queryFileProperties() const override
            {
                return properties;
            }

        private:
            Owned<IEventIterator> input;
            EventFileProperties properties;
        };

        return new CPropertiesIterator(*input.getClear(), properties);
    }

private:
    Linked<IPropertyTree> events;
    EventFileProperties properties;
    bool overrideProperties{false};
};

class EventDumpTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventDumpTests);
        CPPUNIT_TEST(testParseMetaFlags);
        CPPUNIT_TEST(testMetaFieldPresence);
        CPPUNIT_TEST(testStructuredOutputEquivalence);
        CPPUNIT_TEST(testProtraceOutput);
        CPPUNIT_TEST(testProtraceOperation);
        CPPUNIT_TEST(testProtraceRejectsModel);
        CPPUNIT_TEST(testProtraceRejectsAmbiguousMetadata);
        CPPUNIT_TEST(testProtraceRejectsMixedSourceEvents);
        CPPUNIT_TEST(testProtraceEmptyOperation);
        CPPUNIT_TEST(testProtraceServiceNameFilter);
    CPPUNIT_TEST_SUITE_END();

    static std::vector<CEvent> createStructuredTestEvents()
    {
        std::vector<CEvent> events;
        for (unsigned type = EventNone + 1; type < EventMax; ++type)
        {
            EventType eventType = (EventType)type;
            CEvent event;
            event.reset(eventType);
            for (EventAttr attr : queryEventAttributeIds(eventType))
            {
                switch (queryEventAttributeType(attr))
                {
                case EATbool:
                    event.setValue(attr, true);
                    break;
                case EATtimestamp:
                    event.setValue(attr, (__uint64)(1000 + type));
                    break;
                case EATstring:
                case EATtraceid:
                    {
                        VStringBuffer value("%s-%u", queryEventAttributeName(attr), type);
                        event.setValue(attr, value.str());
                        break;
                    }
                default:
                    {
                        __uint64 value = 100 + type;
                        if (attr == EvAttrFileId)
                            value = 100;
                        else if (attr == EvAttrFileOffset)
                            value = 0x1000;
                        else if (attr == EvAttrNodeKind)
                            value = 2;
                        else if (attr == EvAttrSearchFlags)
                            value = 0x28;
                        else if (attr == EvAttrTask)
                            value = (unsigned)EventTask::Processing;
                        else if (attr == EvAttrEventThreadId)
                            value = 7;
                        else if (attr == EvAttrActivityId)
                            value = 42;
                        event.setValue(attr, value);
                        break;
                    }
                }
            }
            events.emplace_back(std::move(event));
        }
        return events;
    }

    static void visitStructuredEvents(IEventVisitor& visitor, const std::vector<CEvent>& source, const EventFileProperties& properties)
    {
        visitor.visitFile(properties);
        for (const CEvent& sourceEvent : source)
        {
            CEvent event = sourceEvent;
            visitor.visitEvent(event, properties);
        }
        visitor.departFile(properties);
    }

    void testStructuredOutputEquivalence()
    {
        START_TEST
        const DumpMetaFlag metaFlags = DumpMetaFlag::All;
        Owned<CMetaInfoState> metaState = new CMetaInfoState();
        Owned<IEventVisitationLink> collector = metaState->getCollector();
        std::vector<CEvent> events = createStructuredTestEvents();
        for (CEvent& event : events)
            collector->visitEvent(event, queryTestEventFileProperties());

        EventFileProperties properties = queryTestEventFileProperties();
        properties.startTimestamp = 1000;

        Owned<IEventPTreeCreator> treeCreator = createEventPTreeCreator(*metaState, metaFlags);
        visitStructuredEvents(treeCreator->queryVisitor(), events, properties);
        IPropertyTree* expected = treeCreator->queryTree();
        CPPUNIT_ASSERT(expected != nullptr);
        StringBuffer serializedTree;
        toYAML(expected, serializedTree, 2, 0);
        Owned<IPropertyTree> expectedYaml = createPTreeFromYAMLString(serializedTree.str());

        {
            StringBuffer output;
            Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(output);
            Owned<IEventVisitor> visitor = createDumpYAMLEventVisitor(*stream, *metaState, metaFlags);
            visitStructuredEvents(*visitor, events, properties);
            stream->flush();
            Owned<IPropertyTree> actual = createPTreeFromYAMLString(output.str());
            if (!areMatchingPTrees(expectedYaml, actual))
            {
                StringBuffer expectedText;
                StringBuffer actualText;
                toXML(expectedYaml, expectedText, 0, XML_Format | XML_SortTags);
                toXML(actual, actualText, 0, XML_Format | XML_SortTags);
                CPPUNIT_FAIL(VStringBuffer("yaml output differs\nExpected:\n%s\nActual:\n%s", expectedText.str(), actualText.str()));
            }
        }
        END_TEST
    }

    static void configureProtraceOutput(CTestDumpEventsOp& op, IBufferedSerialOutputStream& stream)
    {
        op.setOutput(stream);
        op.setFormat(OutputFormat::protrace);
    }

    static void configureProtraceOperation(CTestDumpEventsOp& op, IBufferedSerialOutputStream& stream)
    {
        op.setInputPath("synthetic.events");
        configureProtraceOutput(op, stream);
    }

    static void visitProtraceEvents(IProtraceEventExporter& exporter, const EventFileProperties& properties, IPropertyTree& events)
    {
        Owned<IEventIterator> input = createPropertyTreeEvents(events);
        exporter.queryPreScanVisitor().visitFile(properties);
        CEvent event;
        while (input->nextEvent(event))
            exporter.queryPreScanVisitor().visitEvent(event, properties);
        exporter.queryPreScanVisitor().departFile(properties);

        input.setown(createPropertyTreeEvents(events));
        exporter.queryTerminalVisitor().visitFile(properties);
        while (input->nextEvent(event))
            exporter.queryTerminalVisitor().visitEvent(event, properties);
    }

    void testParseMetaFlags()
    {
        // Empty string -> All
        CDumpEventsOp op;
        op.setMetaFlags("");
        CPPUNIT_ASSERT_EQUAL((unsigned)DumpMetaFlag::All, (unsigned)op.queryMetaFlags());

        // Comma-separated -> combined bitmask
        op.setMetaFlags("ServiceName,Path");
        CPPUNIT_ASSERT_EQUAL((unsigned)(DumpMetaFlag::ServiceName | DumpMetaFlag::Path), (unsigned)op.queryMetaFlags());

        // Case insensitivity
        op.setMetaFlags("plane,LOGICALFILENAME");
        CPPUNIT_ASSERT_EQUAL((unsigned)(DumpMetaFlag::Plane | DumpMetaFlag::LogicalFileName), (unsigned)op.queryMetaFlags());

        // Unknown meta flag
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(op.setMetaFlags("ServiceName,Unknown,Path"), "Unknown meta flag: Unknown");
    }

    void testMetaFieldPresence()
    {
        START_TEST
        Owned<CMetaInfoState> metaState = new CMetaInfoState();
        Owned<IEventVisitationLink> collector = metaState->getCollector();

        // Seed plane mapping
        CEvent planeInfo;
        planeInfo.reset(MetaPlaneInformation);
        planeInfo.setValue(EvAttrPlane, "myplane");
        planeInfo.setValue(EvAttrPath, "/var/lib/myplane");
        collector->visitEvent(planeInfo, queryTestEventFileProperties());

        // Seed file ID -> path mapping
        CEvent fileInfo;
        fileInfo.reset(MetaFileInformation);
        fileInfo.setValue(EvAttrFileId, (unsigned)100);
        fileInfo.setValue(EvAttrPath, "/var/lib/myplane/somefile.dat");
        collector->visitEvent(fileInfo, queryTestEventFileProperties());

        // Seed trace ID -> service name mapping
        CEvent queryStart;
        queryStart.reset(EventQueryStart);
        queryStart.setValue(EvAttrEventTraceId, "trace-123");
        queryStart.setValue(EvAttrServiceName, "myservice");
        collector->visitEvent(queryStart, queryTestEventFileProperties());

        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        Owned<IEventVisitor> vJSON = createDumpJSONEventVisitor(*stream, *metaState, DumpMetaFlag::All);

        // 1. Test absence (missing attributes)
        CEvent eEmpty;
        eEmpty.reset(EventQueryStart);
        vJSON->visitEvent(eEmpty, queryTestEventFileProperties());
        stream->flush();

        const char* resultEmpty = out.str();
        const char* metadataNames[] =
        {
            EVENT_META_SERVICE_NAME,
            EVENT_META_PLANE,
            EVENT_META_PATH,
            EVENT_META_LOGICAL_FILE_NAME
        };
        for (const char* metadataName : metadataNames)
        {
            VStringBuffer quotedMetadataName("\"%s\"", metadataName);
            CPPUNIT_ASSERT(strstr(resultEmpty, quotedMetadataName.str()) == nullptr);
        }

        out.clear();

        // 2. Test presence (attributes satisfied)
        CEvent ePopulated;
        ePopulated.reset(EventIndexCacheMiss);
        ePopulated.setValue(EvAttrEventTraceId, "trace-123");
        ePopulated.setValue(EvAttrFileId, (unsigned)100);

        vJSON->visitEvent(ePopulated, queryTestEventFileProperties());
        stream->flush();

        const char* resultPopulated = out.str();
        const char* metadataValues[] =
        {
            "myservice",
            "myplane",
            "/var/lib/myplane/somefile.dat"
        };
        for (const char* metadataValue : metadataValues)
        {
            VStringBuffer quotedMetadataValue("\"%s\"", metadataValue);
            CPPUNIT_ASSERT(strstr(resultPopulated, quotedMetadataValue.str()) != nullptr);
        }
        VStringBuffer quotedLogicalFileName("\"%s\"", EVENT_META_LOGICAL_FILE_NAME);
        CPPUNIT_ASSERT(strstr(resultPopulated, quotedLogicalFileName.str()) != nullptr);
        END_TEST
    }

    void testProtraceOutput()
    {
        START_TEST
        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        Owned<CMetaInfoState> metaState = new CMetaInfoState();
        Owned<IEventVisitationLink> metaCollector = metaState->getCollector();
        CEvent fileInfo;
        fileInfo.reset(MetaFileInformation);
        fileInfo.setValue(EvAttrFileId, (unsigned)100);
        fileInfo.setValue(EvAttrPath, "/var/lib/index.dat");
        metaCollector->visitEvent(fileInfo, queryTestEventFileProperties());
        Owned<IProtraceEventExporter> exporter = createProtraceEventExporter(*stream, *metaState);
        EventFileProperties properties = queryTestEventFileProperties();
        properties.startTimestamp = 1000;

        Owned<IPropertyTree> events = createPTreeFromXMLString(
            "<events filename='test.events'>"
                "<event type='QueryStart' EventTimestamp='900' EventThreadId='7' EventTraceId='trace-id' ServiceName='query-service'/>"
                "<event type='TaskStart' EventTimestamp='1200' EventThreadId='7' Task='6' ActivityId='42'/>"
                "<event type='TaskStart' EventTimestamp='1250' EventThreadId='7' Task='7' DataSize='5678' InMemorySize='1234'/>"
                "<event type='IndexCacheHit' EventTimestamp='1300' EventThreadId='7' FileId='100' FileOffset='4096' NodeKind='2' SearchFlags='40' InMemorySize='1' ExpandTime='0'/>"
                "<event type='IndexLoad' EventTimestamp='1500' EventThreadId='7' FileId='100' FileOffset='8192' NodeKind='1' SearchFlags='0' InMemorySize='2' ReadTime='100' ExpandTime='50'/>"
            "</events>");
        visitProtraceEvents(*exporter, properties, *events);
        stream->flush();

        CPPUNIT_ASSERT(strstr(out.str(), "  \"type\": \"manifest\",") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "  \"base_ts\": 900,") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "  \"sortedEvents\": true,") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "  \"slots_per_cpu\"") == nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "\"displayMappings\"") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "\"threads\"") == nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "  \"stringtable\": [\"query-service\",\"/var/lib/index.dat\"]") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "[0, 7, 17, 0, 1]") != nullptr);
        VStringBuffer taskStartLine("[0, 7, %u, 300, 42]", getTaskStartOp(EventTask::Processing));
        CPPUNIT_ASSERT(strstr(out.str(), taskStartLine.str()) != nullptr);
        VStringBuffer compressionStartLine("[0, 7, %u, 350, %llu]", getTaskStartOp(EventTask::Compressing), ((__uint64)5678 << 32) | 1234);
        CPPUNIT_ASSERT(strstr(out.str(), compressionStartLine.str()) != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "[0, 7, 1, 400, 100, 4138]") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "[0, 7, 3, 450, 150, 100, 8193]") != nullptr);
        END_TEST
    }

    void testProtraceOperation()
    {
        START_TEST
        Owned<IPropertyTree> events = createPTreeFromXMLString(
            "<events filename='test.events'>"
                "<event type='FileInformation' FileId='100' Path='/var/lib/index.dat'/>"
                "<event type='QueryStart' EventTimestamp='1000' EventThreadId='7' ServiceName='query-service'/>"
                "<event type='QueryStart' EventTimestamp='900' EventThreadId='7' ServiceName='query-service'/>"
            "</events>");
        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        CTestDumpEventsOp op(*events);
        configureProtraceOperation(op, *stream);
        op.acceptEvents("QueryStart");

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        CPPUNIT_ASSERT(strstr(out.str(), "\"sortedEvents\"") == nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "\"displayMappings\"") == nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "\"query-service\"") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "\"/var/lib/index.dat\"") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "[0, 7, 17, 100") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "[0, 7, 17, 0") != nullptr);

        StringBuffer multiplexedOut;
        Owned<IBufferedSerialOutputStream> multiplexedStream = createBufferedSerialOutputStream(multiplexedOut);
        CTestDumpEventsOp multiplexedOp(*events);
        multiplexedOp.setInputPath("first.events");
        multiplexedOp.setInputPath("second.events");
        configureProtraceOutput(multiplexedOp, *multiplexedStream);
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(multiplexedOp.doOp(), "expected multiplexed Protrace input to be rejected");
        END_TEST
    }

    void testProtraceRejectsModel()
    {
        START_TEST
        Owned<IPropertyTree> events = createPTreeFromXMLString(
            "<events filename='test.events'>"
                "<event type='FileInformation' FileId='100' Path='/var/lib/index.dat'/>"
                "<event type='IndexCacheHit' EventTimestamp='100' EventThreadId='7' EventTraceId='trace-1' FileId='100' FileOffset='0' NodeKind='0' SearchFlags='0' InMemorySize='1' ExpandTime='1'/>"
                "<event type='QueryStart' EventTimestamp='50' EventThreadId='7' ServiceName='query-service'/>"
            "</events>");
        Owned<IPropertyTree> model = createPTreeFromXMLString(
            "<model>"
                "<storage><plane name='a' readTime='1'/></storage>"
                "<memory/>"
            "</model>");
        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        CTestDumpEventsOp op(*events);
        configureProtraceOperation(op, *stream);
        CPPUNIT_ASSERT(op.acceptModel(*model));

        CPPUNIT_ASSERT_THROWS_IEXCEPTION(op.doOp(), "expected modeled Protrace input to be rejected");
        END_TEST
    }

    template <typename T>
    void assertProtraceRejectsAmbiguousMetadata(EventFileProperties& properties, T& value, T ambiguousValue)
    {
        T originalValue = value;
        value = ambiguousValue;
        Owned<IPropertyTree> events = createPTreeFromXMLString(
            "<events filename='test.events'>"
                "<event type='QueryStart' EventTimestamp='1000' EventThreadId='7' ServiceName='query-service'/>"
            "</events>");
        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        CTestDumpEventsOp op(*events, properties);
        configureProtraceOperation(op, *stream);

        CPPUNIT_ASSERT_THROWS_IEXCEPTION(op.doOp(), "expected ambiguous Protrace input to be rejected");
        value = originalValue;
    }

    void testProtraceRejectsAmbiguousMetadata()
    {
        START_TEST
        EventFileProperties properties = queryTestEventFileProperties();

        assertProtraceRejectsAmbiguousMetadata(properties, properties.version, AmbiguousVersion);
        assertProtraceRejectsAmbiguousMetadata(properties, properties.channelId, AmbiguousChannelId);
        assertProtraceRejectsAmbiguousMetadata(properties, properties.replicaId, AmbiguousReplicaId);
        assertProtraceRejectsAmbiguousMetadata(properties, properties.instanceId, AmbiguousInstanceId);
        assertProtraceRejectsAmbiguousMetadata(properties, properties.options.includeTraceIds, EventFileOption::Ambiguous);
        assertProtraceRejectsAmbiguousMetadata(properties, properties.options.includeThreadIds, EventFileOption::Ambiguous);
        assertProtraceRejectsAmbiguousMetadata(properties, properties.options.includeStackTraces, EventFileOption::Ambiguous);
        END_TEST
    }

    void testProtraceRejectsMixedSourceEvents()
    {
        START_TEST
        Owned<IPropertyTree> events = createPTreeFromXMLString(
            "<events filename='test.events'>"
                "<event type='QueryStart' EventTimestamp='1000' EventThreadId='7' ChannelId='1' ServiceName='query-service'/>"
                "<event type='QueryStart' EventTimestamp='1100' EventThreadId='7' ChannelId='2' ServiceName='query-service'/>"
            "</events>");
        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        CTestDumpEventsOp op(*events);
        configureProtraceOperation(op, *stream);

        CPPUNIT_ASSERT_THROWS_IEXCEPTION(op.doOp(), "expected mixed Protrace source events to be rejected");
        END_TEST
    }

    void testProtraceEmptyOperation()
    {
        START_TEST
        Owned<IPropertyTree> events = createPTreeFromXMLString("<events filename='empty.events'/>");
        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        CTestDumpEventsOp op(*events);
        configureProtraceOperation(op, *stream);

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        CPPUNIT_ASSERT(strstr(out.str(), "\"type\": \"manifest\"") != nullptr);
        END_TEST
    }

    void testProtraceServiceNameFilter()
    {
        START_TEST
        Owned<IPropertyTree> events = createPTreeFromXMLString(
            "<events filename='test.events'>"
                "<event type='IndexCacheMiss' EventTimestamp='900' EventThreadId='7' EventTraceId='trace-1' FileId='100' FileOffset='0' NodeKind='2' SearchFlags='0'/>"
                "<event type='QueryStart' EventTimestamp='1000' EventThreadId='7' EventTraceId='trace-1' ServiceName='query-service'/>"
            "</events>");
        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        CTestDumpEventsOp op(*events);
        configureProtraceOperation(op, *stream);
        CPPUNIT_ASSERT(op.acceptMetaAttribute(EVENT_META_SERVICE_NAME, "query-service"));

        CPPUNIT_ASSERT(op.doOp());
        stream->flush();

        CPPUNIT_ASSERT(strstr(out.str(), "[0, 7, 2, 0, 100, 2]") != nullptr);
        CPPUNIT_ASSERT(strstr(out.str(), "[0, 7, 17, 100, 1]") != nullptr);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( EventDumpTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( EventDumpTests, "eventdump" );

#endif // _USE_CPPUNIT
