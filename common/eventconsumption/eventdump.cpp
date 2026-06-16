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

void CDumpEventsOp::setFormat(OutputFormat _format)
{
    format = _format;
}

bool CDumpEventsOp::preScanRequired() const
{
    // ServiceName metadata is derived from EventTraceId -> ServiceName mappings that
    // may be observed later across input files due to clock skew, so request a pre-scan.
    if (hasMetaFlag(metaFlags, DumpMetaFlag::ServiceName))
        return true;
    return CEventConsumingOp::preScanRequired();
}

bool CDumpEventsOp::doOp()
{
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
    case OutputFormat::tree:
        {
            Owned<IEventPTreeCreator> creator = createEventPTreeCreator(queryMetaInfoState(), metaFlags);
            if (traverseEvents(creator->queryVisitor()))
            {
                StringBuffer yaml;
                toYAML(creator->queryTree(), yaml, 2, 0);
                out->put(yaml.length(), yaml.str());
                out->put(1, "\n");
                return true;
            }
            return false;
        }
    default:
        throw makeStringExceptionV(-1, "unsupported output format: %d", (int)format);
    }
    return traverseEvents(*visitor);
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

#include "unittests.hpp"

class EventDumpTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EventDumpTests);
        CPPUNIT_TEST(testParseMetaFlags);
        CPPUNIT_TEST(testMetaFieldPresence);
    CPPUNIT_TEST_SUITE_END();

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
        collector->visitEvent(planeInfo);

        // Seed file ID -> path mapping
        CEvent fileInfo;
        fileInfo.reset(MetaFileInformation);
        fileInfo.setValue(EvAttrFileId, (unsigned)100);
        fileInfo.setValue(EvAttrPath, "/var/lib/myplane/somefile.dat");
        collector->visitEvent(fileInfo);

        // Seed trace ID -> service name mapping
        CEvent queryStart;
        queryStart.reset(EventQueryStart);
        queryStart.setValue(EvAttrEventTraceId, "trace-123");
        queryStart.setValue(EvAttrServiceName, "myservice");
        collector->visitEvent(queryStart);

        StringBuffer out;
        Owned<IBufferedSerialOutputStream> stream = createBufferedSerialOutputStream(out);
        Owned<IEventVisitor> vJSON = createDumpJSONEventVisitor(*stream, *metaState, DumpMetaFlag::All);

        // 1. Test absence (missing attributes)
        CEvent eEmpty;
        eEmpty.reset(EventQueryStart);
        vJSON->visitEvent(eEmpty);
        stream->flush();

        const char* resultEmpty = out.str();
        CPPUNIT_ASSERT(strstr(resultEmpty, "\"meta.ServiceName\"") == nullptr);
        CPPUNIT_ASSERT(strstr(resultEmpty, "\"meta.Plane\"") == nullptr);
        CPPUNIT_ASSERT(strstr(resultEmpty, "\"meta.Path\"") == nullptr);
        CPPUNIT_ASSERT(strstr(resultEmpty, "\"meta.LogicalFileName\"") == nullptr);

        out.clear();

        // 2. Test presence (attributes satisfied)
        CEvent ePopulated;
        ePopulated.reset(EventIndexCacheMiss);
        ePopulated.setValue(EvAttrEventTraceId, "trace-123");
        ePopulated.setValue(EvAttrFileId, (unsigned)100);

        vJSON->visitEvent(ePopulated);
        stream->flush();

        const char* resultPopulated = out.str();
        CPPUNIT_ASSERT(strstr(resultPopulated, "\"meta.ServiceName\"") != nullptr);
        CPPUNIT_ASSERT(strstr(resultPopulated, "\"myservice\"") != nullptr);
        CPPUNIT_ASSERT(strstr(resultPopulated, "\"meta.Plane\"") != nullptr);
        CPPUNIT_ASSERT(strstr(resultPopulated, "\"myplane\"") != nullptr);
        CPPUNIT_ASSERT(strstr(resultPopulated, "\"meta.Path\"") != nullptr);
        CPPUNIT_ASSERT(strstr(resultPopulated, "\"/var/lib/myplane/somefile.dat\"") != nullptr);
        CPPUNIT_ASSERT(strstr(resultPopulated, "\"meta.LogicalFileName\"") != nullptr);
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( EventDumpTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( EventDumpTests, "eventdump" );

#endif // _USE_CPPUNIT
