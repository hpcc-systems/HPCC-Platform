/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

/*
 * Jlib regression tests
 *
 */

#ifdef _USE_CPPUNIT
#include <memory>
#include <chrono>
#include <algorithm>
#include "jsem.hpp"
#include "jfile.hpp"
#include "jdebug.hpp"
#include "jset.hpp"
#include "rmtfile.hpp"
#include "jlzw.hpp"
#include "jqueue.hpp"
#include "jregexp.hpp"
#include "jsecrets.hpp"
#include "jutil.hpp"
#include "junicode.hpp"

#include "opentelemetry/sdk/common/attribute_utils.h"
#include "opentelemetry/sdk/resource/resource.h"

#include "unittests.hpp"

#define CPPUNIT_ASSERT_EQUAL_STR(x, y) CPPUNIT_ASSERT_EQUAL(std::string(x),std::string(y))

static const unsigned oneMinute = 60000; // msec

class JTraceThreader : public Thread
{
public:
    JTraceThreader(const char * name, ISpan * _parentSpan, bool _declareSubSpan) : parentSpan(_parentSpan), declareSubSpan(_declareSubSpan), Thread(name) {}

    virtual int run()
    {
        DBGLOG("JTraceThreader: '%s' running...", this->getName());
        if (declareSubSpan)
        {
            VStringBuffer subspanName("%s_subspan", this->getName());

            Owned<ISpan> subSpan = parentSpan->createInternalSpan(subspanName.str());
            DBGLOG("JTraceThreader: '%s' running...", subspanName.str());
            DBGLOG("JTraceThreader: '%s' ending...", subspanName.str());
        }
        DBGLOG("JTraceThreader: '%s' ending...", this->getName());
        
        return 0;
    }

private:
    ISpan * parentSpan = nullptr;
    bool declareSubSpan = false;
};

class JlibTraceTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibTraceTest);
        //Invalid since tracemanager initialized at component load time 
        //CPPUNIT_TEST(testTraceDisableConfig);
        CPPUNIT_TEST(testStringArrayPropegatedServerSpan);
        CPPUNIT_TEST(testDisabledTracePropegatedValues);
        CPPUNIT_TEST(testIDPropegation);
        CPPUNIT_TEST(testTraceConfig);
        CPPUNIT_TEST(testRootServerSpan);
        CPPUNIT_TEST(testPropegatedServerSpan);
        CPPUNIT_TEST(testInvalidPropegatedServerSpan);
        CPPUNIT_TEST(testInternalSpan);
        CPPUNIT_TEST(manualTestTraceInfoLogging);
        CPPUNIT_TEST(testMultiNestedSpanTraceOutput);
        CPPUNIT_TEST(testNullSpan);
        CPPUNIT_TEST(testClientSpanGlobalID);
        CPPUNIT_TEST(testEnsureTraceID);

        //CPPUNIT_TEST(testJTraceJLOGExporterprintResources);
        //CPPUNIT_TEST(testJTraceJLOGExporterprintAttributes);
        CPPUNIT_TEST(manualTestsDeclaredSpanStartTime);
    CPPUNIT_TEST_SUITE_END();

    const char * simulatedGlobalYaml = R"!!(global:
    tracing:
        disable: false
        exporterx:
          type: OTLP
          endpoint: "localhost:4317"
          useSslCredentials: true
          sslCredentialsCACcert: "ssl-certificate"
        processor:
          type: batch
          typex: simple
    )!!";

    const char * disableTracingYaml = R"!!(global:
    tracing:
        disable: true
    )!!";

    //Mock http headers from request
    void createMockHTTPHeaders(IProperties * mockHTTPHeaders, bool validOtel)
    {
        if (validOtel)
        {
            //Trace parent declared in http headers
            mockHTTPHeaders->setProp("traceparent", "00-beca49ca8f3138a2842e5cf21402bfff-4b960b3e4647da3f-01");
            mockHTTPHeaders->setProp("tracestate", "hpcc=4b960b3e4647da3f");
        }
        else
        {
            //valid otel traceparent header name, invalid value
            mockHTTPHeaders->setProp("traceparent", "00-XYZe5cf21402bfff-4b960b-f1");
            //invalid otel tracestate header name
            mockHTTPHeaders->setProp("state", "hpcc=4b960b3e4647da3f");
        }

        //HPCC specific headers to be propagated
        mockHTTPHeaders->setProp(kGlobalIdHttpHeaderName, "IncomingUGID");
        mockHTTPHeaders->setProp(kCallerIdHttpHeaderName, "IncomingCID");
    }

protected:

    /*void testJTraceJLOGExporterprintAttributes()
    {
        StringBuffer out;
        testJLogExporterPrintAttributes(out, {}, "attributes");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected non-empty printattributes", true, out.length() == 0);


        testJLogExporterPrintAttributes(out, {{"url", "https://localhost"}, {"content-length", 562}, {"content-type", "html/text"}}, "attributes");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty printattributes", false, out.length() == 0);

        Owned<IPropertyTree> jtraceAsTree;
        try
        {
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected leading non-comma char in printattributes", true, out.charAt(0) == ',');

            out.setCharAt(0, '{');
            out.append("}");

            jtraceAsTree.setown(createPTreeFromJSONString(out.str()));
        }
        catch (IException *e)
        {
            StringBuffer msg;
            msg.append("Unexpected printAttributes format failure detected: ");
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_ASSERT_MESSAGE(msg.str(), false);
        }

        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected printresources format failure detected", true, jtraceAsTree != nullptr);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("attributes"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("attributes/url"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("attributes/content-length"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("attributes/content-type"));
    }*/

    /*void testJTraceJLOGExporterprintResources()
    {
        StringBuffer out;
        auto dummyAttributes = opentelemetry::sdk::resource::ResourceAttributes
        {
            {"service.name", "shoppingcart"},
            {"service.instance.id", "instance-12"}
        };
        auto dummyResources = opentelemetry::sdk::resource::Resource::Create(dummyAttributes);

        testJLogExporterPrintResources(out, dummyResources);

        Owned<IPropertyTree> jtraceAsTree;
        try
        {
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty printresources return", false, out.length() == 0);

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected leading non-comma char in printresources return", true, out.charAt(0) == ',');

            out.setCharAt(0, '{');
            out.append("}");

            jtraceAsTree.setown(createPTreeFromJSONString(out.str()));
        }
        catch (IException *e)
        {
            StringBuffer msg;
            msg.append("Unexpected printresources format failure detected: ");
            e->errorMessage(msg);
            e->Release();
            CPPUNIT_ASSERT_MESSAGE(msg.str(), false);
        }

        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected printresources format failure detected", true, jtraceAsTree != nullptr);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("resources"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("resources/service.name"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("resources/service.instance.id"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("resources/telemetry.sdk.language"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("resources/telemetry.sdk.version"));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Missing resource attribute detected", true, jtraceAsTree->hasProp("resources/telemetry.sdk.name"));
    }*/

    //not able to programmatically test yet, but can visually inspect trace output
    void manualTestsDeclaredSpanStartTime()
    {
        Owned<IProperties> emptyMockHTTPHeaders = createProperties();
        SpanTimeStamp declaredSpanStartTime;
        declaredSpanStartTime.now(); // must be initialized via now(), or setMSTickTime
        MilliSleep(125);

        {
            //duration should be at least 125 milliseconds
            Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("declaredSpanStartTime", emptyMockHTTPHeaders, &declaredSpanStartTime);
            //{ "type": "span", "name": "declaredSpanStartTime", "trace_id": "0a2eff24e1996540056745aaeb2f5824", "span_id": "46d0faf8b4da893e",
            //"start": 1702672311203213259, "duration": 125311051 }
        }

        auto reqStartMSTick = msTick();
        // a good test would track chrono::system_clock::now() at the point of span creation
        // ensure a measurable sleep time between reqStartMSTick and msTickOffsetTimeStamp
        // then compare OTel reported span start timestamp to span creation-time timestamp
        SpanTimeStamp msTickOffsetTimeStamp;
        msTickOffsetTimeStamp.setMSTickTime(reqStartMSTick);
        //sleep for 50 milliseconds after span creation and mstick offset, expect at least 50 milliseconds duration output
        MilliSleep(50);

        {
            SpanTimeStamp nowTimeStamp; //not used, printed out as "start" time for manual comparison
            nowTimeStamp.now();
            {
                Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("msTickOffsetStartTime", emptyMockHTTPHeaders, &msTickOffsetTimeStamp);
            }

            DBGLOG("MsTickOffset span actual start-time timestamp: %lld", (long long)(nowTimeStamp.systemClockTime).count());
            //14:49:13.776139   904 MsTickOffset span actual start-time timestamp: 1702669753775893057
            //14:49:13.776082   904 { "type": "span", "name": "msTickOffsetStartTime", "trace_id": "6e89dd6082ff647daed523089f032240", "span_id": "fd359b41a0a9626d", 
            //"start": 1702669753725771035, "duration": 50285323 }
            //Actual start - declared start: 1702669753775893057-1702669753725771035 = 50162022
        }

        //uninitialized SpanTimeStamp will be ignored, and current time will be used
        SpanTimeStamp uninitializedTS;
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected initialized spanTimeStamp", false, uninitializedTS.isInitialized());
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected initialized spanTimeStamp", true, uninitializedTS.systemClockTime == std::chrono::nanoseconds::zero());
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected initialized spanTimeStamp", true, uninitializedTS.steadyClockTime == std::chrono::nanoseconds::zero());
        {
            Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("uninitializeddeclaredSpanStartTime", emptyMockHTTPHeaders, &uninitializedTS);
            //sleep for 75 milliseconds after span creation, expect at least 75 milliseconds duration output
            MilliSleep(75);

            //14:22:37.865509 30396 { "type": "span", "name": "uninitializeddeclaredSpanStartTime", "trace_id": "f7844c5c09b413e008f912ded0e12dec", "span_id": "7fcf9042a090c663", 
            //"start": 1702668157790080022,
            //"duration": 75316248 }
        }
    }

    void testTraceDisableConfig()
    {
        Owned<IPropertyTree> testTree = createPTreeFromYAMLString(disableTracingYaml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> traceConfig = testTree->getPropTree("global");

        //Not valid, due to tracemanager initialized at component load time
        initTraceManager("somecomponent", traceConfig, nullptr);
    }

    void testEnsureTraceID()
    {
        SpanFlags flags = SpanFlags::EnsureTraceId;
        Owned<IProperties> emptyMockHTTPHeaders = createProperties();
        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("noRemoteParentEnsureTraceID", emptyMockHTTPHeaders, flags);

        Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
        serverSpan->getSpanContext(retrievedSpanCtxAttributes.get());

        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty TraceID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("traceID")));
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty SpanID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("spanID")));
    }

    void testIDPropegation()
    {
        Owned<IProperties> mockHTTPHeaders = createProperties();
        createMockHTTPHeaders(mockHTTPHeaders, true);

        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", mockHTTPHeaders);
        //at this point the serverSpan should have the following context attributes
        //traceID, spanID, remoteParentSpanID, traceFlags, traceState, globalID, callerID

        //retrieve serverSpan context with the intent to interrogate attributes
        {
            Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
            serverSpan->getSpanContext(retrievedSpanCtxAttributes.get());

            CPPUNIT_ASSERT_MESSAGE("Unexpected GlobalID detected",
             strsame("IncomingUGID", retrievedSpanCtxAttributes->queryProp(kGlobalIdHttpHeaderName)));
            CPPUNIT_ASSERT_MESSAGE("Unexpected CallerID detected",
             strsame("IncomingCID", retrievedSpanCtxAttributes->queryProp(kCallerIdHttpHeaderName)));
            CPPUNIT_ASSERT_MESSAGE("Unexpected Declared Parent SpanID detected",
             strsame("4b960b3e4647da3f", retrievedSpanCtxAttributes->queryProp("remoteParentSpanID")));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty TraceID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("traceID")));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty SpanID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("spanID")));
        }

        //retrieve serverSpan context with the intent to propagate it to a remote child span
        {
            Owned<IProperties> retrievedClientHeaders = createProperties();
            serverSpan->getClientHeaders(retrievedClientHeaders.get());

            CPPUNIT_ASSERT_EQUAL_MESSAGE("getClientHeaders failed to produce traceParent!", true, retrievedClientHeaders->hasProp("traceparent"));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected Otel traceparent header len detected", (size_t)55,
             strlen(retrievedClientHeaders->queryProp("traceparent")));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("getClientHeaders failed to produce traceState!", true, retrievedClientHeaders->hasProp("tracestate"));
            const char * tracestate = retrievedClientHeaders->queryProp("tracestate");
            CPPUNIT_ASSERT_MESSAGE("Unexpected traceState detected",
             strsame("hpcc=4b960b3e4647da3f", retrievedClientHeaders->queryProp("tracestate")));
        }
    }

    void testTraceConfig()
    {
        Owned<IPropertyTree> testTree = createPTreeFromYAMLString(simulatedGlobalYaml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> traceConfig = testTree->getPropTree("global");

         initTraceManager("somecomponent", traceConfig, nullptr);
    }

    void testNullSpan()
    {
        if (!queryTraceManager().isTracingEnabled())
        {
            DBGLOG("Skipping testNullSpan, tracing is not enabled");
            return;
        }

        Owned<ISpan> nullSpan = getNullSpan();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected nullptr nullspan detected", true, nullSpan != nullptr);

        {
            Owned<IProperties> headers = createProperties(true);
            nullSpan->getClientHeaders(headers);
        }

        Owned<ISpan> nullSpanChild = nullSpan->createClientSpan("nullSpanChild");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected nullptr nullSpanChild detected", true, nullSpanChild != nullptr);
    }

    void testClientSpan()
    {
        Owned<IProperties> emptyMockHTTPHeaders = createProperties();
        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", emptyMockHTTPHeaders);

        Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
        serverSpan->getSpanContext(retrievedSpanCtxAttributes);

        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty spanID detected", true, retrievedSpanCtxAttributes->hasProp("spanID"));
        const char * serverSpanID = retrievedSpanCtxAttributes->queryProp("spanID");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty traceID detected", true, retrievedSpanCtxAttributes->hasProp("traceID"));
        const char * serverTraceID = retrievedSpanCtxAttributes->queryProp("traceID");

        {
            Owned<ISpan> internalSpan = serverSpan->createClientSpan("clientSpan");

            //retrieve clientSpan context with the intent to propogate otel and HPCC context
            {
                Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
                internalSpan->getSpanContext(retrievedSpanCtxAttributes);

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing localParentSpanID detected", true,
                 retrievedSpanCtxAttributes->hasProp("localParentSpanID"));

                CPPUNIT_ASSERT_MESSAGE("Mismatched localParentSpanID detected",
                 strsame(serverSpanID, retrievedSpanCtxAttributes->queryProp("localParentSpanID")));

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing remoteParentID detected", true,
                 retrievedSpanCtxAttributes->hasProp("remoteParentID"));

                CPPUNIT_ASSERT_MESSAGE("Unexpected CallerID detected",
                 strsame(serverTraceID, retrievedSpanCtxAttributes->queryProp("remoteParentID")));

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected GlobalID detected", false,
                retrievedSpanCtxAttributes->hasProp(kGlobalIdHttpHeaderName));
                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected CallerID detected", false,
                retrievedSpanCtxAttributes->hasProp(kCallerIdHttpHeaderName));

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected Declared Parent SpanID detected", false,
                retrievedSpanCtxAttributes->hasProp("remoteParentSpanID"));

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty TraceID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("traceID")));
                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty SpanID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("spanID")));
            }
        }
    }

    void testInternalSpan()
    {
        Owned<IProperties> emptyMockHTTPHeaders = createProperties();
        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", emptyMockHTTPHeaders);

        Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
        serverSpan->getSpanContext(retrievedSpanCtxAttributes);

        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty spanID detected", true, retrievedSpanCtxAttributes->hasProp("spanID"));
        const char * serverSpanID = retrievedSpanCtxAttributes->queryProp("spanID");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty traceID detected", true, retrievedSpanCtxAttributes->hasProp("traceID"));
        const char * serverTraceID = retrievedSpanCtxAttributes->queryProp("traceID");

        {
            Owned<ISpan> internalSpan = serverSpan->createInternalSpan("internalSpan");

            //retrieve internalSpan context with the intent to interrogate attributes
            {
                Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
                internalSpan->getSpanContext(retrievedSpanCtxAttributes);

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing localParentSpanID detected", true,
                 retrievedSpanCtxAttributes->hasProp("localParentSpanID"));

                CPPUNIT_ASSERT_MESSAGE("Mismatched localParentSpanID detected",
                 strsame(serverSpanID, retrievedSpanCtxAttributes->queryProp("localParentSpanID")));

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected remoteParentSpanID detected", false,
                 retrievedSpanCtxAttributes->hasProp("remoteParentSpanID"));

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected GlobalID detected", false,
                 retrievedSpanCtxAttributes->hasProp(kGlobalIdHttpHeaderName));
                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected CallerID detected", false,
                 retrievedSpanCtxAttributes->hasProp(kCallerIdHttpHeaderName));


                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected Declared Parent SpanID detected", false,
                 retrievedSpanCtxAttributes->hasProp("remoteParentSpanID"));

                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty TraceID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("traceID")));
                CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty SpanID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("spanID")));
            }
        }
    }

    void testRootServerSpan()
    {
        Owned<IProperties> emptyMockHTTPHeaders = createProperties();
        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", emptyMockHTTPHeaders);

        //retrieve serverSpan context with the intent to propagate it to a remote child span
        {
            Owned<IProperties> retrievedClientHeaders = createProperties();
            serverSpan->getClientHeaders(retrievedClientHeaders);

            CPPUNIT_ASSERT_EQUAL_MESSAGE("getClientHeaders failed to produce traceParent!", true, retrievedClientHeaders->hasProp("traceparent"));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected Otel traceparent header len detected", (size_t)55,
             strlen(retrievedClientHeaders->queryProp("traceparent")));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("detected unexpected tracestate from getClientHeaders", false, retrievedClientHeaders->hasProp("tracestate"));
        }

        //retrieve serverSpan context with the intent to interrogate attributes
        {
            Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
            serverSpan->getSpanContext(retrievedSpanCtxAttributes);
            //at this point the serverSpan should have the following context attributes
            //traceID, spanID //but no remoteParentSpanID, globalID, callerID

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected GlobalID detected", false,
             retrievedSpanCtxAttributes->hasProp(kGlobalIdHttpHeaderName));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected CallerID detected", false,
             retrievedSpanCtxAttributes->hasProp(kCallerIdHttpHeaderName));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected Declared Parent SpanID detected", false,
             retrievedSpanCtxAttributes->hasProp("remoteParentSpanID"));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty TraceID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("traceID")));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty SpanID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("spanID")));
        }
    }

    void testInvalidPropegatedServerSpan()
    {
        Owned<IProperties> mockHTTPHeaders = createProperties();
        createMockHTTPHeaders(mockHTTPHeaders, false);
        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("invalidPropegatedServerSpan", mockHTTPHeaders);

        Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
        serverSpan->getClientHeaders(retrievedSpanCtxAttributes.get());

        const char * traceParent = retrievedSpanCtxAttributes->queryProp("remoteParentSpanID");
        DBGLOG("testInvalidPropegatedServerSpan: traceparent: %s", traceParent);
    }

    void testDisabledTracePropegatedValues()
    {
        //only interested in propegated values, no local trace/span
        //usefull if tracemanager.istraceenabled() is false
        bool isTraceEnabled = queryTraceManager().isTracingEnabled();

        if (isTraceEnabled)
            return;

        Owned<IProperties> mockHTTPHeaders = createProperties();
        createMockHTTPHeaders(mockHTTPHeaders, true);

        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", mockHTTPHeaders);
        //at this point the serverSpan should have the following context attributes
        //remoteParentSpanID, globalID, callerID

        Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
        serverSpan->getSpanContext(retrievedSpanCtxAttributes.get());

        CPPUNIT_ASSERT_MESSAGE("Unexpected GlobalID detected",
            strsame("IncomingUGID", retrievedSpanCtxAttributes->queryProp(kGlobalIdHttpHeaderName)));
        CPPUNIT_ASSERT_MESSAGE("Unexpected CallerID detected",
            strsame("IncomingCID", retrievedSpanCtxAttributes->queryProp(kCallerIdHttpHeaderName)));

        CPPUNIT_ASSERT_MESSAGE("Unexpected Declared Parent SpanID detected",
            strsame("4b960b3e4647da3f", retrievedSpanCtxAttributes->queryProp("remoteParentSpanID")));
    }

    void manualTestTraceInfoLogging()
    {
        queryStderrLogMsgHandler()->setMessageFields(MSGFIELD_time|MSGFIELD_microTime|MSGFIELD_milliTime| MSGFIELD_trace | MSGFIELD_span);

        Owned<IProperties> mockHTTPHeaders = createProperties();
        createMockHTTPHeaders(mockHTTPHeaders, true);

        {
            Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("level1Span", mockHTTPHeaders);
            DBGLOG("Level 1 span active!: '%s'", "level1Span");
            //Log output should include traceID and spanID columns
            {
                Owned<ISpan> clientSpan = serverSpan->createClientSpan("level2Span");
                DBGLOG("Level 2 span active!: '%s'", "level2Span");
                //Log output should include same traceID as level1, new spanid
                {
                    Owned<ISpan> internalSpan = clientSpan->createInternalSpan("level3Span");
                    DBGLOG("Level 3 span active!: '%s'", "level3Span");
                    //Log output should include same traceID as level1, new spanid
                    {
                        Owned<ISpan> internalSpan2 = internalSpan->createInternalSpan("level4Span");
                        DBGLOG("Level 4 span active!: '%s'", "level4Span");
                        //Log output should include same traceID as level1, new spanid
                        LOG(MCdebugProgress, unknownJob, "Level 4 log event");
                        //Log output should include same traceID as level1, same spanid as level4span
                    }
                    DBGLOG("Level 4 span complete!: '%s'", "level4Span");
                    //Log output should include same traceID as level1, level3span spanid

                    JTraceThreader * traceThread = new JTraceThreader("level3SpanSubThread", internalSpan.get(), false);
                    traceThread->start();
                    traceThread->join();
                    //Thread output should include same traceID as level1, level3span spanid
                }
                DBGLOG("Level 3 span complete!: '%s'", "level3Span");
                //Log output should include same traceID as level1, level2span spanid
                JTraceThreader * traceThread = new JTraceThreader("level3SpanSubThread", clientSpan.get(), true);
                traceThread->start();
                traceThread->join();
                //Thread output should include same traceID as level1, new spanid
            }
            DBGLOG("Level 2 span complete!: '%s'", "level2Span");
            //Log output should include same traceID and spanID as level1
        }
        DBGLOG("Level 1 span complete!: '%s'", "level1Span");
        //Log output should include report UNK traceID and spanID
        {
            Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("level1Span", mockHTTPHeaders);
            DBGLOG("New Level 1 span active!: '%s'", "level1Span");
            //Log output should include traceID and spanID columns with new span value
        }
    }

    void testMultiNestedSpanTraceOutput()
    {
        Owned<IProperties> mockHTTPHeaders = createProperties();
        createMockHTTPHeaders(mockHTTPHeaders, true);
        StringBuffer out;
        {
            Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", mockHTTPHeaders);
            DBGLOG("Top level span active!: '%s'", "propegatedServerSpan");
            {
                Owned<ISpan> clientSpan = serverSpan->createClientSpan("clientSpan");
                DBGLOG("2nd level span active!: '%s'", "clientSpan");
                {
                    Owned<ISpan> internalSpan = clientSpan->createInternalSpan("internalSpan");
                    DBGLOG("3rd level span active!: '%s'", "internalSpan");
                    {
                        Owned<ISpan> internalSpan2 = internalSpan->createInternalSpan("internalSpan2");
                        DBGLOG("4th level span active!: '%s'", "internalSpan2");

                        out.set("{");
                        internalSpan2->toString(out);
                        out.append("}");
                    }

                    DBGLOG("4th level span complete!: '%s'", "internalSpan2");

                    {
                        Owned<IPropertyTree> jtraceAsTree;
                        try
                        {
                            jtraceAsTree.setown(createPTreeFromJSONString(out.str()));
                        }
                        catch (IException *e)
                        {
                            StringBuffer msg;
                            msg.append("Unexpected toString format failure detected: ");
                            e->errorMessage(msg);
                            e->Release();
                            CPPUNIT_ASSERT_MESSAGE(msg.str(), false);
                        }

                        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected toString format failure detected", true, jtraceAsTree != nullptr);
                        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing 'TraceID' entry in toString output", true, jtraceAsTree->hasProp("TraceID"));
                        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing 'SpanID' entry in toString output", true, jtraceAsTree->hasProp("SpanID"));
                        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing 'Name' entry in toString output", true, jtraceAsTree->hasProp("Name"));
                        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing 'Type' entry in toString output", true, jtraceAsTree->hasProp("Type"));
                        CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected missing 'ParentSpan/SpanID' entry in toString output", true, jtraceAsTree->hasProp("ParentSpan/SpanID"));
                    }
                }
                DBGLOG("3rd level span complete!: '%s'", "internalSpan");
            }
            DBGLOG("2nd level span complete!: '%s'", "clientSpan");
        }
        DBGLOG("Top level span complete!: '%s'", "propegatedServerSpan");
    }

    void testClientSpanGlobalID()
    {
        Owned<IProperties> mockHTTPHeaders = createProperties();
        createMockHTTPHeaders(mockHTTPHeaders, true); //includes global ID

        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", mockHTTPHeaders);
        Owned<ISpan> clientSpan = serverSpan->createClientSpan("clientSpanWithGlobalID");

        //retrieve serverSpan context with the intent to interrogate attributes
        {
            Owned<IProperties> retrievedClientSpanCtxAttributes = createProperties();
            clientSpan->getSpanContext(retrievedClientSpanCtxAttributes.get());

            CPPUNIT_ASSERT_MESSAGE("Unexpected GlobalID detected",
             strsame("IncomingUGID", retrievedClientSpanCtxAttributes->queryProp(kGlobalIdHttpHeaderName)));
            CPPUNIT_ASSERT_MESSAGE("Unexpected CallerID detected",
             strsame("IncomingCID", retrievedClientSpanCtxAttributes->queryProp(kCallerIdHttpHeaderName)));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty TraceID detected", false, isEmptyString(retrievedClientSpanCtxAttributes->queryProp("traceID")));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty SpanID detected", false, isEmptyString(retrievedClientSpanCtxAttributes->queryProp("spanID")));
        }
    }

    void testPropegatedServerSpan()
    {
        Owned<IProperties> mockHTTPHeaders = createProperties();
        createMockHTTPHeaders(mockHTTPHeaders, true);

        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("propegatedServerSpan", mockHTTPHeaders);
        //at this point the serverSpan should have the following context attributes
        //traceID, spanID, remoteParentSpanID, traceFlags, traceState, globalID, callerID

        //retrieve serverSpan context with the intent to interrogate attributes
        {
            Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
            serverSpan->getSpanContext(retrievedSpanCtxAttributes.get());

            CPPUNIT_ASSERT_MESSAGE("Unexpected GlobalID detected",
             strsame("IncomingUGID", retrievedSpanCtxAttributes->queryProp(kGlobalIdHttpHeaderName)));
            CPPUNIT_ASSERT_MESSAGE("Unexpected CallerID detected",
             strsame("IncomingCID", retrievedSpanCtxAttributes->queryProp(kCallerIdHttpHeaderName)));

            CPPUNIT_ASSERT_MESSAGE("Unexpected Declared Parent SpanID detected",
             strsame("4b960b3e4647da3f", retrievedSpanCtxAttributes->queryProp("remoteParentSpanID")));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty TraceID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("traceID")));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected empty SpanID detected", false, isEmptyString(retrievedSpanCtxAttributes->queryProp("spanID")));
        }

        //retrieve serverSpan client headers with the intent to propagate them onto the next hop
        {
            Owned<IProperties> retrievedClientHeaders = createProperties();
            serverSpan->getClientHeaders(retrievedClientHeaders.get());

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected Otel traceparent header len detected", (size_t)55,
             strlen(retrievedClientHeaders->queryProp("traceparent")));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("getClientHeaders failed to produce traceState!", true, retrievedClientHeaders->hasProp("tracestate"));
            const char * tracestate = retrievedClientHeaders->queryProp("tracestate");
            CPPUNIT_ASSERT_MESSAGE("Unexpected traceState detected",
             strsame("hpcc=4b960b3e4647da3f", retrievedClientHeaders->queryProp("tracestate")));
        }
    }

    void testStringArrayPropegatedServerSpan()
    {
         StringArray mockHTTPHeadersSA;
        //mock opentel traceparent context 
        mockHTTPHeadersSA.append("traceparent:00-beca49ca8f3138a2842e5cf21402bfff-4b960b3e4647da3f-01");
        //mock opentel tracestate https://www.w3.org/TR/trace-context/#trace-context-http-headers-format
        mockHTTPHeadersSA.append("tracestate:hpcc=4b960b3e4647da3f");
        mockHTTPHeadersSA.append("HPCC-Global-Id:someGlobalID");
        mockHTTPHeadersSA.append("HPCC-Caller-Id:IncomingCID");

        Owned<ISpan> serverSpan = queryTraceManager().createServerSpan("StringArrayPropegatedServerSpan", mockHTTPHeadersSA);
        //at this point the serverSpan should have the following context attributes
        //traceID, spanID, remoteParentSpanID, traceFlags, traceState, globalID, callerID

        //retrieve serverSpan context with the intent to interrogate attributes
        {
            Owned<IProperties> retrievedSpanCtxAttributes = createProperties();
            serverSpan->getSpanContext(retrievedSpanCtxAttributes.get());

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected GlobalID detected", true,
             strsame("someGlobalID", retrievedSpanCtxAttributes->queryProp(kGlobalIdHttpHeaderName)));
            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected CallerID detected", true,
             strsame("IncomingCID", retrievedSpanCtxAttributes->queryProp(kCallerIdHttpHeaderName)));

            CPPUNIT_ASSERT_EQUAL_MESSAGE("Unexpected Declared Parent SpanID detected", true,
             strsame("4b960b3e4647da3f", retrievedSpanCtxAttributes->queryProp("remoteParentSpanID")));
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibTraceTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibTraceTest, "JlibTraceTest" );


class JlibSemTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibSemTest);
        CPPUNIT_TEST(testSimple);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testTimedAvailable(Semaphore & sem)
    {
        //Shouldn't cause a reschedule, definitely shouldn't wait for 100s
        if(!sem.wait(100))
            ASSERT(false);
    }

    void testTimedElapsed(Semaphore & sem, unsigned time)
    {
        unsigned now = msTick();
        sem.wait(time);
        unsigned taken = msTick() - now;
        VStringBuffer errMsg("values: time: %u, taken: %u", time, taken);
        CPPUNIT_ASSERT_MESSAGE(errMsg.str(), taken >= time && taken < time + oneMinute);
        PROGLOG("%s", errMsg.str());
    }

    void testSimple()
    {
        //Some very basic semaphore tests.
        Semaphore sem;
        sem.signal();
        sem.wait();
        testTimedElapsed(sem, 100);
        sem.signal();
        testTimedAvailable(sem);

        sem.reinit(2);
        sem.wait();
        testTimedAvailable(sem);
        testTimedElapsed(sem, 5);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibSemTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibSemTest, "JlibSemTest" );


class JlibSemTestStress : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibSemTestStress);
        CPPUNIT_TEST(testSimple);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testTimedElapsed(Semaphore & sem, unsigned time, unsigned loopCount)
    {
        unsigned __int64 sumTaken = 0;
        unsigned maxTaken = 0;
        unsigned timeLimit = time + oneMinute;
        unsigned numberOfOut = 0;
        bool isSignaled = false;

        PROGLOG("Start loop");
        for (int i = 0 ; i <= loopCount; i++)
        {
            unsigned now = msTick();
            if (sem.wait(time))
            {
                isSignaled = true;
                break;
            }
            unsigned taken = msTick() - now;
            sumTaken += taken;
            maxTaken = (taken > maxTaken ? taken : maxTaken);
            numberOfOut += (taken > timeLimit ? 1 : 0);
        }

        VStringBuffer errMsg("values: time: %d, loop: %d, sum taken: %llu, average taken: %llu, max taken: %d, out of limit: %d times, signaled: %s",
                                time, loopCount, sumTaken, sumTaken/loopCount, maxTaken, numberOfOut, (isSignaled ? "yes" : "no"));
        CPPUNIT_ASSERT_MESSAGE(errMsg.str(), 0 == numberOfOut && !isSignaled );
        PROGLOG("%s", errMsg.str());
    }

    void testSimple()
    {
        //Very basic semaphore stress tests.
        Semaphore sem;
        sem.signal();
        if (!sem.wait(1000))
        {
            VStringBuffer errMsg("Semaphore stalled (%s:%d)", sanitizeSourceFile(__FILE__), __LINE__);
            CPPUNIT_FAIL(errMsg.str());
        }
        testTimedElapsed(sem, 5, 1000);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibSemTestStress );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibSemTestStress, "JlibSemTestStress" );

/* =========================================================== */

class JlibSetTest : public CppUnit::TestFixture
{
protected:

    void testBitsetHelpers()
    {
        CPPUNIT_ASSERT_EQUAL(0U, countTrailingUnsetBits(1U));
        CPPUNIT_ASSERT_EQUAL(31U, countLeadingUnsetBits(1U));
        CPPUNIT_ASSERT_EQUAL(1U, getMostSignificantBit(1U));
        CPPUNIT_ASSERT_EQUAL(4U, countTrailingUnsetBits(0x110U));
        CPPUNIT_ASSERT_EQUAL(23U, countLeadingUnsetBits(0x110U));
        CPPUNIT_ASSERT_EQUAL(9U, getMostSignificantBit(0x110U));
        CPPUNIT_ASSERT_EQUAL(0U, countTrailingUnsetBits(0xFFFFFFFFU));
        CPPUNIT_ASSERT_EQUAL(0U, countLeadingUnsetBits(0xFFFFFFFFU));
        CPPUNIT_ASSERT_EQUAL(32U, getMostSignificantBit(0xFFFFFFFFU));
        CPPUNIT_ASSERT_EQUAL(52U, countTrailingUnsetBits(I64C(0x1010000000000000U)));
    }

    void testSet1(bool initial, IBitSet *bs, unsigned start, unsigned numBits, bool setValue, bool clearValue)
    {
        unsigned end = start+numBits;
        if (initial)
            bs->incl(start, end-1);
        for (unsigned i=start; i < end; i++)
        {
            ASSERT(bs->test(i) == clearValue);
            bs->set(i, setValue);
            ASSERT(bs->test(i) == setValue);

            bs->set(i+5, setValue);
            ASSERT(bs->scan(0, setValue) == i);
            ASSERT(bs->scan(i+1, setValue) == i+5);
            bs->set(i, clearValue);
            bs->set(i+5, clearValue);
            //Clearing i+5 above may extend the set - so need to calculate the end carefully
            unsigned last = i+5 < end ? end : i + 6;
            unsigned match1 = bs->scan(0, setValue);
            CPPUNIT_ASSERT_EQUAL((unsigned)(initial ? last : -1), match1);

            bs->invert(i);
            ASSERT(bs->test(i) == setValue);
            bs->invert(i);
            ASSERT(bs->test(i) == clearValue);

            bool wasSet = bs->testSet(i, setValue);
            ASSERT(wasSet == clearValue);
            bool wasSet2 = bs->testSet(i, clearValue);
            ASSERT(wasSet2 == setValue);
            ASSERT(bs->test(i) == clearValue);

            bs->set(i, setValue);
            unsigned match = bs->scanInvert(0, setValue);
            ASSERT(match == i);
            ASSERT(bs->test(i) == clearValue);
        }
        bs->reset();
        if (initial)
        {
            bs->incl(start, end);
            bs->excl(start+5, end-5);
        }
        else
            bs->incl(start+5, end-5);
        unsigned inclStart = bs->scan(start, setValue);
        ASSERT((start+5) == inclStart);
        unsigned inclEnd = bs->scan(start+5, clearValue);
        ASSERT((end-5) == (inclEnd-1));
    }

    void testSet(bool initial, unsigned passes, bool timed)
    {
        unsigned now = msTick();
        bool setValue = !initial;
        bool clearValue = initial;
        const unsigned numBits = 400;
        for (unsigned pass=0; pass < passes; pass++)
        {
            Owned<IBitSet> bs = createThreadSafeBitSet();
            testSet1(initial, bs, 0, numBits, setValue, clearValue);
        }
        if (timed)
        {
            unsigned elapsed = msTick()-now;
            DBGLOG("Bit test (%u) %d passes time taken = %dms", initial, passes, elapsed);
        }
        now = msTick();
        for (unsigned pass=0; pass < passes; pass++)
        {
            Owned<IBitSet> bs = createBitSet();
            testSet1(initial, bs, 0, numBits, setValue, clearValue);
        }
        if (timed)
        {
            unsigned elapsed = msTick()-now;
            DBGLOG("Bit test [thread-unsafe version] (%u) %d passes time taken = %dms", initial, passes, elapsed);
        }
        now = msTick();
        size32_t bitSetMemSz = getBitSetMemoryRequirement(numBits+5);
        MemoryBuffer mb;
        void *mem = mb.reserveTruncate(bitSetMemSz);
        for (unsigned pass=0; pass < passes; pass++)
        {
            Owned<IBitSet> bs = createBitSet(bitSetMemSz, mem);
            testSet1(initial, bs, 0, numBits, setValue, clearValue);
        }
        if (timed)
        {
            unsigned elapsed = msTick()-now;
            DBGLOG("Bit test [thread-unsafe version, fixed memory] (%u) %d passes time taken = %dms\n", initial, passes, elapsed);
        }
    }
};

class JlibSetTestQuick : public JlibSetTest
{
public:
    CPPUNIT_TEST_SUITE(JlibSetTestQuick);
        CPPUNIT_TEST(testBitsetHelpers);
        CPPUNIT_TEST(testSimple);
    CPPUNIT_TEST_SUITE_END();

    void testSimple()
    {
        testSet(false, 100, false);
        testSet(true, 100, false);
    }

};

class JlibSetTestStress : public JlibSetTest
{
public:
    CPPUNIT_TEST_SUITE(JlibSetTestStress);
        CPPUNIT_TEST(testParallel);
        CPPUNIT_TEST(testSimple);
    CPPUNIT_TEST_SUITE_END();

    void testSimple()
    {
        testSet(false, 10000, true);
        testSet(true, 10000, true);
    }
protected:

    class CBitThread : public CSimpleInterfaceOf<IInterface>, implements IThreaded
    {
        IBitSet &bitSet;
        unsigned startBit, numBits;
        bool initial, setValue, clearValue;
        CThreaded threaded;
        Owned<IException> exception;
        CppUnit::Exception *cppunitException;
    public:
        CBitThread(IBitSet &_bitSet, unsigned _startBit, unsigned _numBits, bool _initial)
            : threaded("CBitThread", this), bitSet(_bitSet), startBit(_startBit), numBits(_numBits), initial(_initial)
        {
            cppunitException = NULL;
            setValue = !initial;
            clearValue = initial;
        }
        void start() { threaded.start(); }
        void join()
        {
            threaded.join();
            if (exception)
                throw exception.getClear();
            else if (cppunitException)
                throw cppunitException;
        }
        virtual void threadmain() override
        {
            try
            {
                unsigned endBit = startBit+numBits-1;
                if (initial)
                    bitSet.incl(startBit, endBit);
                for (unsigned i=startBit; i < endBit; i++)
                {
                    ASSERT(bitSet.test(i) == clearValue);
                    bitSet.set(i, setValue);
                    ASSERT(bitSet.test(i) == setValue);
                    if (i < (endBit-1))
                        ASSERT(bitSet.scan(i, clearValue) == i+1); // find next unset (should be i+1)
                    bitSet.set(i, clearValue);
                    bitSet.invert(i);
                    ASSERT(bitSet.test(i) == setValue);
                    bitSet.invert(i);
                    ASSERT(bitSet.test(i) == clearValue);

                    bool wasSet = bitSet.testSet(i, setValue);
                    ASSERT(wasSet == clearValue);
                    bool wasSet2 = bitSet.testSet(i, clearValue);
                    ASSERT(wasSet2 == setValue);
                    ASSERT(bitSet.test(i) == clearValue);

                    bitSet.set(i, setValue);
                    unsigned match = bitSet.scanInvert(startBit, setValue);
                    ASSERT(match == i);
                    ASSERT(bitSet.test(i) == clearValue);
                }
            }
            catch (IException *e)
            {
                exception.setown(e);
            }
            catch (CppUnit::Exception &e)
            {
                cppunitException = e.clone();
            }
        }
    };
    unsigned testParallelRun(IBitSet &bitSet, unsigned nThreads, unsigned bitsPerThread, bool initial)
    {
        IArrayOf<CBitThread> bitThreads;
        unsigned bitStart = 0;
        unsigned bitEnd = 0;
        for (unsigned t=0; t<nThreads; t++)
        {
            bitThreads.append(* new CBitThread(bitSet, bitStart, bitsPerThread, initial));
            bitStart += bitsPerThread;
        }
        unsigned now = msTick();
        for (unsigned t=0; t<nThreads; t++)
            bitThreads.item(t).start();
        Owned<IException> exception;
        CppUnit::Exception *cppunitException = NULL;
        for (unsigned t=0; t<nThreads; t++)
        {
            try
            {
                bitThreads.item(t).join();
            }
            catch (IException *e)
            {
                EXCLOG(e, NULL);
                if (!exception)
                    exception.setown(e);
                else
                    e->Release();
            }
            catch (CppUnit::Exception *e)
            {
                cppunitException = e;
            }
        }
        if (exception)
            throw exception.getClear();
        else if (cppunitException)
            throw *cppunitException;
        return msTick()-now;
    }

    void testSetParallel(bool initial)
    {
        unsigned numBits = 1000000; // 10M
        unsigned nThreads = getAffinityCpus();
        unsigned bitsPerThread = numBits/nThreads;
        bitsPerThread = ((bitsPerThread + (BitsPerItem-1)) / BitsPerItem) * BitsPerItem; // round up to multiple of BitsPerItem
        numBits = bitsPerThread*nThreads; // round

        fprintf(stdout, "testSetParallel, testing bit set of size : %d, nThreads=%d\n", numBits, nThreads);

        Owned<IBitSet> bitSet = createThreadSafeBitSet();
        unsigned took = testParallelRun(*bitSet, nThreads, bitsPerThread, initial);
        fprintf(stdout, "Thread safe parallel bit set test (%u) time taken = %dms\n", initial, took);

        size32_t bitSetMemSz = getBitSetMemoryRequirement(numBits);
        MemoryBuffer mb;
        void *mem = mb.reserveTruncate(bitSetMemSz);
        bitSet.setown(createBitSet(bitSetMemSz, mem));
        took = testParallelRun(*bitSet, nThreads, bitsPerThread, initial);
        fprintf(stdout, "Thread unsafe parallel bit set test (%u) time taken = %dms\n", initial, took);
    }

    void testParallel()
    {
        testSetParallel(false);
        testSetParallel(true);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibSetTestQuick );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibSetTestQuick, "JlibSetTestQuick" );
CPPUNIT_TEST_SUITE_REGISTRATION( JlibSetTestStress );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibSetTestStress, "JlibSetTestStress" );

/* =========================================================== */
class JlibFileIOTestTiming : public CppUnit::TestFixture
{
protected:
    unsigned rs, nr10pct, nr150pct;
    char *record;
    StringBuffer tmpfile;

    CPPUNIT_TEST_SUITE( JlibFileIOTestTiming );
        CPPUNIT_TEST(testIOSmall);
        CPPUNIT_TEST(testIOLarge);
    CPPUNIT_TEST_SUITE_END();

public:
    JlibFileIOTestTiming()
    {
        HardwareInfo hdwInfo;
        getHardwareInfo(hdwInfo);
        rs = 65536;
        unsigned nr = (unsigned)(1024.0 * (1024.0 * (double)hdwInfo.totalMemory / (double)rs));
        nr10pct = nr / 10;
        nr150pct = (unsigned)((double)nr * 1.5);
        record = (char *)malloc(rs);
        for (unsigned i=0;i<rs;i++)
            record[i] = 'a';
        record[rs-1] = '\n';

        tmpfile.set("JlibFileIOTest.txt");
    }

    ~JlibFileIOTestTiming()
    {
        free(record);
    }

protected:
    void testIO(unsigned nr, const char *server)
    {
        IFile *ifile;
        IFileIO *ifileio;
        unsigned fsize = (unsigned)(((double)nr * (double)rs) / (1024.0 * 1024.0));

        fflush(NULL);

        for(int j=0; j<2; j++)
        {
            if (j==0)
                DBGLOG("File size: %d (MB) Cache", fsize);
            else
                DBGLOG("File size: %d (MB) Nocache", fsize);

            if (server != NULL)
            {
                SocketEndpoint ep;
                ep.set(server, 7100);
                ifile = createRemoteFile(ep, tmpfile);
                DBGLOG("Remote: (%s)", server);
            }
            else
            {
                ifile = createIFile(tmpfile);
                DBGLOG("Local:");
            }

            ifile->remove();

            unsigned st = msTick();

            IFEflags extraFlags = IFEcache;
            if (j==1)
                extraFlags = IFEnocache;
            ifileio = ifile->open(IFOcreate, extraFlags);

#if 0 // for testing default and explicitly set share mode to Windows dafilesrv
            if (server != NULL)
                ifile->setShareMode((IFSHmode)IFSHread);
#endif

            try
            {
                ifile->setFilePermissions(0666);
            }
            catch (...)
            {
                DBGLOG("ifile->setFilePermissions() exception");
            }

            unsigned iter = nr / 40;
            if (iter < 1)
                iter = 1;

            __int64 pos = 0;
            for (unsigned i=0;i<nr;i++)
            {
                ifileio->write(pos, rs, record);
                pos += rs;
                if ((i % iter) == 0)
                {
                    fprintf(stdout,".");
                    fflush(NULL);
                }
            }

            ifileio->close();

            double rsec = (double)(msTick() - st)/1000.0;
            unsigned iorate = (unsigned)((double)fsize / rsec);

            DBGLOG("write - elapsed time = %6.2f (s) iorate = %4d (MB/s)", rsec, iorate);

            st = msTick();

            extraFlags = IFEcache;
            if (j==1)
                extraFlags = IFEnocache;
            ifileio = ifile->open(IFOread, extraFlags);

            pos = 0;
            for (unsigned i=0;i<nr;i++)
            {
                ifileio->read(pos, rs, record);
                pos += rs;
                if ((i % iter) == 0)
                {
                    fprintf(stdout,".");
                    fflush(NULL);
                }
            }

            ifileio->close();

            rsec = (double)(msTick() - st)/1000.0;
            iorate = (unsigned)((double)fsize / rsec);

            DBGLOG("read -- elapsed time = %6.2f (s) iorate = %4d (MB/s)", rsec, iorate);

            ifileio->Release();
            ifile->remove();
            ifile->Release();
        }
    }

    void testIOSmall()
    {
        testIO(nr10pct, NULL);
    }

    void testIOLarge()
    {
        testIO(nr150pct, NULL);
    }


};

class JlibFileIOTestStress : public JlibFileIOTestTiming
{
protected:
    CPPUNIT_TEST_SUITE( JlibFileIOTestStress );
        CPPUNIT_TEST(testIORemote);
    CPPUNIT_TEST_SUITE_END();

    void testIORemote()
    {
        const char * server = ".";
        testIO(nr10pct, server);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibFileIOTestTiming );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibFileIOTestTiming, "JlibFileIOTestTiming" );
CPPUNIT_TEST_SUITE_REGISTRATION( JlibFileIOTestStress );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibFileIOTestTiming, "JlibFileIOTestStress" );

/* =========================================================== */
class JlibContainsRelPathsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibContainsRelPathsTest);
        CPPUNIT_TEST(testWindowsPaths);
        CPPUNIT_TEST(testPosixPaths);
    CPPUNIT_TEST_SUITE_END();

    bool testContainsRelPaths(const char * path)
    {
        return containsRelPaths(path);
    };
public:
    void testWindowsPaths()
    {
        CPPUNIT_ASSERT(testContainsRelPaths("a\\b\\c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths(".\\a\\b\\c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("..\\a\\b\\c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("...\\a\\b\\c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\.\\a\\b\\c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("\\..\\a\\b\\c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("\\...\\a\\b\\c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c\\.") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c\\..") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c\\...") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\.\\b\\c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\..\\b\\c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\...\\b\\c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c.d\\e") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c..d\e") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c...d\\e") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("\\a\\b\\c.d.e\\f") == false);
    }
    void testPosixPaths()
    {
        CPPUNIT_ASSERT(testContainsRelPaths("a/b/c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("~/a/b/c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("/a~/b/c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c~") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("./a/b/c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("../a/b/c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths(".../a/b/c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/./a/b/c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("/../a/b/c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("/.../a/b/c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c/.") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c/..") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c/...") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/./c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/../c") == true);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/.../c") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c.d/e") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c..d/e") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c...d/e") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("/a/b/c.d.e/f") == false);
        CPPUNIT_ASSERT(testContainsRelPaths("abc\\def/../g/h") == true); // The PathSepChar should be '/'.
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibContainsRelPathsTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibContainsRelPathsTest, "JlibContainsRelPathsTest");

/* =========================================================== */

class JlibStringBufferTiming : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( JlibStringBufferTiming );
        CPPUNIT_TEST(testSwap);
    CPPUNIT_TEST_SUITE_END();

public:
    void testSwap()
    {
        StringBuffer l;
        StringBuffer r;
        for (unsigned len=0; len<40; len++)
        {
            const unsigned numIter = 100000000;
            cycle_t start = get_cycles_now();

            for (unsigned pass=0; pass < numIter; pass++)
            {
                l.swapWith(r);
            }
            cycle_t elapsed = get_cycles_now() - start;
            DBGLOG("Each iteration of size %u took %.2f nanoseconds", len, (double)cycle_to_nanosec(elapsed) / numIter);
            l.append("a");
            r.append("b");
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibStringBufferTiming );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibStringBufferTiming, "JlibStringBufferTiming" );



/* =========================================================== */

static const unsigned split4_2[] = {0, 2, 4 };
static const unsigned split100_2[] = {0, 50, 100  };
static const unsigned split100_10[] = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100  };
static const unsigned split7_10[] = {0,1,1,2,3,3,4,5,6,6,7 };
static const unsigned split10_3[] = {0,3,7,10 };
static const unsigned split58_10[] = {0,6,12,17,23,29,35,41,46,52,58 };
static const unsigned split9_2T[] = { 0,5,9 };
static const unsigned split9_2F[] = { 0,4,9 };
static const unsigned split15_3[] = { 0,5,10,15 };

class JlibQuantileTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( JlibQuantileTest );
        CPPUNIT_TEST(testQuantile);
        CPPUNIT_TEST(testRandom);
    CPPUNIT_TEST_SUITE_END();

public:
    JlibQuantileTest()
    {
    }

    void testQuantilePos(unsigned numItems, unsigned numDivisions, bool roundUp, const unsigned * expected)
    {
        if (numDivisions == 0)
            return;

        QuantilePositionIterator iter(numItems, numDivisions, roundUp);
        QuantileFilterIterator filter(numItems, numDivisions, roundUp);

        unsigned prevPos = 0;
        iter.first();
        for (unsigned i=0; i <= numDivisions; i++)
        {
            //Check the values from the quantile iterator match those that are expected
            unsigned pos = (unsigned)iter.get();
#if 0
            DBGLOG("(%d,%d) %d=%d", numItems, numDivisions, i, pos);
#endif
            if (expected)
                CPPUNIT_ASSERT_EQUAL(expected[i], pos);

            //Check that the quantile filter correctly returns true and false for subsequent calls.
            while (prevPos < pos)
            {
                CPPUNIT_ASSERT(!filter.get());
                filter.next();
                prevPos++;
            }

            if (prevPos == pos)
            {
                CPPUNIT_ASSERT(filter.get());
                filter.next();
                prevPos++;
            }
            iter.next();
        }
    }

    void testQuantile()
    {
        testQuantilePos(4, 2, false, split4_2);
        testQuantilePos(100, 2, false, split100_2);
        testQuantilePos(100, 10, false, split100_10);
        testQuantilePos(7, 10, false, split7_10);
        testQuantilePos(10, 3, false, split10_3);
        testQuantilePos(10, 3, true, split10_3);
        testQuantilePos(58, 10, false, split58_10);
        //testQuantilePos(9, 2, true, split9_2T);
        testQuantilePos(9, 2, false, split9_2F);
        testQuantilePos(15, 3, false, split15_3);
        testQuantilePos(1231, 57, false, NULL);
        testQuantilePos(1, 63, false, NULL);
        testQuantilePos(10001, 17, false, NULL);
    }
    void testRandom()
    {
        //test various random combinations to ensure the results are consistent.
        for (unsigned i=0; i < 10; i++)
            testQuantilePos(random() % 1000000, random() % 10000, true, NULL);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibQuantileTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibQuantileTest, "JlibQuantileTest" );

/* =========================================================== */

class JlibTimingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( JlibTimingTest );
        CPPUNIT_TEST(testMsTick);
        CPPUNIT_TEST(testNsTick);
        CPPUNIT_TEST(testGetCyclesNow);
        CPPUNIT_TEST(testStdChrono);
        CPPUNIT_TEST(testGetTimeOfDay);
        CPPUNIT_TEST(testClockGetTimeReal);
        CPPUNIT_TEST(testClockGetTimeMono);
        CPPUNIT_TEST(testTimestampNow);
        CPPUNIT_TEST(testTime);
    CPPUNIT_TEST_SUITE_END();

public:
    static constexpr unsigned scale = 10;
    static constexpr unsigned iters = 1000000 * scale;
    JlibTimingTest()
    {
    }

    void testMsTick()
    {
        unsigned startTime = msTick();
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
            value += msTick();
        DBGLOG("msTick() %uns = %u", (msTick()-startTime)/scale, value);
    }
    void testNsTick()
    {
        unsigned startTime = msTick();
        unsigned __int64 value = 0;
        for (unsigned i=0; i < iters; i++)
            value += nsTick();
        DBGLOG("nsTick() %uns = %" I64F "u", (msTick()-startTime)/scale, value);
    }
    void testGetCyclesNow()
    {
        unsigned startTime = msTick();
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
            value += get_cycles_now();
        DBGLOG("get_cycles_now() %uns = %u", (msTick()-startTime)/scale, value);
    }
    void testStdChrono()
    {
        unsigned startTime = msTick();
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
            value += std::chrono::high_resolution_clock::now().time_since_epoch().count();
        DBGLOG("std::chrono::high_resolution_clock::now() %uns = %u", (msTick()-startTime)/scale, value);
    }
    void testGetTimeOfDay()
    {
        unsigned startTime = msTick();
        struct timeval tv;
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
        {
            gettimeofday(&tv, NULL);
            value += tv.tv_sec;
        }
        DBGLOG("gettimeofday() %uns = %u", (msTick()-startTime)/scale, value);
    }
    void testClockGetTimeReal()
    {
        unsigned startTime = msTick();
        struct timespec ts;
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
        {
            clock_gettime(CLOCK_REALTIME, &ts);
            value += ts.tv_sec;
        }
        DBGLOG("clock_gettime(REALTIME) %uns = %u", (msTick()-startTime)/scale, value);
    }
    void testClockGetTimeMono()
    {
        unsigned startTime = msTick();
        struct timespec ts;
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
        {
            clock_gettime(CLOCK_MONOTONIC, &ts);
            value += ts.tv_sec;
        }
        DBGLOG("clock_gettime(MONOTONIC) %uns = %u", (msTick()-startTime)/scale, value);
    }
    void testTimestampNow()
    {
        unsigned startTime = msTick();
        struct timespec ts;
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
        {
            value += getTimeStampNowValue();
        }
        DBGLOG("getTimeStampNowValue() %uns = %u", (msTick()-startTime)/scale, value);
    }
    void testTime()
    {
        unsigned startTime = msTick();
        struct timespec ts;
        unsigned value = 0;
        for (unsigned i=0; i < iters; i++)
        {
            value += (unsigned)time(nullptr);
        }
        DBGLOG("time() %uns = %u", (msTick()-startTime)/scale, value);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibTimingTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibTimingTest, "JlibTimingTest" );

/* =========================================================== */
class JlibReaderWriterTestTiming : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibReaderWriterTestTiming);
    CPPUNIT_TEST(testCombinations);
    CPPUNIT_TEST_SUITE_END();

    const static unsigned spinScaling = 1000;

    static unsigned spinCalculation(unsigned prev, unsigned scale)
    {
        unsigned value = prev;
        for (unsigned i = 0; i < scale*spinScaling; i++)
        {
            value = (value * 0x1234FEDB + 0x87654321);
        }
        return value;
    }

    class Reader : public Thread
    {
    public:
        Reader(IRowQueue & _source, Semaphore & _doneSem, unsigned _workScale)
            : Thread("Reader"), source(_source), doneSem(_doneSem), workScale(_workScale), work(0)
        {
        }

        virtual int run()
        {
            for (;;)
            {
                const void * next;
                if (!source.dequeue(next))
                    break;
                if (!next)
                    break;
                std::atomic<byte> * value = (std::atomic<byte> *)next;
                (*value)++;
                if (workScale)
                    work = spinCalculation(work, workScale);
            }
            doneSem.signal();
            return 0;
        }

    private:
        IRowQueue & source;
        Semaphore & doneSem;
        volatile unsigned work;
        unsigned workScale;
    };

    class WriterBase : public Thread
    {
    public:
        WriterBase(IRowQueue & _target, size_t _len, byte * _buffer, Semaphore & _startSem, Semaphore & _doneSem, unsigned _workScale)
            : Thread("Writer"), target(_target), len(_len), buffer(_buffer), startSem(_startSem), doneSem(_doneSem), workScale(_workScale), work(0)
        {
        }

    protected:
        size_t len;
        byte * buffer;
        IRowQueue & target;
        Semaphore & startSem;
        Semaphore & doneSem;
        volatile unsigned work;
        unsigned workScale;
    };
    class Writer : public WriterBase
    {
    public:
        Writer(IRowQueue & _target, size_t _len, byte * _buffer, Semaphore & _startSem, Semaphore & _doneSem, unsigned _workScale)
            : WriterBase(_target, _len, _buffer, _startSem, _doneSem, _workScale)
        {
        }

        virtual int run()
        {
            startSem.wait();
            for (size_t i = 0; i < len; i++)
            {
                if (workScale)
                    work = spinCalculation(work, workScale);
                target.enqueue(buffer + i);
            }
            target.noteWriterStopped();
            doneSem.signal();
            return 0;
        }
    };
public:
    const static size_t bufferSize = 0x100000;//0x100000*64;
    void testQueue(IRowQueue & queue, unsigned numProducers, unsigned numConsumers, unsigned queueElements, unsigned readerWork, unsigned writerWork)
    {
        const size_t sizePerProducer = bufferSize / numProducers;
        const size_t testSize = sizePerProducer * numProducers;

        OwnedMalloc<byte> buffer(bufferSize, true);
        Semaphore startSem;
        Semaphore writerDoneSem;
        Semaphore stopSem;

        Reader * * consumers = new Reader *[numConsumers];
        for (unsigned i2 = 0; i2 < numConsumers; i2++)
        {
            consumers[i2] = new Reader(queue, stopSem, readerWork);
            consumers[i2]->start();
        }

        WriterBase * * producers = new WriterBase *[numProducers];
        for (unsigned i1 = 0; i1 < numProducers; i1++)
        {
            producers[i1] = new Writer(queue, sizePerProducer, buffer + i1 * sizePerProducer, startSem, writerDoneSem, writerWork);
            producers[i1]->start();
        }

        cycle_t startTime = get_cycles_now();

        //Start the writers
        startSem.signal(numProducers);

        //Wait for the writers to complete
        for (unsigned i7 = 0; i7 < numProducers; i7++)
            writerDoneSem.wait();

        //Wait for the readers to complete
        for (unsigned i3 = 0; i3 < numConsumers; i3++)
            stopSem.wait();

        cycle_t stopTime = get_cycles_now();

        //All bytes should have been changed to 1, if not a queue item got lost.
        unsigned failures = 0;
        unsigned numClear = 0;
        size_t failPos = ~(size_t)0;
        byte failValue = 0;
        for (size_t pos = 0; pos < testSize; pos++)
        {
            if (buffer[pos] != 1)
            {
                failures++;
                if (failPos == ~(size_t)0)
                {
                    failPos = pos;
                    failValue = buffer[pos];
                }
            }

            if (buffer[pos] == 0)
                numClear++;
        }

        unsigned timeMs = cycle_to_nanosec(stopTime - startTime) / 1000000;
        unsigned expectedReadWorkTime = (unsigned)(((double)unitWorkTimeMs * readerWork) / numConsumers);
        unsigned expectedWriteWorkTime = (unsigned)(((double)unitWorkTimeMs * writerWork) / numProducers);
        unsigned expectedWorkTime = std::max(expectedReadWorkTime, expectedWriteWorkTime);
        if (failures)
        {
            DBGLOG("Fail: Test %u producers %u consumers %u queueItems %u(%u) mismatches fail(@%u=%u)", numProducers, numConsumers, queueElements, failures, numClear, (unsigned)failPos, failValue);
            ASSERT(failures == 0);
        }
        else
            DBGLOG("Pass: Test %u(@%u) producers %u(@%u) consumers %u queueItems in %ums [%dms]", numProducers, writerWork, numConsumers, readerWork, queueElements, timeMs, timeMs-expectedWorkTime);

        for (unsigned i4 = 0; i4 < numConsumers; i4++)
        {
            consumers[i4]->join();
            consumers[i4]->Release();
        }
        delete[] consumers;

        for (unsigned i5 = 0; i5 < numProducers; i5++)
        {
            producers[i5]->join();
            producers[i5]->Release();
        }
        delete[] producers;
    }

    void testQueue(unsigned numProducers, unsigned numConsumers, unsigned numElements = 0, unsigned readWork = 0, unsigned writeWork = 0)
    {
        unsigned queueElements = (numElements != 0) ? numElements : (numProducers + numConsumers) * 2;
        Owned<IRowQueue> queue = createRowQueue(numConsumers, numProducers, queueElements, 0);
        testQueue(*queue, numProducers, numConsumers, queueElements, readWork, writeWork);
    }

    void testWorkQueue(unsigned numProducers, unsigned numConsumers, unsigned numElements)
    {
        for (unsigned readWork = 1; readWork <= 8; readWork = readWork * 2)
        {
            for (unsigned writeWork = 1; writeWork <= 8; writeWork = writeWork * 2)
            {
                testQueue(numProducers, numConsumers, numElements, readWork, writeWork);
            }
        }
    }
    void testCombinations()
    {
        // 1:1
        for (unsigned i=0; i < 10; i++)
            testQueue(1, 1, 10);

        //One to Many
        testQueue(1, 10, 5);
        testQueue(1, 5, 5);
        testQueue(1, 5, 10);
        testQueue(1, 127, 10);
        testQueue(1, 127, 127);

        //Many to One
        testQueue(10, 1, 5);
        testQueue(5, 1, 5);
        testQueue(5, 1, 10);
        testQueue(127, 1, 127);

        cycle_t startTime = get_cycles_now();
        volatile unsigned value = 0;
        for (unsigned pass = 0; pass < 10; pass++)
        {
            for (unsigned i2 = 0; i2 < bufferSize; i2++)
                value = spinCalculation(value, 1);
        }
        cycle_t stopTime = get_cycles_now();
        unitWorkTimeMs = cycle_to_nanosec(stopTime - startTime) / (1000000 * 10);
        DBGLOG("Work(1) takes %ums", unitWorkTimeMs);

        //How does it scale with number of queue elements?
        for (unsigned elem = 16; elem < 256; elem *= 2)
        {
            testQueue(16, 1, elem, 1, 1);
        }

#if 1
        //Many to Many
        for (unsigned readWork = 1; readWork <= 8; readWork = readWork * 2)
        {
            for (unsigned writeWork = 1; writeWork <= 8; writeWork = writeWork * 2)
            {
                testQueue(1, 1, 63, readWork, writeWork);
                testQueue(1, 2, 63, readWork, writeWork);
                testQueue(1, 4, 63, readWork, writeWork);
                testQueue(1, 8, 63, readWork, writeWork);
                testQueue(1, 16, 63, readWork, writeWork);
                testQueue(2, 1, 63, readWork, writeWork);
                testQueue(4, 1, 63, readWork, writeWork);
                testQueue(8, 1, 63, readWork, writeWork);
                testQueue(16, 1, 63, readWork, writeWork);

                testQueue(2, 2, 63, readWork, writeWork);
                testQueue(4, 4, 63, readWork, writeWork);
                testQueue(8, 8, 63, readWork, writeWork);
                testQueue(16, 8, 63, readWork, writeWork);
                testQueue(16, 16, 63, readWork, writeWork);
                testQueue(32, 1, 63, readWork, writeWork);
                testQueue(64, 1, 63, readWork, writeWork);
                testQueue(1, 32, 63, readWork, writeWork);
                testQueue(1, 64, 63, readWork, writeWork);
            }

        }
#else
        //Many to Many
        testWorkQueue(1, 1, 63);
        testWorkQueue(1, 2, 63);
        testWorkQueue(1, 4, 63);
        testWorkQueue(1, 8, 63);
        testWorkQueue(1, 16, 63);
        testWorkQueue(2, 1, 63);
        testWorkQueue(4, 1, 63);
        testWorkQueue(8, 1, 63);
        testWorkQueue(16, 1, 63);

        testWorkQueue(2, 2, 63);
        testWorkQueue(4, 4, 63);
        testWorkQueue(8, 8, 63);
#endif

        testQueue(2, 2, 4);
        testQueue(2, 2, 8);
        testQueue(2, 2, 16);
        testQueue(2, 2, 32);
        testQueue(2, 2, 100);
    }

protected:
    unsigned unitWorkTimeMs;
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibReaderWriterTestTiming);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibReaderWriterTestTiming, "JlibReaderWriterTestTiming");

/* =========================================================== */

class JlibWildMatchBase : public CppUnit::TestFixture
{
protected:
    void testSet(unsigned length, const char * const * patterns, bool reportTiming)
    {
        std::unique_ptr<char[]> search(generateSearchString(length));
        CCycleTimer timer;
        testPatterns(search.get(), patterns);
        if (reportTiming)
            DBGLOG("%u: %u ms", length, timer.elapsedMs());
    }

    char * generateSearchString(size_t len)
    {
        char * target = new char[len+1];
        fillSearchString(target, len);
        target[len] = 0;
        return target;
    }

    void fillSearchString(char * target, size_t len)
    {
        for (unsigned repeat=0; ; repeat++)
        {
            for (unsigned char fill = 'a'; fill <= 'z'; fill++)
            {
                for (unsigned i=0; i < repeat; i++)
                {
                    *target++ = fill;
                    if (--len == 0)
                        return;
                }
            }
        }
    }

    void testPatterns(const char * search, const char * const * patterns)
    {
        for (const char * const * cur = patterns; *cur; cur++)
        {
            const char * pattern = *cur;
            bool expected = true;
            bool nocase = false;
            if (*pattern == '!')
            {
                expected = false;
                pattern++;
            }
            if (*pattern == '~')
            {
                nocase = true;
                pattern++;
            }
            bool evaluated = WildMatch(search, pattern, nocase);
            CPPUNIT_ASSERT_EQUAL_MESSAGE(pattern, expected, evaluated);
        }
    }
};

const char * const patterns10 [] = {
        "!a",
        "abcdefghij",
        "??????????",
        "?*c?*e*",
        "!??*b?*h*",
        "a*",
        "*j",
        "a*j",
        "a**j",
        "a***************j",
        "abcde*fghij",
        "!abcde*?*fghij",
        "*a*j*",
        "*a*c*e*g*j*",
        "a?c?e?g??j",
        "a?c?e?g?*?j",
        "!~A",
        "!A*",
        "~A*",
        "~*J",
        "~A*J",
        "~A**J",
        "~A***************J",
        "~*A*J*",
        "~*A*C*E*G*J*",
        "~*A*B*C*D*E*F*G*H*I*J*",
        "~*A*?*?*?*J*",
        "~*A*?C*?E*?*J*",
        "~*A*C?*E?*?*J*",
        "!~*A*.B*C*D*E*F*G*H*I*J*",
        nullptr
};

const char * const patterns100 [] = {
        "a*",
        "*h",
        "a*h",
        "a**h",
        "a***************h",
        "*a*j*",
        "*a*c*e*g*j*",
        "!a*jj*fff",
        "!a*jj*zzz",
        "a*jj*fff*",
        "*aa*jj*fff*",
        "!a*jj*zy*",
        nullptr
};

const char * const patternsLarge [] = {
        "!*a*zy*",
        "a*",
        "a*h*",
        "!a*jj*ab",
        "!a*jj*zy",
        "a*jj*fff*",
        "!a*jj*zy*",
/*        "!a*c*e*g*i*k*zy*", will completely destroy the performance*/
        nullptr
};

class JlibWildMatchCore : public JlibWildMatchBase
{
    CPPUNIT_TEST_SUITE(JlibWildMatchCore);
        CPPUNIT_TEST(testWildMatch);
    CPPUNIT_TEST_SUITE_END();

public:
    void testWildMatch()
    {
        testSet(10, patterns10, false);
        testSet(100, patterns100, false);
        testSet(1000, patternsLarge, false);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibWildMatchCore);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibWildMatchCore, "JlibWildMatchCore");


class JlibWildMatchTiming : public JlibWildMatchBase
{
    CPPUNIT_TEST_SUITE(JlibWildMatchTiming);
        CPPUNIT_TEST(testWildMatch);
    CPPUNIT_TEST_SUITE_END();

public:
    void testWildMatch()
    {
        testSet(10000, patternsLarge, true);
        testSet(100000, patternsLarge, true);
        testSet(1000000, patternsLarge, true);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibWildMatchTiming);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibWildMatchTiming, "JlibWildMatchTiming");


const EnumMapping mapping[] = {
        { 1, "one" },
        { 3, "three" },
        { 5, "five" },
        {0, nullptr }
};
const char * strings[] = { "zero", "one", "two", "three", "four", nullptr };
class JlibMapping : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibMapping);
        CPPUNIT_TEST(testEnum);
        CPPUNIT_TEST(testMatch);
    CPPUNIT_TEST_SUITE_END();

public:
    void testEnum()
    {
        CPPUNIT_ASSERT(streq("one", getEnumText(1, mapping)));
        CPPUNIT_ASSERT(streq("three", getEnumText(3, mapping)));
        CPPUNIT_ASSERT(streq("five", getEnumText(5, mapping)));
        CPPUNIT_ASSERT(streq("two", getEnumText(2, mapping, "two")));
        CPPUNIT_ASSERT(!getEnumText(2, mapping, nullptr));
        CPPUNIT_ASSERT_EQUAL(1, getEnum("one", mapping));
        CPPUNIT_ASSERT_EQUAL(3, getEnum("three", mapping));
        CPPUNIT_ASSERT_EQUAL(5, getEnum("five", mapping));
        CPPUNIT_ASSERT_EQUAL(99, getEnum("seven", mapping, 99));
    }
    void testMatch()
    {
        CPPUNIT_ASSERT_EQUAL(0U, matchString("zero", strings));
        CPPUNIT_ASSERT_EQUAL(1U, matchString("one", strings));
        CPPUNIT_ASSERT_EQUAL(4U, matchString("four", strings));
        CPPUNIT_ASSERT_EQUAL(UINT_MAX, matchString("ten", strings));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibMapping);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibMapping, "JlibMapping");

class JlibIPTTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibIPTTest);
        CPPUNIT_TEST(test);
        CPPUNIT_TEST(testMarkup);
        CPPUNIT_TEST(testRootArrayMarkup);
        CPPUNIT_TEST(testArrayMarkup);
        CPPUNIT_TEST(testMergeConfig);
        CPPUNIT_TEST(testRemoveReuse);
        CPPUNIT_TEST(testSpecialTags);
    CPPUNIT_TEST_SUITE_END();

public:
    void testArrayMarkup()
    {
            static constexpr const char * yamlFlowMarkup = R"!!({a: {
      b: valb,
      c: [valc],
      d: [vald1,vald2],
      e: [{x: valex1, y: valey1}],
      f: {x: valfx1, y: valfy1},
      g: [{x: valgx1, y: valgy1},{x: valgx2, y: valgy2}],
      h: !el valh,
      i: {
        j: {
          b: valb,
          c: [valc],
          d: [vald1,vald2],
          e: [{x: valex1, y: valey1}],
          f: {x: valfx1, y: valfy1},
          g: [{x: valgx1, y: valgy1},{x: valgx2, y: valgy2}],
          h: !el valh,
        },
        k: [{
          b: valb,
          c: [valc],
          d: [vald1,vald2],
          e: [{x: valex1, y: valey1}],
          f: {x: valfx1, y: valfy1},
          g: [{x: valgx1, y: valgy1},{x: valgx2, y: valgy2}],
          h: !el valh,
          }],
        l: [{
              b: valb,
              c: [valc],
              d: [vald1,vald2],
              e: [{x: valex1, y: valey1}],
              f: {x: valfx1, y: valfy1},
              g: [{x: valgx1, y: valgy1},{x: valgx2, y: valgy2}],
              h: !el valh,
          },
          {
              b: valb,
              c: [valc],
              d: [vald1,vald2],
              e: [{x: valex1, y: valey1}],
              f: {x: valfx1, y: valfy1},
              g: [{x: valgx1, y: valgy1},{x: valgx2, y: valgy2}],
              h: !el valh,
          }],
      }
    }
    }
    )!!";

            static constexpr const char * yamlBlockMarkup = R"!!(a:
  b: valb
  c:
  - valc
  d:
  - vald1
  - vald2
  e:
  - x: valex1
    y: valey1
  f:
    x: valfx1
    y: valfy1
  g:
  - x: valgx1
    y: valgy1
  - x: valgx2
    y: valgy2
  h: !el valh
  i:
    j:
      b: valb
      c:
      - valc
      d:
      - vald1
      - vald2
      e:
      - x: valex1
        y: valey1
      f:
        x: valfx1
        y: valfy1
      g:
      - x: valgx1
        y: valgy1
      - x: valgx2
        y: valgy2
      h: !el valh
    k:
    - b: valb
      c:
      - valc
      d:
      - vald1
      - vald2
      e:
      - x: valex1
        y: valey1
      f:
        x: valfx1
        y: valfy1
      g:
      - x: valgx1
        y: valgy1
      - x: valgx2
        y: valgy2
      h: !el valh
    l:
    - b: valb
      c:
      - valc
      d:
      - vald1
      - vald2
      e:
      - x: valex1
        y: valey1
      f:
        x: valfx1
        y: valfy1
      g:
      - x: valgx1
        y: valgy1
      - x: valgx2
        y: valgy2
      h: !el valh
    - b: valb
      c:
      - valc
      d:
      - vald1
      - vald2
      e:
      - x: valex1
        y: valey1
      f:
        x: valfx1
        y: valfy1
      g:
      - x: valgx1
        y: valgy1
      - x: valgx2
        y: valgy2
      h: !el valh
)!!";

            StringBuffer ml;

            Owned<IPropertyTree> yamlFlow = createPTreeFromYAMLString(yamlFlowMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);
            toYAML(yamlFlow, ml.clear(), 0, YAML_SortTags|YAML_HideRootArrayObject);
            CPPUNIT_ASSERT(streq(ml, yamlBlockMarkup));

            Owned<IPropertyTree> yamlBlock = createPTreeFromYAMLString(yamlBlockMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);
            toYAML(yamlBlock, ml.clear(), 0, YAML_SortTags|YAML_HideRootArrayObject);
            CPPUNIT_ASSERT(streq(ml, yamlBlockMarkup));
        }

    void testMergeConfig()
    {
            static constexpr const char * yamlLeft = R"!!({a: {
      b: gone,
      bk: kept,
      c: [gone],
      ck: [kept],
      d: [gone1,gone2],
      dk: [kept1,kept2],
      e: [{name: merged, x: gone, z: kept},{altname: merged, x: gone, z: kept},{name: kept, x: kept, y: kept}, {unnamed: kept, x: kept, y: kept}],
      ek: [{name: kept, x: kept, y: kept}, {unnamed: kept, x: kept, y: kept}],
      f: [{unnamed: gone, x: gone, y: gone}, {unnamed: gone2, x: gone2, y: gone2}],
      kept: {x: kept, y: kept},
      merged: {x: gone, z: kept}
      }
    }
)!!";

            static constexpr const char * yamlBlockLeft = R"!!(a:
  b: gone
  bk: kept
  c:
  - gone
  ck:
  - kept
  d:
  - gone1
  - gone2
  dk:
  - kept1
  - kept2
  e:
  - name: merged
    x: gone
    z: kept
  - altname: merged
    x: gone
    z: kept
  - name: kept
    x: kept
    y: kept
  - unnamed: kept
    x: kept
    y: kept
  ek:
  - name: kept
    x: kept
    y: kept
  - unnamed: kept
    x: kept
    y: kept
  f:
  - unnamed: gone
    x: gone
    y: gone
  - unnamed: gone2
    x: gone2
    y: gone2
  kept:
    x: kept
    y: kept
  merged:
    x: gone
    z: kept
)!!";

            static constexpr const char * yamlRight = R"!!({a: {
      b: updated,
      c: [added],
      d: [added1,added2],
      e: [{name: merged, x: updated, y: added},{altname: merged, x: updated, y: added},{name: added, x: added, y: added}, {unnamed: added, x: added, y: added}],
      f: [{unnamed: kept, x: kept, y: kept}, {unnamed: kept2, x: kept2, y: kept2}],
      added: {x: added, y: added},
      merged: {x: updated, y: added}
      }
    }
)!!";

            static constexpr const char * yamlBlockRight = R"!!(a:
  b: updated
  c:
  - added
  d:
  - added1
  - added2
  e:
  - name: merged
    x: updated
    y: added
  - altname: merged
    x: updated
    y: added
  - name: added
    x: added
    y: added
  - unnamed: added
    x: added
    y: added
  f:
  - unnamed: kept
    x: kept
    y: kept
  - unnamed: kept2
    x: kept2
    y: kept2
  added:
    x: added
    y: added
  merged:
    x: updated
    y: added
)!!";

            static constexpr const char * yamlMerged = R"!!(a:
  b: updated
  bk: kept
  added:
    x: added
    y: added
  c:
  - added
  ck:
  - kept
  d:
  - added1
  - added2
  dk:
  - kept1
  - kept2
  e:
  - name: merged
    x: updated
    y: added
    z: kept
  - altname: merged
    x: updated
    y: added
    z: kept
  - name: kept
    x: kept
    y: kept
  - unnamed: kept
    x: kept
    y: kept
  - name: added
    x: added
    y: added
  - unnamed: added
    x: added
    y: added
  ek:
  - name: kept
    x: kept
    y: kept
  - unnamed: kept
    x: kept
    y: kept
  f:
  - unnamed: kept
    x: kept
    y: kept
  - unnamed: kept2
    x: kept2
    y: kept2
  kept:
    x: kept
    y: kept
  merged:
    x: updated
    y: added
    z: kept
)!!";

        StringBuffer ml;

        Owned<IPropertyTree> treeLeft = createPTreeFromYAMLString(yamlLeft, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> treeRight = createPTreeFromYAMLString(yamlRight, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        mergeConfiguration(*treeLeft, *treeRight, "@altname");
        toYAML(treeLeft, ml.clear(), 0, YAML_SortTags|YAML_HideRootArrayObject);
        CPPUNIT_ASSERT(streq(ml, yamlMerged));

        Owned<IPropertyTree> treeBlockLeft = createPTreeFromYAMLString(yamlBlockLeft, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> treeBlockRight = createPTreeFromYAMLString(yamlBlockRight, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        mergeConfiguration(*treeBlockLeft, *treeBlockRight, "@altname");
        toYAML(treeBlockLeft, ml.clear(), 0, YAML_SortTags|YAML_HideRootArrayObject);
        CPPUNIT_ASSERT(streq(ml, yamlMerged));
    }

    void testRootArrayMarkup()
    {
        static constexpr const char * xmlMarkup = R"!!(<__array__>
 <__item__ a="val1a" b="val2a"/>
 <__item__ a="val1b" b="val2b"/>
 <__item__ a="val1c" b="val2c"/>
</__array__>
)!!";

        static constexpr const char * jsonMarkup = R"!!([
 {
  "@a": "val1a",
  "@b": "val2a"
 },
 {
  "@a": "val1b",
  "@b": "val2b"
 },
 {
  "@a": "val1c",
  "@b": "val2c"
 }
])!!";

        static constexpr const char * yamlMarkup = R"!!(- a: val1a
  b: val2a
- a: val1b
  b: val2b
- a: val1c
  b: val2c
)!!";

        Owned<IPropertyTree> xml = createPTreeFromXMLString(xmlMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> yaml = createPTreeFromYAMLString(yamlMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> json = createPTreeFromJSONString(jsonMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);

        CPPUNIT_ASSERT(areMatchingPTrees(xml, json));
        CPPUNIT_ASSERT(areMatchingPTrees(xml, yaml));

        StringBuffer ml;
        toXML(xml, ml, 0, XML_Format|XML_SortTags);
        CPPUNIT_ASSERT(streq(ml, xmlMarkup));

        toYAML(xml, ml.clear(), 0, YAML_SortTags|YAML_HideRootArrayObject);
        CPPUNIT_ASSERT(streq(ml, yamlMarkup));

        toJSON(xml, ml.clear(), 0, JSON_Format|JSON_SortTags|JSON_HideRootArrayObject);
        CPPUNIT_ASSERT(streq(ml, jsonMarkup));
    }
    void testMarkup()
    {
        static constexpr const char * xmlMarkup = R"!!(  <__object__ attr1="attrval1" attr2="attrval2">
   <binmixed bin="1" xsi:type="SOAP-ENC:base64">
    CwAAAA==   </binmixed>
   <binsimple xsi:type="SOAP-ENC:base64">
    CwAAAA==   </binsimple>
   <element1>scalarvalue</element1>
   <item a="1"
         b="2"
         c="3"
         d="4"/>
   <item a="2"/>
   <item a="3"/>
   <scalars>
    <valX>x</valX>
    <valX>x</valX>
    <valY>y</valY>
    <valY>y</valY>
    <valZ>z</valZ>
   </scalars>
   <sub1 subattr1="sav1">
    sub1val
   </sub1>
   <sub2 subattr2="sav2">
    sub2val
   </sub2>
   <subX subattr3="sav3">
    subXval
   </subX>
   cpptestval
  </__object__>
)!!";
        static constexpr const char * yamlMarkup = R"!!(attr1: attrval1
attr2: attrval2
binmixed:
  bin: 1
  ^: !binary |-
    CwAAAA==
binsimple: !binary |-
  CwAAAA==
element1: !el scalarvalue
item:
- a: 1
  b: 2
  c: 3
  d: 4
- a: 2
- a: 3
scalars:
  valX:
  - x
  - x
  valY:
  - y
  - y
  valZ: !el z
sub1:
  subattr1: sav1
  ^: !el sub1val
sub2:
  subattr2: sav2
  ^: !el sub2val
subX:
  subattr3: sav3
  ^: !el subXval
^: !el cpptestval
)!!";

        static constexpr const char * jsonMarkup = R"!!({
   "@attr1": "attrval1",
   "@attr2": "attrval2",
   "binmixed": {
    "@bin": "1",
    "#valuebin": "CwAAAA=="
   },
   "binsimple": {
    "#valuebin": "CwAAAA=="
   },
   "element1": "scalarvalue",
   "item": [
    {
     "@a": "1",
     "@b": "2",
     "@c": "3",
     "@d": "4"
    },
    {
     "@a": "2"
    },
    {
     "@a": "3"
    }
   ],
   "scalars": {
    "valX": [
     "x",
     "x"
    ],
    "valY": [
     "y",
     "y"
    ],
    "valZ": "z"
   },
   "sub1": {
    "@subattr1": "sav1",
    "#value": "sub1val"
   },
   "sub2": {
    "@subattr2": "sav2",
    "#value": "sub2val"
   },
   "subX": {
    "@subattr3": "sav3",
    "#value": "subXval"
   },
   "#value": "cpptestval"
  })!!";

        Owned<IPropertyTree> xml = createPTreeFromXMLString(xmlMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> yaml = createPTreeFromYAMLString(yamlMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);
        Owned<IPropertyTree> json = createPTreeFromJSONString(jsonMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);

        CPPUNIT_ASSERT(areMatchingPTrees(xml, yaml));
        CPPUNIT_ASSERT(areMatchingPTrees(xml, json));

        //if we want the final compares to be less fragile (test will have to be updated if formatting changes) we could reparse and compare trees again
        StringBuffer ml;
        toXML(xml, ml, 2, XML_Format|XML_SortTags);
        CPPUNIT_ASSERT(streq(ml, xmlMarkup));

        toYAML(yaml, ml.clear(), 2, YAML_SortTags|YAML_HideRootArrayObject);
        CPPUNIT_ASSERT(streq(ml, yamlMarkup));

        toJSON(json, ml.clear(), 2, JSON_Format|JSON_SortTags|JSON_HideRootArrayObject);
        CPPUNIT_ASSERT(streq(ml, jsonMarkup));
    }
    void test()
    {
        Owned<IPropertyTree> testTree = createPTreeFromXMLString(
                "<cpptest attr1='attrval1' attr2='attrval2'>"
                " <sub1 subattr1='sav1'>sub1val</sub1>"
                " <sub2 subattr2='sav2'>sub2val</sub2>"
                " <subX subattr3='sav3'>subXval</subX>"
                " <item a='1' b='2' c='3' d='4'/>"
                " <item a='2'/>"
                " <item a='3'/>"
                " <array>"
                "  <valX>x</valX>"
                "  <valX>x</valX>"
                "  <valY>y</valY>"
                "  <valY>y</valY>"
                "  <valZ>z</valZ>"
                " </array>"
                " <binprop bin='1' xsi:type='SOAP-ENC:base64'>CwAAAA==</binprop>"
                "cpptestval"
                "</cpptest>");
        MemoryBuffer mb;
        byte * data = (byte *)mb.reserveTruncate(4*1024+1); // Must be > PTREE_COMPRESS_THRESHOLD (see top of jptree.cpp)
        for (unsigned i=0; i < mb.length(); i++)
            data[i] = (byte)i;

        testTree->addProp("binprop/subbinprop", "nonbinval1");
        testTree->addPropBin("binprop/subbinprop", mb.length(), mb.toByteArray());
        testTree->addProp("binprop/subbinprop", "nonbinval2");
        testTree->addPropBin("binprop/subbinprop", mb.length(), mb.toByteArray());

        // test some sets in prep. for 'get' tests
        CPPUNIT_ASSERT(testTree->renameProp("subX", "subY"));
        IPropertyTree *subY = testTree->queryPropTree("subY");
        CPPUNIT_ASSERT(testTree->renameTree(subY, "sub3"));

        IPropertyTree *subtest = testTree->setPropTree("subtest");
        subtest = testTree->addPropTree("subtest", createPTree());
        CPPUNIT_ASSERT(subtest != nullptr);
        subtest = testTree->queryPropTree("subtest[2]");
        CPPUNIT_ASSERT(subtest != nullptr);
        subtest->setProp("str", "str1");
        subtest->addProp("str", "str2");
        subtest->appendProp("str[2]", "-more");

        subtest->setPropBool("bool", true);
        subtest->addPropBool("bool", false);

        subtest->setPropInt("int", 1);
        subtest->addPropInt("int", 2);

        subtest->setPropInt64("int64", 1);
        subtest->addPropInt64("int64", 2);

        mb.clear().append("binstr1");
        subtest->setPropBin("bin", mb.length(), mb.toByteArray());
        mb.clear().append("binstr2");
        subtest->addPropBin("bin", mb.length(), mb.toByteArray());
        mb.clear().append("-more");
        subtest->appendPropBin("bin[1]", mb.length(), mb.toByteArray());


        // position insertion.
        testTree->addProp("newprop", "v1");
        testTree->addProp("newprop", "v2");
        testTree->addProp("newprop[2]", "v3");
        CPPUNIT_ASSERT(streq("v3", testTree->queryProp("newprop[2]")));

        CPPUNIT_ASSERT(testTree->hasProp("sub1"));
        CPPUNIT_ASSERT(testTree->hasProp("sub1/@subattr1"));
        CPPUNIT_ASSERT(testTree->hasProp("sub2/@subattr2"));
        CPPUNIT_ASSERT(testTree->hasProp("@attr1"));
        CPPUNIT_ASSERT(!testTree->isBinary("@attr1"));
        CPPUNIT_ASSERT(!testTree->isBinary("sub1"));
        CPPUNIT_ASSERT(testTree->isBinary("binprop"));
        CPPUNIT_ASSERT(!testTree->isCompressed("binprop"));
        CPPUNIT_ASSERT(!testTree->isBinary("binprop/subbinprop[1]"));
        CPPUNIT_ASSERT(testTree->isBinary("binprop/subbinprop[2]"));
        CPPUNIT_ASSERT(!testTree->isCompressed("binprop/subbinprop[3]"));
        CPPUNIT_ASSERT(testTree->isCompressed("binprop/subbinprop[4]"));

        // testing if subX was renamed correctly
        CPPUNIT_ASSERT(!testTree->hasProp("subX"));
        CPPUNIT_ASSERT(!testTree->hasProp("subY"));
        CPPUNIT_ASSERT(testTree->hasProp("sub3"));

        StringBuffer astr;
        CPPUNIT_ASSERT(testTree->getProp("sub1", astr));
        CPPUNIT_ASSERT(streq("sub1val", astr.str()));
        CPPUNIT_ASSERT(streq("sub2val", testTree->queryProp("sub2")));

        subtest = testTree->queryPropTree("subtest[2]");
        CPPUNIT_ASSERT(subtest != nullptr);

        CPPUNIT_ASSERT(subtest->getPropBool("bool[1]"));
        CPPUNIT_ASSERT(!subtest->getPropBool("bool[2]"));

        CPPUNIT_ASSERT(1 == subtest->getPropInt("int[1]"));
        CPPUNIT_ASSERT(2 == subtest->getPropInt("int[2]"));

        CPPUNIT_ASSERT(1 == subtest->getPropInt64("int64[1]"));
        CPPUNIT_ASSERT(2 == subtest->getPropInt64("int64[2]"));

        subtest->getPropBin("bin[1]", mb.clear());
        const char *ptr = (const char *)mb.toByteArray();
        CPPUNIT_ASSERT(streq("binstr1", ptr)); // NB: null terminator was added at set time.
        CPPUNIT_ASSERT(streq("-more", ptr+strlen("binstr1")+1)); // NB: null terminator was added at set time.
        subtest->getPropBin("bin[2]", mb.clear());
        CPPUNIT_ASSERT(streq("binstr2", mb.toByteArray())); // NB: null terminator was added at set time.

        CPPUNIT_ASSERT(testTree->hasProp("subtest/bin[2]"));
        CPPUNIT_ASSERT(testTree->removeProp("subtest/bin[2]"));
        CPPUNIT_ASSERT(!testTree->hasProp("subtest/bin[2]"));

        CPPUNIT_ASSERT(testTree->hasProp("subtest"));
        CPPUNIT_ASSERT(testTree->removeTree(subtest)); // this is subtest[2]
        subtest = testTree->queryPropTree("subtest"); // now just 1
        CPPUNIT_ASSERT(testTree->removeTree(subtest));
        CPPUNIT_ASSERT(!testTree->hasProp("subtest"));

        IPropertyTree *item3 = testTree->queryPropTree("item[@a='3']");
        CPPUNIT_ASSERT(nullptr != item3);
        CPPUNIT_ASSERT(2 == testTree->queryChildIndex(item3));

        CPPUNIT_ASSERT(streq("item", item3->queryName()));

        Owned<IPropertyTreeIterator> iter = testTree->getElements("item");
        unsigned a=1;
        ForEach(*iter)
        {
            CPPUNIT_ASSERT(a == iter->query().getPropInt("@a"));
            ++a;
        }

        Owned<IAttributeIterator> attrIter = testTree->queryPropTree("item[1]")->getAttributes();
        CPPUNIT_ASSERT(4 == attrIter->count());
        unsigned i = 0;
        ForEach(*attrIter)
        {
            const char *name = attrIter->queryName();
            const char *val = attrIter->queryValue();
            CPPUNIT_ASSERT('a'+i == *(name+1));
            CPPUNIT_ASSERT('1'+i == *val);
            ++i;
        }

        IPropertyTree *array = testTree->queryPropTree("array");
        CPPUNIT_ASSERT(array != nullptr);
        CPPUNIT_ASSERT(array->hasChildren());
        CPPUNIT_ASSERT(3 == array->numUniq());
        CPPUNIT_ASSERT(5 == array->numChildren());

        CPPUNIT_ASSERT(!testTree->isCaseInsensitive());
        CPPUNIT_ASSERT(3 == testTree->getCount("sub*"));

        testTree->addPropInt("newitem", 1);
        testTree->addPropInt("newitem", 2);
        testTree->addPropInt("./newitem", 3);
        testTree->addPropInt("././newitem", 4);

        Owned<IPropertyTreeIterator> xIter = testTree->getElements("./newitem");
        unsigned match=1;
        ForEach(*xIter)
        {
            CPPUNIT_ASSERT(match == xIter->query().getPropInt(nullptr));
            ++match;
        }
    }
    void testRemoveReuse()
    {
        Owned<IPropertyTree> t = createPTree();
        t->addPropInt("a", 1);
        t->addPropInt("a", 2);
        Owned<IPropertyTree> a1 = t->getPropTree("a[1]");
        Owned<IPropertyTree> a2 = t->getPropTree("a[2]");
        CPPUNIT_ASSERT(t->removeProp("a[2]"));
        CPPUNIT_ASSERT(t->removeProp("a"));
        CPPUNIT_ASSERT(!a1->isArray(nullptr));
        CPPUNIT_ASSERT(!a2->isArray(nullptr));
        IPropertyTree *na2 = t->addPropTree("na", a2.getClear());
        CPPUNIT_ASSERT(!na2->isArray(nullptr));
        IPropertyTree *na1 = t->addPropTree("na", a1.getClear());
        CPPUNIT_ASSERT(na1->isArray(nullptr));
        CPPUNIT_ASSERT(na2->isArray(nullptr));
    }
    void testPtreeEncode(const char *input, const char *expected=nullptr)
    {
        static unsigned id = 0;
        StringBuffer encoded;
        encodePTreeName(encoded, input);
        StringBuffer decoded;
        decodePtreeName(decoded, encoded.str());
        //DBGLOG("\nptree name[%d]: %s --> %s --> %s", id++, input, encoded.str(), decoded.str());
        CPPUNIT_ASSERT(streq(input, decoded.str()));
        if (expected)
        {
            CPPUNIT_ASSERT(streq(expected, encoded.str()));
        }
    }
    void testSpecialTags()
    {
        try {
        //Check a null tag is supported and preserved.
        static constexpr const char * jsonEmptyNameMarkup = R"!!({
   "": "default",
   "end": "x"
  })!!";

            Owned<IPropertyTree> json = createPTreeFromJSONString(jsonEmptyNameMarkup, ipt_none, ptr_ignoreWhiteSpace, nullptr);

            //if we want the final compares to be less fragile (test will have to be updated if formatting changes) we could reparse and compare trees again
            StringBuffer ml;
            toJSON(json, ml.clear(), 2, JSON_Format|JSON_SortTags|JSON_HideRootArrayObject);
            CPPUNIT_ASSERT(streq(ml, jsonEmptyNameMarkup));

            testPtreeEncode("@a/b*c=d\\e.f", "@a_fb_x2A_c_x3D_d_be.f");
            testPtreeEncode("A/B*C=D\\E.F", "A_fB_x2A_C_x3D_D_bE.F");
            testPtreeEncode("_x123_blue.berry", "__x123__blue.berry");
            testPtreeEncode("_>>_here_**", "___x3E3E___here___x2A2A_");
            testPtreeEncode("出/售\\耐\"'久", "出_f售_b耐_Q_q久");
            testPtreeEncode("@出售'\"耐久", "@_xE587BAE594AE__q_Q_xE88090E4B985_");
            testPtreeEncode("出售耐久", "出售耐久");
            testPtreeEncode("@出/售\\耐'久", "@_xE587BA__f_xE594AE__b_xE88090__q_xE4B985_");
            testPtreeEncode("出/售\\耐'久", "出_f售_b耐_q久");
            testPtreeEncode("space space", "space_sspace");

            static constexpr const char * jsonMarkup = R"!!(
{
    "Nothing_toEncodeHere": {
        "@attributes_need_unicode_encoded_出售耐久": "encode",
        "but_elements_support_it_出售耐久": "no encode",
        "e出\\n售耐/久@Aa_a": {
        "@\\naa_bb食品并/林": "atvalue",
        "abc/d @ \"e'f": ["value"],
        "": "noname"
        }
    }
}
)!!";

            static constexpr const char * yamlMarkup = R"!!(
Nothing_toEncodeHere:
  attributes_need_unicode_encoded_出售耐久: "encode"
  but_elements_support_it_出售耐久: ["no encode"]
  "e出\\n售耐/久@Aa_a":
    "\\naa_bb食品并/林": "atvalue"
    "abc/d @ \"e'f": ["value"]
    "": [noname]
)!!";

            static constexpr const char * toXMLOutput = R"!!(<Nothing_toEncodeHere attributes__need__unicode__encoded___xE587BAE594AEE88090E4B985_="encode">
 <but_elements_support_it_出售耐久>no encode</but_elements_support_it_出售耐久>
 <e出_bn售耐_f久_aAa__a _bnaa__bb_xE9A39FE59381E5B9B6__f_xE69E97_="atvalue">
  <_0>noname</_0>
  <abc_fd_s_a_s_Qe_qf>value</abc_fd_s_a_s_Qe_qf>
 </e出_bn售耐_f久_aAa__a>
</Nothing_toEncodeHere>
)!!";

            static constexpr const char * toJSONOutput = R"!!({
 "Nothing_toEncodeHere": {
  "@attributes_need_unicode_encoded_出售耐久": "encode",
  "but_elements_support_it_出售耐久": "no encode",
  "e出\n售耐/久@Aa_a": {
   "@\naa_bb食品并/林": "atvalue",
   "": "noname",
   "abc/d @ "e'f": "value"
  }
 }
})!!";

            static constexpr const char * toYAMLOutput = R"!!(Nothing_toEncodeHere:
  attributes_need_unicode_encoded_出售耐久: encode
  but_elements_support_it_出售耐久:
  - no encode
  e出\n售耐/久@Aa_a:
    \naa_bb食品并/林: atvalue
    '':
    - noname
    abc/d @ "e'f:
    - value
)!!";

            Owned<IPropertyTree> jsonTree = createPTreeFromJSONString(jsonMarkup);
            IPropertyTree *xmlRoot = jsonTree->queryPropTree("*[1]");
            toXML(xmlRoot, ml.clear(), 0, XML_SortTags|XML_Format);
            //printf("\nJSONXML:\n%s\n", ml.str());
            CPPUNIT_ASSERT(streq(toXMLOutput, ml.str()));
            toJSON(jsonTree, ml.clear(), 0, JSON_SortTags|JSON_HideRootArrayObject|JSON_Format);
            //printf("\nJSON:\n%s\n", ml.str());
            CPPUNIT_ASSERT(streq(toJSONOutput, ml.str()));

            Owned<IPropertyTree> yamlTree = createPTreeFromYAMLString(yamlMarkup);
            xmlRoot = jsonTree->queryPropTree("*[1]");
            toXML(xmlRoot, ml.clear(), 0, XML_SortTags|XML_Format);
            //printf("\nYAMLXML:\n%s\n", ml.str());
            CPPUNIT_ASSERT(streq(toXMLOutput, ml.str()));
            toYAML(yamlTree, ml.clear(), 0, YAML_SortTags|YAML_HideRootArrayObject);
            //printf("\nYAML:\n%s\n", ml.str());
            CPPUNIT_ASSERT(streq(toYAMLOutput, ml.str()));

//build xpath test
            static constexpr const char * jsonMarkupForXpathTest = R"!!(
{
  "A": {
    "B": {
      "@a/b.@100": "encoded attribute found",
      "x\\y~Z@W": "encoded element found",
      "rst": "element found",
      "q_n_a": "underscore element found",
      "element出": "xpath does not support unicode?"
        }
    }
}
)!!";

            Owned<IPropertyTree> jsonTreeForXpathTest = createPTreeFromJSONString(jsonMarkupForXpathTest);

            StringBuffer xpath;
            appendPTreeXPathName(xpath.set("A/B/"), "@a/b.@100"); //attribute will be encoded
            const char *val = jsonTreeForXpathTest->queryProp(xpath);
            CPPUNIT_ASSERT(val && streq(val, "encoded attribute found")); //xpath works for unicode here because for attributes it gets encoded

            appendPTreeXPathName(xpath.set("A/B/"), "x\\y~Z@W"); //element will be encoded
            val = jsonTreeForXpathTest->queryProp(xpath);
            CPPUNIT_ASSERT(val && streq(val, "encoded element found"));

            appendPTreeXPathName(xpath.set("A/B/"), "rst"); //will not be encoded
            val = jsonTreeForXpathTest->queryProp(xpath);
            CPPUNIT_ASSERT(val && streq(val, "element found"));

            appendPTreeXPathName(xpath.set("A/B/"), "q_n_a"); //will not be encoded
            val = jsonTreeForXpathTest->queryProp(xpath);
            CPPUNIT_ASSERT(val && streq(val, "underscore element found"));

            appendPTreeXPathName(xpath.set("A/B/"), "element出"); //will not be encoded (elements support unicode)
            val = jsonTreeForXpathTest->queryProp(xpath);
            CPPUNIT_ASSERT(!val); //PTree can hold unicode in element names but perhaps xpath can not access it?
        }
        catch (IException *e)
        {
            StringBuffer msg;
            printf("\nPTREE: Exception %d - %s", e->errorCode(), e->errorMessage(msg).str());
            EXCLOG(e, nullptr);
            throw;
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibIPTTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibIPTTest, "JlibIPTTest");



#include "jdebug.hpp"
#include "jmutex.hpp"


class AtomicTimingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(AtomicTimingTest);
        CPPUNIT_TEST(runAllTests);
    CPPUNIT_TEST_SUITE_END();

public:

    class CasCounter
    {
    public:
        CasCounter() = default;
        CasCounter(unsigned __int64 _value) : value{_value} {}

        operator unsigned __int64() { return value; }
        unsigned __int64 operator = (unsigned __int64 _value)
        {
            value = _value;
            return _value;
        }

        unsigned __int64 operator ++(int)
        {
            unsigned __int64 expected = value.load();
            while (!value.compare_exchange_weak(expected, expected + 1))
            {
            }
            return expected+1;
        }

        std::atomic<unsigned __int64> value = { 0 };
    };

    template <typename LOCK, typename BLOCK, typename COUNTER, unsigned NUMVALUES, unsigned NUMLOCKS>
    class LockTester
    {
    public:
        LockTester()
        {
            value1 = 0;
        }

        class LockTestThread : public Thread
        {
        public:
            LockTestThread(Semaphore & _startSem, Semaphore & _endSem, LOCK & _lock1, COUNTER & _value1, LOCK & _lock2, COUNTER * _extraValues, unsigned _numIterations)
                : startSem(_startSem), endSem(_endSem),
                  lock1(_lock1), value1(_value1),
                  lock2(_lock2), extraValues(_extraValues),
                  numIterations(_numIterations)
            {
            }

            virtual void execute()
            {
                {
                    BLOCK block(lock1);
                    value1++;
                    if (NUMVALUES >= 2)
                        extraValues[1]++;
                    if (NUMVALUES >= 3)
                        extraValues[2]++;
                    if (NUMVALUES >= 4)
                        extraValues[3]++;
                    if (NUMVALUES >= 5)
                        extraValues[4]++;
                }
                if (NUMLOCKS == 2)
                {
                    BLOCK block(lock2);
                    extraValues[1]++;
                }
            }

            virtual int run()
            {
                startSem.wait();
                for (unsigned i = 0; i < numIterations; i++)
                    execute();
                endSem.signal();
                return 0;
            }

        protected:
            Semaphore & startSem;
            Semaphore & endSem;
            LOCK & lock1;
            LOCK & lock2;
            COUNTER & value1;
            COUNTER * extraValues;
            const unsigned numIterations;
        };

        unsigned __int64 run(const char * title, unsigned numThreads, unsigned numIterations)
        {
            value1 = 0;
            for (unsigned ix = 1; ix < NUMVALUES; ix++)
                extraValues[ix] = 0;
            for (unsigned i = 0; i < numThreads; i++)
            {
                LockTestThread * next = new LockTestThread(startSem, endSem, lock, value1, lock, extraValues, numIterations);
                threads.append(*next);
                next->start();
            }

            cycle_t startCycles = get_cycles_now();
            startSem.signal(numThreads);
            for (unsigned i2 = 0; i2 < numThreads; i2++)
                endSem.wait();
            cycle_t endCycles = get_cycles_now();
            unsigned __int64 expected = (unsigned __int64)numIterations * numThreads;
            unsigned __int64 averageTime = cycle_to_nanosec(endCycles - startCycles) / (numIterations * numThreads);
            DBGLOG("%s@%u/%u threads(%u) %" I64F "uns/iteration lost(%" I64F "d)", title, NUMVALUES, NUMLOCKS, numThreads, averageTime, expected - value1);
            for (unsigned i3 = 0; i3 < numThreads; i3++)
                threads.item(i3).join();
            return averageTime;
        }

    protected:
        IArrayOf<LockTestThread> threads;
        Semaphore startSem;
        Semaphore endSem;
        LOCK lock;
        COUNTER value1;
        COUNTER extraValues[NUMVALUES];
    };

    #define DO_TEST(LOCK, CLOCK, COUNTER, NUMVALUES, NUMLOCKS)   \
    { \
        const char * title = #LOCK "," #COUNTER;\
        LockTester<LOCK, CLOCK, COUNTER, NUMVALUES, NUMLOCKS> tester;\
        uncontendedTimes.append(tester.run(title, 1, numIterations));\
        minorTimes.append(tester.run(title, 2, numIterations));\
        typicalTimes.append(tester.run(title, numCores / 2, numIterations));\
        tester.run(title, numCores, numIterations);\
        tester.run(title, numCores + 1, numIterations);\
        contendedTimes.append(tester.run(title, numCores * 2, numIterations));\
    }

    //Use to common out a test
    #define XDO_TEST(LOCK, CLOCK, COUNTER, NUMVALUES, NUMLOCKS)   \
    { \
        uncontendedTimes.append(0);\
        minorTimes.append(0);\
        typicalTimes.append(0);\
        contendedTimes.append(0);\
    }


    class Null
    {};

    const unsigned numIterations = 1000000;
    const unsigned numCores = std::max(getAffinityCpus(), 16U);
    void runAllTests()
    {
        DO_TEST(CriticalSection, CriticalBlock, unsigned __int64, 1, 1);
        DO_TEST(CriticalSection, CriticalBlock, unsigned __int64, 2, 1);
        DO_TEST(CriticalSection, CriticalBlock, unsigned __int64, 5, 1);
        DO_TEST(CriticalSection, CriticalBlock, unsigned __int64, 1, 2);
        DO_TEST(SpinLock, SpinBlock, unsigned __int64, 1, 1);
        DO_TEST(SpinLock, SpinBlock, unsigned __int64, 2, 1);
        DO_TEST(SpinLock, SpinBlock, unsigned __int64, 5, 1);
        DO_TEST(SpinLock, SpinBlock, unsigned __int64, 1, 2);
        DO_TEST(Null, Null, std::atomic<unsigned __int64>, 1, 1);
        DO_TEST(Null, Null, std::atomic<unsigned __int64>, 2, 1);
        DO_TEST(Null, Null, std::atomic<unsigned __int64>, 5, 1);
        DO_TEST(Null, Null, std::atomic<unsigned __int64>, 1, 2);
        DO_TEST(Null, Null, RelaxedAtomic<unsigned __int64>, 1, 1);
        DO_TEST(Null, Null, RelaxedAtomic<unsigned __int64>, 5, 1);
        DO_TEST(Null, Null, CasCounter, 1, 1);
        DO_TEST(Null, Null, CasCounter, 5, 1);
        DO_TEST(Null, Null, unsigned __int64, 1, 1);
        DO_TEST(Null, Null, unsigned __int64, 2, 1);
        DO_TEST(Null, Null, unsigned __int64, 5, 1);

        //Read locks will fail to prevent values being lost, but the timings are useful in comparison with CriticalSection
        DO_TEST(ReadWriteLock, ReadLockBlock, unsigned __int64, 1, 1);
        DO_TEST(ReadWriteLock, ReadLockBlock, unsigned __int64, 2, 1);
        DO_TEST(ReadWriteLock, ReadLockBlock, unsigned __int64, 5, 1);
        DO_TEST(ReadWriteLock, ReadLockBlock, unsigned __int64, 1, 2);
        DO_TEST(ReadWriteLock, WriteLockBlock, unsigned __int64, 1, 1);
        DO_TEST(ReadWriteLock, WriteLockBlock, unsigned __int64, 2, 1);
        DO_TEST(ReadWriteLock, WriteLockBlock, unsigned __int64, 5, 1);
        DO_TEST(ReadWriteLock, WriteLockBlock, unsigned __int64, 1, 2);

        DBGLOG("Summary");
        summariseTimings("Uncontended", uncontendedTimes);
        summariseTimings("Minor", minorTimes);
        summariseTimings("Typical", typicalTimes);
        summariseTimings("Over", contendedTimes);
    }

    void summariseTimings(const char * option, UInt64Array & times)
    {
        DBGLOG("%11s 1x: cs(%3" I64F "u) spin(%3" I64F "u) atomic(%3" I64F "u) ratomic(%3" I64F "u) cas(%3" I64F "u) rd(%3" I64F "u) wr(%3" I64F "u)   "
                    "5x: cs(%3" I64F "u) spin(%3" I64F "u) atomic(%3" I64F "u) ratomic(%3" I64F "u) cas(%3" I64F "u) rd(%3" I64F "u) wr(%3" I64F "u)", option,
                    times.item(0), times.item(4), times.item(8), times.item(12), times.item(14), times.item(19), times.item(23),
                    times.item(2), times.item(6), times.item(10), times.item(13), times.item(15), times.item(21), times.item(25));
    }

private:
    UInt64Array uncontendedTimes;
    UInt64Array minorTimes;
    UInt64Array typicalTimes;
    UInt64Array contendedTimes;
};

CPPUNIT_TEST_SUITE_REGISTRATION(AtomicTimingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(AtomicTimingTest, "AtomicTimingStressTest");


//=====================================================================================================================

class MachineInfoTimingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(MachineInfoTimingTest);
        CPPUNIT_TEST(runAllTests);
    CPPUNIT_TEST_SUITE_END();

public:

    void getSystemTiming()
    {
        const unsigned num = 10000;
        SystemInfo temp;
        CCycleTimer timer;
        for (unsigned i=0; i < num; i++)
            temp.update(ReadAllInfo);
        DBGLOG("Time to get system cpu activity = %" I64F "uns", timer.elapsedNs()/num);
    }
    void getProcessTiming()
    {
        const unsigned num = 10000;
        ProcessInfo temp;
        CCycleTimer timer;
        for (unsigned i=0; i < num; i++)
            temp.update(ReadAllInfo);
        DBGLOG("Time to get process cpu activity = %" I64F "uns", timer.elapsedNs()/num);
    }
    void runAllTests()
    {
        getSystemTiming();
        getSystemTiming(); // Second call seems to be faster - so more representative
        getProcessTiming();
        getProcessTiming(); // Second call seems to be faster - so more representative

        SystemInfo prevSystem;
        ProcessInfo prevProcess;
        ProcessInfo curProcess(ReadAllInfo);
        SystemInfo curSystem(ReadAllInfo);
        volatile unsigned x = 0;
        for (unsigned i=0; i < 10; i++)
        {
            prevProcess = curProcess;
            prevSystem = curSystem;
            curProcess.update(ReadAllInfo);
            curSystem.update(ReadAllInfo);
            SystemProcessInfo deltaProcess = curProcess - prevProcess;
            SystemProcessInfo deltaSystem = curSystem - prevSystem;
            if (deltaSystem.getTotalNs())
            {
                DBGLOG(" System: User(%u) System(%u) Total(%u) %u%% Ctx(%" I64F "u)  ",
                        (unsigned)(deltaSystem.getUserNs() / 1000000), (unsigned)(deltaSystem.getSystemNs() / 1000000), (unsigned)(deltaSystem.getTotalNs() / 1000000),
                        (unsigned)((deltaSystem.getUserNs() * 100) / deltaSystem.getTotalNs()), deltaSystem.getNumContextSwitches());
                DBGLOG(" Process: User(%u) System(%u) Total(%u) %u%% Ctx(%" I64F "u)",
                        (unsigned)(deltaProcess.getUserNs() / 1000000), (unsigned)(deltaProcess.getSystemNs() / 1000000), (unsigned)(deltaProcess.getTotalNs() / 1000000),
                        (unsigned)((deltaProcess.getUserNs() * 100) / deltaSystem.getTotalNs()), deltaProcess.getNumContextSwitches());
            }

            for (unsigned j=0; j < i*100000000; j++)
                x += j*j;
            Sleep(1000);
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(MachineInfoTimingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(MachineInfoTimingTest, "MachineInfoTimingTest");




class JlibIOTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibIOTest);
        CPPUNIT_TEST(test);
    CPPUNIT_TEST_SUITE_END();

public:
    void test()
    {
        unsigned numTestLines = 1000;

        const char *newlines[] = { "\n", "\r\n" };

        for (unsigned pEol=0; pEol<2; pEol++) // twice, once for preserveEols=false, once for preserveEols=true
        {
            for (unsigned nl=0; nl<2; nl++) // twice, once for each type of newline
            {
                const char *testTxt = " : Some random text for test line";
                OwnedIFile iFile = createIFile("JlibIOTest.txt");
                CRC32 writeCrc, readCrc;
                {
                    OwnedIFileIO iFileIO = iFile->open(IFOcreate);
                    OwnedIFileIOStream stream = createIOStream(iFileIO);
                    for (unsigned l=0; l<numTestLines; l++)
                    {
                        VStringBuffer line("%u%s%s", l+1, testTxt, newlines[nl]);
                        stream->write(line.length(), line.str());
                        writeCrc.tally(line.length(), line.str());
                    }
                }

                {
                    OwnedIFileIO iFileIO = iFile->open(IFOread);
                    OwnedIFileIOStream stream = createIOStream(iFileIO); // NB: unbuffered
                    Owned<IStreamLineReader> lineReader = createLineReader(stream, 0==pEol, strlen(testTxt)); // NB: deliberately make chunkSize small so will end up having to read more
                    while (true)
                    {
                        StringBuffer line;
                        if (!lineReader->readLine(line))
                        {
                            if (pEol==1)
                                line.append(newlines[nl]);
                            readCrc.tally(line.length(), line.str());
                        }
                        else
                            break;
                    }
                }
                CPPUNIT_ASSERT(writeCrc.get() == readCrc.get());
            }
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(JlibIOTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(JlibIOTest, "JlibIOTest");


class JlibCompressionTestsStress : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibCompressionTestsStress);
        CPPUNIT_TEST(test);
    CPPUNIT_TEST_SUITE_END();

    static constexpr size32_t sz = 100*0x100000; // 100MB
    enum CompressOpt { RowCompress, AllRowCompress, BlockCompress, CompressToBuffer };
public:
    void test()
    {
        try
        {
            MemoryBuffer src;
            src.ensureCapacity(sz);
            const char *aesKey = "012345678901234567890123";
            Owned<ICompressHandlerIterator> iter = getCompressHandlerIterator();

            StringBuffer tmp;
            unsigned card = 0;
            size32_t rowSz = 0;
            while (true)
            {
                size32_t cLen = src.length();
                if (cLen > sz)
                    break;
                src.append(cLen);
                tmp.clear().appendf("%10u", cLen);
                src.append(tmp.length(), tmp.str());
                src.append(++card % 52);
                src.append(crc32((const char *)&cLen, sizeof(cLen), 0));
                unsigned ccrc = crc32((const char *)&card, sizeof(card), 0);
                tmp.clear().appendf("%10u", ccrc);
                src.append(tmp.length(), tmp.str());
                tmp.clear().appendf("%20u", (++card % 10));
                src.append(tmp.length(), tmp.str());
                if (0 == rowSz)
                    rowSz = src.length();
                else
                {
                    dbgassertex(0 == (src.length() % rowSz));
                }
            }

            DBGLOG("Algorithm(options)  || Comp(ms) || Deco(ms) || 200MB/s (w,r)   || 1GB/s (w,r)     || 5GB/s (w,r)     || Ratio [cLen]");
            DBGLOG("                    ||          ||          || 2Gb/s           || 10Gb/s          || 50Gb/s          ||");

            unsigned time200MBs = transferTimeMs(sz, 200000000);
            unsigned time1GBs = transferTimeMs(sz, 1000000000);
            unsigned time5GBs = transferTimeMs(sz, 5000000000);
            DBGLOG("%19s || %8u || %8u || %4u(%4u,%4u) || %4u(%4u,%4u) || %4u(%4u,%4u) || %5.2f [%u]", "uncompressed", 0, 0,
                time200MBs, time200MBs, time200MBs, time1GBs, time1GBs, time1GBs, time5GBs, time5GBs, time5GBs, 1.0, sz);
            ForEach(*iter)
            {
                ICompressHandler &handler = iter->query();
                const char * type = handler.queryType();
                //Ignore unusual compressors with no expanders...
                if (strieq(type, "randrow"))
                    continue;
                const char * options = streq("AES", handler.queryType()) ? aesKey: "";
                if (streq(type, "LZ4HC"))
                {
                    testCompressor(handler, "hclevel=3", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=4", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=5", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=6", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=8", rowSz, src.length(), src.bytes(), RowCompress);
                    testCompressor(handler, "hclevel=10", rowSz, src.length(), src.bytes(), RowCompress);
                }
                testCompressor(handler, options, rowSz, src.length(), src.bytes(), RowCompress);
                testCompressor(handler, options, rowSz, src.length(), src.bytes(), CompressToBuffer);
                if (streq(type, "LZ4"))
                {
                    testCompressor(handler, "allrow", rowSz, src.length(), src.bytes(), AllRowCompress); // block doesn't affect the compressor, just tracing
                    testCompressor(handler, "block", rowSz, src.length(), src.bytes(), BlockCompress); // block doesn't affect the compressor, just tracing
                }
           }
        }
        catch (IException *e)
        {
            EXCLOG(e, nullptr);
            throw;
        }
    }

    unsigned transferTimeMs(__int64 size, __int64 bytesPerSecond)
    {
        return (unsigned)((size * 1000) / bytesPerSecond);
    }

    void testCompressor(ICompressHandler &handler, const char * options, size32_t rowSz, size32_t srcLen, const byte * src, CompressOpt opt)
    {
        Owned<ICompressor> compressor = handler.getCompressor(options);

        MemoryBuffer compressed;
        CCycleTimer timer;
        const byte * ptr = src;
        switch (opt)
        {
            case RowCompress:
            {
                compressor->open(compressed, sz);
                compressor->startblock();
                const byte *ptrEnd = ptr + srcLen;
                while (ptr != ptrEnd)
                {
                    compressor->write(ptr, rowSz);
                    ptr += rowSz;
                }
                compressor->commitblock();
                compressor->close();
                break;
            }
            case AllRowCompress:
            {
                compressor->open(compressed, sz);
                compressor->startblock();
                compressor->write(ptr, sz);
                compressor->commitblock();
                compressor->close();
                break;
            }
            case BlockCompress:
            {
                void * target = compressed.reserve(sz);
                unsigned written = compressor->compressBlock(sz, target, srcLen, ptr);
                compressed.setLength(written);
                break;
            }
            case CompressToBuffer:
            {
                compressToBuffer(compressed, srcLen, ptr, handler.queryMethod(), options);
                break;
            }
        }

        cycle_t compressCycles = timer.elapsedCycles();
        Owned<IExpander> expander = handler.getExpander(options);
        MemoryBuffer tgt;
        timer.reset();
        if (opt==CompressToBuffer)
        {
            decompressToBuffer(tgt, compressed, options);
        }
        else
        {
            size32_t required = expander->init(compressed.bytes());
            tgt.reserveTruncate(required);
            expander->expand(tgt.bufferBase());
            tgt.setWritePos(required);
        }
        cycle_t decompressCycles = timer.elapsedCycles();

        float ratio = (float)(srcLen) / compressed.length();

        StringBuffer name(handler.queryType());
        if (opt == CompressToBuffer)
            name.append("-c2b");
        if (options && *options)
            name.append("-").append(options);


        if (name.length() > 19)
            name.setLength(19);

        unsigned compressTime = (unsigned)cycle_to_millisec(compressCycles);
        unsigned decompressTime = (unsigned)cycle_to_millisec(decompressCycles);
        unsigned compressedTime = compressTime + decompressTime;
        unsigned copyTime200MBs = transferTimeMs(compressed.length(), 200000000);
        unsigned copyTime1GBs = transferTimeMs(compressed.length(), 1000000000);
        unsigned copyTime5GBs = transferTimeMs(compressed.length(), 5000000000);
        unsigned time200MBs = copyTime200MBs + compressedTime;
        unsigned time1GBs = copyTime1GBs + compressedTime;
        unsigned time5GBs = copyTime5GBs + compressedTime;
        DBGLOG("%19s || %8u || %8u || %4u(%4u,%4u) || %4u(%4u,%4u) || %4u(%4u,%4u) || %5.2f [%u]", name.str(), compressTime, decompressTime,
            time200MBs, copyTime200MBs + compressTime, copyTime200MBs + decompressTime,
            time1GBs, copyTime1GBs + compressTime, copyTime1GBs + decompressTime,
            time5GBs, copyTime5GBs + compressTime, copyTime5GBs + decompressTime,
             ratio, compressed.length());

        CPPUNIT_ASSERT(tgt.length() >= sz);
        CPPUNIT_ASSERT(0 == memcmp(src, tgt.bufferBase(), sz));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibCompressionTestsStress );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibCompressionTestsStress, "JlibCompressionTestsStress" );

class JlibFriendlySizeTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibFriendlySizeTest);
        CPPUNIT_TEST(test);
    CPPUNIT_TEST_SUITE_END();

public:
    void test()
    {
        CPPUNIT_ASSERT(friendlyStringToSize("0") == 0);
        CPPUNIT_ASSERT(friendlyStringToSize("2000") == 2000);
        CPPUNIT_ASSERT(friendlyStringToSize("1K") == 1000);
        CPPUNIT_ASSERT(friendlyStringToSize("2Ki") == 2048);
        CPPUNIT_ASSERT(friendlyStringToSize("1M") == 1000000);
        CPPUNIT_ASSERT(friendlyStringToSize("2Mi") == 2048*1024);
        CPPUNIT_ASSERT(friendlyStringToSize("1G") == 1000000000);
        CPPUNIT_ASSERT(friendlyStringToSize("2Gi") == 2048llu*1024*1024);
        CPPUNIT_ASSERT(friendlyStringToSize("1T") == 1000000000000ll);
        CPPUNIT_ASSERT(friendlyStringToSize("2Ti") == 2048llu*1024*1024*1024);
        CPPUNIT_ASSERT(friendlyStringToSize("1P") == 1000000000000000ll);
        CPPUNIT_ASSERT(friendlyStringToSize("2Pi") == 2048llu*1024*1024*1024*1024);
        CPPUNIT_ASSERT(friendlyStringToSize("1E") == 1000000000000000000ll);
        CPPUNIT_ASSERT(friendlyStringToSize("2Ei") == 2048llu*1024*1024*1024*1024*1024);
        try
        {
            friendlyStringToSize("1Kb");
            CPPUNIT_ASSERT(false);
        }
        catch (IException *E)
        {
            E->Release();
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibFriendlySizeTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibFriendlySizeTest, "JlibFriendlySizeTest" );


static stat_type readCheckStatisticValue(const char * cur, StatisticMeasure measure)
{
    const char * end = nullptr;
    stat_type ret = readStatisticValue(cur, &end, measure);
    CPPUNIT_ASSERT(end && *end == '!');
    return ret;
}

class JlibStatsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(JlibStatsTest);
        CPPUNIT_TEST(test);
    CPPUNIT_TEST_SUITE_END();

public:
    void test()
    {
        StringBuffer temp;
        CPPUNIT_ASSERT(readCheckStatisticValue("100s!", SMeasureTimeNs) ==  U64C(100000000000));
        CPPUNIT_ASSERT(readCheckStatisticValue("100ms!", SMeasureTimeNs) == U64C(100000000));
        CPPUNIT_ASSERT(readCheckStatisticValue("100us!", SMeasureTimeNs) == U64C(100000));
        CPPUNIT_ASSERT(readCheckStatisticValue("100ns!", SMeasureTimeNs) == U64C(100));

        CPPUNIT_ASSERT_EQUAL(U64C(1000000000), readCheckStatisticValue("0:0:1!", SMeasureTimeNs));
        CPPUNIT_ASSERT_EQUAL(U64C(60000000000), readCheckStatisticValue("0:1:0!", SMeasureTimeNs));
        CPPUNIT_ASSERT_EQUAL(U64C(3600000000000), readCheckStatisticValue("1:0:0!", SMeasureTimeNs));
        CPPUNIT_ASSERT_EQUAL(U64C(3600123456789), readCheckStatisticValue("1:0:0.123456789!", SMeasureTimeNs));
        CPPUNIT_ASSERT_EQUAL(U64C(1000), readCheckStatisticValue("0:0:0.000001!", SMeasureTimeNs));
        CPPUNIT_ASSERT_EQUAL(U64C(3600000000000), readCheckStatisticValue("1:0:0!", SMeasureTimeNs));
        CPPUNIT_ASSERT_EQUAL(U64C(86412123456789), readCheckStatisticValue("1d 0:0:12.123456789!", SMeasureTimeNs));
        CPPUNIT_ASSERT_EQUAL(U64C(86460123456789), readCheckStatisticValue("1d 0:1:0.123456789!", SMeasureTimeNs));

        CPPUNIT_ASSERT_EQUAL(U64C(1000), readCheckStatisticValue("1970-01-01T00:00:00.001Z!", SMeasureTimestampUs));
        CPPUNIT_ASSERT_EQUAL(std::string("1970-01-01T00:00:00.001Z"), std::string(formatStatistic(temp.clear(), 1000, SMeasureTimestampUs)));
        CPPUNIT_ASSERT_EQUAL(U64C(1608899696789000), readCheckStatisticValue("2020-12-25T12:34:56.789Z!", SMeasureTimestampUs));
        CPPUNIT_ASSERT_EQUAL(std::string("2020-12-25T12:34:56.789Z"), std::string(formatStatistic(temp.clear(), U64C(1608899696789000), SMeasureTimestampUs)));
        CPPUNIT_ASSERT_EQUAL(U64C(1608899696789000), readCheckStatisticValue("1608899696789000!", SMeasureTimestampUs));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JlibStatsTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JlibStatsTest, "JlibStatsTest" );

class HashTableTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( HashTableTests );
        CPPUNIT_TEST(testTimedCache);
    CPPUNIT_TEST_SUITE_END();

    void testTimedCache()
    {
        unsigned hv = 0;
        unsigned __int64 inputHvSum = 0;
        unsigned __int64 lookupHvSum = 0;
        CTimeLimitedCache<unsigned, unsigned> cache(100); // 100ms timeout
        for (unsigned i=0; i<10; i++)
        {
            hv = hashc((const byte *)&i,sizeof(i), hv);
            inputHvSum += hv;
            cache.add(i, hv);
            unsigned lookupHv = 0;
            CPPUNIT_ASSERT(cache.get(i, lookupHv));
            lookupHvSum += lookupHv;
        }
        CPPUNIT_ASSERT(inputHvSum == lookupHvSum);
        MilliSleep(50);
        CPPUNIT_ASSERT(nullptr != cache.query(0, true)); // touch
        MilliSleep(60);
        // all except 0 that was touched should have expired
        CPPUNIT_ASSERT(nullptr != cache.query(0));
        for (unsigned i=1; i<10; i++)
            CPPUNIT_ASSERT(nullptr == cache.query(i));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( HashTableTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( HashTableTests, "HashTableTests" );

class BlockedTimingTests : public CppUnit::TestFixture
{
    static constexpr bool trace = false;

    CPPUNIT_TEST_SUITE( BlockedTimingTests );
        CPPUNIT_TEST(testStandard);
        CPPUNIT_TEST(testStandard2);
        CPPUNIT_TEST(testStandard3);
        CPPUNIT_TEST(testLightweight);
        CPPUNIT_TEST(testLightweight2);
    CPPUNIT_TEST_SUITE_END();

    void testStandard()
    {
        BlockedTimeTracker tracker;

        __uint64 blockTime;
        {
            BlockedSection block(tracker);
            MilliSleep(15);
            blockTime = tracker.getWaitingNs();
        }
        __uint64 postBlockTime = tracker.getWaitingNs();
        __uint64 expected = 15000000;
        CPPUNIT_ASSERT(blockTime >= expected);
        CPPUNIT_ASSERT(blockTime <= expected + 2000000);
        CPPUNIT_ASSERT(postBlockTime - blockTime <= 1000000);
        if (trace)
            DBGLOG("%" I64F "u %" I64F "u", blockTime-50000000, postBlockTime-blockTime);
    }

    void testStandard2()
    {
        BlockedTimeTracker tracker;

        __uint64 blockTime;
        {
            BlockedSection block3(tracker);
            MilliSleep(10);
            {
                BlockedSection block2(tracker);
                MilliSleep(20);
                {
                    BlockedSection block2(tracker);
                    MilliSleep(3);
                    blockTime = tracker.getWaitingNs();
                }
            }
        }
        __uint64 postBlockTime = tracker.getWaitingNs();
        __uint64 expected = 10000000 + 2 * 20000000 + 3 * 3000000;
        CPPUNIT_ASSERT(blockTime >= expected);
        CPPUNIT_ASSERT(blockTime <= expected + 2000000);
        CPPUNIT_ASSERT(postBlockTime - blockTime <= 1000000);
        if (trace)
            DBGLOG("%" I64F "u %" I64F "u", blockTime-expected, postBlockTime-blockTime);
    }

    void testStandard3()
    {
        BlockedTimeTracker tracker;

        __uint64 blockTime;
        {
            auto action = COnScopeExit([&](){ tracker.noteComplete(); });
            auto action2(COnScopeExit([&](){ tracker.noteComplete(); }));
            tracker.noteWaiting();
            tracker.noteWaiting();

            MilliSleep(15);
            blockTime = tracker.getWaitingNs();
        }
        __uint64 postBlockTime = tracker.getWaitingNs();
        __uint64 expected = 15000000 * 2;
        CPPUNIT_ASSERT(blockTime >= expected);
        CPPUNIT_ASSERT(blockTime <= expected + 2000000);
        CPPUNIT_ASSERT(postBlockTime - blockTime <= 1000000);
        if (trace)
            DBGLOG("%" I64F "u %" I64F "u", blockTime-50000000, postBlockTime-blockTime);
    }

    void testLightweight()
    {
        LightweightBlockedTimeTracker tracker;

        __uint64 blockTime;
        {
            LightweightBlockedSection block(tracker);
            MilliSleep(50);
            blockTime = tracker.getWaitingNs();
        }
        __uint64 postBlockTime = tracker.getWaitingNs();
        __uint64 expected = 50000000;
        CPPUNIT_ASSERT(blockTime >= expected);
        CPPUNIT_ASSERT(blockTime <= expected + 2000000);
        CPPUNIT_ASSERT(postBlockTime - blockTime <= 1000000);
        if (trace)
            DBGLOG("%" I64F "u %" I64F "u\n", blockTime-50000000, postBlockTime-blockTime);
    }

    void testLightweight2()
    {
        LightweightBlockedTimeTracker tracker;

        __uint64 blockTime;
        {
            LightweightBlockedSection block3(tracker);
            MilliSleep(10);
            {
                LightweightBlockedSection block2(tracker);
                MilliSleep(20);
                {
                    LightweightBlockedSection block2(tracker);
                    MilliSleep(3);
                    blockTime = tracker.getWaitingNs();
                }
            }
        }
        __uint64 postBlockTime = tracker.getWaitingNs();
        __uint64 expected = 10000000 + 2 * 20000000 + 3 * 3000000;
        CPPUNIT_ASSERT(blockTime >= expected);
        CPPUNIT_ASSERT(blockTime <= expected + 2000000);
        CPPUNIT_ASSERT(postBlockTime - blockTime <= 1000000);
        if (trace)
            DBGLOG("%" I64F "u %" I64F "u", blockTime-expected, postBlockTime-blockTime);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( BlockedTimingTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( BlockedTimingTests, "BlockedTimingTests" );




class JLibUnicodeTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JLibUnicodeTest);
        CPPUNIT_TEST(testConversions);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testConvert(UTF32 codepoint, UtfReader::UtfFormat readFormat, unsigned (*writeFunc)(void * vtarget, unsigned maxLength, UTF32 ch))
    {
        constexpr unsigned maxlen = 8;
        byte temp[maxlen];
        unsigned len;
        len = writeFunc(temp, maxlen, codepoint);

        UtfReader reader(readFormat, false);
        reader.set(len, temp);
        UTF32 value = reader.next();
        ASSERT_EQUAL(codepoint, value);
    }

    void testConvertUtf8(UTF32 codepoint)
    {
        constexpr unsigned maxlen = 8;
        byte temp[maxlen];
        unsigned len;
        len = writeUtf8(temp, maxlen, codepoint);
        const byte * data = temp;
        UTF32 value = readUtf8Character(len, data);
        ASSERT_EQUAL(codepoint, value);
        ASSERT_EQUAL(len, (unsigned)(data - temp));
    }

    void testConvert(UTF32 codepoint)
    {
        testConvertUtf8(codepoint);
        testConvert(codepoint, UtfReader::Utf8, writeUtf8);
        testConvert(codepoint, UtfReader::Utf16le, writeUtf16le);
        testConvert(codepoint, UtfReader::Utf16be, writeUtf16be);
        testConvert(codepoint, UtfReader::Utf32le, writeUtf32le);
        testConvert(codepoint, UtfReader::Utf32be, writeUtf32be);
    }

    void testConversions()
    {
        unsigned range = 10;
        for (unsigned low : { 0U, 0x80U, 100U, 0x7fU, 0x7ffU, 0xffffU, 0x0010FFFFU-(range-1U) })
        {
            for (unsigned delta = 0; delta < 10; delta++)
                testConvert(low + delta);
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JLibUnicodeTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JLibUnicodeTest, "JLibUnicodeTest" );

#ifdef _USE_OPENSSL
#include <jencrypt.hpp>

class JLibOpensslAESTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JLibOpensslAESTest);
        CPPUNIT_TEST(test);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testOne(unsigned len, const char *intext)
    {
        /* A 256 bit key */
        unsigned char key[] = { 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
                                0x38, 0x39, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35,
                                0x36, 0x37, 0x38, 0x39, 0x30, 0x31, 0x32, 0x33,
                                0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30, 0x31
                              };
        constexpr const char * prefix = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        unsigned lenPrefix = strlen(prefix);

        MemoryBuffer ciphertext1, ciphertext2, decrypted1, decrypted2;

        ciphertext1.append(lenPrefix, prefix);
        ciphertext2.append(lenPrefix, prefix);
        openssl::aesEncrypt(key, 32, intext, len, ciphertext1);
        jlib::aesEncrypt(key, 32, intext, len, ciphertext2);

        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes(), prefix, lenPrefix) == 0);
        CPPUNIT_ASSERT(memcmp(ciphertext2.bytes(), prefix, lenPrefix) == 0);
        if (len)
            CPPUNIT_ASSERT(ciphertext1.length() > len + lenPrefix);
        else
            CPPUNIT_ASSERT(ciphertext1.length() == len + lenPrefix);
        CPPUNIT_ASSERT(ciphertext1.length() <= len + lenPrefix + 16);
        CPPUNIT_ASSERT(ciphertext1.length()==ciphertext2.length());
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes(), ciphertext2.bytes(), ciphertext1.length()) == 0);
        
        unsigned cipherlen = ciphertext1.length() - lenPrefix;

        /* Decrypt the ciphertext */
        decrypted1.append(lenPrefix, prefix);
        openssl::aesDecrypt(key, 32, ciphertext1.bytes()+lenPrefix, cipherlen, decrypted1);
        CPPUNIT_ASSERT(decrypted1.length() == len+lenPrefix);
        CPPUNIT_ASSERT(memcmp(decrypted1.bytes(), prefix, lenPrefix) == 0);
        CPPUNIT_ASSERT(memcmp(decrypted1.bytes()+lenPrefix, intext, len) == 0);
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes(), ciphertext2.bytes(), ciphertext1.length()) == 0); // check input unchanged

        decrypted2.append(lenPrefix, prefix);
        jlib::aesDecrypt(key, 32, ciphertext2.bytes()+lenPrefix, cipherlen, decrypted2);
        CPPUNIT_ASSERT(memcmp(decrypted2.bytes(), prefix, lenPrefix) == 0);
        CPPUNIT_ASSERT(decrypted2.length() == len + lenPrefix);
        CPPUNIT_ASSERT(memcmp(decrypted2.bytes() + lenPrefix, intext, len) == 0);
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes(), ciphertext2.bytes(), ciphertext1.length()) == 0); // check input unchanged

        // Now test in-place decrypt
        ciphertext1.append(4, "XXXX");   // Marker
        unsigned decryptedlen = openssl::aesDecryptInPlace(key, 32, (void *)(ciphertext1.bytes() + lenPrefix), cipherlen);
        CPPUNIT_ASSERT(decryptedlen == len);
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes()+lenPrefix, intext, len) == 0);
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes()+lenPrefix+cipherlen, "XXXX", 4) == 0);

        ciphertext2.append(4, "XXXX");   // Marker
        decryptedlen = jlib::aesDecryptInPlace(key, 32, (void *)(ciphertext2.bytes() + lenPrefix), cipherlen);
        CPPUNIT_ASSERT(decryptedlen == len);
        CPPUNIT_ASSERT(memcmp(ciphertext2.bytes()+lenPrefix, intext, len) == 0);
        CPPUNIT_ASSERT(memcmp(ciphertext2.bytes()+lenPrefix+cipherlen, "XXXX", 4) == 0);

        // Now in-place encrypt
        ciphertext1.clear().append(lenPrefix, prefix).append(len, intext);
        ciphertext1.append(16, "1234123412341234"); // Filler to be used by AES padding
        ciphertext1.append(4, "WXYZ");   // Marker - check this is untouched
        unsigned encryptedlen = openssl::aesEncryptInPlace(key, 32, (void *)(ciphertext1.bytes() + lenPrefix), len, len+16);
        CPPUNIT_ASSERT(encryptedlen >= len);
        CPPUNIT_ASSERT(encryptedlen <= len+16);
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes()+lenPrefix+len+16, "WXYZ", 4) == 0);
        CPPUNIT_ASSERT(len == 0 || memcmp(ciphertext1.bytes()+lenPrefix, intext, len) != 0);  // Check it actually did encrypt!
        decryptedlen = openssl::aesDecryptInPlace(key, 32, (void *)(ciphertext1.bytes() + lenPrefix), encryptedlen);
        CPPUNIT_ASSERT(decryptedlen == len);
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes()+lenPrefix, intext, len) == 0);
        CPPUNIT_ASSERT(memcmp(ciphertext1.bytes()+lenPrefix+len+16, "WXYZ", 4) == 0);

        ciphertext2.clear().append(lenPrefix, prefix).append(len, intext);
        ciphertext2.append(16, "1234123412341234"); // Filler to be used by AES padding
        ciphertext2.append(4, "ABCD");   // Marker - check this is untouched
        encryptedlen = jlib::aesEncryptInPlace(key, 32, (void *)(ciphertext2.bytes() + lenPrefix), len, len+16);
        CPPUNIT_ASSERT(encryptedlen >= len);
        CPPUNIT_ASSERT(encryptedlen <= len+16);
        CPPUNIT_ASSERT(memcmp(ciphertext2.bytes()+lenPrefix+len+16, "ABCD", 4) == 0);
        CPPUNIT_ASSERT(len == 0 || memcmp(ciphertext2.bytes()+lenPrefix, intext, len) != 0);  // Check it actually did encrypt!
        decryptedlen = jlib::aesDecryptInPlace(key, 32, (void *)(ciphertext2.bytes() + lenPrefix), encryptedlen);
        CPPUNIT_ASSERT(decryptedlen == len);
        CPPUNIT_ASSERT(memcmp(ciphertext2.bytes()+lenPrefix, intext, len) == 0);
        CPPUNIT_ASSERT(memcmp(ciphertext2.bytes()+lenPrefix+len+16, "ABCD", 4) == 0);

        // Test some error cases
        if (len)
        {
            ciphertext1.clear().append(lenPrefix, prefix).append(len, intext);
            ciphertext1.append(4, "WXYZ");   // Marker - check this is untouched
            try
            {
                encryptedlen = openssl::aesEncryptInPlace(key, 32, (void *)(ciphertext1.bytes() + lenPrefix), len, len);
                CPPUNIT_ASSERT(!"Should have reported insufficient length");
            }
            catch (IException *E)
            {
                CPPUNIT_ASSERT(memcmp(ciphertext1.bytes()+lenPrefix+len, "WXYZ", 4) == 0);
                E->Release();
            }
            ciphertext2.clear().append(lenPrefix, prefix).append(len, intext);
            ciphertext2.append(4, "ABCD");   // Marker - check this is untouched
            try
            {
                encryptedlen = jlib::aesEncryptInPlace(key, 32, (void *)(ciphertext2.bytes() + lenPrefix), len, len);
                CPPUNIT_ASSERT(!"Should have reported insufficient length");
            }
            catch (IException *E)
            {
                CPPUNIT_ASSERT(memcmp(ciphertext2.bytes()+lenPrefix+len, "ABCD", 4) == 0);
                E->Release();
            }
        }        
    }

    void test()
    {
        try
        {
            /* Message to be encrypted */
            const char *plaintext = "The quick brown fox jumps over the lazy dog";
            for (unsigned l = 0; l < strlen(plaintext); l++)
                testOne(l, plaintext);
        }
        catch (IException * e)
        {
            EXCLOG(e, "Exception in AES unit test");
            throw;
        }
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( JLibOpensslAESTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JLibOpensslAESTest, "JLibOpensslAESTest" );
#endif

class JLibSecretsTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JLibSecretsTest);
        CPPUNIT_TEST(setup);
        CPPUNIT_TEST(testUpdate1);
        CPPUNIT_TEST(testUpdate2);
        CPPUNIT_TEST(testBackgroundUpdate);
        CPPUNIT_TEST(testKeyEncoding);
    CPPUNIT_TEST_SUITE_END();

    //Each test creates a different instance of the class(!) so member values cannot be used to pass items
    //from one test to another
    StringBuffer secretRoot;

protected:
    void checkSecret(const IPropertyTree * match, const char * key, const char * expectedValue)
    {
        if (match)
        {
            const char * secretValue = match->queryProp(key);
            if (secretValue)
            {
                CPPUNIT_ASSERT_EQUAL_STR(secretValue, expectedValue);
            }
            else
            {
                //IPropertyTree doesn't allow blank values, so a missing value is the same as a blank value
                //We should probably revisit some day, but it is likely to break existing code if we do.
                CPPUNIT_ASSERT_EQUAL_STR("", expectedValue);
            }
        }
        else
            CPPUNIT_ASSERT_EQUAL_STR("", expectedValue);
    }
    void checkSecret(const char * secret, const char * key, const char * expectedValue)
    {
        Owned<const IPropertyTree> match = getSecret("testing", secret);
        checkSecret(match, key, expectedValue);
    }

    void checkSecret(ISyncedPropertyTree * secret, const char * key, const char * expectedValue)
    {
        Owned<const IPropertyTree> match = secret->getTree();
        checkSecret(match, key, expectedValue);
    }

    bool hasSecret(const char * name)
    {
        Owned<const IPropertyTree> match = getSecret("testing", name);
        return match != nullptr;
    }

    void initPath()
    {
        char cwd[1024];
        CPPUNIT_ASSERT(GetCurrentDirectory(1024, cwd));
        secretRoot.set(cwd).append(PATHSEPCHAR).append("unittest-secrets");
        secretRoot.append(PATHSEPCHAR).append("testing"); // catgegory
    }

    void setup()
    {
        char cwd[1024];
        CPPUNIT_ASSERT(GetCurrentDirectory(1024, cwd));
        secretRoot.append(cwd).append(PATHSEPCHAR).append("unittest-secrets");

        recursiveRemoveDirectory(secretRoot);
        CPPUNIT_ASSERT(recursiveCreateDirectory(secretRoot.str()));
        setSecretMount(secretRoot);
        setSecretTimeout(100); // Set the timeout so we can check it is working.

        secretRoot.append(PATHSEPCHAR).append("testing"); // catgegory
        CPPUNIT_ASSERT(recursiveCreateDirectory(secretRoot.str()));
    }

    void testUpdate1()
    {
        initPath(); // secretRoot needs to be called for each test

        CPPUNIT_ASSERT(!hasSecret("secret1"));
        writeTestingSecret("secret1", "value", "secret1Value");
        //Secret should not appear yet - null should be cached.
        CPPUNIT_ASSERT(!hasSecret("secret1"));

        Owned<ISyncedPropertyTree> secret2 = getSyncedSecret("testing", "secret2", nullptr, nullptr);
        CPPUNIT_ASSERT(!secret2->isValid());
        CPPUNIT_ASSERT(!secret2->isStale());

        MilliSleep(50);
        //Secret should not appear yet - null should be cached.
        CPPUNIT_ASSERT(!hasSecret("secret1"));
        CPPUNIT_ASSERT(!secret2->isValid());
        CPPUNIT_ASSERT(!secret2->isStale());

        MilliSleep(100);
        //Secret1 should now be updated - enough time has passed
        checkSecret("secret1", "value", "secret1Value");
        CPPUNIT_ASSERT(!secret2->isValid());
        CPPUNIT_ASSERT(secret2->isStale());

        //Cleanup
        writeTestingSecret("secret1", "value", nullptr);
    }

    void testUpdate2()
    {
        initPath(); // secretRoot needs to be called for each test

        Owned<ISyncedPropertyTree> secret3 = getSyncedSecret("testing", "secret3", nullptr, nullptr);
        unsigned version = secret3->getVersion();
        CPPUNIT_ASSERT(!secret3->isValid());
        CPPUNIT_ASSERT(!secret3->isStale());
        writeTestingSecret("secret3", "value", "secret3Value");
        CPPUNIT_ASSERT(!secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version, secret3->getVersion());

        //After sleep new value should not have been picked up
        MilliSleep(50);
        CPPUNIT_ASSERT(!secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version, secret3->getVersion());

        //After sleep new value should now have been picked up
        MilliSleep(100);
        checkSecret("secret3", "value", "secret3Value");
        unsigned version2 = secret3->getVersion();
        CPPUNIT_ASSERT(!secret3->isStale());
        CPPUNIT_ASSERT(secret3->isValid());
        CPPUNIT_ASSERT(version != version2);

        //Sleep and check that the hash value has not changed
        MilliSleep(200);
        checkSecret("secret3", "value", "secret3Value");
        CPPUNIT_ASSERT(!secret3->isStale());
        CPPUNIT_ASSERT(secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version2, secret3->getVersion());

        //Remove the secret - should have no immediate effect
        writeTestingSecret("secret3", "value", nullptr);
        CPPUNIT_ASSERT(!secret3->isStale());
        CPPUNIT_ASSERT(secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version2, secret3->getVersion());

        MilliSleep(50);
        CPPUNIT_ASSERT(!secret3->isStale());
        CPPUNIT_ASSERT(secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version2, secret3->getVersion());
        checkSecret("secret3", "value", "secret3Value");

        MilliSleep(100);
        CPPUNIT_ASSERT(secret3->isStale()); // Value has gone, but the old value is still returned
        CPPUNIT_ASSERT(secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version2, secret3->getVersion());
        checkSecret("secret3", "value", "secret3Value");

        //Update the value = the change should not be seen until the cache entry expires
        writeTestingSecret("secret3", "value", "secret3NewValue");
        CPPUNIT_ASSERT(secret3->isStale());
        CPPUNIT_ASSERT(secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version2, secret3->getVersion());
        checkSecret("secret3", "value", "secret3Value");

        MilliSleep(50);
        CPPUNIT_ASSERT(secret3->isStale());
        CPPUNIT_ASSERT(secret3->isValid());
        CPPUNIT_ASSERT_EQUAL(version2, secret3->getVersion());
        checkSecret("secret3", "value", "secret3Value");

        MilliSleep(100);
        //These functions do not check for up to date values, so they return the same as before
        CPPUNIT_ASSERT(secret3->isStale()); // Value still appears to be out of date
        CPPUNIT_ASSERT(secret3->isValid());

        //The getVersion() should force the value to be updated
        unsigned version3 = secret3->getVersion();
        CPPUNIT_ASSERT(version2 != version3);
        CPPUNIT_ASSERT(!secret3->isStale()); // New value has now been picked up
        CPPUNIT_ASSERT(secret3->isValid());
        checkSecret("secret3", "value", "secret3NewValue");

        //Finally check that writing a blank value is spotted as a change.
        writeTestingSecret("secret3", "value", "");
        MilliSleep(150);
        //Check the version to ensure that the value has been updated
        CPPUNIT_ASSERT(version3 != secret3->getVersion());
        CPPUNIT_ASSERT(secret3->isValid());
        checkSecret("secret3", "value", "");

        //Cleanup
        writeTestingSecret("secret3", "value", nullptr);
    }

    void testBackgroundUpdate()
    {
        initPath(); // secretRoot needs to be called for each test
        startSecretUpdateThread(20);    // 100ms expiry, check every 5ms for items expiring in 20ms time.

        //--------- First check that a missed secret is checked in the background ---------
        Owned<ISyncedPropertyTree> secret4 = getSyncedSecret("testing", "secret4", nullptr, nullptr);
        CPPUNIT_ASSERT(!secret4->isValid());
        CPPUNIT_ASSERT(!secret4->isStale());

        //Sleep for less than the update interval
        MilliSleep(50);
        CPPUNIT_ASSERT(!secret4->isValid());
        CPPUNIT_ASSERT(!secret4->isStale());

        //Sleep so the cache entry should have expired, and no data around to make it not stale.
        MilliSleep(60);
        CPPUNIT_ASSERT(!secret4->isValid());
        CPPUNIT_ASSERT(secret4->isStale());

        //--------- Now update the value in the background ---------
        //First check that a missed secret is checked in the background
        Owned<ISyncedPropertyTree> secret5 = getSyncedSecret("testing", "secret5", nullptr, nullptr);
        CPPUNIT_ASSERT(!secret5->isValid());
        CPPUNIT_ASSERT(!secret5->isStale());
        //And write a value so it is picked up on the next refresh
        writeTestingSecret("secret5", "value", "secret5Value");

        //Sleep for less than the update interval
        MilliSleep(50); // elapsed=50
        CPPUNIT_ASSERT(!secret5->isValid());
        CPPUNIT_ASSERT(!secret5->isStale());

        //Sleep so the cache entry should have expired and the value reread since reading ahead
        MilliSleep(60); // elapsed=110 = 80 + 30
        CPPUNIT_ASSERT(secret5->isValid());
        CPPUNIT_ASSERT(!secret5->isStale());

        //Sleep again so it is not accessed within the timeout period - it should now be marked as stale but valid
        MilliSleep(100); // elapsed=210 = 80 + 80 + 50
        CPPUNIT_ASSERT(secret5->isValid());
        CPPUNIT_ASSERT(secret5->isStale());

        //--------- Check that accessing the function marks the value so it is refreshed ---------
        Owned<ISyncedPropertyTree> secret6 = getSyncedSecret("testing", "secret6", nullptr, nullptr);
        CPPUNIT_ASSERT(!secret6->isValid());
        CPPUNIT_ASSERT(!secret6->isStale());
        //And write a value so it is picked up on the next refresh
        writeTestingSecret("secret6", "value", "secret6Value");

        //Sleep for less than the update interval
        MilliSleep(50); // elapsed=50
        CPPUNIT_ASSERT(!secret6->isValid());
        CPPUNIT_ASSERT(!secret6->isStale());

        //Sleep so the cache entry should have expired and the value reread since reading ahead
        MilliSleep(60); // elapsed=110 = 80 + 30
        CPPUNIT_ASSERT(secret6->isValid());
        CPPUNIT_ASSERT(!secret6->isStale());
        unsigned version1 = secret6->getVersion(); // Mark the value as accessed, but too early to be refreshed
        writeTestingSecret("secret6", "value", "secret6Value2");

        MilliSleep(40); // elapsed=150 = 80 + 70
        CPPUNIT_ASSERT(secret6->isValid());
        CPPUNIT_ASSERT(!secret6->isStale());
        unsigned version2 = secret6->getVersion(); // Mark the value as accessed, but too early to be refreshed
        CPPUNIT_ASSERT(version2 == version1);

        MilliSleep(30); // elapsed=180 = 80 + 80 + 20
        CPPUNIT_ASSERT(secret6->isValid());
        CPPUNIT_ASSERT(!secret6->isStale());
        unsigned version3 = secret6->getVersion(); // Mark the value as accessed, but will now have been refreshed
        CPPUNIT_ASSERT(version3 != version1);
        checkSecret(secret4, "value", "");
        checkSecret(secret5, "value", "secret5Value");
        checkSecret(secret6, "value", "secret6Value2");

        //Cleanup
        writeTestingSecret("secret5", "value", nullptr);
        writeTestingSecret("secret6", "value", nullptr);
        stopSecretUpdateThread();
    }

    void testKeyEncoding()
    {
        for (auto category : { "abc", "def" })
        {
            for (auto name : { "x", "y" })
            {
                for (auto vault : { "vaultx", "" })
                {
                    for (auto version : { "", "v1" })
                    {
                        std::string encoded = testBuildSecretKey(category, name, vault, version);

                        std::string readCategory;
                        std::string readName;
                        std::string readVaultId;
                        std::string readVersion;
                        testExpandSecretKey(readCategory, readName, readVaultId, readVersion, encoded.c_str());

                        CPPUNIT_ASSERT_EQUAL_STR(category, readCategory.c_str());
                        CPPUNIT_ASSERT_EQUAL_STR(name, readName.c_str());
                        CPPUNIT_ASSERT_EQUAL_STR(vault, readVaultId.c_str());
                        CPPUNIT_ASSERT_EQUAL_STR(version, readVersion.c_str());
                    }
                }
            }
        }
    }

    void writeTestingSecret(const char * secret, const char * key, const char * value)
    {
        StringBuffer filename;
        filename.append(secretRoot).append(PATHSEPCHAR).append(secret);
        CPPUNIT_ASSERT(recursiveCreateDirectory(filename.str()));

        filename.append(PATHSEPCHAR).append(key);

        Owned<IFile> file = createIFile(filename.str());
        if (value)
        {
            Owned<IFileIO> io = file->open(IFOcreate);
            io->write(0, strlen(value), value);
        }
        else
            file->remove();
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( JLibSecretsTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( JLibSecretsTest, "JLibSecretsTest" );



#endif // _USE_CPPUNIT
