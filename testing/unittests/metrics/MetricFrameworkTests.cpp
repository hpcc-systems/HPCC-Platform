/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#include <cppunit/TestFixture.h>
#include "unittests.hpp"
#include <algorithm>

#include "jmetrics.hpp"

using namespace hpccMetrics;

class MetricFrameworkTestSink : public MetricSink
{
public:
    explicit MetricFrameworkTestSink(const char *name) :
        MetricSink(name, "test") { }

    ~MetricFrameworkTestSink() override = default;

    void startCollection(MetricsReporter *_pReporter) override
    {
        pReporter = _pReporter;
        isCollecting = true;
    }

    void stopCollection() override
    {
        isCollecting = false;
    }

    std::vector<std::shared_ptr<IMetric>> getReportMetrics() const
    {
        return pReporter->queryMetricsForReport(name);
    }

    void reset()
    {
        isCollecting = false;
    }

public:
    bool isCollecting = false;
};


MetricsReporter frameworkTestReporter;

static MetricFrameworkTestSink *pTestSink = new MetricFrameworkTestSink("testsink");
bool sinkAdded = false;


class MetricFrameworkTests : public CppUnit::TestFixture
{
public:
    MetricFrameworkTests()
    {
        if (!sinkAdded)
        {
            sinkAdded = true;
            frameworkTestReporter.addSink(pTestSink, "testsink");
        }
        pTestSink->reset();
    }

    CPPUNIT_TEST_SUITE(MetricFrameworkTests);
        CPPUNIT_TEST(Test_counter_metric_increments_properly);
        CPPUNIT_TEST(Test_gauge_metric_updates_properly);
        CPPUNIT_TEST(Test_custom_metric);
        CPPUNIT_TEST(Test_reporter_calls_sink_to_start_and_stop_collection);
        CPPUNIT_TEST(Test_reporter_manages_metrics_properly);
    CPPUNIT_TEST_SUITE_END();

protected:


    void Test_counter_metric_increments_properly()
    {
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("test-counter", "description");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected initial value to be 0", 0, static_cast<int>(pCounter->queryValue()));

        //
        // Test default increment (by 1)
        pCounter->inc();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected value to be 1 after default increment", 1, static_cast<int>(pCounter->queryValue()));

        //
        // Test increment by > 1
        pCounter->inc(2);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected value to be 3 after increment by 2", 3, static_cast<int>(pCounter->queryValue()));
    }


    void Test_gauge_metric_updates_properly()
    {
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("test-gauge", "description");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected initial value to be 0", 0, static_cast<int>(pGauge->queryValue()));

        //
        // Test initial setting of gauge
        pGauge->set(25);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected value to be 25 after setting", 25, static_cast<int>(pGauge->queryValue()));

        //
        // Test updating gauge
        pGauge->add(10);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected value to be 35", 35, static_cast<int>(pGauge->queryValue()));

        pGauge->add(-5);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected value to be 30", 30, static_cast<int>(pGauge->queryValue()));
    }


    void Test_custom_metric()
    {
        int customCounter = 0;
        std::shared_ptr<CustomMetric<int>> pCustomCounter = std::make_shared<CustomMetric<int>>("custom-counter", "description", METRICS_COUNTER, customCounter);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected initial value to be 0", 0, static_cast<int>(pCustomCounter->queryValue()));

        customCounter++;
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected value to be 1 after default increment", 1, static_cast<int>(pCustomCounter->queryValue()));
    }


    void Test_reporter_calls_sink_to_start_and_stop_collection()
    {
        frameworkTestReporter.startCollecting();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Test sink start collection was not called", true, pTestSink->isCollecting);
        frameworkTestReporter.stopCollecting();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Test sink stop collection was not called", false, pTestSink->isCollecting);
    }


    void Test_reporter_manages_metrics_properly()
    {
        int numAdded;
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("test-counter", "description");
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("test-gauge", "description");
        frameworkTestReporter.addMetric(pCounter);
        frameworkTestReporter.addMetric(pGauge);
        numAdded = 2;

        frameworkTestReporter.startCollecting();

        //
        // Make sure the initial list is correct
        auto numMetrics = frameworkTestReporter.queryMetricsForReport("testsink").size();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(VStringBuffer("Expected %d metrics to be returned", numAdded).str(), numAdded, static_cast<int>(numMetrics));

        //
        // Add a metric while reporting is enabled and make sure it is returned
        std::shared_ptr<CounterMetric> pNewCounter = std::make_shared<CounterMetric>("test-newcounter", "description");
        frameworkTestReporter.addMetric(pNewCounter);
        numAdded++;

        numMetrics = frameworkTestReporter.queryMetricsForReport("testsink").size();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(VStringBuffer("Expected %d metrics after adding new metric", numAdded).str(), numAdded, static_cast<int>(numMetrics));

        //
        // Destroy a metric and ensure it is no longer in the list of report metrics
        pNewCounter = nullptr;
        numAdded--;

        numMetrics = frameworkTestReporter.queryMetricsForReport("testsink").size();
        CPPUNIT_ASSERT_EQUAL_MESSAGE(VStringBuffer("Expected %d metrics after destroying metric", numAdded).str(), numAdded, static_cast<int>(numMetrics));

        frameworkTestReporter.stopCollecting();
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( MetricFrameworkTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( MetricFrameworkTests, "MetricFrameworkTests" );

#endif
