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
#include "metricunittests.hpp"
#include <algorithm>

#include "jmetrics.hpp"

#include "MetricFrameworkTestSink.hpp"

static MetricsReporter &reporter = queryMetricsReporter();

static MetricFrameworkTestSink *pTestSink = new MetricFrameworkTestSink("testsink");
bool sinkAdded = false;

void usage()
{
    printf("\n"
           "Usage:\n"
           "    metricframeworkunittests\n"
           "\n");
}

int main(int argc, char* argv[])
{
    InitModuleObjects();
    bool wasSuccessful = false;
    {
        // New scope as we need the TestRunner to be destroyed before unloading the dlls...
        CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
        CppUnit::TextUi::TestRunner runner;
        CppUnit::Test *all = registry.makeTest();
        int numTests = all->getChildTestCount();
        for (int i = 0; i < numTests; i++)
        {
            CppUnit::Test *sub = all->getChildTestAt(i);
            runner.addTest(sub);
        }
        wasSuccessful = runner.run( "", false );
    }
    return wasSuccessful ? 0 : 1; // 0 == exit code success
}


class MetricFrameworkTests : public CppUnit::TestFixture
{
public:
    MetricFrameworkTests()
    {
        if (!sinkAdded)
        {
            sinkAdded = true;
            reporter.addSink(pTestSink, "testsink");
        }
        pTestSink->reset();
    }

    CPPUNIT_TEST_SUITE(MetricFrameworkTests);
        CPPUNIT_TEST(Test_it_increments_counter_metric_by_1_by_default);
        CPPUNIT_TEST(Test_it_increments_counter_metric);
        CPPUNIT_TEST(Test_it_can_set_gauge_metric);
        CPPUNIT_TEST(Test_it_updates_gauge_metric);
        CPPUNIT_TEST(Test_it_updates_custom_metric);
        CPPUNIT_TEST(Test_reporter_calls_sink_to_start_collection);
        CPPUNIT_TEST(Test_reporter_calls_sink_to_stop_collection);
        CPPUNIT_TEST(Test_reporter_returns_correct_list_of_metrics_during_report);
        CPPUNIT_TEST(Test_can_add_metric_after_collecting_started);
        CPPUNIT_TEST(Test_removes_destroyed_metric_after_collecting_started);
    CPPUNIT_TEST_SUITE_END();

protected:


    void Test_it_increments_counter_metric_by_1_by_default()
    {
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("test-counter", "description");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected initial value to be 0", 0, static_cast<int>(pCounter->queryValue()));

        pCounter->inc();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected value to be 1 after default increment", 1, static_cast<int>(pCounter->queryValue()));
    }


    void Test_it_increments_counter_metric()
    {
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("test-counter", "description");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected initial value to be 0", 0, static_cast<int>(pCounter->queryValue()));

        pCounter->inc(2);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected value to be 2 after increment by 2", 2, static_cast<int>(pCounter->queryValue()));
    }


    void Test_it_can_set_gauge_metric()
    {
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("test-gauge", "description");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected initial value to be 0", 0, static_cast<int>(pGauge->queryValue()));

        pGauge->set(25);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected value to be 25 after setting", 25, static_cast<int>(pGauge->queryValue()));
    }


    void Test_it_updates_gauge_metric()
    {
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("test-gauge", "description");
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected initial value to be 0", 0, static_cast<int>(pGauge->queryValue()));

        pGauge->set(25);
        pGauge->add(10);
        pGauge->add(-5);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Gauge metric expected value to be 30 after setting", 30, static_cast<int>(pGauge->queryValue()));
    }


    void Test_it_updates_custom_metric()
    {
        int customCounter = 0;
        std::shared_ptr<CustomMetric<int>> pCustomCounter = std::make_shared<CustomMetric<int>>("custom-counter", "description", METRICS_COUNTER, customCounter);
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected initial value to be 0", 0, static_cast<int>(pCustomCounter->queryValue()));

        customCounter++;
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Counter metric expected value to be 1 after default increment", 1, static_cast<int>(pCustomCounter->queryValue()));
    }


    void Test_reporter_calls_sink_to_start_collection()
    {
        reporter.startCollecting();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Test sink start collection was not called", true, pTestSink->isStartCollectionCalled());
        reporter.stopCollecting();
    }

    void Test_reporter_calls_sink_to_stop_collection()
    {
        reporter.startCollecting();
        reporter.stopCollecting();
        CPPUNIT_ASSERT_EQUAL_MESSAGE("Test sink stop collection was not called", true, pTestSink->isStopCollectionCalled());
    }

    void Test_reporter_returns_correct_list_of_metrics_during_report()
    {
        int numAdded;
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("test-counter", "description");
        reporter.addMetric(pCounter);
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("test-gauge", "description");
        reporter.addMetric(pGauge);
        numAdded = 2;

        reporter.startCollecting();

        auto numMetrics = reporter.queryMetricsForReport("testsink").size();

        char msg[256];
        sprintf(msg, "Expected %d metrics to be returned", numAdded);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(msg, numAdded, static_cast<int>(numMetrics));
    }

    void Test_can_add_metric_after_collecting_started()
    {
        char msg[256];
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("test-counter", "description");
        reporter.addMetric(pCounter);

        //
        // Verify the number of metrics
        int numBeforeCollectionStarted = 1;
        reporter.startCollecting();
        auto numReportMetrics = reporter.queryMetricsForReport("testsink").size();
        sprintf(msg, "Expected %d metrics to be returned", numBeforeCollectionStarted);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(msg, numBeforeCollectionStarted, static_cast<int>(numReportMetrics));

        //
        // Now add a new metric
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("test-gauge", "description");
        reporter.addMetric(pGauge);
        int numAfterCollectionStarted = 2;

        //
        // Verify the new number and that the newly added metric is present
        auto reportMetrics = reporter.queryMetricsForReport("testsink");
        numReportMetrics = reportMetrics.size();
        sprintf(msg, "Expected %d metrics to be returned", numAfterCollectionStarted);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(msg, numAfterCollectionStarted, static_cast<int>(numReportMetrics));

        auto it = std::find (reportMetrics.begin(), reportMetrics.end(), pGauge);

        CPPUNIT_ASSERT_MESSAGE("Did not find dynamically added metric", it != reportMetrics.end());
    }

    void Test_removes_destroyed_metric_after_collecting_started()
    {
        char msg[256];

        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("test-counter", "description");
        reporter.addMetric(pCounter);

        //
        // Verify the number of metrics
        int numBeforeCollectionStarted = 1;
        reporter.startCollecting();
        auto numReportMetrics = reporter.queryMetricsForReport("testsink").size();
        sprintf(msg, "Expected %d metrics to be returned", numBeforeCollectionStarted);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(msg, numBeforeCollectionStarted, static_cast<int>(numReportMetrics));

        //
        // Now add a new metric
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("test-gauge", "description");
        reporter.addMetric(pGauge);
        int numAfterCollectionStarted = 2;

        //
        // Verify that metric was added
        numReportMetrics = reporter.queryMetricsForReport("testsink").size();
        sprintf(msg, "Expected %d metrics to be returned", numAfterCollectionStarted);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(msg, numAfterCollectionStarted, static_cast<int>(numReportMetrics));

        //
        // Remove the metric by clearing the pointer reference
        pGauge = nullptr;
        int numAfterRemoval = 1;

        //
        // Verify the metric no longer present
        auto reportMetrics = reporter.queryMetricsForReport("testsink");
        numReportMetrics = reportMetrics.size();
        sprintf(msg, "Expected %d metrics to be returned", numAfterRemoval);
        CPPUNIT_ASSERT_EQUAL_MESSAGE(msg, numAfterRemoval, static_cast<int>(numReportMetrics));

        auto it = std::find (reportMetrics.begin(), reportMetrics.end(), pGauge);

        CPPUNIT_ASSERT_MESSAGE("Dynamic metric was not removed", it == reportMetrics.end());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( MetricFrameworkTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( MetricFrameworkTests, "MetricFrameworkTests" );

#endif
