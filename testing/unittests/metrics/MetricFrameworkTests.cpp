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

    void startCollection(MetricsManager *_pManager) override
    {
        pManager = _pManager;
        isCollecting = true;
    }

    void stopCollection() override
    {
        isCollecting = false;
    }

    std::vector<std::shared_ptr<IMetric>> getReportMetrics() const
    {
        return pManager->queryMetricsForReport(name);
    }

public:
    bool isCollecting = false;
};


class MetricFrameworkTests : public CppUnit::TestFixture
{
public:
    MetricFrameworkTests()
    {
        pTestSink = new MetricFrameworkTestSink("testsink");
        frameworkTestManager.addSink(pTestSink, "testsink");
    }

    ~MetricFrameworkTests() = default;

    CPPUNIT_TEST_SUITE(MetricFrameworkTests);
        CPPUNIT_TEST(Test_valid_and_invalid_metric_names);
        CPPUNIT_TEST(Test_counter_metric_increments_properly);
        CPPUNIT_TEST(Test_gauge_metric_updates_properly);
        CPPUNIT_TEST(Test_custom_metric);
        CPPUNIT_TEST(Test_manager_calls_sink_to_start_and_stop_collection);
        CPPUNIT_TEST(Test_manager_manages_metrics_properly);
        CPPUNIT_TEST(Test_scoped_updater_classes);
        CPPUNIT_TEST(Test_metric_meta_data);
        CPPUNIT_TEST(Test_gauge_by_counters_metric);
        CPPUNIT_TEST(Test_histogram_metric);
    CPPUNIT_TEST_SUITE_END();

protected:

    void Test_valid_and_invalid_metric_names()
    {
        std::vector<std::string> invalid_names = {
            {".startswithperiod"},
            {"endswithperiod."},
            {"9startswithnumber"},
            {"has_underscore"},
            {"has&^%$#@!-~`+=|[]{}?/<>,_specialcharacters"}
        };

        for (const auto &badName: invalid_names)
        {
            std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>(badName.c_str(), "description", SMeasureCount);
            bool added = false;
            try
            {
                added = frameworkTestManager.addMetric(pCounter);
            }
            catch (IException *e)
            {
                added = false;
                e->Release();
            }
            CPPUNIT_ASSERT(!added);
        }

        std::vector<std::string> valid_names = {
            {"noperiod"},
            {"has.period"},
            {"has99numbers"},
            {"has.99.numbers.and.periods"},
            {"has.CaPiToL.LeTtErS"}
        };

        for (const auto &goodName: valid_names)
        {
            std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>(goodName.c_str(), "description", SMeasureCount);
            bool added = frameworkTestManager.addMetric(pCounter);
            CPPUNIT_ASSERT(added);
        }
    }

    void Test_counter_metric_increments_properly()
    {
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("testcounter", "description", SMeasureCount);
        CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(pCounter->queryValue()));
        int expectedValue = 0;

        //
        // Test default increment (by 1)
        pCounter->inc(1);
        expectedValue++;
        int counterValue = pCounter->queryValue();
        CPPUNIT_ASSERT_EQUAL(expectedValue, counterValue);

        //
        // Test increment by > 1
        pCounter->inc(2);
        expectedValue += 2;
        counterValue = pCounter->queryValue();
        CPPUNIT_ASSERT_EQUAL(expectedValue, counterValue);

        //
        // Test fast increment
        pCounter->fastInc(3);
        expectedValue += 3;
        counterValue = pCounter->queryValue();
        CPPUNIT_ASSERT_EQUAL(expectedValue, counterValue);
    }


    void Test_gauge_metric_updates_properly()
    {
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("testgauge", "description", SMeasureCount);
        int gaugeValue = pGauge->queryValue();
        CPPUNIT_ASSERT_EQUAL(0, gaugeValue);

        //
        // Test initial setting of gauge
        pGauge->set(25);
        gaugeValue = pGauge->queryValue();
        CPPUNIT_ASSERT_EQUAL(25, gaugeValue);

        //
        // Test updating gauge
        pGauge->adjust(10);
        gaugeValue = pGauge->queryValue();
        CPPUNIT_ASSERT_EQUAL(35, gaugeValue);

        pGauge->adjust(-5);
        gaugeValue = pGauge->queryValue();
        CPPUNIT_ASSERT_EQUAL(30, gaugeValue);
    }


    void Test_custom_metric()
    {
        int customCounter = 0;
        std::shared_ptr<CustomMetric<int>> pCustomCounter = std::make_shared<CustomMetric<int>>("customcounter", "description", METRICS_COUNTER, customCounter, SMeasureCount);
        int customCounterValue = pCustomCounter->queryValue();
        CPPUNIT_ASSERT_EQUAL(0, customCounterValue);

        customCounter++;
        customCounterValue = pCustomCounter->queryValue();
        CPPUNIT_ASSERT_EQUAL(1, customCounterValue);
    }


    void Test_manager_calls_sink_to_start_and_stop_collection()
    {
        frameworkTestManager.startCollecting();
        CPPUNIT_ASSERT_EQUAL(true, pTestSink->isCollecting);
        frameworkTestManager.stopCollecting();
        CPPUNIT_ASSERT_EQUAL(false, pTestSink->isCollecting);
    }


    void Test_manager_manages_metrics_properly()
    {
        int numAdded;
        std::shared_ptr<CounterMetric> pCounter = std::make_shared<CounterMetric>("testcounter", "description", SMeasureCount);
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("testgauge", "description", SMeasureCount);
        frameworkTestManager.addMetric(pCounter);
        frameworkTestManager.addMetric(pGauge);
        numAdded = 2;

        frameworkTestManager.startCollecting();

        //
        // Make sure the initial list is correct
        int numMetrics = frameworkTestManager.queryMetricsForReport("testsink").size();
        CPPUNIT_ASSERT_EQUAL(numAdded, numMetrics);

        //
        // Add a metric while reporting is enabled and make sure it is returned
        std::shared_ptr<CounterMetric> pNewCounter = std::make_shared<CounterMetric>("testnewcounter", "description", SMeasureCount);
        frameworkTestManager.addMetric(pNewCounter);
        numAdded++;

        numMetrics = frameworkTestManager.queryMetricsForReport("testsink").size();
        CPPUNIT_ASSERT_EQUAL(numAdded, numMetrics);

        //
        // Destroy a metric and ensure it is no longer in the list of report metrics
        pNewCounter = nullptr;
        numAdded--;

        numMetrics = frameworkTestManager.queryMetricsForReport("testsink").size();
        CPPUNIT_ASSERT_EQUAL(numAdded, numMetrics);

        frameworkTestManager.stopCollecting();
    }


    void Test_scoped_updater_classes()
    {
        std::shared_ptr<GaugeMetric> pGauge = std::make_shared<GaugeMetric>("testgauge", "description", SMeasureCount);
        CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(pGauge->queryValue()));

        //
        // Test the scoped updater with a value of 1. Should increment in the block, and return
        // to 0 when exiting the block
        {
            ScopedGaugeUpdater gaugeUpdater(*pGauge, 1);
            CPPUNIT_ASSERT_EQUAL(1, static_cast<int>(pGauge->queryValue()));
        }
        CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(pGauge->queryValue()));

        //
        // Same test, but a value > 1
        {
            ScopedGaugeUpdater gaugeUpdater(*pGauge, 3);
            CPPUNIT_ASSERT_EQUAL(3, static_cast<int>(pGauge->queryValue()));
        }
        CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(pGauge->queryValue()));

        //
        // Test the scoped decrementer
        pGauge->adjust(1);
        CPPUNIT_ASSERT_EQUAL(1, static_cast<int>(pGauge->queryValue()));
        {
            ScopedGaugeDecrementer gaugeDecrementer(*pGauge, 1);
            CPPUNIT_ASSERT_EQUAL(1, static_cast<int>(pGauge->queryValue()));
        }
        CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(pGauge->queryValue()));

    }


    void Test_metric_meta_data()
    {

        //
        // Test that two metrics with the same base name, but differing meta data are added w/o error. Note code shows passing meta data as an externally
        // constructed vector and as a vector constructed in place.
        MetricMetaData metaData1{{"key1", "value1"}};
        std::shared_ptr<CounterMetric> pCounter1 = std::make_shared<CounterMetric>("requests.completed", "description", SMeasureCount, metaData1);
        std::shared_ptr<CounterMetric> pCounter2 = std::make_shared<CounterMetric>("requests.completed", "description", SMeasureCount,
                                                                                   MetricMetaData{{"key1", "value2"}});

        frameworkTestManager.addMetric(pCounter1);

        bool success = false;
        try
        {
            success = frameworkTestManager.addMetric(pCounter2);
        }
        catch (IException *e)
        {
            success = false;
            e->Release();
        }
        CPPUNIT_ASSERT(success);

        //
        // Test that two metrics with the same base name and meta data returns a false when adding to indicate a non-unique name
        MetricMetaData metaData2{{"key1", "value1"}};
        std::shared_ptr<CounterMetric> pCounter3 = std::make_shared<CounterMetric>("requests.queued", "description", SMeasureCount, metaData2);
        std::shared_ptr<CounterMetric> pCounter4 = std::make_shared<CounterMetric>("requests.queued", "description", SMeasureCount,
                                                                                   MetricMetaData{{"key1", "value1"}});
        frameworkTestManager.addMetric(pCounter3);
        success = false;
        try
        {
            // Expect a return of false if not debug (not a unique named metric)
            success = !frameworkTestManager.addMetric(pCounter4);
        }
        catch (IException *e)
        {
            success = true;  // exception expected in debug mode
            e->Release();
        }
        CPPUNIT_ASSERT(success);


        //
        // Delete a metric and try to add a like named metric to ensure it does not report an existing metric
        std::shared_ptr<CounterMetric> pCounter5 = std::make_shared<CounterMetric>("duprequests.queued", "description", SMeasureCount);
        std::shared_ptr<CounterMetric> pCounter6 = std::make_shared<CounterMetric>("duprequests.queued", "description", SMeasureCount);
        frameworkTestManager.addMetric(pCounter5);
        pCounter5.reset();
        success = false;
        try
        {
            // Expect a return of true since the like named metric was deleted
            success = frameworkTestManager.addMetric(pCounter6);
        }
        catch (IException *e)
        {
            success = false;
            e->Release();
        }
        CPPUNIT_ASSERT(success);
    }


    void Test_gauge_by_counters_metric()
    {
        std::shared_ptr<CounterMetric> pCounterTotal = std::make_shared<CounterMetric>("requests.received", "description", SMeasureCount);
        std::shared_ptr<CounterMetric> pCounterStarted = std::make_shared<CounterMetric>("requests.started", "description", SMeasureCount);

        std::shared_ptr<GaugeMetricFromCounters> pCounterGauge = std::make_shared<GaugeMetricFromCounters>("requests.waiting", "description", SMeasureCount, pCounterTotal, pCounterStarted);

        pCounterTotal->inc(4);
        pCounterStarted->inc(2);
        CPPUNIT_ASSERT_EQUAL(2, static_cast<int>(pCounterGauge->queryValue()));

        pCounterStarted->inc(2);
        CPPUNIT_ASSERT_EQUAL(0, static_cast<int>(pCounterGauge->queryValue()));
    }


    void Test_histogram_metric()
    {
        //std::vector<BucketDef> bucketDefs = {{"le 2", 2}, {"le 4", 4}, {"le 8", 8} };
        std::vector<__uint64> bucketDefs = {2, 4, 8};
        std::shared_ptr<HistogramMetric> pHistogram = std::make_shared<HistogramMetric>("requests.dist", "description", SMeasureCount, bucketDefs);

        std::vector<__uint64> values;

        //
        // Verify that the correct number of buckets is returned
        values = pHistogram->queryHistogramValues();
        CPPUNIT_ASSERT_EQUAL(bucketDefs.size()+1, values.size());

        //
        // Add a measurement and make sure it goes into the correct bucket
        int sumMeasurements = 0;

        pHistogram->recordMeasurement(4);
        sumMeasurements += 4;
        checkHistogramBucketResult(pHistogram, sumMeasurements, {0,1,0,0});

        //
        // Add same measurement and make sure the bucket increments
        pHistogram->recordMeasurement(4);
        sumMeasurements += 4;
        checkHistogramBucketResult(pHistogram, sumMeasurements, {0,2,0,0});

        //
        // Add a measurement that is beyond the highest bucket and make sure it goes in the
        // last "inf" bucket
        pHistogram->recordMeasurement(20);
        sumMeasurements += 20;
        checkHistogramBucketResult(pHistogram, sumMeasurements, {0,2,0,1});

        //
        // Just to make sure, increment another bucket
        pHistogram->recordMeasurement(8);
        sumMeasurements += 8;
        checkHistogramBucketResult(pHistogram, sumMeasurements, {0,2,1,1});

        //
        // Test scaled histogram for ns.
        // Duplicate the convenience function so we don't actually add the metric to the manager
        double nsToCyclesScaleFactor = 1.0 / getCycleToNanoScale();
        std::vector<__uint64> timeBuckets = {500000000, 1000000000, 2000000000 };
        std::shared_ptr<ScaledHistogramMetric> pScaledHistogram = std::shared_ptr<ScaledHistogramMetric>(new ScaledHistogramMetric("requests.time", "description", SMeasureTimeNs, timeBuckets, nsToCyclesScaleFactor));

        __uint64 totalDelay = 0;

        //
        // test a duration < 500ms
        {
            HistogramExecutionTimer h(pScaledHistogram);
            usleep(250*1000);
            totalDelay += 250000000;
        }

        __uint64 delta = 50000000;  // (50 ms)

        checkHistogramBucketResult(pScaledHistogram, -1, {1,0,0,0});
        checkHistogramValue(pScaledHistogram, totalDelay, delta);

        //
        // duration > 500ms, < 1000mx
        {
            HistogramExecutionTimer h(pScaledHistogram);
            usleep(750*1000);
            totalDelay += 750000000;
        }
        checkHistogramBucketResult(pScaledHistogram, -1, {1,1,0,0});
        checkHistogramValue(pScaledHistogram, totalDelay, delta);

        //
        // duration > 1000ms, < 2000ms
        {
            HistogramExecutionTimer h(pScaledHistogram);
            usleep(1250*1000);
            totalDelay += 1250000000;
        }
        checkHistogramBucketResult(pScaledHistogram, -1, {1,1,1,0});
        checkHistogramValue(pScaledHistogram, totalDelay, delta);

        //
        // duration > 2000ms (increments the inf bucket)
        {
            HistogramExecutionTimer h(pScaledHistogram);
            usleep(2500*1000);
            totalDelay += 2500000000;
        }
        checkHistogramBucketResult(pScaledHistogram, -1, {1,1,1,1});
        checkHistogramValue(pScaledHistogram, totalDelay, delta);
    }


    void checkHistogramBucketResult(std::shared_ptr<IMetric> pHistogram, int expectedSum, const std::vector<int> &expectedValues)
    {
        std::vector<__uint64> values = pHistogram->queryHistogramValues();
        auto size = expectedValues.size();
        for (unsigned i=0; i<size; ++i)
        {
            CPPUNIT_ASSERT_EQUAL(static_cast<int>(values[i]), expectedValues[i]);
        }

        // expected sum check if < 0
        if (expectedSum >= 0)
            CPPUNIT_ASSERT_EQUAL(static_cast<int>(pHistogram->queryValue()), expectedSum);
    }


    void checkHistogramValue(const std::shared_ptr<IMetric> &pHistogram, __uint64 expectedValue, __uint64 error)
    {
        __uint64 value = pHistogram->queryValue();

        bool result = ((expectedValue >= (value - error)) && (expectedValue <= (value + error)));
        CPPUNIT_ASSERT(result);
    }

protected:
    MetricsManager frameworkTestManager;
    MetricFrameworkTestSink *pTestSink;
};

CPPUNIT_TEST_SUITE_REGISTRATION( MetricFrameworkTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( MetricFrameworkTests, "MetricFrameworkTests" );

#endif
