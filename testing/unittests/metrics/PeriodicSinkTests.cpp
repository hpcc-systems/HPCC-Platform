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

#include "jptree.hpp"
#include "jmetrics.hpp"

using namespace hpccMetrics;

class PeriodicTestSink : public PeriodicMetricSink
{
public:
    explicit PeriodicTestSink(const char *name, const IPropertyTree *pSettingsTree) :
        PeriodicMetricSink(name, "test", pSettingsTree) { }

    ~PeriodicTestSink() = default;

protected:
    virtual void prepareToStartCollecting() override
    {
        prepareCalled = true;
        numCollections = 0;
    }

    virtual void collectingHasStopped() override
    {
        stopCollectionNotificationCalled = true;
    }

    void doCollection() override
    {
        numCollections++;
    }

public:
    bool prepareCalled = false;
    bool stopCollectionNotificationCalled = false;
    int numCollections = 0;
};


MetricsReporter periodicSinkTestReporter;

const char *periodicSinkSettingsTestYml = R"!!(period: 2
)!!";

int period = 2;

PeriodicTestSink *pPeriodicTestSink = nullptr;

class PeriodicSinkTests : public CppUnit::TestFixture
{
    public:
        PeriodicSinkTests()
        {
            if (pPeriodicTestSink == nullptr)
            {
                //
                // Load the settings then set the period using the global var
                Owned<IPropertyTree> pSettings = createPTreeFromYAMLString(periodicSinkSettingsTestYml, ipt_none, ptr_ignoreWhiteSpace, nullptr);
                pSettings->setPropInt("@period", period);
                pPeriodicTestSink = new PeriodicTestSink("periodic_test_sink", pSettings);
                periodicSinkTestReporter.addSink(pPeriodicTestSink, "periodic_test_sink");
            }
        }

    CPPUNIT_TEST_SUITE(PeriodicSinkTests);
        CPPUNIT_TEST(Test_sets_period_correctly);
    CPPUNIT_TEST_SUITE_END();

    protected:

        void Test_sets_period_correctly()
        {
            //
            // To test setting the period correctly, start collection and delay a multiple of that period.
            // Stop collection and ask the test sink how many collections were done. If the count is +/- 1
            // from the wait period multiple used, then we are close enough
            int multiple = 3;
            periodicSinkTestReporter.startCollecting();

            //
            // Check that the sink called to prepare for collection
            CPPUNIT_ASSERT_MESSAGE("Expected sink to report it was collecting", pPeriodicTestSink->prepareCalled);

            // wait... then stop collecting
            sleep(multiple * period);
            periodicSinkTestReporter.stopCollecting();

            int numCollections = pPeriodicTestSink->numCollections;

            bool numReportsCorrect = (numCollections >= multiple-1) && (numCollections <= multiple+1);
            CPPUNIT_ASSERT_EQUAL_MESSAGE(VStringBuffer("The number of reports was incorrect, it was expected to be between %d and %d", multiple-1, multiple+1).str(), true, numReportsCorrect);

            //
            // Verify collection was stopped
            CPPUNIT_ASSERT_MESSAGE("Stop collection was not called", pPeriodicTestSink->stopCollectionNotificationCalled);
        }
};

CPPUNIT_TEST_SUITE_REGISTRATION( PeriodicSinkTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( PeriodicSinkTests, "PeriodicSinkTests" );

#endif
