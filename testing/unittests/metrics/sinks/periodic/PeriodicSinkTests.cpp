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

#include "jptree.hpp"
#include "jmetrics.hpp"

#include "PeriodicTestSink.hpp"

static MetricsReporter &reporter = queryMetricsReporter();

const char *periodicSinkSettingsTestYml = R"!!(period: 2
)!!";

int period = 5;

PeriodicTestSink *pPeriodicTestSink = nullptr;

void usage()
{
    printf("\n"
           "Usage:\n"
           "    periodicsinktests>\n"
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
                reporter.addSink(pPeriodicTestSink, "periodic_test_sink");
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
            int multiple = 5;
            reporter.startCollecting();

            //
            // Check that the sink is collecting
            CPPUNIT_ASSERT_MESSAGE("Expected sink to report it was collecting", pPeriodicTestSink->isCurrentlyCollectiing());

            // wait... then stop collecing
            sleep(multiple * period);
            reporter.stopCollecting();

            int numCollections = pPeriodicTestSink->getNumCollections();

            char msg[256];
            sprintf(msg, "The number of reports was incorrect, it was expected to be between %d and %d", multiple-1, multiple+1);
            bool numReportsCorrect = (numCollections >= multiple-1) && (numCollections <= multiple+1);
            CPPUNIT_ASSERT_EQUAL_MESSAGE(msg, true, numReportsCorrect);

            //
            // Verify that some flags are also set properly after collection has stopped
            CPPUNIT_ASSERT_MESSAGE("Collection stop flag not set properly", pPeriodicTestSink->isCollectionStopped());
            CPPUNIT_ASSERT_MESSAGE("Stop collection was not called", pPeriodicTestSink->isCollectionStoppedCalled());
        }
};

CPPUNIT_TEST_SUITE_REGISTRATION( PeriodicSinkTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( PeriodicSinkTests, "PeriodicSinkTests" );

#endif
