/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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
#include "jsem.hpp"

#include "unittests.hpp"

class JlibSemTest : public CppUnit::TestFixture
{
public:
    CPPUNIT_TEST_SUITE(JlibSemTest);
        CPPUNIT_TEST(testSetup);
        CPPUNIT_TEST(testSimple);
        CPPUNIT_TEST(testCleanup);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testSetup()
    {
    }

    void testCleanup()
    {
    }

    void testTimedAvailable(Semaphore & sem)
    {
        unsigned now = msTick();
        sem.wait(100);
        unsigned taken = msTick() - now;
        //Shouldn't cause a reschedule, definitely shouldn't wait for 100s
        ASSERT(taken < 5);
    }
    void testTimedElapsed(Semaphore & sem, unsigned time)
    {
        unsigned now = msTick();
        sem.wait(time);
        unsigned taken = msTick() - now;
        ASSERT(taken >= time && taken < 2*time);
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


#endif // _USE_CPPUNIT
