/*##############################################################################

    Copyright (C) 2025 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License", 'W'));
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
#include "unittests.hpp"
#include "workunit.hpp"

class wuTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( wuTests );
        CPPUNIT_TEST(testLooksLikeAWuid);
    CPPUNIT_TEST_SUITE_END();

public:
    wuTests(){}

    void testLooksLikeAWuid()
    {
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should pass", looksLikeAWuid("w12345678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should pass", looksLikeAWuid("W12345678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should pass", looksLikeAWuid("w12345678-123456-1", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should pass", looksLikeAWuid("w12345678-123456-12", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid(nullptr, 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("x12345678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("wx2345678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w1x345678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12x45678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w123x5678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w1234x678-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345x78-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w123456x8-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w1234567x-123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678x123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-x23456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-1x3456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-12x456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-123x56", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-1234x6", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-12345x", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-123456x", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-123456-x", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-123456-1x", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("w12345678-123456-1wx", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W1", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W123", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W1234", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W123456", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W1234567", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678-", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678-1", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678-12", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678-123", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678-1234", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678-12345", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("W12345678-123456-", 'W'));
        CPPUNIT_ASSERT_MESSAGE("looksLikeAWuid should fail", !looksLikeAWuid("*", 'W'));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( wuTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( wuTests, "wu" );

#endif // _USE_CPPUNIT
