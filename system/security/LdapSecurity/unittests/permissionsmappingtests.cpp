/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2026 HPCC Systems®.

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

#include "permissions.ipp"
#include "unittests.hpp"

// PermissionProcessor::{ldap2sec,sec2ldap,ldap2newsec,newsec2ldap} are protected, so this
// test-only subclass exposes them as public (standard white-box pattern; no production code
// changes required).
class TestPermissionProcessor : public PermissionProcessor
{
public:
    TestPermissionProcessor(IPropertyTree *cfg) : PermissionProcessor(cfg) {}
    using PermissionProcessor::ldap2sec;
    using PermissionProcessor::sec2ldap;
    using PermissionProcessor::ldap2newsec;
    using PermissionProcessor::newsec2ldap;
};

class PermissionMappingTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PermissionMappingTest);
    CPPUNIT_TEST(testSecAccessRoundTrip);
    CPPUNIT_TEST(testSecAccessSentinelValues);
    CPPUNIT_TEST(testSecAccessKnownLdapValues);
    CPPUNIT_TEST(testSecAccessFullCollapse);
    CPPUNIT_TEST(testSecAccessUnmappedBitsDropped);
    CPPUNIT_TEST(testNewSecAccessRoundTrip);
    CPPUNIT_TEST(testNewSecAccessKnownLdapValues);
    CPPUNIT_TEST(testNewSecAccessFullCollapse);
    CPPUNIT_TEST(testToXpathSpaceAndQuoteRules);
    CPPUNIT_TEST(testToXpathRejectsEmptyOrNull);
    CPPUNIT_TEST_SUITE_END();

    Owned<TestPermissionProcessor> processor;

public:
    void setUp() override
    {
        // PermissionProcessor's constructor only requires a non-NULL config; none of the
        // mapping functions under test touch it, so an empty tree is sufficient.
        processor.setown(new TestPermissionProcessor(createPTree()));
    }

    void tearDown() override
    {
        processor.clear();
    }

    // (characterization)/self-consistency: ldap2sec(sec2ldap(x)) == x for every canonical
    // (non-negative) SecAccessFlags value, i.e. the ones that represent an actual encodable ACL
    // permission. This proves the two directions are mutually consistent, not that either
    // direction matches an external ADS_RIGHT_* spec (see testSecAccessKnownLdapValues for that).
    // SecAccess_Unavailable/SecAccess_Unknown are negative sentinel status codes rather than
    // encodable permissions - see testSecAccessSentinelValues for how sec2ldap()
    // actually handles them.
    void testSecAccessRoundTrip()
    {
        static const SecAccessFlags values[] = { SecAccess_None, SecAccess_Access, SecAccess_Read, SecAccess_Write, SecAccess_Full };
        for (SecAccessFlags value : values)
            CPPUNIT_ASSERT_EQUAL(value, processor->ldap2sec(processor->sec2ldap(value)));
    }

    // (characterization): sec2ldap() only inspects the low byte of its input (see
    // testSecAccessFullCollapse), so the negative sentinel values SecAccess_Unavailable (-1) and
    // SecAccess_Unknown (-255) are not rejected - each aliases to the bit pattern of some real
    // permission and silently round-trips to that permission rather than being preserved or
    // rejected as a sentinel. This documents that quirk rather than asserting it's the
    // correct/intended behaviour.
    void testSecAccessSentinelValues()
    {
        // SecAccess_Unavailable's low byte is 0xFF, so it aliases directly to SecAccess_Full.
        CPPUNIT_ASSERT_EQUAL(0xf01ffu, processor->sec2ldap(SecAccess_Unavailable));
        CPPUNIT_ASSERT_EQUAL(SecAccess_Full, processor->ldap2sec(processor->sec2ldap(SecAccess_Unavailable)));

        // SecAccess_Unknown's low byte (0x01) happens to match SecAccess_Access's bit pattern,
        // so it round-trips to SecAccess_Access rather than being rejected or preserved as Unknown.
        CPPUNIT_ASSERT_EQUAL(0x00080u, processor->sec2ldap(SecAccess_Unknown));
        CPPUNIT_ASSERT_EQUAL(SecAccess_Access, processor->ldap2sec(processor->sec2ldap(SecAccess_Unknown)));
    }

    // (spec-driven): pins sec2ldap()'s output against the documented AD ADS_RIGHT_* bit values
    // it's meant to produce, so a future edit can't silently change the wire encoding while still
    // passing the self-consistency round-trip check above.
    void testSecAccessKnownLdapValues()
    {
        CPPUNIT_ASSERT_EQUAL(0x00000u, processor->sec2ldap(SecAccess_None));
        CPPUNIT_ASSERT_EQUAL(0x00080u, processor->sec2ldap(SecAccess_Access));   // ADS_RIGHT_DS_LIST_OBJECT
        CPPUNIT_ASSERT_EQUAL(0x20094u, processor->sec2ldap(SecAccess_Read));
        CPPUNIT_ASSERT_EQUAL(0x200bcu, processor->sec2ldap(SecAccess_Write));
        CPPUNIT_ASSERT_EQUAL(0xf01ffu, processor->sec2ldap(SecAccess_Full));
    }

    // (characterization): any ldap permission value whose low byte is 0xFF collapses to
    // SecAccess_Full regardless of any bits set above that byte - documenting this quirk rather
    // than asserting it's the only correct interpretation.
    void testSecAccessFullCollapse()
    {
        CPPUNIT_ASSERT_EQUAL(SecAccess_Full, processor->ldap2sec(0x000000FFu));
        CPPUNIT_ASSERT_EQUAL(SecAccess_Full, processor->ldap2sec(0x123400FFu));
    }

    // (characterization): ldap bits that don't correspond to any recognised right are silently
    // dropped rather than rejected or logged.
    void testSecAccessUnmappedBitsDropped()
    {
        CPPUNIT_ASSERT_EQUAL(SecAccess_None, processor->ldap2sec(0x00000001u));
        CPPUNIT_ASSERT_EQUAL(SecAccess_None, processor->ldap2sec(0x00000040u));
    }

    // (characterization)/self-consistency for every individual bit and combination of
    // NewSecAccessFlags bits - these are true independent flags (unlike the legacy
    // SecAccessFlags cascading values), so every combination is meaningful.
    void testNewSecAccessRoundTrip()
    {
        static const NewSecAccessFlags values[] = {
            NewSecAccess_None, NewSecAccess_Access, NewSecAccess_Read, NewSecAccess_Write, NewSecAccess_Full,
            (NewSecAccessFlags)(NewSecAccess_Access | NewSecAccess_Read),
            (NewSecAccessFlags)(NewSecAccess_Access | NewSecAccess_Write),
            (NewSecAccessFlags)(NewSecAccess_Read | NewSecAccess_Write),
            (NewSecAccessFlags)(NewSecAccess_Access | NewSecAccess_Read | NewSecAccess_Write)
        };
        for (NewSecAccessFlags value : values)
            CPPUNIT_ASSERT_EQUAL(value, processor->ldap2newsec(processor->newsec2ldap(value)));
    }

    // (spec-driven): pins newsec2ldap()'s output against the documented AD ADS_RIGHT_* bit values.
    // Note: the production code's Access-bit check tests against the legacy SecAccess_Access
    // constant rather than NewSecAccess_Access; both are defined as 1, so the encoding below is
    // unaffected today, but a future renumbering of either enum would silently break this. The
    // static_assert below makes that coupling explicit and fails the build (rather than just this
    // test) if either enum's Access value ever changes.
    static_assert(static_cast<int>(SecAccess_Access) == static_cast<int>(NewSecAccess_Access), "newsec2ldap() relies on SecAccess_Access and NewSecAccess_Access staying numerically equal");
    void testNewSecAccessKnownLdapValues()
    {
        CPPUNIT_ASSERT_EQUAL(0x00000u, processor->newsec2ldap(NewSecAccess_None));
        CPPUNIT_ASSERT_EQUAL(0x00080u, processor->newsec2ldap(NewSecAccess_Access));
        CPPUNIT_ASSERT_EQUAL(0x20014u, processor->newsec2ldap(NewSecAccess_Read));
        CPPUNIT_ASSERT_EQUAL(0x00028u, processor->newsec2ldap(NewSecAccess_Write));
        CPPUNIT_ASSERT_EQUAL(0xf01ffu, processor->newsec2ldap(NewSecAccess_Full));
    }

    // (characterization): same low-byte-0xFF collapse quirk as SecAccessFlags.
    void testNewSecAccessFullCollapse()
    {
        CPPUNIT_ASSERT_EQUAL(NewSecAccess_Full, processor->ldap2newsec(0x000000FFu));
        CPPUNIT_ASSERT_EQUAL(NewSecAccess_Full, processor->ldap2newsec(0x123400FFu));
    }

    // (spec-driven): toXpath()'s own two documented rules - replace space with ':', reject
    // embedded single quotes - are stated plainly enough to test without inspecting the body.
    void testToXpathSpaceAndQuoteRules()
    {
        StringBuffer out;
        CPPUNIT_ASSERT(toXpath("plainname", out));
        CPPUNIT_ASSERT_EQUAL_STR("plainname", out.str());

        out.clear();
        CPPUNIT_ASSERT(toXpath("a name with spaces", out));
        CPPUNIT_ASSERT_EQUAL_STR("a:name:with:spaces", out.str());

        out.clear();
        CPPUNIT_ASSERT(!toXpath("bad'name", out));
    }

    void testToXpathRejectsEmptyOrNull()
    {
        StringBuffer out;
        CPPUNIT_ASSERT(!toXpath(nullptr, out));
        CPPUNIT_ASSERT(!toXpath("", out));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(PermissionMappingTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(PermissionMappingTest, "PermissionMappingTest");

#endif
