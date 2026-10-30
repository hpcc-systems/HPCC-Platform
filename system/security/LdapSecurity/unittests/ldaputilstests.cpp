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

#include "ldaputils.hpp"
#include "unittests.hpp"

// LdapUtils's DN-manipulation helpers are public static methods (system/security/LdapSecurity/
// ldaputils.hpp) reached directly from ldapconnection.cpp; these tests exercise them through
// their public API -- no white-boxing required. They are pure string/binary logic with no LDAP
// connection dependency.
class LdapUtilsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LdapUtilsTest);
    CPPUNIT_TEST(testBin2StrEmptyBuffer);
    CPPUNIT_TEST(testBin2StrEscapesEachByteAsHex);
    CPPUNIT_TEST(testContainsBasednDetectsDcComponent);
    CPPUNIT_TEST(testContainsBasednRejectsNullOrEmpty);
    CPPUNIT_TEST(testCleanupDnLowercasesAndHandlesNullOrEmpty);
    CPPUNIT_TEST(testNormalizeDnAppendsBasednWhenMissing);
    CPPUNIT_TEST(testNormalizeDnLeavesBasednAloneWhenPresent);
    CPPUNIT_TEST(testGetNameExtractsFirstRdnValue);
    CPPUNIT_TEST(testGetNameFallsBackToWholeStringWithoutEquals);
    CPPUNIT_TEST_SUITE_END();

public:
    // *(spec-driven)*: an empty MemoryBuffer must produce an empty escaped string.
    void testBin2StrEmptyBuffer()
    {
        MemoryBuffer from;
        StringBuffer to;
        LdapUtils::bin2str(from, to);
        CPPUNIT_ASSERT_EQUAL_STR("", to.str());
    }

    // *(spec-driven)*: each input byte is rendered as "\" followed by 2 uppercase hex digits
    // (used to build LDAP-escaped binary attribute values, e.g. objectGUID/objectSid searches).
    void testBin2StrEscapesEachByteAsHex()
    {
        MemoryBuffer from;
        unsigned char bytes[] = { 0x41, 0x0A, 0xFF, 0x00 };
        from.append(sizeof(bytes), bytes);

        StringBuffer to;
        LdapUtils::bin2str(from, to);
        CPPUNIT_ASSERT_EQUAL_STR("\\41\\0A\\FF\\00", to.str());
    }

    // *(spec-driven)*: containsBasedn() looks for the literal "dc=" substring.
    void testContainsBasednDetectsDcComponent()
    {
        CPPUNIT_ASSERT(LdapUtils::containsBasedn("cn=user1,dc=example,dc=com"));
        CPPUNIT_ASSERT(LdapUtils::containsBasedn("dc=example,dc=com"));
        CPPUNIT_ASSERT(!LdapUtils::containsBasedn("cn=user1,ou=users"));
    }

    void testContainsBasednRejectsNullOrEmpty()
    {
        CPPUNIT_ASSERT(!LdapUtils::containsBasedn(nullptr));
        CPPUNIT_ASSERT(!LdapUtils::containsBasedn(""));
    }

    // *(spec-driven)*: cleanupDn() lowercases the DN (LDAP DNs are case-insensitive at the
    // attribute-type level; this normalizes for comparison/storage) and is a no-op for
    // NULL/empty input.
    void testCleanupDnLowercasesAndHandlesNullOrEmpty()
    {
        StringBuffer out;
        LdapUtils::cleanupDn("CN=User1,DC=Example,DC=Com", out);
        CPPUNIT_ASSERT_EQUAL_STR("cn=user1,dc=example,dc=com", out.str());

        StringBuffer emptyOut;
        LdapUtils::cleanupDn(nullptr, emptyOut);
        CPPUNIT_ASSERT_EQUAL_STR("", emptyOut.str());

        StringBuffer emptyOut2;
        LdapUtils::cleanupDn("", emptyOut2);
        CPPUNIT_ASSERT_EQUAL_STR("", emptyOut2.str());
    }

    // *(spec-driven)*: normalizeDn() cleans up (lowercases) the DN and appends the configured
    // basedn if the DN doesn't already contain one.
    void testNormalizeDnAppendsBasednWhenMissing()
    {
        StringBuffer out;
        LdapUtils::normalizeDn("CN=User1", "dc=example,dc=com", out);
        CPPUNIT_ASSERT_EQUAL_STR("cn=user1,dc=example,dc=com", out.str());
    }

    void testNormalizeDnLeavesBasednAloneWhenPresent()
    {
        StringBuffer out;
        LdapUtils::normalizeDn("CN=User1,DC=Other,DC=Com", "dc=example,dc=com", out);
        CPPUNIT_ASSERT_EQUAL_STR("cn=user1,dc=other,dc=com", out.str());
    }

    // *(characterization)*: getName() extracts the value of the first RDN component (the text
    // after the first '=' up to the next ',', or to the end of string if there's no comma).
    void testGetNameExtractsFirstRdnValue()
    {
        StringBuffer name;
        LdapUtils::getName("cn=user1,dc=example,dc=com", name);
        CPPUNIT_ASSERT_EQUAL_STR("user1", name.str());

        StringBuffer name2;
        LdapUtils::getName("cn=onlyrdn", name2);
        CPPUNIT_ASSERT_EQUAL_STR("onlyrdn", name2.str());
    }

    // *(characterization)*: with no '=' at all, getName() falls back to returning the whole
    // input string unchanged.
    void testGetNameFallsBackToWholeStringWithoutEquals()
    {
        StringBuffer name;
        LdapUtils::getName("not-a-dn", name);
        CPPUNIT_ASSERT_EQUAL_STR("not-a-dn", name.str());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(LdapUtilsTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(LdapUtilsTest, "LdapUtilsTest");

#endif // _USE_CPPUNIT
