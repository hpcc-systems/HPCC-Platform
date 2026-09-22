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

#include "ldapsanitization.hpp"
#include "ldaptimeutils.hpp"
#include "aci.ipp"
#include "unittests.hpp"

class LdapSanitizationTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LdapSanitizationTest);
    CPPUNIT_TEST(testAuthenticateFilterUsernameEscaping);
    CPPUNIT_TEST(testFilterEscapeCases);
    CPPUNIT_TEST(testFilterEscapeMaliciousUsernamePatterns);
    CPPUNIT_TEST(testFilterEscapeEmbeddedNul);
    CPPUNIT_TEST(testFilterEscapeHighBytes);
    CPPUNIT_TEST(testDnEscapeCases);
    CPPUNIT_TEST(testSearchStringWildcardFilterInjection);
    CPPUNIT_TEST(testScopeDnComponentEscaping);
    CPPUNIT_TEST(testUsernameContainsLdapUrlForbiddenChars);
    CPPUNIT_TEST_SUITE_END();

public:
    void assertFilterEscape(const char *input, const char *expected)
    {
        StringBuffer escaped;
        appendEscapedLdapFilter(input, escaped);
        CPPUNIT_ASSERT_EQUAL_STR(expected, escaped.str());
    }

    void assertSizedFilterEscape(size_t inputLength, const char *input, const char *expected)
    {
        StringBuffer escaped;
        appendEscapedLdapFilter(inputLength, input, escaped);
        CPPUNIT_ASSERT_EQUAL_STR(expected, escaped.str());
    }

    void assertDnEscape(const char *input, const char *expected)
    {
        StringBuffer escaped;
        escapeLdapDistinguishedName(input, escaped);
        CPPUNIT_ASSERT_EQUAL_STR(expected, escaped.str());
    }

    void testAuthenticateFilterUsernameEscaping()
    {
        StringBuffer escaped;
        StringBuffer filter;
        appendEscapedLdapFilter("adm*in", escaped);
        filter.append("sAMAccountName=").append(escaped);
        CPPUNIT_ASSERT_EQUAL_STR("sAMAccountName=adm\\2ain", filter.str());
    }

    void testFilterEscapeCases()
    {
        static const struct
        {
            const char *input;
            const char *expected;
        } cases[] = {
            { "alice", "alice" },
            { "a*b(c)\\d", "a\\2ab\\28c\\29\\5cd" }
        };

        for (const auto &testCase : cases)
            assertFilterEscape(testCase.input, testCase.expected);
    }

    void testFilterEscapeMaliciousUsernamePatterns()
    {
        static const struct
        {
            const char *input;
            const char *expected;
        } cases[] = {
            { "*)(uid=*)", "\\2a\\29\\28uid=\\2a\\29" },
            { "admin)(|(uid=*))", "admin\\29\\28|\\28uid=\\2a\\29\\29" },
            { "john\\*)(mail=*)", "john\\5c\\2a\\29\\28mail=\\2a\\29" }
        };

        for (const auto &testCase : cases)
        {
            assertFilterEscape(testCase.input, testCase.expected);
        }
    }

    void testFilterEscapeEmbeddedNul()
    {
        const char input[] = { 'a', '\0', 'b' };
        assertSizedFilterEscape(sizeof(input), input, "a\\00b");
    }

    void testFilterEscapeHighBytes()
    {
        // Bytes > 0x7F that are not valid UTF-8 must be hex-escaped per RFC 4515.
        const char input[] = { 'a', '\x80', '\xff', 'b' };
        assertSizedFilterEscape(sizeof(input), input, "a\\80\\ffb");
    }

    void testDnEscapeCases()
    {
        static const struct
        {
            const char *input;
            const char *expected;
        } cases[] = {
            { "#Smith,John+Admin\\Root;", "\\#Smith\\,John\\+Admin\\\\Root\\;" },
            { " test ", "\\ test\\ " },
            { "  test", "\\ \\ test" },
            { "test  ", "test\\ \\ " },
            { "  test  ", "\\ \\ test\\ \\ " },
            { "   ", "\\ \\ \\ " }
        };

        for (const auto &testCase : cases)
            assertDnEscape(testCase.input, testCase.expected);
    }

    // Verifies that a user-supplied search string is escaped before being embedded
    // between wildcards in an LDAP filter.
    // Without escaping, input like ")(objectClass=*" could inject additional filter terms.
    void testSearchStringWildcardFilterInjection()
    {
        static const struct
        {
            const char *searchstr;
            const char *expectedFilter;
        } cases[] = {
            // Benign search — passes through unmodified.
            { "alice",         "(&(objectClass=User)(|(uid=*alice*)))"           },
            // Embedded wildcard character — encoded, not treated as a filter wildcard.
            { "a*",            "(&(objectClass=User)(|(uid=*a\\2a*)))"           },
            // Injection attempt: close the group and append an unrestricted filter.
            { ")(objectClass=*", "(&(objectClass=User)(|(uid=*\\29\\28objectClass=\\2a*)))" },
        };

        for (const auto &tc : cases)
        {
            StringBuffer filter("objectClass=User");
            filter.insert(0, "(&(");
            StringBuffer escapedSearch;
            appendEscapedLdapFilter(tc.searchstr, escapedSearch);
            filter.appendf(")(|(uid=*%s*)))", escapedSearch.str());
            CPPUNIT_ASSERT_EQUAL_STR(tc.expectedFilter, filter.str());
        }
    }

    // Verifies the getPermissionsArray fix: resource scope components are
    // passed through the sized overload of escapeLdapDistinguishedName before
    // being appended to the DN buffer.  Without escaping, a component
    // containing ',' would break the DN structure and could redirect the
    // operation to a different directory entry.
    void testScopeDnComponentEscaping()
    {
        // Comma injection: "sales,ou=admin" must not split the RDN.
        StringBuffer dn;
        const char *component = "sales,ou=admin";
        dn.append("ou=");
        escapeLdapDistinguishedName(strlen(component), component, dn);
        dn.append(",dc=example,dc=com");
        CPPUNIT_ASSERT_EQUAL_STR("ou=sales\\,ou=admin,dc=example,dc=com", dn.str());

        // Sized overload: only the specified byte count is processed.
        dn.clear();
        dn.append("cn=");
        escapeLdapDistinguishedName(5, "team+evil", dn);
        dn.append(",dc=example,dc=com");
        CPPUNIT_ASSERT_EQUAL_STR("cn=team\\+,dc=example,dc=com", dn.str());
    }

    void testUsernameContainsLdapUrlForbiddenChars()
    {
        // Clean usernames — no forbidden chars
        CPPUNIT_ASSERT(!usernameContainsLdapUrlForbiddenChars("alice"));
        CPPUNIT_ASSERT(!usernameContainsLdapUrlForbiddenChars("domain\\alice"));
        CPPUNIT_ASSERT(!usernameContainsLdapUrlForbiddenChars(nullptr));

        // '"' enables ACI clause injection
        CPPUNIT_ASSERT(usernameContainsLdapUrlForbiddenChars("foo\")(userdn=\"ldap:///anyone"));
        CPPUNIT_ASSERT(usernameContainsLdapUrlForbiddenChars("alice\""));

        // '?' truncates the DN component of the ldap:/// URL
        CPPUNIT_ASSERT(usernameContainsLdapUrlForbiddenChars("alice?attrs=*"));
        CPPUNIT_ASSERT(usernameContainsLdapUrlForbiddenChars("alice?"));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(LdapSanitizationTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(LdapSanitizationTest, "LdapSanitizationTest");

// White-box tests for CAci, defined entirely within aci.cpp with no header
// declaration of its own. createAciForTest() (aci.cpp, guarded by
// _USE_CPPUNIT) exposes just enough to construct instances for testing via
// the IAci interface.
class CAciTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(CAciTest);
    CPPUNIT_TEST(testWellFormedAllow);
    CPPUNIT_TEST(testWellFormedDenyGroupdn);
    CPPUNIT_TEST(testOrSeparatedUserdnGroupdn);
    CPPUNIT_TEST(testMissingTargetattrAndTargetClauses);
    CPPUNIT_TEST(testMissingUserGroupDnClause);
    CPPUNIT_TEST(testEmbeddedQuoteTruncatesDnValue);
    CPPUNIT_TEST(testUnterminatedQuotedValueDoesNotOverrun);
    CPPUNIT_TEST(testAdversariallyLongInputDoesNotHang);
    CPPUNIT_TEST(testPermissionFlagConstructorUser);
    CPPUNIT_TEST(testPermissionFlagConstructorGroupDeny);
    CPPUNIT_TEST_SUITE_END();

public:
    void testWellFormedAllow()
    {
        Owned<IAci> aci = createAciForTest(
            "(targetattr = \"*\") (version 3.0;acl \"test_acl\";allow (read,write)"
            "(userdn = \"ldap:///cn=alice,ou=users,o=acme\");)");
        CPPUNIT_ASSERT(!aci->isDeny());
        CPPUNIT_ASSERT_EQUAL((int)(NewSecAccess_Read | NewSecAccess_Write), aci->permission());
        CPPUNIT_ASSERT_EQUAL((unsigned)1, aci->userdns().length());
        CPPUNIT_ASSERT_EQUAL_STR("cn=alice,ou=users,o=acme", aci->userdns().item(0));
        CPPUNIT_ASSERT_EQUAL((unsigned)0, aci->groupdns().length());

        // Serialized form should round trip the permissions and userdn.
        StringBuffer buf;
        aci->serialize(buf);
        CPPUNIT_ASSERT(strstr(buf.str(), "allow (read,write)") != nullptr);
        CPPUNIT_ASSERT(strstr(buf.str(), "userdn = \"ldap:///cn=alice,ou=users,o=acme\"") != nullptr);
    }

    void testWellFormedDenyGroupdn()
    {
        Owned<IAci> aci = createAciForTest(
            "(targetattr = \"mail\") (version 3.0;acl \"deny_acl\";deny (write)"
            "(groupdn = \"ldap:///cn=temps,ou=groups,o=acme\");)");
        CPPUNIT_ASSERT(aci->isDeny());
        CPPUNIT_ASSERT_EQUAL((int)NewSecAccess_Write, aci->permission());
        CPPUNIT_ASSERT_EQUAL((unsigned)0, aci->userdns().length());
        CPPUNIT_ASSERT_EQUAL((unsigned)1, aci->groupdns().length());
        CPPUNIT_ASSERT_EQUAL_STR("cn=temps,ou=groups,o=acme", aci->groupdns().item(0));
    }

    void testOrSeparatedUserdnGroupdn()
    {
        Owned<IAci> aci = createAciForTest(
            "(version 3.0;acl \"multi_acl\";allow (read)"
            "(userdn = \"ldap:///cn=alice,o=acme\" or groupdn = \"ldap:///cn=admins,o=acme\");)");
        CPPUNIT_ASSERT_EQUAL((unsigned)1, aci->userdns().length());
        CPPUNIT_ASSERT_EQUAL_STR("cn=alice,o=acme", aci->userdns().item(0));
        CPPUNIT_ASSERT_EQUAL((unsigned)1, aci->groupdns().length());
        CPPUNIT_ASSERT_EQUAL_STR("cn=admins,o=acme", aci->groupdns().item(0));
    }

    // targetattr/target clauses are optional; missing them should leave the
    // corresponding fields empty rather than crash the parser.
    void testMissingTargetattrAndTargetClauses()
    {
        Owned<IAci> aci = createAciForTest(
            "(version 3.0;acl \"no_target\";allow (read)(userdn = \"ldap:///cn=bob,o=acme\");)");
        CPPUNIT_ASSERT_EQUAL_STR("", aci->target().str());
        CPPUNIT_ASSERT_EQUAL((int)NewSecAccess_Read, aci->permission());
    }

    // A version clause with permissions but no trailing userdn/groupdn clause
    // must not crash and should simply leave both DN lists empty.
    void testMissingUserGroupDnClause()
    {
        Owned<IAci> aci = createAciForTest("(version 3.0;acl \"no_dn\";allow (read));");
        CPPUNIT_ASSERT_EQUAL((unsigned)0, aci->userdns().length());
        CPPUNIT_ASSERT_EQUAL((unsigned)0, aci->groupdns().length());
        CPPUNIT_ASSERT_EQUAL((int)NewSecAccess_Read, aci->permission());
    }

    // The parser does not understand backslash-escaped quotes, so an embedded
    // '"' inside a dn value terminates that value early. This documents the
    // current (injection-relevant) behaviour rather than idealized parsing.
    void testEmbeddedQuoteTruncatesDnValue()
    {
        Owned<IAci> aci = createAciForTest(
            "(version 3.0;acl \"embedded_quote\";allow (read)"
            "(userdn = \"ldap:///cn=\\\"weird\\\",o=acme\");)");
        CPPUNIT_ASSERT_EQUAL((unsigned)1, aci->userdns().length());
        CPPUNIT_ASSERT_EQUAL_STR("cn=\\", aci->userdns().item(0));
    }

    // A quoted value that is never closed must be bounded by the terminating
    // NUL rather than reading past the end of the buffer.
    void testUnterminatedQuotedValueDoesNotOverrun()
    {
        StringBuffer acistr("(targetattr = \"");
        acistr.appendN(2000, 'A');
        Owned<IAci> aci = createAciForTest(acistr.str());
        // Parsing completed (no hang/overrun); target itself is untouched
        // since no "(target " or "target=" clause was present.
        CPPUNIT_ASSERT_EQUAL_STR("", aci->target().str());
    }

    // Pathological, deeply-repeated clause fragments with no closing
    // delimiters must still terminate in bounded time (each parser loop
    // only ever advances or stops at the NUL terminator).
    void testAdversariallyLongInputDoesNotHang()
    {
        StringBuffer acistr("(version 3.0;acl \"long\";allow (read)");
        for (unsigned i = 0; i < 5000; i++)
            acistr.append("(userdn = \"ldap:///cn=x");
        Owned<IAci> aci = createAciForTest(acistr.str());
        // No crash/hang is the primary assertion; permission should still
        // reflect the well-formed prefix that was parsed.
        CPPUNIT_ASSERT_EQUAL((int)NewSecAccess_Read, aci->permission());
    }

    void testPermissionFlagConstructorUser()
    {
        Owned<IAci> aci = createAciForTest(false, NewSecAccess_Full, USER_ACT, "cn=carol,ou=users,o=acme");
        CPPUNIT_ASSERT(!aci->isDeny());
        CPPUNIT_ASSERT_EQUAL((int)NewSecAccess_Full, aci->permission());
        CPPUNIT_ASSERT_EQUAL((unsigned)1, aci->userdns().length());
        CPPUNIT_ASSERT_EQUAL_STR("cn=carol,ou=users,o=acme", aci->userdns().item(0));
        StringBuffer buf;
        aci->serialize(buf);
        CPPUNIT_ASSERT(strstr(buf.str(), "(all)") != nullptr);
    }

    void testPermissionFlagConstructorGroupDeny()
    {
        Owned<IAci> aci = createAciForTest(true, NewSecAccess_Read | NewSecAccess_Access, GROUP_ACT, "cn=admins,ou=groups,o=acme");
        CPPUNIT_ASSERT(aci->isDeny());
        CPPUNIT_ASSERT_EQUAL((unsigned)1, aci->groupdns().length());
        CPPUNIT_ASSERT_EQUAL_STR("cn=admins,ou=groups,o=acme", aci->groupdns().item(0));
        StringBuffer buf;
        aci->serialize(buf);
        CPPUNIT_ASSERT(strstr(buf.str(), "groupdn = \"ldap:///cn=admins,ou=groups,o=acme\"") != nullptr);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(CAciTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(CAciTest, "CAciTest");

// sec2aci() vendor variants: base AciProcessor must refuse (child classes
// override it), while CIPlanetAciProcessor/COpenLdapAciProcessor translate
// SecAccessFlags into their respective ACI permission tokens.
class Sec2AciTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(Sec2AciTest);
    CPPUNIT_TEST(testBaseProcessorThrows);
    CPPUNIT_TEST(testIPlanetSec2Aci);
    CPPUNIT_TEST(testOpenLdapSec2Aci);
    CPPUNIT_TEST_SUITE_END();

private:
    Owned<IPropertyTree> makeCfg()
    {
        Owned<IPropertyTree> cfg = createPTree();
        cfg->setProp("@ldapAddress", "127.0.0.1");
        return cfg;
    }

public:
    void testBaseProcessorThrows()
    {
        Owned<IPropertyTree> cfg = makeCfg();
        AciProcessor processor(cfg);
        StringBuffer buf;
        CPPUNIT_ASSERT_THROWS_IEXCEPTION(processor.sec2aci(SecAccess_Read, buf), "expected base sec2aci to throw");
    }

    void testIPlanetSec2Aci()
    {
        Owned<IPropertyTree> cfg = makeCfg();
        CIPlanetAciProcessor processor(cfg);

        StringBuffer none;
        processor.sec2aci(SecAccess_None, none);
        CPPUNIT_ASSERT_EQUAL_STR("", none.str());

        StringBuffer full;
        processor.sec2aci(SecAccess_Full, full);
        CPPUNIT_ASSERT_EQUAL_STR("all", full.str());

        StringBuffer access;
        processor.sec2aci(SecAccess_Access, access);
        CPPUNIT_ASSERT_EQUAL_STR("compare search", access.str());

        StringBuffer read;
        processor.sec2aci(SecAccess_Read, read);
        CPPUNIT_ASSERT_EQUAL_STR("compare search read", read.str());

        StringBuffer write;
        processor.sec2aci(SecAccess_Write, write);
        CPPUNIT_ASSERT_EQUAL_STR("compare search read write", write.str());
    }

    void testOpenLdapSec2Aci()
    {
        Owned<IPropertyTree> cfg = makeCfg();
        COpenLdapAciProcessor processor(cfg);

        StringBuffer none;
        processor.sec2aci(SecAccess_None, none);
        CPPUNIT_ASSERT_EQUAL_STR("", none.str());

        StringBuffer full;
        processor.sec2aci(SecAccess_Full, full);
        CPPUNIT_ASSERT_EQUAL_STR("r,w,d", full.str());

        // SecAccess_Access alone matches neither the Read nor Write bitmask
        // tests in the implementation, so it yields no tokens.
        StringBuffer access;
        processor.sec2aci(SecAccess_Access, access);
        CPPUNIT_ASSERT_EQUAL_STR("", access.str());

        StringBuffer read;
        processor.sec2aci(SecAccess_Read, read);
        CPPUNIT_ASSERT_EQUAL_STR("r", read.str());

        StringBuffer write;
        processor.sec2aci(SecAccess_Write, write);
        CPPUNIT_ASSERT_EQUAL_STR("r,w", write.str());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(Sec2AciTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(Sec2AciTest, "Sec2AciTest");

class LdapTimeUtilsTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(LdapTimeUtilsTest);
    CPPUNIT_TEST(testValidGeneralizedTime);
    CPPUNIT_TEST(testRejectsMalformedInput);
    CPPUNIT_TEST_SUITE_END();

public:
    // Confirms the parsed date/time survives a round trip: undoing the UTC->local
    // adjustment applied by parseLdapGeneralizedTime() should reproduce the original
    // "YYYYMMDDHHMMSSZ" fields exactly, regardless of the local timezone under test.
    void assertParsesTo(const char *val, const char *expectedGmt)
    {
        CDateTime dt;
        CPPUNIT_ASSERT(parseLdapGeneralizedTime(dt, (unsigned)strlen(val), val));
        dt.adjustTime(-dt.queryUtcToLocalDelta());
        StringBuffer str;
        dt.getString(str, false);
        CPPUNIT_ASSERT_EQUAL_STR(expectedGmt, str.str());
    }

    void testValidGeneralizedTime()
    {
        // Standard case
        assertParsesTo("20260115120000Z", "2026-01-15T12:00:00");
        // Midnight/leap-year day
        assertParsesTo("20240229000000Z", "2024-02-29T00:00:00");
        // End-of-year boundary
        assertParsesTo("20251231235959Z", "2025-12-31T23:59:59");
    }

    void testRejectsMalformedInput()
    {
        CDateTime dt;
        // Too short
        CPPUNIT_ASSERT(!parseLdapGeneralizedTime(dt, 5, "2026Z"));
        // Missing trailing 'Z'
        CPPUNIT_ASSERT(!parseLdapGeneralizedTime(dt, 15, "20260115120000X"));
        // Non-digit embedded
        CPPUNIT_ASSERT(!parseLdapGeneralizedTime(dt, 15, "2026011A120000Z"));
        // Null value
        CPPUNIT_ASSERT(!parseLdapGeneralizedTime(dt, 15, nullptr));
        // Empty string
        CPPUNIT_ASSERT(!parseLdapGeneralizedTime(dt, 0, ""));
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(LdapTimeUtilsTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(LdapTimeUtilsTest, "LdapTimeUtilsTest");

#endif
