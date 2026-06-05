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

#endif
