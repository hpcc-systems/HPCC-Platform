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

#include "ldapconnection.hpp"
#include "unittests.hpp"

// White-box coverage for CLdapClient's password-expiration logic
// (ldapconnection.cpp).
//
// Of the three functions named in the originating issue, only the
// attribute/server-type dispatch performed by getPasswordExpiration() can be
// exercised hermetically:
//
//  - calcPWExpiry() unconditionally calls the free function getMaxPwdAge(),
//    which issues a real ldap_search_ext_s() over a live connection pool.
//  - isAccountPwdNeverExpires() unconditionally constructs a
//    CLDAPGetValuesLenWrapper on its very first line, i.e. it always calls
//    ldap_get_values_len() - there is no LDAP-free branch to test.
//  - getPasswordExpiration() itself also touches LDAP once dispatch decides
//    a value must actually be fetched, and CLdapClient cannot be constructed
//    at all without a reachable LDAP host (CLdapConfig's constructor probes
//    the configured server(s) and throws if none respond).
//
// getPasswordExpiration()'s branch-selection logic - which attribute names
// are recognised, and whether a "never expires" short-circuit or a
// directory-type mismatch applies - is nonetheless genuinely security
// relevant (an incorrect dispatch could cause passwords to never expire, or
// to be evaluated against the wrong directory type). It was therefore
// extracted into the free, dependency-free function
// selectPasswordExpirationDispatch() (declared in ldapconnection.hpp,
// defined in ldapconnection.cpp) so it can be tested here without
// constructing a CLdapClient/CLdapConfig or touching LDAP at all.
// getPasswordExpiration() itself is unchanged behaviourally: it simply
// switches on this function's result.
class PasswordExpiryTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PasswordExpiryTest);
    CPPUNIT_TEST(testPwdLastSetDomainNeverExpires);
    CPPUNIT_TEST(testPwdLastSetAccountNeverExpires);
    CPPUNIT_TEST(testPwdLastSetNeedsRetrieval);
    CPPUNIT_TEST(testPwdLastSetAttributeNameIsCaseInsensitive);
    CPPUNIT_TEST(test389dsAttributeOn389dsServer);
    CPPUNIT_TEST(test389dsAttributeOnNon389dsServers);
    CPPUNIT_TEST(test389dsAttributeNameIsCaseInsensitive);
    CPPUNIT_TEST(testUnrecognisedAttributeName);
    CPPUNIT_TEST_SUITE_END();

public:
    void testPwdLastSetDomainNeverExpires()
    {
        // Domain policy says passwords never expire: short-circuit regardless
        // of the per-account flag.
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("pwdLastSet", ACTIVE_DIRECTORY, true, false) ==
                        PasswordExpirationDispatch::NeverExpires);
    }

    void testPwdLastSetAccountNeverExpires()
    {
        // Per-account UF_DONT_EXPIRE_PASSWD flag says never expires, even
        // though domain policy would otherwise require expiration.
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("pwdLastSet", ACTIVE_DIRECTORY, false, true) ==
                        PasswordExpirationDispatch::NeverExpires);
    }

    void testPwdLastSetNeedsRetrieval()
    {
        // Neither domain nor account overrides apply: the attribute value
        // must actually be fetched and parsed.
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("pwdLastSet", ACTIVE_DIRECTORY, false, false) ==
                        PasswordExpirationDispatch::RetrieveAdPwdLastSet);
    }

    void testPwdLastSetAttributeNameIsCaseInsensitive()
    {
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("PWDLASTSET", ACTIVE_DIRECTORY, false, false) ==
                        PasswordExpirationDispatch::RetrieveAdPwdLastSet);
    }

    void test389dsAttributeOn389dsServer()
    {
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("passwordExpirationTime", LDAP_389DS, false, false) ==
                        PasswordExpirationDispatch::Retrieve389dsExpirationTime);
    }

    void test389dsAttributeOnNon389dsServers()
    {
        // passwordExpirationTime is a 389ds-specific precomputed attribute;
        // any other directory type is a mismatch and must not be looked up.
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("passwordExpirationTime", ACTIVE_DIRECTORY, false, false) ==
                        PasswordExpirationDispatch::UnsupportedAttribute);
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("passwordExpirationTime", OPEN_LDAP, false, false) ==
                        PasswordExpirationDispatch::UnsupportedAttribute);
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("passwordExpirationTime", LDAPSERVER_UNKNOWN, false, false) ==
                        PasswordExpirationDispatch::UnsupportedAttribute);
    }

    void test389dsAttributeNameIsCaseInsensitive()
    {
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("PASSWORDEXPIRATIONTIME", LDAP_389DS, false, false) ==
                        PasswordExpirationDispatch::Retrieve389dsExpirationTime);
    }

    void testUnrecognisedAttributeName()
    {
        // An attribute name that is neither of the two recognised ones is
        // rejected regardless of server type or never-expires flags.
        CPPUNIT_ASSERT(selectPasswordExpirationDispatch("someOtherAttribute", LDAP_389DS, true, true) ==
                        PasswordExpirationDispatch::UnsupportedAttribute);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(PasswordExpiryTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(PasswordExpiryTest, "PasswordExpiryTest");

#endif // _USE_CPPUNIT
