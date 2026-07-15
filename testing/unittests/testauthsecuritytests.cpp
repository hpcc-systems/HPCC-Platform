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

#include "jptree.hpp"
#include "jtime.hpp"
#include "jencrypt.hpp"
#include "SecureUser.hpp"
#include "testauthSecurity.hpp"
#include "unittests.hpp"

class CTrackingSecUser : public CSecureUser
{
    bool hasPasswordExpiration = false;
    CDateTime passwordExpiration;

public:
    CTrackingSecUser(const char* name, const char* password)
        : CSecureUser(name, password)
    {
    }

    bool setPasswordExpiration(CDateTime& expirationDate) override
    {
        passwordExpiration.set(expirationDate);
        hasPasswordExpiration = true;
        return true;
    }

    CDateTime& getPasswordExpiration(CDateTime& expirationDate) override
    {
        if (hasPasswordExpiration)
            expirationDate.set(passwordExpiration);
        else
            expirationDate.clear();
        return expirationDate;
    }

    bool queryStoredPasswordExpiration(CDateTime& expirationDate) const
    {
        if (!hasPasswordExpiration)
            return false;
        expirationDate.set(passwordExpiration);
        return true;
    }
};

class TestAuthSecurityTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(TestAuthSecurityTests);
    CPPUNIT_TEST(testAccessConfigEntriesAreApplied);
    CPPUNIT_TEST(testConfiguredAuthenticateStatusAndExpirationAreApplied);
    CPPUNIT_TEST(testInvalidAuthStateEntriesFallbackToNormalAuthentication);
    CPPUNIT_TEST_SUITE_END();

    Owned<ISecManager> createManager(const char* secMgrXml, const char* bindXml)
    {
        Owned<IPropertyTree> secMgrCfg = createPTreeFromXMLString(secMgrXml);
        Owned<IPropertyTree> bindCfg = createPTreeFromXMLString(bindXml);
        return Owned<ISecManager>(createInstance("testauthSecurity", *secMgrCfg, *bindCfg));
    }

public:
    void testAccessConfigEntriesAreApplied()
    {
        START_TEST
        Owned<ISecManager> manager = createManager(
            "<testauthSecurity>"
                "<userAccess userName='reader'>"
                    "<defaults resource='None' fileScope='None' eclWUScope='None'/>"
                    "<resources>"
                        "<WsWorkunits>"
                            "<features OwnWorkunitsAccess='Full' OtherFeature='Read'/>"
                        "</WsWorkunits>"
                    "</resources>"
                    "<fileScopes>"
                        "<fileScope name='scope' access='Full'/>"
                        "<fileScope name='scope::readonly' access='Read'/>"
                        "<fileScope name='' access='Access'/>"
                        "<fileScope name='ignored'/>"
                    "</fileScopes>"
                    "<eclWUScopes reader='Full' team='Access'/>"
                "</userAccess>"
            "</testauthSecurity>",
            "<EspBinding serviceType='WsWorkunits'/>");

        CTrackingSecUser user("reader", "reader");

        CPPUNIT_ASSERT_EQUAL(SecAccess_Full, manager->getAccessFlagsEx(RT_DEFAULT, user, "OwnWorkunitsAccess"));
        CPPUNIT_ASSERT_EQUAL(SecAccess_Read, manager->getAccessFlagsEx(RT_DEFAULT, user, "OtherFeature"));
        CPPUNIT_ASSERT_EQUAL(SecAccess_None, manager->getAccessFlagsEx(RT_DEFAULT, user, "UnknownFeature"));

        CPPUNIT_ASSERT_EQUAL(SecAccess_Full, manager->authorizeFileScope(user, "scope::dataset"));
        CPPUNIT_ASSERT_EQUAL(SecAccess_Read, manager->authorizeFileScope(user, "scope::readonly::dataset"));
        CPPUNIT_ASSERT_EQUAL(SecAccess_None, manager->authorizeFileScope(user, "ignored::dataset"));

        CPPUNIT_ASSERT_EQUAL(SecAccess_Full, manager->authorizeWorkunitScope(user, "reader", nullptr));
        CPPUNIT_ASSERT_EQUAL(SecAccess_Access, manager->authorizeWorkunitScope(user, "team", nullptr));
        CPPUNIT_ASSERT_EQUAL(SecAccess_None, manager->authorizeWorkunitScope(user, "other", nullptr));
        END_TEST
    }

    void testConfiguredAuthenticateStatusAndExpirationAreApplied()
    {
        START_TEST
        Owned<ISecManager> manager = createManager(
            "<testauthSecurity>"
                "<userAccess userName='pwexpired'"
                            " authenticateStatus='AS_PASSWORD_VALID_BUT_EXPIRED'"
                            " passwordExpiration='2024-01-01'>"
                    "<defaults resource='Full'/>"
                "</userAccess>"
            "</testauthSecurity>",
            "<EspBinding serviceType='WsWorkunits'/>");

        CTrackingSecUser user("pwexpired", "pwexpired");
        SecAccessFlags access = manager->getAccessFlagsEx(RT_DEFAULT, user, "AnyFeature");

        CPPUNIT_ASSERT_EQUAL(SecAccess_Unavailable, access);
        CPPUNIT_ASSERT_EQUAL(AS_PASSWORD_VALID_BUT_EXPIRED, user.getAuthenticateStatus());

        CDateTime expected;
        expected.setDateString("2024-01-01");
        CDateTime actual;
        CPPUNIT_ASSERT(user.queryStoredPasswordExpiration(actual));
        CPPUNIT_ASSERT(actual == expected);
        END_TEST
    }

    void testInvalidAuthStateEntriesFallbackToNormalAuthentication()
    {
        START_TEST
        Owned<ISecManager> manager = createManager(
            "<testauthSecurity>"
                "<userAccess userName='fallback'"
                            " authenticateStatus='AS_NOT_A_REAL_STATUS'"
                            " passwordExpiration='not-a-date'>"
                    "<defaults resource='Read'/>"
                "</userAccess>"
            "</testauthSecurity>",
            "<EspBinding serviceType='WsWorkunits'/>");

        CTrackingSecUser user("fallback", "fallback");
        SecAccessFlags access = manager->getAccessFlagsEx(RT_DEFAULT, user, "AnyFeature");

        CPPUNIT_ASSERT_EQUAL(SecAccess_Read, access);
        CPPUNIT_ASSERT_EQUAL(AS_AUTHENTICATED, user.getAuthenticateStatus());

        CDateTime actual;
        CPPUNIT_ASSERT(!user.queryStoredPasswordExpiration(actual));
        END_TEST
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TestAuthSecurityTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(TestAuthSecurityTests, "TestAuthSecurityTests");

#endif
