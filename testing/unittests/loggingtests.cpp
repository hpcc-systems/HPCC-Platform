/*##############################################################################

    Copyright (C) 2021 HPCC Systems®.

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
#include "loggingmanager.hpp"
#include "modularlogagent.ipp"
#include "unittests.hpp"
#include <vector>

using namespace ModularLogAgent;

class LoggingMockAgentTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( LoggingMockAgentTests );
        CPPUNIT_TEST(testHasService_None);
        CPPUNIT_TEST(testHasService_All);
        CPPUNIT_TEST(testGetTransactionSeed_True);
        CPPUNIT_TEST(testGetTransactionSeed_False);
        CPPUNIT_TEST(testGetTransactionSeed_Bad);
        CPPUNIT_TEST(testUpdateLog_Good);
        CPPUNIT_TEST(testUpdateLog_Bad);
        CPPUNIT_TEST(testGetTransactionId_Good);
        CPPUNIT_TEST(testGetTransactionId_Bad);
    CPPUNIT_TEST_SUITE_END();
public:
    void testHasService_None()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock"/>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));

        checkHasService("hasService-none/seed", agent, LGSTGetTransactionSeed, false);
        checkHasService("hasService-none/update", agent, LGSTUpdateLOG, false);
        checkHasService("hasService-none/id", agent, LGSTGetTransactionID, false);
    }
    void testHasService_All()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
              <GetTransactionSeed seed-id="12345" status-code="2" status-message="status 2"/>
              <UpdateLog response="update response" status-code="1" status-message="status 1"/>
              <GetTransactionId id="12345T67890"/>
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));

        checkHasService("hasService-all/seed", agent, LGSTGetTransactionSeed, true);
        checkHasService("hasService-all/update", agent, LGSTUpdateLOG, true);
        checkHasService("hasService-all/id", agent, LGSTGetTransactionID, true);
    }

    void testGetTransactionSeed_True()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
              <GetTransactionSeed seed-id="67890" status-code="0" status-message="status 0"/>
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));
        
        checkGetTransactionSeed("getTransactionSeed-true", agent, true, "67890", 0, "status 0");
    }
    void testGetTransactionSeed_False()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
              <GetTransactionSeed seed-id="12345" status-code="2" status-message="status 2"/>
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));
        
        checkGetTransactionSeed("getTransactionSeed-false", agent, false, "12345", 2, "status 2");
    }
    void testGetTransactionSeed_Bad()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));
        
        checkGetTransactionSeed("getTransactionSeed-bad", agent, false, "", -1, "unsupported request (getTransactionSeed)");
    }

    void testUpdateLog_Good()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
              <UpdateLog response="update response" status-code="1" status-message="status 1"/>
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));

        checkUpdateLog("updateLog-good", agent, "update response", 1, "status 1");
    }
    void testUpdateLog_Bad()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));

        checkUpdateLog("updateLog-bad", agent, "", -1, "unsupported request (updateLog)");
    }

    void testGetTransactionId_Good()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
              <GetTransactionId id="12345T67890"/>
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));

        checkGetTransactionId("getTransactionId-good", agent, "12345T67890");
    }
    void testGetTransactionId_Bad()
    {
        const char* agentConfig = R"!!!(
            <LogAgent name="mock" module="mock">
            </LogAgent>
        )!!!";
        Owned<IEspLogAgent> agent(createAgent(agentConfig));

        checkGetTransactionId("getTransactionId-bad", agent, "unsupported request (getTransactionID)");
    }

    LoggingMockAgentTests() {}

    IEspLogAgent* createAgent(const char* agentConfig)
    {
        Owned<IPTree> config(createPTreeFromXMLString(agentConfig));
        Owned<CModuleFactory> factory(new CModuleFactory());
        Owned<CEspLogAgent> agent(new CEspLogAgent(*factory.getClear()));
        agent->init("mock", "LogAgent", config, "unittests");
        agent->initVariants(config);
        return agent.getClear();
    }
    void checkHasService(const char* test, IEspLogAgent* agent, LOGServiceType type, bool expected)
    {
        if (agent->hasService(type) != expected)
        {
            fprintf(stdout, "\nTest(%s) hasService(%d) expected %s but got %s\n", test, type, (expected ? "true" : "false"), (expected ? "false" : "true"));
            CPPUNIT_ASSERT(false);
        }
    }
    void checkGetTransactionSeed(const char* test, IEspLogAgent* agent, bool expectResult, const char* expectSeedId, int expectStatusCode, const char* expectStatusMessage)
    {
        Owned<IEspGetTransactionSeedRequest> req(createGetTransactionSeedRequest());
        Owned<IEspGetTransactionSeedResponse> resp(createGetTransactionSeedResponse());
        bool result = agent->getTransactionSeed(*req, *resp);
        bool pass = true;

        if (result != expectResult)
        {
            fprintf(stdout, "\nTest(%s) expected result %s but got %s\n", test, (expectResult ? "true" : "false"), (result ? "true" : "false"));
            pass = false;
        }
        if (!streq(resp->getSeedId(), expectSeedId))
        {
            fprintf(stdout, "\nTest(%s) expected SeedId '%s' but got '%s'\n", test, expectSeedId, resp->getSeedId());
            pass = false;
        }
        if (resp->getStatusCode() != expectStatusCode)
        {
            fprintf(stdout, "\nTest(%s) expected StatusCode %d but got %d\n", test, expectStatusCode, resp->getStatusCode());
            pass = false;
        }
        if (!streq(resp->getStatusMessage(), expectStatusMessage))
        {
            fprintf(stdout, "\nTest(%s) expected StatusMessage '%s' but got '%s'\n", test, expectStatusMessage, resp->getStatusMessage());
            pass = false;
        }
        CPPUNIT_ASSERT(pass);
    }
    void checkUpdateLog(const char* test, IEspLogAgent* agent, const char* expectResponse, int expectStatusCode, const char* expectStatusMessage)
    {
        Owned<IEspUpdateLogRequestWrap> req(new CUpdateLogRequestWrap(nullptr, nullptr, nullptr));
        Owned<IEspUpdateLogResponse>    resp(createUpdateLogResponse());
        bool pass = true;

        agent->updateLog(*req, *resp);
        if (!streq(resp->getResponse(), expectResponse))
        {
            fprintf(stdout, "\nTest(%s) expected Response '%s' but got '%s'\n", test, expectResponse, resp->getResponse());
            pass = false;
        }
        if (resp->getStatusCode() != expectStatusCode)
        {
            fprintf(stdout, "\nTest(%s) expected StatusCode %d but got %d\n", test, expectStatusCode, resp->getStatusCode());
            pass = false;
        }
        if (!streq(resp->getStatusMessage(), expectStatusMessage))
        {
            fprintf(stdout, "\nTest(%s) expected StatusMessage '%s' but got '%s'\n", test, expectStatusMessage, resp->getStatusMessage());
            pass = false;
        }
        CPPUNIT_ASSERT(pass);
    }
    void checkGetTransactionId(const char* test, IEspLogAgent* agent, const char* expect)
    {
        StringBuffer actual;

        agent->getTransactionID(nullptr, actual);
        if (!streq(actual, expect))
        {
            fprintf(stdout, "\nTest(%s) expected '%s' but got '%s'\n", test, expect, actual.str());
            CPPUNIT_ASSERT(false);
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( LoggingMockAgentTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( LoggingMockAgentTests, "loggingmockagenttests" );

class LoggingIdFilterTests : public CppUnit::TestFixture
{
    constexpr static const char* managerConfig = R"!!!(
        <LoggingManager>
          <LogAgent name="alpha" type="LogAgent" plugin="modularlogagent" module="mock">
            <GetTransactionSeed seed-id="alpha" status-code="0" status-message="ok"/>
            <UpdateLog response="alpha response" status-code="0" status-message="ok"/>
            <GetTransactionId id="alpha1"/>
          </LogAgent>
          <LogAgent name="beta" type="LogAgent" plugin="modularlogagent" module="mock">
            <Variant group="foo"/>
            <GetTransactionSeed seed-id="beta" status-code="0" status-message="ok"/>
            <UpdateLog response="beta response" status-code="0" status-message="ok"/>
            <GetTransactionId id="beta1"/>
          </LogAgent>
          <LogAgent name="gamma" type="LogAgent" plugin="modularlogagent" module="mock">
            <Variant group="bar"/>
            <GetTransactionSeed seed-id="gamma" status-code="0" status-message="ok"/>
            <UpdateLog response="gamma response" status-code="0" status-message="ok"/>
            <GetTransactionId id="gamma1"/>
          </LogAgent>
          <LogAgent name="delta" type="LogAgent" plugin="modularlogagent" module="mock">
            <Variant group="foo"/>
            <Variant group="bar"/>
            <GetTransactionSeed seed-id="delta" status-code="0" status-message="ok"/>
            <UpdateLog response="delta response" status-code="0" status-message="ok"/>
            <GetTransactionId id="delta1"/>
          </LogAgent>
        </LoggingManager>
    )!!!";

    CLoggingManagerLoader m_loader;

    CPPUNIT_TEST_SUITE( LoggingIdFilterTests );
        CPPUNIT_TEST(testGroupFilter);
        CPPUNIT_TEST(testHasService);
        CPPUNIT_TEST(testGetTransactionSeed);
        CPPUNIT_TEST(testGetTransactionId);
    CPPUNIT_TEST_SUITE_END();

public:
    void testGroupFilter()
    {
        Owned<ILoggingManager> manager(createManager());
        checkGroupFilterWithHasFilteredService("groupfilter-seed", manager, LGSTGetTransactionSeed, "", 1, 4);
        checkGroupFilterWithHasFilteredService("groupfilter-seed", manager, LGSTGetTransactionSeed, "foo", 2, 3);
        checkGroupFilterWithHasFilteredService("groupfilter-seed", manager, LGSTGetTransactionSeed, "bar", 2, 3);
        checkGroupFilterWithHasFilteredService("groupfilter-seed", manager, LGSTGetTransactionSeed, "baz", 0, 5);
    }

    void testHasService()
    {
        Owned<ILoggingManager> manager(createManager());
        checkHasFilteredServiceForGroup("hasService-seed", manager, LGSTGetTransactionSeed, "", true);
        checkHasFilteredServiceForGroup("hasService-seed", manager, LGSTGetTransactionSeed, "foo", true);
        checkHasFilteredServiceForGroup("hasService-seed", manager, LGSTGetTransactionSeed, "bar", true);
        checkHasFilteredServiceForGroup("hasService-seed", manager, LGSTGetTransactionSeed, "baz", false);
    }

    void testGetTransactionSeed()
    {
        checkGetFilteredTransactionSeedForGroup("getTransactionSeed", "bar", true, "gamma", "Transaction Seed");
        checkGetFilteredTransactionSeedForGroup("getTransactionSeed", "baz", false, "Seed", "Failed");
    }

    void testGetTransactionId()
    {
        checkGetFilteredTransactionIdForGroup("getTransactionId", "bar", true, "gamma1", "");
        checkGetFilteredTransactionIdForGroup("getTransactionId", "baz", false, "", "");
    }

    LoggingIdFilterTests()
        : m_loader(nullptr, nullptr, nullptr, nullptr)
    {
    }

    ILoggingManager* createManager()
    {
        Owned<IPTree> config(createPTreeFromXMLString(managerConfig));
        Owned<ILoggingManager> manager(m_loader.create(*config));

        manager->init(config, "unittest");
        return manager.getClear();
    }

    void checkGroupFilterWithHasFilteredService(const char* test, ILoggingManager* manager, LOGServiceType service, const char* group, unsigned expectedPass, unsigned expectedFail)
    {
        using Variants = std::vector<Linked<const IEspLogAgentVariant> >;
        Variants pass;
        Variants fail;
        VariantGroupFilter groupFilter(group);
        EspLogAgentIdFilter wrapper = [&](const IEspLogAgentVariant& variant) {
            if (groupFilter(variant))
                pass.emplace_back(&variant);
            else
                fail.emplace_back(&variant);
            return false;
        };

        if (!group)
            group = "";

        manager->hasFilteredService(service, wrapper);
        if (pass.size() != expectedPass || fail.size() != expectedFail)
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected pass/fail of %u/%u but got %zu/%zu\n", test, group, expectedPass, expectedFail, pass.size(), fail.size());
            CPPUNIT_ASSERT(false);
        }
        for (const Linked<const IEspLogAgentVariant>& v : pass)
        {
            if (!streq(v->getGroup(), group))
            {
                fprintf(stdout, "\nTest(%s) group '%s' passed variant (%s/%s)\n", test, group, v->getName(), v->getGroup());
                CPPUNIT_ASSERT(false);
            }
        }
    }
    void checkHasFilteredServiceForGroup(const char* test, ILoggingManager* manager, LOGServiceType service, const char* group, bool expectedResult)
    {
        if (!group)
            group = "";
        
        VariantGroupFilter groupFilter(group);

        bool actual = manager->hasFilteredService(service, groupFilter);
        if (actual != expectedResult)
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected result %s but got %s", test, group, (expectedResult ? "true" : "false"), (actual ? "true" : "false"));
            CPPUNIT_ASSERT(false);
        }
    }
    void checkGetFilteredTransactionSeedForGroup(const char* test, const char* group, bool expectedResult, const char* expectedSeed, const char* expectedStatus)
    {
        if (!group)
            group = "";
        
        Owned<ILoggingManager> manager(createManager());
        StringBuffer actualSeed, actualStatus;
        bool actualResult = manager->getFilteredTransactionSeed(actualSeed, actualStatus, VariantGroupFilter(group));
        bool pass = true;
        if (actualResult != expectedResult)
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected result %s but got %s", test, group, (expectedResult ? "true" : "false"), (actualResult ? "true" : "false"));
            pass = false;
        }
        if (!streq(actualSeed, expectedSeed))
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected seed %s but got %s", test, group, expectedSeed, actualSeed.str());
            pass = false;
        }
        if (!hasPrefix(actualStatus, expectedStatus, true))
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected status starting with %s but got %s", test, group, expectedStatus, actualStatus.str());
            pass = false;
        }
        CPPUNIT_ASSERT(pass);
    }
    void checkGetFilteredTransactionIdForGroup(const char* test, const char* group, bool expectedResult, const char* expectedId, const char* expectedStatus)
    {
        if (!group)
            group = "";
        
        Owned<ILoggingManager> manager(createManager());
        StringBuffer actualId, actualStatus;
        bool actualResult = manager->getFilteredTransactionID(nullptr, actualId, actualStatus, VariantGroupFilter(group));
        bool pass = true;
        if (actualResult != expectedResult)
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected result %s but got %s", test, group, (expectedResult ? "true" : "false"), (actualResult ? "true" : "false"));
            pass = false;
        }
        if (!streq(actualId, expectedId))
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected ID %s but got %s", test, group, expectedId, actualId.str());
            pass = false;
        }
        if (!hasPrefix(actualStatus, expectedStatus, true))
        {
            fprintf(stdout, "\nTest(%s) group '%s' expected status starting with %s but got %s", test, group, expectedStatus, actualStatus.str());
            pass = false;
        }
        CPPUNIT_ASSERT(pass);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( LoggingIdFilterTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( LoggingIdFilterTests, "loggingidfiltertests" );



#endif // _USE_CPPUNIT