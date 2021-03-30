/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
 * Config Manager 2.0 Unit tests to validate specific HPCC features
 *
 */

#ifdef _USE_CPPUNIT

#include <cppunit/TestFixture.h>
#include "unittests.hpp"

// Config2 manager includes
#include "jexcept.hpp"
#include "mod_template_support/TemplateException.hpp"
#include "mod_template_support/TemplateExecutionException.hpp"
#include "EnvironmentMgr.hpp"
#include <string>
#include <iterator>
#include <algorithm>
#include <vector>


class ConfigMgrHPCCTests : public CppUnit::TestFixture
{
    public:
        ConfigMgrHPCCTests()
        {
            m_templateDir = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "testing/configmgr/templates/";
            CFG2_CONFIG_DIR = std::string(hpccBuildInfo.componentDir) + PATHSEPSTR + "configschema" + PATHSEPSTR + "xsd" + PATHSEPSTR;
            CFG2_SOURCE_DIR = hpccBuildInfo.configDir;
            m_xmlEnvDir = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "testing/configmgr/schema/environments/";
        }

        CPPUNIT_TEST_SUITE(ConfigMgrHPCCTests);
            CPPUNIT_TEST(duplicatePortSameService);
            CPPUNIT_TEST(portConflictSameHWInstance);
            CPPUNIT_TEST(portConflictAccrossProcesses);
        CPPUNIT_TEST_SUITE_END();

    protected:
        bool loadEnvironment(const std::string &envFile)
        {
            bool rc;
            if (m_pEnvMgr == nullptr)
            {
                m_pEnvMgr = getEnvironmentMgrInstance(EnvironmentType::XML);
                std::map<std::string, std::string> cfgParms;
                rc = m_pEnvMgr->loadSchema(CFG2_CONFIG_DIR, CFG2_MASTER_CONFIG_FILE, cfgParms);
                CPPUNIT_ASSERT_MESSAGE("Unable to load configuration schema, error = " + m_pEnvMgr->getLastSchemaMessage(), rc);
            }

            //
            // discard the current envionment and load the new one
            m_pEnvMgr->discardEnvironment();
            rc = m_pEnvMgr->loadEnvironment(envFile);
            CPPUNIT_ASSERT_MESSAGE("Unable to load environment file, error = " + m_pEnvMgr->getLastEnvironmentMessage(), rc);
            return rc;
        }

        void duplicatePortSameService()
        {
            printf("\nTesting binding two ESP Services of the same type on the same port as invalid...");
            if (loadEnvironment(m_xmlEnvDir + "hpcc_port_conflict_test1.xml"))
            {
                Status status;
                m_pEnvMgr->validate(status, false);
                auto msgs = status.getMessages(statusMsg::error, false, "", "port");
                CPPUNIT_ASSERT_MESSAGE("Port conflict was NOT detected", !msgs.empty());
            }
            printf("Test Complete.\n");

            printf("\n\nTesting binding two ESP Services of the same type on different ports as valid...");
            if (loadEnvironment(m_xmlEnvDir + "hpcc_port_conflict_test2.xml"))
            {
                Status status;
                m_pEnvMgr->validate(status, false);
                auto msgs = status.getMessages(statusMsg::error, false, "", "port");
                CPPUNIT_ASSERT_MESSAGE("Port conflict was detected when there are none", msgs.empty());
            }
            printf("Test Complete.\n");
        }

        void portConflictSameHWInstance()
        {
            printf("\nTesting two ESP processes with a port collision on an instance...");
            if (loadEnvironment(m_xmlEnvDir + "hpcc_port_conflict_test3.xml"))
            {
                Status status;
                m_pEnvMgr->validate(status, false);
                auto msgs = status.getMessages(statusMsg::error, false, "", "");
                CPPUNIT_ASSERT_MESSAGE("Port conflict was NOT detected", !msgs.empty());
            }
            printf("Test Complete.\n");
        }

        void portConflictAccrossProcesses()
        {
            printf("\nTesting two processes with a port collision on an instance...");
            if (loadEnvironment(m_xmlEnvDir + "hpcc_port_conflict_test4.xml"))
            {
                Status status;
                m_pEnvMgr->validate(status, false);
                auto msgs = status.getMessages(statusMsg::error, false, "", "");
                CPPUNIT_ASSERT_MESSAGE("Port conflict was NOT detected", !msgs.empty());
            }
            printf("Test Complete.\n");
        }



    private:

        EnvironmentMgr  *m_pEnvMgr = nullptr;
        std::string m_templateDir;
        std::string CFG2_MASTER_CONFIG_FILE = "environment.xsd";
        std::string CFG2_CONFIG_DIR;
        std::string CFG2_SOURCE_DIR;
        std::string m_xmlEnvDir;

};

CPPUNIT_TEST_SUITE_REGISTRATION( ConfigMgrHPCCTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ConfigMgrHPCCTests, "ConfigMgrHPCCTests" );

#endif
