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
 * Config Manager 2.0 Unit tests
 *
 */

#ifdef _USE_CPPUNIT

#include <cppunit/TestFixture.h>
#include "unittests.hpp"

// Config manager includes
#include "SchemaItem.hpp"
#include "InsertableItem.hpp"
#include "jexcept.hpp"
#include "Exceptions.hpp"
#include "EnvironmentMgr.hpp"
#include <iterator>
#include <algorithm>

//
// This class validates that the system XSDs are wellformed and parse with no errors
class ConfigMgr2ValidateXSDs : public CppUnit::TestFixture
{
    public:

        CPPUNIT_TEST_SUITE(ConfigMgr2ValidateXSDs);
            CPPUNIT_TEST(LoadAndParse);
        CPPUNIT_TEST_SUITE_END();


    protected:

        void LoadAndParse()
        {
            printf("\nConfigMgr 2.0 - Load and Parse - Verifying configuration XSDs are compliant...");
            //
            // Standard configuration for HPCC
            std::string CFG2_MASTER_CONFIG_FILE = "environment.xsd";
            std::string CFG2_CONFIG_DIR = std::string(hpccBuildInfo.componentDir) + PATHSEPSTR + "configschema" + PATHSEPSTR + "xsd" + PATHSEPSTR;
            std::string CFG2_SOURCE_DIR = hpccBuildInfo.configSourceDir;

            //
            // Create the environment
            printf("\n  Creating XML environment manager instance");
            bool rc = true;
            m_pEnvMgr = getEnvironmentMgrInstance(EnvironmentType::XML);
            CPPUNIT_ASSERT_MESSAGE("Unable to allocate an environment manager", m_pEnvMgr != nullptr);

            //
            // Load all the XSDs to ensure they parse correctly
            printf("\n  Loading XSDs");
            std::map<std::string, std::string> cfgParms;
            rc = m_pEnvMgr->loadSchema(CFG2_CONFIG_DIR, CFG2_MASTER_CONFIG_FILE, cfgParms);
            CPPUNIT_ASSERT_MESSAGE("Unable to load configuration schema, error = " + m_pEnvMgr->getLastSchemaMessage(), rc);

            //
            // Test complete
            printf("\n  Test complete");
        }


    private:

        EnvironmentMgr  *m_pEnvMgr;
};


CPPUNIT_TEST_SUITE_REGISTRATION( ConfigMgr2ValidateXSDs );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ConfigMgr2ValidateXSDs, "ConfigMgr2ValidateXSDs" );

#endif
