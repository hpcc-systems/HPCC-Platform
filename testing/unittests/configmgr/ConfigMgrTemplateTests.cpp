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

// Config2 manager includes
#include "jexcept.hpp"
#include "mod_template_support/TemplateException.hpp"
#include "EnvironmentMgr.hpp"
#include "build-config.h"
#include <string>
#include <iterator>
#include <algorithm>
#include <vector>
#include "mod_template_support/EnvModTemplate.hpp"
#include "mod_template_support/IPAddressRangeInput.hpp"

//
// This class validates that the system XSDs are wellformed and parse with no errors
class ConfigMgrTemplateTests : public CppUnit::TestFixture
{
    public:

        CPPUNIT_TEST_SUITE(ConfigMgrTemplateTests);
            CPPUNIT_TEST(Test_LoadingTemplates);
            CPPUNIT_TEST(Test_Inputs);
            CPPUNIT_TEST(Test_SubstitutionTests);
            CPPUNIT_TEST(Test_OperationsTests);
        CPPUNIT_TEST_SUITE_END();


    protected:

        //
        // input tests
        struct templateInfo {
            templateInfo(const std::string &_json, bool _wantException, const std::string &_msg, bool _expectBadTemplate = false) :
                    json(_json), expectFail(_wantException), testMsg(_msg), expectBadTemplate(_expectBadTemplate) {}
            std::string json;
            bool expectFail;
            bool expectBadTemplate;
            std::string testMsg;
        };


        bool loadEnvironment(const std::string &envFilename)
        {
            std::string xmlEnvDir = INSTALL_DIR PATHSEPSTR "testing/configmgr/schema/environments/";
            m_pEnvMgr->discardEnvironment();   // in case one is currently loaded
            return(m_pEnvMgr->loadEnvironment(xmlEnvDir + envFilename));
        }


        void CreateEnvironmentManager(const std::string &schemaFilename, const std::string &environmentFile)
        {
            //
            // Standard configuration for HPCC
            std::string schemaXSDDir = INSTALL_DIR PATHSEPSTR "testing/configmgr/schema/xsd/";

            //
            // Create an environment manager and load a schema and environment
            printf("\n  Creating XML environment manager instance... ");
            bool rc = true;
            m_pEnvMgr = getEnvironmentMgrInstance(EnvironmentType::XML);
            CPPUNIT_ASSERT_MESSAGE("Unable to allocate an environment manager", m_pEnvMgr != nullptr);
            printf("complete.");

            //
            // Load the schema
            printf("\n  Loading schema (%s...", schemaFilename.c_str());
            std::map<std::string, std::string> cfgParms;
            rc = m_pEnvMgr->loadSchema(schemaXSDDir, schemaFilename, cfgParms);
            CPPUNIT_ASSERT_MESSAGE("Unable to load configuration schema, error = " + m_pEnvMgr->getLastSchemaMessage(), rc);

            //
            // Load the environment if specified
            if (!environmentFile.empty())
            {
                bool rc = loadEnvironment(environmentFile);
                CPPUNIT_ASSERT_MESSAGE("Unable to load environment, error = " + m_pEnvMgr->getLastEnvironmentMessage(), rc);
            }
            printf("complete.");
        }


        void LoadTemplate(const std::string &templateFilename)
        {
            printf("\n  Instantiating template %s ...", templateFilename.c_str());
            std::string templateSchema = INSTALL_DIR PATHSEPSTR "componentfiles/configschema/templates/schema/ModTemplateSchema.json";
            if (m_pTemplate != nullptr)
                delete m_pTemplate;
            m_pTemplate = new EnvModTemplate(m_pEnvMgr, templateSchema);
            if (!templateFilename.empty())
            {
                templateInfo ti(templateFilename, false, "Loading...");
                doTemplateLoad(ti);  // just reuse the same method used to test loading
            }
        }


        void doTemplateLoad(const templateInfo &info)
        {
            std::string assertMsg = "'" + info.testMsg + "' ";
            bool rc;
            printf("\n  %s...", info.testMsg.c_str());
            try
            {
                if (info.json.find(".json") != std::string::npos)
                {
                    std::string templateDir = INSTALL_DIR PATHSEPSTR "testing/configmgr/templates/";
                    m_pTemplate->loadTemplateFromFile(templateDir + info.json);
                }
                else
                {
                    m_pTemplate->loadTemplateFromJson(info.json);
                }

                rc = !info.expectFail;
                if (!rc)
                {
                    assertMsg += "passed when a failure was expected\nJSON = " + info.json;
                }
            }
            catch (TemplateException &e)
            {
                if (e.isBadJson()) // || e.isInvalidTemplate())
                {
                    rc = false;
                    assertMsg += e.what();
                }
                else
                {
                    rc = info.expectFail;
                    if (!rc)
                    {
                        assertMsg = "failed when it was expected to pass\nJSON = " + info.json + "\nError = " + e.what();
                    }
                }
            }
            CPPUNIT_ASSERT_MESSAGE(assertMsg.c_str(), rc);
            printf("passed.");
        }


        void Test_LoadingTemplates()
        {
            bool rc = false;
            printf("\n*** ConfigMgr 2.0 - Templates - Loading Only ***");
            CreateEnvironmentManager("Simple.xsd", "");

            //
            // Simple template create
            printf("\n  Instantiating a template...");
            std::string templateSchema = INSTALL_DIR PATHSEPSTR "componentfiles/configschema/templates/schema/ModTemplateSchema.json";
            EnvModTemplate *pTemplate = new EnvModTemplate(m_pEnvMgr, templateSchema);
            printf("complete.");

            //
            // Load valid JSON from a string
            printf("\n  Valid JSON, from string...");
            try {
                pTemplate->loadTemplateFromJson("{ \"key\" : \"value\" }");
                rc = false;  // the load should throw
            } catch (TemplateException &te) {
                rc = te.isInvalidTemplate();
            }
            CPPUNIT_ASSERT_MESSAGE("Unable to load JSON from a string.", rc);
            printf("passed.");

            //
            // Load invalid JSON from a string and make sure it detects invalid JSON
            printf("\n  Testing invalid JSON parse from a string...");
            try {
                pTemplate->loadTemplateFromJson("{ \"key : \"value\" }");
                rc = false;
            } catch (TemplateException &te) {
                rc = te.isBadJson();
            }
            CPPUNIT_ASSERT_MESSAGE("Unable to detect invalid JSON from a string.", rc);
            printf("passed.");

            //
            // Load valid JSON from a file (still not a valid template)
            printf("\n  Testing valid JSON parse from file (pareFromFileTest.json)...");
            try {
                pTemplate->loadTemplateFromFile(m_templateDir + "ParseFromFileTest.json");
                rc = true;
            } catch (TemplateException &te) {
                rc = te.isInvalidTemplate();  // this is the expected error
            }
            CPPUNIT_ASSERT_MESSAGE("Unable to parse valid JSON.", rc);
            printf("passed.");

            //
            // Test complete
            delete pTemplate;
            printf("\n  Tests complete");
        }


        void Test_Inputs() {
            std::string msg;
            bool rc = false;
            printf("\n*** ConfigMgr 2.0 - Templates - Inputs ***");
            CreateEnvironmentManager("Simple.xsd", "");
            LoadTemplate("");

            std::string json;
            std::vector<templateInfo> inputTests;

            // Inputs testing: validate required values
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : 7 }", true, "Invalid Inputs section type (non array)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : [] }", false, "Valid Inputs section type (array)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : [\"bad\"] }", true, "Invalid Inputs array element (non-object)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : [{}] }", true, "Invalid Inputs array element (missing required values)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : [{\"name\":\"x\"}] }", true, "Invalid Inputs array element (missing required values)"));

            // Inputs: test types
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : [{\"name\":\"x\",\"type\":\"string\"}] }", false, "Valid Inputs array element (type 'string')"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : [{\"name\":\"x\",\"type\":\"badtype\"}] }", true, "Invalid Input type"));

            // Inputs: duplicate name
            json = "{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"inputs\" : [{\"name\":\"x\",\"type\":\"string\"}, {\"name\":\"x\",\"type\":\"string\"}] }";
            inputTests.emplace_back(templateInfo(json, true, "Invalid, Duplicate input name"));

            //
            // Loop through these simple load tests
            for (auto &inputTest: inputTests)
            {
                doTemplateLoad(inputTest);
            }

            // Inputs; retrieving by name with >1 inputs
            json =  R"({"name" : "x", "type" : "new", "operations" : [], "inputs" : [)";
            json += R"({"name":"x","type":"string", "prompt":"prompt","description":"description","tooltip":"tooltip"})";
            json += R"(,{"name":"y","type":"string"})";
            json += R"(,{"name":"ips","type":"iprange"})";
            json += R"(,{"name":"preset","type":"string","value":"myvalue"})";
            json += "] }";
            templateInfo ti1(json, false, "Valid multi-inputs");
            doTemplateLoad(ti1);

            printf("\n  Test getting inputs from template...");
            std::vector<std::shared_ptr<Input>> inputs = m_pTemplate->getInputs();
            CPPUNIT_ASSERT_MESSAGE("Number of inputs was not 4", inputs.size() == 4);
            printf("passed.");

            printf("\n  Test retrieving first defined input 'x'...");
            auto pInput = m_pTemplate->getInput("x", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'x'", pInput);
            printf("passed.");

            printf("\n  Test retrieving second defined input 'y'...");
            pInput = m_pTemplate->getInput("y", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'y'", pInput);
            printf("passed.");

            //
            // Test string type inputs (and other input attributes)
            printf("\n  Test retrieving string input 'x'...");
            pInput = m_pTemplate->getInput("x", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'x'", pInput);
            printf("passed.");

            printf("\n  Test retrieving 'prompt'...");
            std::string returnedPrompt = pInput->getUserPrompt();
            msg = "Expected prompt value 'prompt' did not match returned value '" + returnedPrompt + "'";
            CPPUNIT_ASSERT_MESSAGE(msg, returnedPrompt == "prompt");
            printf("passed.");

            printf("\n  Test retrieving 'description'...");
            std::string returnedDesc = pInput->getDescription();
            msg = "Expected description value 'description' did not match returned value '" + returnedDesc + "'";
            CPPUNIT_ASSERT_MESSAGE(msg, returnedDesc == "description");
            printf("passed.");

            printf("\n  Test retrieving 'tooltip'...");
            std::string returnedTooltip = pInput->getTooltip();
            msg = "Expected tooltip value 'tooltip' did not match returned value '" + returnedTooltip + "'";
            CPPUNIT_ASSERT_MESSAGE(msg, returnedTooltip == "tooltip");
            printf("passed.");

            //
            // test string type inputs
            printf("\n  Test retrieving string input 'y'...");
            pInput = m_pTemplate->getInput("y", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'y'", pInput);
            printf("passed.");

            printf("\n  Test setting and getting string value...");
            if (pInput)
            {
                pInput->setValue("aaaBBBccc");
                CPPUNIT_ASSERT_MESSAGE("Get value did not match the set value (aaaBBBccc)", pInput->getValue(0) == "aaaBBBccc");
            }
            else
            {
                CPPUNIT_ASSERT_MESSAGE("Unable to dynamic cast input poniter to expected type of InputValue<std::string>", false);
            }
            printf("passed.");

            //
            // IPrange inputs
            printf("\n  Testing IPRange input...");
            printf("\n  Testing getting an IPRange input pointer...");
            pInput = m_pTemplate->getInput("ips");
            std::shared_ptr<IPAddressRangeInput> pIpRangeInput = std::dynamic_pointer_cast<IPAddressRangeInput>(pInput);
            if (!pIpRangeInput)
            {
                CPPUNIT_ASSERT_MESSAGE("Unable to dynamic cast input poniter to expected type of IPRange", false);
            }
            printf("passed.");

            printf("\n  Set single IP address...");
            try
            {
                pIpRangeInput->setValue("1.2.3.4");
                rc = pIpRangeInput->getValue(0) == "1.2.3.4";
                CPPUNIT_ASSERT_MESSAGE("Return value did not match expected value", rc);
            }
            catch (TemplateException &te)
            {
                rc = false;
            }

            printf("\n  Set multiple IP addresses (';' separated IP addresses)...");
            try
            {
                pIpRangeInput->setValue("1.2.3.4;1.2.3.5");
                rc = pIpRangeInput->getValue(0) == "1.2.3.4";
            }
            catch (TemplateException &te)
            {
                rc = false;
            }
            printf("complete");

            printf("\n  Set a single range (1.2.3.1-5)...");
            try
            {
                pIpRangeInput->setValue("1.2.3.1-5");
                CPPUNIT_ASSERT_MESSAGE("Expected 5 values for variable", pIpRangeInput->getNumValues() == 5);

                rc  = pIpRangeInput->getValue(0) == "1.2.3.1";
                rc &= pIpRangeInput->getValue(1) == "1.2.3.2";
                rc &= pIpRangeInput->getValue(2) == "1.2.3.3";
                rc &= pIpRangeInput->getValue(3) == "1.2.3.4";
                rc &= pIpRangeInput->getValue(4) == "1.2.3.5";
                CPPUNIT_ASSERT_MESSAGE("Expected values were not returned correctly", rc);
            }
            catch (TemplateException &te)
            {
                CPPUNIT_ASSERT_MESSAGE(te.what(), false);
            }
            printf("complete");

            printf("\n  Set a valid and arange (3.4.5.6;1.2.3.1-5)...");
            try
            {
                pIpRangeInput->setValue("3.4.5.6;1.2.3.1-5");
                CPPUNIT_ASSERT_MESSAGE("Expected 6 values for variable", pIpRangeInput->getNumValues() == 6);

                rc  = pIpRangeInput->getValue(0) == "3.4.5.6";
                rc &= pIpRangeInput->getValue(1) == "1.2.3.1";
                rc &= pIpRangeInput->getValue(2) == "1.2.3.2";
                rc &= pIpRangeInput->getValue(3) == "1.2.3.3";
                rc &= pIpRangeInput->getValue(4) == "1.2.3.4";
                rc &= pIpRangeInput->getValue(5) == "1.2.3.5";
                CPPUNIT_ASSERT_MESSAGE("Expected values were not returned correctly", rc);
            }
            catch (TemplateException &te)
            {
                CPPUNIT_ASSERT_MESSAGE(te.what(), false);
            }
            printf("complete");

            printf("\n  Preset value...");
            pInput = m_pTemplate->getInput("preset", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'preset'", pInput);
            CPPUNIT_ASSERT_MESSAGE("Variable does not have expected value", pInput->getValue(0) == "myvalue");
            printf("complete");

            //
            // Test complete
            delete m_pTemplate;
            printf("\n  Test complete");
        }


        void Test_SubstitutionTests()
        {
            std::string msg;
            bool rc = false;
            printf("\n*** ConfigMgr 2.0 - Templates - Variable substitution tests ***");
            CreateEnvironmentManager("Simple.xsd", "template_test.xml");
            LoadTemplate("");

            templateInfo si("SubstitutionTest-1.json", false, "Loading variable substitution test 1 template");
            doTemplateLoad(si);
            printf("complete.");

            printf("\n  Executing template...");
            m_pTemplate->execute();
            printf("complete.");

            delete m_pTemplate;
        }


        void Test_OperationsTests()
        {
            std::string msg;
            bool rc = false;
            printf("\n*** ConfigMgr 2.0 - Templates - Execute Templates ***");
            CreateEnvironmentManager("Simple.xsd", "template_test.xml");

            //
            // Test a simple add of a sub element with two attributes
            //   load the template
            //   get inuts (both string) and set values (should be two)
            //   execute the template
            //   retrieve node from changed environment to validate added properly
            LoadTemplate("OperationsTest1.json");

            m_pTemplate->execute();



            delete m_pTemplate;
        }


    private:

        EnvironmentMgr  *m_pEnvMgr;
        std::string m_templateDir = INSTALL_DIR PATHSEPSTR "testing/configmgr/templates/";
        EnvModTemplate *m_pTemplate;
};

// To run just this test: unittests <testname> where <testname> is the name registered with the CPPUNIT_TEST_SUITE_NAMED_REGISTRATION macro

CPPUNIT_TEST_SUITE_REGISTRATION( ConfigMgrTemplateTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ConfigMgrTemplateTests, "ConfigMgrTemplateTests" );

#endif
