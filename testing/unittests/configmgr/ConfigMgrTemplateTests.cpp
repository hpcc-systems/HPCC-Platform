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
#include "mod_template_support/TemplateExecutionException.hpp"
#include "EnvironmentMgr.hpp"
#include <string>
#include <iterator>
#include <algorithm>
#include <vector>
#include "mod_template_support/EnvModTemplate.hpp"
#include "mod_template_support/IPAddressRangeVariable.hpp"

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
            CPPUNIT_TEST(Test_FindNodeTests);
            CPPUNIT_TEST(Test_CreateNodeTests);
        CPPUNIT_TEST_SUITE_END();


    protected:

        //
        // input tests
        struct templateInfo {
            templateInfo(const std::string &_json, bool _wantException, const std::string &_msg, bool _expectBadTemplate = false) :
                    json(_json), expectFail(_wantException), expectBadTemplate(_expectBadTemplate), testMsg(_msg) {}
            std::string json;
            bool expectFail;
            bool expectBadTemplate;
            std::string testMsg;
        };


        bool loadEnvironment(const std::string &envFilename)
        {
            bool rc = true;
            CPPUNIT_ASSERT_MESSAGE("No envionrment manager loaded", m_pEnvMgr != nullptr);
            std::string xmlEnvDir = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "testing/configmgr/schema/environments/";
            m_pEnvMgr->discardEnvironment();   // in case one is currently loaded

            if (!m_pEnvMgr->loadEnvironment(xmlEnvDir + envFilename))
            {
                std::string msg = "There was a problem loading environment " + envFilename + "Error: " + m_pEnvMgr->getLastEnvironmentMessage();
                CPPUNIT_ASSERT_MESSAGE(msg, false);
                rc = false;
            }
            return rc;
        }


        void CreateEnvironmentManager(const std::string &schemaFilename)
        {
            //
            // Standard configuration for HPCC
            std::string schemaXSDDir = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "testing/configmgr/schema/xsd/";

            //
            // Create an environment manager and load a schema and environment
            bool rc = true;
            m_pEnvMgr = getEnvironmentMgrInstance(EnvironmentType::XML);
            CPPUNIT_ASSERT_MESSAGE("Unable to allocate an environment manager", m_pEnvMgr != nullptr);

            //
            // Load the schema
            std::map<std::string, std::string> cfgParms;
            rc = m_pEnvMgr->loadSchema(schemaXSDDir, schemaFilename, cfgParms);
            CPPUNIT_ASSERT_MESSAGE("Unable to load configuration schema, error = " + m_pEnvMgr->getLastSchemaMessage(), rc);
        }


        void LoadTemplate(const std::string &templateFilename)
        {
            CPPUNIT_ASSERT_MESSAGE("No envionrment manager loaded", m_pEnvMgr != nullptr);
            std::string templateSchema = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "componentfiles/configschema/templates/schema/ModTemplateSchema.json";
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
            try
            {
                if (info.json.find(".json") != std::string::npos)
                {
                    std::string templateDir = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "testing/configmgr/templates/";
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
                if (!e.isTemplateInvalid())
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
        }


        void executeTemplate(bool expectFailure)
        {
            try
            {
                m_pTemplate->execute();
            }
            catch (TemplateExecutionException &e)
            {
                if (!expectFailure)
                {
                    std::string assertMsg;
                    assertMsg = "Failed when expected to pass, msg = ";
                    assertMsg += e.what();
                    CPPUNIT_ASSERT_MESSAGE(assertMsg, false);
                }
            }
        }

        void Test_LoadingTemplates()
        {
            // ConfigMgr 2.0 - testing only loading templates
            bool rc = false;
            CreateEnvironmentManager("Simple.xsd");

            //
            // Simple template create
            std::string templateSchema = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "componentfiles/configschema/templates/schema/ModTemplateSchema.json";
            auto *pTemplate = new EnvModTemplate(m_pEnvMgr, templateSchema);

            //
            // Load valid JSON from a string
            try {
                pTemplate->loadTemplateFromJson("{ \"key\" : \"value\" }");
                rc = false;  // the load should throw
            } catch (TemplateException &te) {
                rc = te.isTemplateInvalid();
            }
            CPPUNIT_ASSERT_MESSAGE("Unable to load JSON from a string.", rc);

            //
            // Load invalid JSON from a string and make sure it detects invalid JSON
            try {
                pTemplate->loadTemplateFromJson("{ key\" : \"value\" }");
                rc = false;
            } catch (TemplateException &te) {
                rc = true;
            }
            CPPUNIT_ASSERT_MESSAGE("Unable to detect invalid JSON from a string.", rc);

            //
            // Load valid JSON from a file (still not a valid template)
            try {
                pTemplate->loadTemplateFromFile(m_templateDir + "ParseFromFileTest.json");
                rc = true;
            } catch (TemplateException &te) {
                rc = te.isTemplateInvalid();  // this is the expected error
            }
            CPPUNIT_ASSERT_MESSAGE("Unable to parse valid JSON.", rc);

            //
            // Test complete
            delete pTemplate;
        }


        void Test_Inputs() {
            // ConfigMgr 2.0 - testing templates inputs
            std::string msg;
            bool rc = false;
            CreateEnvironmentManager("Simple.xsd");
            LoadTemplate("");

            std::string json;
            std::vector<templateInfo> inputTests;

            // Inputs testing: validate required values
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : 7 }", true, "Invalid Inputs section type (non array)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : [] }", false, "Valid Inputs section type (array)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : [\"bad\"] }", true, "Invalid Inputs array element (non-object)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : [{}] }", true, "Invalid Inputs array element (missing required values)"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : [{\"name\":\"x\"}] }", true, "Invalid Inputs array element (missing required values)"));

            // Inputs: test types
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : [{\"name\":\"x\",\"type\":\"string\"}] }", false, "Valid Inputs array element (type 'string')"));
            inputTests.emplace_back(templateInfo("{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : [{\"name\":\"x\",\"type\":\"badtype\"}] }", true, "Invalid Variable type"));

            // Inputs: duplicate name
            json = "{ \"name\" : \"x\", \"type\" : \"new\", \"operations\" : [], \"variables\" : [{\"name\":\"x\",\"type\":\"string\"}, {\"name\":\"x\",\"type\":\"string\"}] }";
            inputTests.emplace_back(templateInfo(json, true, "Invalid, Duplicate input name"));

            //
            // Loop through these simple load tests
            for (auto &inputTest: inputTests)
            {
                doTemplateLoad(inputTest);
            }

            // Inputs; retrieving by name with >1 inputs
            json =  R"({"name" : "x", "type" : "new", "operations" : [], "variables" : [)";
            json += R"({"name":"x","type":"string", "prompt":"prompt","description":"description"})";
            json += R"(,{"name":"y","type":"string"})";
            json += R"(,{"name":"ips","type":"iprange"})";
            json += R"(,{"name":"preset","type":"string","values": ["myvalue"]})";
            json += "] }";
            templateInfo ti1(json, false, "Valid multi-inputs");
            doTemplateLoad(ti1);

            std::vector<std::shared_ptr<Variable>> inputs = m_pTemplate->getVariables();
            CPPUNIT_ASSERT_MESSAGE("Number of inputs was not 4", inputs.size() == 4);

            auto pInput = m_pTemplate->getVariable("x", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'x'", pInput);

            pInput = m_pTemplate->getVariable("y", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'y'", pInput);

            //
            // Test string type inputs (and other input attributes)
            pInput = m_pTemplate->getVariable("x", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'x'", pInput);

            std::string returnedPrompt = pInput->getUserPrompt();
            msg = "Expected prompt value 'prompt' did not match returned value '" + returnedPrompt + "'";
            CPPUNIT_ASSERT_MESSAGE(msg, returnedPrompt == "prompt");

            std::string returnedDesc = pInput->getDescription();
            msg = "Expected description value 'description' did not match returned value '" + returnedDesc + "'";
            CPPUNIT_ASSERT_MESSAGE(msg, returnedDesc == "description");


            //
            // test string type inputs
            pInput = m_pTemplate->getVariable("y", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'y'", pInput);

            if (pInput)
            {
                pInput->addValue("aaaBBBccc");
                CPPUNIT_ASSERT_MESSAGE("Get value did not match the set value (aaaBBBccc)", pInput->getValue(0) == "aaaBBBccc");
            }
            else
            {
                CPPUNIT_ASSERT_MESSAGE("Unable to dynamic cast input poniter to expected type of InputValue<std::string>", false);
            }

            //
            // IPrange inputs
            pInput = m_pTemplate->getVariable("ips");
            if (!pInput)
            {
                CPPUNIT_ASSERT_MESSAGE("Unable to find input 'ips'", false);
            }

            try
            {
                pInput->addValue("1.2.3.4");
                rc = pInput->getValue(0) == "1.2.3.4";
                CPPUNIT_ASSERT_MESSAGE("Return value did not match expected value", rc);
            }
            catch (TemplateException &te)
            {
                rc = false;
            }

            try
            {
                pInput->addValue("1.2.3.4;1.2.3.5");
                rc = pInput->getValue(0) == "1.2.3.4";
            }
            catch (TemplateException &te)
            {
                rc = false;
            }

            try
            {
                pInput->addValue("1.2.3.1-5");
                CPPUNIT_ASSERT_MESSAGE("Expected 5 values for variable", pInput->getNumValues() == 5);

                rc  = pInput->getValue(0) == "1.2.3.1";
                rc &= pInput->getValue(1) == "1.2.3.2";
                rc &= pInput->getValue(2) == "1.2.3.3";
                rc &= pInput->getValue(3) == "1.2.3.4";
                rc &= pInput->getValue(4) == "1.2.3.5";
                CPPUNIT_ASSERT_MESSAGE("Expected values were not returned correctly", rc);
            }
            catch (TemplateException &te)
            {
                CPPUNIT_ASSERT_MESSAGE(te.what(), false);
            }

            try
            {
                pInput->addValue("3.4.5.6;1.2.3.1-5");
                CPPUNIT_ASSERT_MESSAGE("Expected 6 values for variable", pInput->getNumValues() == 6);

                rc  = pInput->getValue(0) == "3.4.5.6";
                rc &= pInput->getValue(1) == "1.2.3.1";
                rc &= pInput->getValue(2) == "1.2.3.2";
                rc &= pInput->getValue(3) == "1.2.3.3";
                rc &= pInput->getValue(4) == "1.2.3.4";
                rc &= pInput->getValue(5) == "1.2.3.5";
                CPPUNIT_ASSERT_MESSAGE("Expected values were not returned correctly", rc);
            }
            catch (TemplateException &te)
            {
                CPPUNIT_ASSERT_MESSAGE(te.what(), false);
            }

            pInput = m_pTemplate->getVariable("preset", false);
            CPPUNIT_ASSERT_MESSAGE("Unable to find input named 'preset'", pInput);
            CPPUNIT_ASSERT_MESSAGE("Variable does not have expected value", pInput->getValue(0) == "myvalue");

            //
            // Inputs from a file
            //
            // - need an InputsTest1.json that defines a number of inputs, no operations needed
            // - need an InputsForInputsTest1-1.json file that has the inputs
            //     all defined properly
            // - InputsForInputsTest1-2.json has an incorrect input name expect to fail


            //
            // Test complete
            delete m_pTemplate;
        }


        void Test_SubstitutionTests()
        {
            // ConfigMgr 2.0 - testing templates variable substitution
            std::string msg;
            bool rc = false;
            CreateEnvironmentManager("Simple.xsd");
            LoadTemplate("");  // Just loads the schema

            if (loadEnvironment("template_test.xml"))
            {
                templateInfo si("SubstitutionTest-1.json", false, "Loading variable substitution test 1 template");
                doTemplateLoad(si);
                m_pTemplate->execute();
            }
            delete m_pTemplate;
        }




        void Test_OperationsTests()
        {
            // ConfigMgr 2.0 - test execution of templates
            std::string msg;
            bool rc = false;
            CreateEnvironmentManager("Simple.xsd");

            //
            // Test a simple add of a sub element with two attributes
            //   load the template
            //   get inputs (both string) and set values (should be two)
            //   execute the template
            //   retrieve node from changed environment to validate added properly
            LoadTemplate("OperationsTest1.json");
            loadEnvironment("findnodes_test.xml");
            executeTemplate(false);

            //
            // Test a duplicate save node id input name where it is OK for a duplicate save name
            LoadTemplate("OperationsTest2.json");
            loadEnvironment("findnodes_test.xml");
            executeTemplate(false);

            //
            // Test a duplicate save node id input name where it is not OK for a duplicate save name
            LoadTemplate("OperationsTest3.json");
            loadEnvironment("findnodes_test.xml");
            executeTemplate(true);  // expect this to fail

            //
            // Test a duplicate save of attribute values to the same input var name
            LoadTemplate("OperationsTest4.json");
            loadEnvironment("findnodes_test.xml");
            executeTemplate(false);

            //
            // Test a duplicate save of attribute values to the same input var name where not allowed
            LoadTemplate("OperationsTest5.json");
            loadEnvironment("findnodes_test.xml");
            executeTemplate(true);

            delete m_pTemplate;
        }


        //
        // Tests the action find_node
        void Test_FindNodeTests()
        {
            // ConfigMgr 2.0 test finding nodes
            std::string msg;
            bool rc = false;
            CreateEnvironmentManager("Simple.xsd");
            loadEnvironment("findnodes_test.xml");

            //
            // Execute the template, then query the environment to validate. Note that there
            // may some modifications done in order to be able to verify
            LoadTemplate("FindNodeTest1.json");
            executeTemplate(false);
        }


        //
        // Tests the action find_node
        void Test_CreateNodeTests()
        {
            // ConfigMgr 2.0 - test creating nodes
            std::string msg;
            bool rc = false;
            CreateEnvironmentManager("Simple.xsd");
            loadEnvironment("createnode_test1.xml");

            //
            // Execute the template, then query the environment to validate. Note that there
            // may some modifications done in order to be able to verify
            LoadTemplate("CreateNodeTest1.json");
            executeTemplate(false);
        }



    private:

        EnvironmentMgr  *m_pEnvMgr;
        std::string m_templateDir = std::string(hpccBuildInfo.installDir) + PATHSEPSTR + "testing/configmgr/templates/";
        EnvModTemplate *m_pTemplate;
};

// To run just this test: unittests <testname> where <testname> is the name registered with the CPPUNIT_TEST_SUITE_NAMED_REGISTRATION macro

CPPUNIT_TEST_SUITE_REGISTRATION( ConfigMgrTemplateTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( ConfigMgrTemplateTests, "ConfigMgrTemplateTests" );

#endif
