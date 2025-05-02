/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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
#include "rtlformat.hpp"
#include "unittests.hpp"

#define CPPUNIT_ASSERT_EQUAL_STR(x, y) CPPUNIT_ASSERT_EQUAL(std::string(x ? x : ""), std::string(y ? y : ""))

class CommonJsonWriterTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(CommonJsonWriterTest);
    CPPUNIT_TEST(testNullValues);
    CPPUNIT_TEST(testEmptyStringValues);
    CPPUNIT_TEST(testBasicTypes);
    CPPUNIT_TEST(testNesting);
    CPPUNIT_TEST(testArrays);
    CPPUNIT_TEST(testFormatFlags);
    CPPUNIT_TEST(testOutputQuoted);
    CPPUNIT_TEST(testComplexStructure);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp() {}
    void tearDown() {}

    void testNullValues()
    {
        // Test with default flags (no indentation)
        CommonJsonWriter writer(XWFnoindent);

        // Test null string output
        const char *pChar = nullptr;
        writer.outputString(0, pChar, "fieldname");
        CPPUNIT_ASSERT_EQUAL_STR("\"fieldname\": \"\"", writer.str());
        writer.clear();

        // Test null string output utf8
        writer.outputUtf8(0, pChar, "fieldname");
        CPPUNIT_ASSERT_EQUAL_STR(",\"fieldname\": \"\"", writer.str());
        writer.clear();
    }

    void testEmptyStringValues()
    {
        // Test with default flags (no indentation)
        CommonJsonWriter writer(XWFnoindent);

        // Test empty string output
        writer.outputString(0, "", "fieldname");
        CPPUNIT_ASSERT_EQUAL_STR("\"fieldname\": \"\"", writer.str());
        writer.clear();

        // Test empty string output utf8
        writer.outputUtf8(0, "", "fieldname");
        CPPUNIT_ASSERT_EQUAL_STR(",\"fieldname\": \"\"", writer.str());
        writer.clear();
    }

    void testBasicTypes()
    {
        // Test with default flags (no indentation)
        CommonJsonWriter writer(XWFnoindent);

        // Test string output
        writer.outputString(5, "hello", "greeting");
        CPPUNIT_ASSERT_EQUAL_STR("\"greeting\": \"hello\"", writer.str());
        writer.clear();

        // Test boolean output
        writer.outputBool(true, "flag");
        CPPUNIT_ASSERT_EQUAL_STR(",\"flag\": true", writer.str());
        writer.clear();

        // Test integer output
        writer.outputInt(123, sizeof(int), "count");
        CPPUNIT_ASSERT_EQUAL_STR(",\"count\": 123", writer.str());
        writer.clear();

        // Test real output
        writer.outputReal(123.45, "value");
        CPPUNIT_ASSERT_EQUAL_STR(",\"value\": 123.45", writer.str());
        writer.clear();
    }

    void testNesting()
    {
        CommonJsonWriter writer(0); // Use default flags with indentation

        // Start an object
        writer.outputBeginNested("person", true);

        // Add some fields
        writer.outputString(4, "John", "name");
        writer.outputInt(30, sizeof(int), "age");

        // Nested object
        writer.outputBeginNested("address", true);
        writer.outputString(11, "Main Street", "street");
        writer.outputString(8, "New York", "city");
        writer.outputEndNested("address");

        // Close the main object
        writer.outputEndNested("person");

        // Verify the JSON structure
        const char *expected =
            "\n\"person\": {\n"
            " \"name\": \"John\", \n"
            " \"age\": 30, \n"
            " \"address\": {\n"
            "  \"street\": \"Main Street\", \n"
            "  \"city\": \"New York\"}}";

        CPPUNIT_ASSERT_EQUAL_STR(expected, writer.str());
    }

    void testArrays()
    {
        CommonJsonWriter writer(0);

        // Test array output
        writer.outputBeginArray("numbers");

        // Output some array items
        writer.outputInt(1, sizeof(int), nullptr);
        writer.outputInt(2, sizeof(int), nullptr);
        writer.outputInt(3, sizeof(int), nullptr);

        writer.outputEndArray("numbers");

        const char *expected =
            "\"numbers\": [\n"
            " \"#value\": 1, \n"
            " \"#value\": 2, \n"
            " \"#value\": 3]";

        CPPUNIT_ASSERT_EQUAL_STR(expected, writer.str());
    }

    void testFormatFlags()
    {
        // Test with no indentation
        {
            CommonJsonWriter writer(XWFnoindent);
            writer.outputBeginNested("person", true);
            writer.outputString(4, "John", "name");
            writer.outputInt(30, sizeof(int), "age");
            writer.outputEndNested("person");

            CPPUNIT_ASSERT_EQUAL_STR("\"person\": {\"name\": \"John\", \"age\": 30}", writer.str());
        }

        // Test with trim option
        {
            CommonJsonWriter writer(XWFtrim);
            writer.outputString(10, "John      ", "name"); // String with trailing spaces
            CPPUNIT_ASSERT_EQUAL_STR("\n\"name\": \"John\"", writer.str());
        }

        // Test with optional fields
        {
            CommonJsonWriter writer(XWFopt);
            writer.outputString(0, "", "empty"); // Empty string should be omitted
            CPPUNIT_ASSERT_EQUAL_STR("", writer.str());

            writer.outputString(5, "hello", "greeting");
            CPPUNIT_ASSERT_EQUAL_STR("\n\"greeting\": \"hello\"", writer.str());
        }
    }

    void testOutputQuoted()
    {
        CommonJsonWriter writer(XWFnoindent);

        writer.outputQuoted("{custom:format}");
        CPPUNIT_ASSERT_EQUAL_STR("\"{custom:format}\"", writer.str());
        writer.clear();

        // Test mixing quoted and standard output
        writer.outputBeginNested("data", true);
        writer.outputQuoted("preformatted:json");
        writer.outputString(5, "value", "field");
        writer.outputEndNested("data");

        CPPUNIT_ASSERT_EQUAL_STR(",\"data\": {\"preformatted:json\", \"field\": \"value\"}", writer.str());
    }

    void testComplexStructure()
    {
        CommonJsonWriter writer(0);

        // Create a complex nested structure
        writer.outputBeginNested("root", true);

        // Add a person
        writer.outputBeginNested("person", true);
        writer.outputString(4, "Jane", "name");
        writer.outputInt(28, sizeof(int), "age");

        // Add contact info
        writer.outputBeginNested("contact", true);
        writer.outputString(12, "555-123-4567", "phone");
        writer.outputString(17, "jane@example.com", "email");
        writer.outputEndNested("contact");

        // Add an array of hobbies
        writer.outputBeginArray("hobbies");
        writer.outputString(7, "reading", nullptr);
        writer.outputString(7, "cycling", nullptr);
        writer.outputString(6, "hiking", nullptr);
        writer.outputEndArray("hobbies");

        // Add work history
        writer.outputBeginArray("jobs");

        // First job
        writer.outputBeginNested(nullptr, true); // Unnamed object in array
        writer.outputString(9, "Developer", "title");
        writer.outputString(9, "2018-2020", "period");
        writer.outputEndNested(nullptr);

        // Second job
        writer.outputBeginNested(nullptr, true);
        writer.outputString(12, "Team Manager", "title");
        writer.outputString(9, "2020-2023", "period");
        writer.outputEndNested(nullptr);

        writer.outputEndArray("jobs");

        // Close person and root
        writer.outputEndNested("person");
        writer.outputEndNested("root");

        // The resulting JSON should be a properly formatted complex structure
        // We'll just check a few key structural elements
        std::string result = writer.str();
        CPPUNIT_ASSERT(result.find("\"root\": {") != std::string::npos);
        CPPUNIT_ASSERT(result.find("\"person\": {") != std::string::npos);
        CPPUNIT_ASSERT(result.find("\"hobbies\": [") != std::string::npos);
        CPPUNIT_ASSERT(result.find("\"jobs\": [") != std::string::npos);
        CPPUNIT_ASSERT(result.find("\"title\": \"Team Manager\"") != std::string::npos);

        // Check proper nesting level
        CPPUNIT_ASSERT(result.find("   \"email\"") != std::string::npos); // Should be indented 3 spaces
    }
};

// Add the test suite macros outside the class definition
CPPUNIT_TEST_SUITE_REGISTRATION(CommonJsonWriterTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(CommonJsonWriterTest, "CommonJsonWriterTest");

#endif // _USE_CPPUNIT
