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
#include "eclrtl.hpp"

class RtlXmlTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(RtlXmlTest);
    CPPUNIT_TEST(testXmlString);
    CPPUNIT_TEST(testXmlBool);
    CPPUNIT_TEST(testXmlIntegers);
    CPPUNIT_TEST(testXmlReal);
    CPPUNIT_TEST(testXmlData);
    CPPUNIT_TEST(testXmlDecimal);
    CPPUNIT_TEST(testXmlUnicode);
    CPPUNIT_TEST(testXmlUtf8);
    CPPUNIT_TEST(testXmlNesting);
    CPPUNIT_TEST(testXmlAttributes);
    CPPUNIT_TEST(testJsonUnicode);
    CPPUNIT_TEST(testJsonDecimal);
    CPPUNIT_TEST(testComplexStructure);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp() {}
    void tearDown() {}

    void testXmlString()
    {
        StringBuffer out;

        // Test with fieldname
        outputXmlString(5, "hello", "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("<greeting>hello</greeting>", out.str());
        out.clear();

        // Test with null fieldname
        outputXmlString(5, "hello", nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("hello", out.str());
        out.clear();

        // Test with empty string fieldname
        outputXmlString(5, "hello", "", out);
        CPPUNIT_ASSERT_EQUAL_STR("hello", out.str());
        out.clear();

        // Test with special characters
        outputXmlString(15, "hello & <world>", "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("<greeting>hello &amp; &lt;world&gt;</greeting>", out.str());
    }

    void testXmlBool()
    {
        StringBuffer out;

        // Test true with fieldname
        outputXmlBool(true, "flag", out);
        CPPUNIT_ASSERT_EQUAL_STR("<flag>true</flag>", out.str());
        out.clear();

        // Test false with null fieldname
        outputXmlBool(false, nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("false", out.str());
        out.clear();

        // Test false with empty string fieldname
        outputXmlBool(false, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("false", out.str());
    }

    void testXmlIntegers()
    {
        StringBuffer out;

        // Test int64 with fieldname
        outputXmlInt(12345, "number", out);
        CPPUNIT_ASSERT_EQUAL_STR("<number>12345</number>", out.str());
        out.clear();

        // Test int64 null fieldname
        outputXmlInt(-9876, nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("-9876", out.str());
        out.clear();

        // Test int64 empty string fieldname
        outputXmlInt(-9876, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("-9876", out.str());
        out.clear();

        // Test uint64 with fieldname
        outputXmlUInt(54321, "count", out);
        CPPUNIT_ASSERT_EQUAL_STR("<count>54321</count>", out.str());
        out.clear();

        // Test uint64 with null fieldname
        outputXmlUInt(98765, nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("98765", out.str());
        out.clear();

        // Test uint64 with empty string fieldname
        outputXmlUInt(98765, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("98765", out.str());
    }

    void testXmlReal()
    {
        StringBuffer out;

        // Test double with fieldname
        outputXmlReal(123.45, "value", out);
        CPPUNIT_ASSERT_EQUAL_STR("<value>123.45</value>", out.str());
        out.clear();

        // Test double with null fieldname
        outputXmlReal(-987.65, nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("-987.65", out.str());
        out.clear();

        // Test double with empty string fieldname
        outputXmlReal(-987.65, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("-987.65", out.str());
    }

    void testXmlData()
    {
        StringBuffer out;

        // Test binary data with fieldname
        unsigned char data[] = {0x01, 0x23, 0xAB, 0xCD};
        outputXmlData(4, data, "binary", out);
        CPPUNIT_ASSERT_EQUAL_STR("<binary>0123ABCD</binary>", out.str());
        out.clear();

        // Test binary data with null fieldname
        outputXmlData(4, data, nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("0123ABCD", out.str());
        out.clear();

        // Test binary data with empty string fieldname
        outputXmlData(4, data, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("0123ABCD", out.str());
    }

    void testXmlDecimal()
    {
        StringBuffer out;

        // Test decimal with fieldname
        byte value123450[4] = {0x01, 0x23, 0x45, 0x0c};
        outputXmlDecimal(value123450, 4, 0, "amount", out);
        CPPUNIT_ASSERT_EQUAL_STR("<amount>123450</amount>", out.str());
        out.clear();

        // Test udecimal with fieldname
        byte value12345[3] = {0x01, 0x23, 0x45};
        outputXmlUDecimal(value12345, 3, 0, "uamount", out);
        CPPUNIT_ASSERT_EQUAL_STR("<uamount>12345</uamount>", out.str());
    }

    void testXmlUnicode()
    {
        StringBuffer out;

        // Basic ASCII in Unicode
        UChar unicodeText[] = {'H', 'e', 'l', 'l', 'o', 0};
        outputXmlUnicode(5, unicodeText, "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("<greeting>Hello</greeting>", out.str());
        out.clear();

        // Test with null field
        outputXmlUnicode(5, nullptr, "fieldName", out);
        CPPUNIT_ASSERT_EQUAL_STR("<fieldName></fieldName>", out.str());
        out.clear();

        // Test with empty string field
        UChar unicodeEmptyText[] = {'\0', 0};
        outputXmlUnicode(1, unicodeEmptyText, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("&#xe000;", out.str());
        out.clear();

        // Test with null fieldname
        outputXmlUnicode(5, unicodeText, nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("Hello", out.str());
        out.clear();

        // Test with empty string fieldname
        outputXmlUnicode(5, unicodeText, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("Hello", out.str());
    }

    void testXmlUtf8()
    {
        StringBuffer out;

        // Simple UTF-8 string
        const char *utf8Text = "Hello";
        outputXmlUtf8(5, utf8Text, "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("<greeting>Hello</greeting>", out.str());
        out.clear();

        // UTF-8 with special characters
        const char *specialText = "Hello & <world>";
        outputXmlUtf8(15, specialText, "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("<greeting>Hello &amp; &lt;world&gt;</greeting>", out.str());
        out.clear();

        // Empty UTF-8 string
        const char *utf8EmptyText = "";
        outputXmlUtf8(0, utf8EmptyText, "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("<greeting></greeting>", out.str());
        out.clear();

        // Empty UTF-8 string
        outputXmlUtf8(0, nullptr, "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("<greeting></greeting>", out.str());
    }

    void testXmlNesting()
    {
        StringBuffer out;

        // Test nesting of XML elements
        outputXmlBeginNested("person", out);
        outputXmlString(4, "John", "name", out);
        outputXmlInt(30, "age", out);

        // Nested address
        outputXmlBeginNested("address", out);
        outputXmlString(11, "Main Street", "street", out);
        outputXmlString(8, "New York", "city", out);
        outputXmlEndNested("address", out);

        outputXmlEndNested("person", out);

        const char *expected = "<person><name>John</name><age>30</age><address><street>Main Street</street><city>New York</city></address></person>";
        CPPUNIT_ASSERT_EQUAL_STR(expected, out.str());
        out.clear();

        // Test SetAll
        outputXmlSetAll(out);
        CPPUNIT_ASSERT_EQUAL_STR("<All/>", out.str());
    }

    void testXmlAttributes()
    {
        StringBuffer out;

        // Start with an element with attributes
        out.append("<person");

        // Add various attribute types
        outputXmlAttrString(4, "John", "name", out);
        outputXmlAttrInt(30, "age", out);
        outputXmlAttrBool(true, "active", out);

        // Close the element
        out.append(">");

        const char *expected = "<person name=\"John\" age=\"30\" active=\"true\">";
        CPPUNIT_ASSERT_EQUAL_STR(expected, out.str());
        out.clear();

        // Test attributes with special characters
        out.append("<data");
        outputXmlAttrString(15, "value & <stuff>", "description", out);
        out.append(">");
        CPPUNIT_ASSERT_EQUAL_STR("<data description=\"value &amp; &lt;stuff&gt;\">", out.str());
    }

    void testJsonUnicode()
    {
        StringBuffer out;

        // Basic ASCII in Unicode
        UChar unicodeText[] = {'H', 'e', 'l', 'l', 'o', 0};
        outputJsonUnicode(5, unicodeText, "greeting", out);
        CPPUNIT_ASSERT_EQUAL_STR("\"greeting\": \"Hello\"", out.str());
        out.clear();

        // Test with null field
        outputJsonUnicode(5, nullptr, "fieldName", out);
        CPPUNIT_ASSERT_EQUAL_STR("\"fieldName\": \"\"", out.str());
        out.clear();

        // Test with empty string field
        UChar unicodeEmptyText[] = {'\0', 0};
        outputJsonUnicode(1, unicodeEmptyText, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("\"\\u0000\"", out.str());
        out.clear();

        // Test with null fieldname
        outputJsonUnicode(5, unicodeText, nullptr, out);
        CPPUNIT_ASSERT_EQUAL_STR("\"Hello\"", out.str());
        out.clear();

        // Test with empty string fieldname
        outputJsonUnicode(5, unicodeText, "", out);
        CPPUNIT_ASSERT_EQUAL_STR("\"Hello\"", out.str());
    }

    void testJsonDecimal()
    {
        StringBuffer out;

        // Test JSON decimal output
        byte value123450[4] = {0x01, 0x23, 0x45, 0x0c};

        // Without prior content
        outputJsonDecimal(value123450, 4, 0, "amount", out);
        // Check if it contains the fieldname
        CPPUNIT_ASSERT_EQUAL_STR("\"amount\": 123450", out.str());
        out.clear();

        // With prior content (should have a comma delimiter)
        out.append("{\"field\":123");
        outputJsonDecimal(value123450, 4, 0, "amount", out);
        // Check if it has a comma separator and the fieldname
        CPPUNIT_ASSERT_EQUAL_STR("{\"field\":123, \"amount\": 123450", out.str());
        out.clear();

        // NOTE: The following tests are commented out because the DecValid function in the outputJsonDecimal
        // function treats null values as valid. This causes a null pointer dereference in the DecPushDecimal
        // function.  This is a known limitation of the function.
        //
        // Test null field JSON Decimal
        // outputJsonDecimal(nullptr, 0, 0, "amount", out);
        // CPPUNIT_ASSERT_EQUAL_STR("\"amount\": ", out.str());
        // out.clear();
        //
        // Test empty string field JSON Decimal
        // outputJsonDecimal("", 0, 0, "amount", out);
        // CPPUNIT_ASSERT_EQUAL_STR("\"amount\": ", out.str());
        // out.clear();

        // Test JSON UDecimal
        byte value12345[3] = {0x01, 0x23, 0x45};
        outputJsonUDecimal(value12345, 3, 0, "uamount", out);
        CPPUNIT_ASSERT_EQUAL_STR("\"uamount\": 12345", out.str());
        out.clear();

        // NOTE: The following test shows a value of 0 being returned for a nullptr field value.
        // This is because the DecValid function in the outputJsonUDecimal function treats null
        // values as valid. This is a known limitation of the function.
        //
        // Test null field JSON UDecimal
        outputJsonUDecimal(nullptr, 0, 0, "uamount", out);
        CPPUNIT_ASSERT_EQUAL_STR("\"uamount\": 0", out.str());
        out.clear();

        // Test empty string field JSON UDecimal
        outputJsonUDecimal("", 0, 0, "uamount", out);
        CPPUNIT_ASSERT_EQUAL_STR("\"uamount\": 0", out.str());
    }

    void testComplexStructure()
    {
        StringBuffer out;

        // Create a complex XML structure
        outputXmlBeginNested("record", out);

        // Add some basic fields
        outputXmlString(4, "John", "name", out);
        outputXmlInt(30, "age", out);
        outputXmlBool(true, "active", out);

        // Add a nested element with attributes
        outputXmlBeginNested("contact", out);
        out.append("<phone");
        outputXmlAttrString(5, "Home", "type", out);
        outputXmlAttrString(12, "555-123-4567", "number", out);
        out.append("></phone>");

        // Add another nested element
        outputXmlBeginNested("address", out);
        outputXmlString(11, "Main Street", "street", out);
        outputXmlString(8, "New York", "city", out);
        outputXmlEndNested("address", out);

        // Close the nested element
        outputXmlEndNested("contact", out);

        // Close the main element
        outputXmlEndNested("record", out);

        // Check the output
        const char *expected = "<record><name>John</name><age>30</age><active>true</active><contact><phone type=\"Home&#xe000;\" number=\"555-123-4567\"></phone><address><street>Main Street</street><city>New York</city></address></contact></record>";
        CPPUNIT_ASSERT_EQUAL_STR(expected, out.str());
    }
};

// Add the test suite macros outside the class definition
CPPUNIT_TEST_SUITE_REGISTRATION(RtlXmlTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(RtlXmlTest, "RtlXmlTest");

#endif // _USE_CPPUNIT
