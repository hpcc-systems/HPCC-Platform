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
#include "unittests.hpp"
#include "xml2ecl.hpp"
#include <sstream>
#include <string>

class Xml2EclTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(Xml2EclTests);
        CPPUNIT_TEST(testSimpleElement);
        CPPUNIT_TEST(testNestedElements);
        CPPUNIT_TEST(testAttributes);
        CPPUNIT_TEST(testRepeatedElements);
        CPPUNIT_TEST(testMixedContent);
        CPPUNIT_TEST(testEmptyElements);
        CPPUNIT_TEST(testStreamInput);
        CPPUNIT_TEST(testMultipleFiles);
        CPPUNIT_TEST(testNamespaceStripping);
        CPPUNIT_TEST(testNorootFlag);
    CPPUNIT_TEST_SUITE_END();

public:
    void testSimpleElement()
    {
        const char* xml = "<person><name>test</name><age>30</age></person>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        // The wrapper creates an outer object with 'person' field
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
    }
    
    void testNestedElements()
    {
        const char* xml = "<root><person><name>test</name><address><city>Austin</city></address></person></root>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
    }
    
    void testAttributes()
    {
        const char* xml = "<node id=\"123\" name=\"test\"><value>data</value></node>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        // Should have the 'node' field
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        
        // The node object should have attributes (@id, @name) and value element
        auto* nodeObj = dynamic_cast<ObjectItem*>(fields[0].second.get());
        CPPUNIT_ASSERT(nodeObj != nullptr);
        
        const auto& nodeFields = nodeObj->getFields();
        // Should have @id, @name, and value fields
        CPPUNIT_ASSERT(nodeFields.size() >= 3);
    }
    
    void testRepeatedElements()
    {
        const char* xml = "<root><item>1</item><item>2</item><item>3</item></root>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("root"), fields[0].first);
        
        // root should contain an array of items
        auto* rootObj = dynamic_cast<ObjectItem*>(fields[0].second.get());
        CPPUNIT_ASSERT(rootObj != nullptr);
        
        const auto& rootFields = rootObj->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, rootFields.size());
        
        // items should be an array
        auto* arrayItem = dynamic_cast<ArrayItem*>(rootFields[0].second.get());
        CPPUNIT_ASSERT(arrayItem != nullptr);
    }
    
    void testMixedContent()
    {
        const char* xml = "<node id=\"1\">text content</node>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        
        // node should have @id attribute and _inner_value for text
        auto* nodeObj = dynamic_cast<ObjectItem*>(fields[0].second.get());
        CPPUNIT_ASSERT(nodeObj != nullptr);
        CPPUNIT_ASSERT(nodeObj->getFields().size() >= 1);
    }
    
    void testEmptyElements()
    {
        const char* xml = "<root><empty/><item></item></root>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
    }
    
    void testStreamInput()
    {
        const char* xml = "<test><value>data</value></test>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
    }
    
    void testMultipleFiles()
    {
        // This test demonstrates schema merging across files
        const char* xml1 = "<person><name>test</name><age>30</age></person>";
        const char* xml2 = "<person><name>test2</name><city>Austin</city></person>";
        
        std::istringstream input1(xml1);
        auto schema1 = parseXmlStream(input1);
        
        std::istringstream input2(xml2);
        auto schema2 = parseXmlStream(input2);
        
        // Merge schemas
        mergeSchemaItems(schema1.get(), schema2.get());
        
        auto* objItem = dynamic_cast<ObjectItem*>(schema1.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        // After merging, should have unified schema
        CPPUNIT_ASSERT(objItem->getFields().size() >= 1);
    }
    
    void testNamespaceStripping()
    {
        const char* xml = "<soapenv:Envelope><soapenv:Header><ns:auth>token</ns:auth></soapenv:Header></soapenv:Envelope>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        // Should have stripped 'soapenv:' prefix, so field should be 'Envelope'
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("Envelope"), fields[0].first);
    }
    
    void testNorootFlag()
    {
        // Test XML with repeated root elements which should set needsNoroot flag
        const char* xml = "<test><person id=\"1\"><name>Alice</name></person><person id=\"2\"><name>Bob</name></person></test>";
        
        std::istringstream input(xml);
        auto schema = parseXmlStream(input);
        
        // The schema itself doesn't track the NOROOT flag,
        // that's handled by parseXmlFileWithFlags and unwrapParsedObject,
        // but we can verify the schema structure is correct
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("test"), fields[0].first);
        
        // test should contain an array of person elements
        auto* testObj = dynamic_cast<ObjectItem*>(fields[0].second.get());
        CPPUNIT_ASSERT(testObj != nullptr);
        
        const auto& testFields = testObj->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, testFields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("person"), testFields[0].first);
        
        // person should be an array
        auto* arrayItem = dynamic_cast<ArrayItem*>(testFields[0].second.get());
        CPPUNIT_ASSERT(arrayItem != nullptr);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(Xml2EclTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(Xml2EclTests, "Xml2EclTests");

#endif // _USE_CPPUNIT
