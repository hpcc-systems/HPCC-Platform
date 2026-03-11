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
#include "json2ecl.hpp"
#include <sstream>
#include <string>

class Json2EclTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(Json2EclTests);
        CPPUNIT_TEST(testSimpleObject);
        CPPUNIT_TEST(testNestedObject);
        CPPUNIT_TEST(testArray);
        CPPUNIT_TEST(testNDJSON);
        CPPUNIT_TEST(testMixedTypes);
        CPPUNIT_TEST(testEmptyObject);
        CPPUNIT_TEST(testNullValues);
        CPPUNIT_TEST(testStreamInput);
        CPPUNIT_TEST(testMultipleFiles);
        CPPUNIT_TEST(testNorootFlag);
    CPPUNIT_TEST_SUITE_END();

public:
    void testSimpleObject()
    {
        const char* json = R"({"name": "test", "age": 30, "active": true})";
        
        std::istringstream input(json);
        auto schema = parseJsonStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        CPPUNIT_ASSERT_EQUAL((size_t)3, objItem->getFields().size());
    }
    
    void testNestedObject()
    {
        const char* json = R"({"person": {"name": "test", "address": {"city": "Austin"}}})";
        
        std::istringstream input(json);
        auto schema = parseJsonStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        // Check for 'person' field
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("person"), fields[0].first);
        
        // Check nested structure
        auto* personObj = dynamic_cast<ObjectItem*>(fields[0].second.get());
        CPPUNIT_ASSERT(personObj != nullptr);
        CPPUNIT_ASSERT_EQUAL((size_t)2, personObj->getFields().size());
    }
    
    void testArray()
    {
        const char* json = R"({"items": [1, 2, 3, 4, 5]})";
        
        std::istringstream input(json);
        auto schema = parseJsonStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        CPPUNIT_ASSERT_EQUAL(std::string("items"), fields[0].first);
        
        // Check array type
        auto* arrayItem = dynamic_cast<ArrayItem*>(fields[0].second.get());
        CPPUNIT_ASSERT(arrayItem != nullptr);
        CPPUNIT_ASSERT(arrayItem->getElementType() != nullptr);
    }
    
    void testNDJSON()
    {
        const char* ndjson = R"({"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35})";
        
        std::istringstream input(ndjson);
        auto schema = parseJsonStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        // Should merge all three objects
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)2, fields.size());
    }
    
    void testMixedTypes()
    {
        const char* json = R"({"value": 123})";
        const char* json2 = R"({"value": "text"})";
        
        std::istringstream input1(json);
        auto schema1 = parseJsonStream(input1);
        
        std::istringstream input2(json2);
        auto schema2 = parseJsonStream(input2);
        
        // Merge schemas
        bool changed = mergeSchemaItems(schema1.get(), schema2.get());
        CPPUNIT_ASSERT(changed);
        
        auto* objItem = dynamic_cast<ObjectItem*>(schema1.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)1, fields.size());
        
        // Value should now be a ValueItem with mixed types
        auto* valueItem = dynamic_cast<ValueItem*>(fields[0].second.get());
        CPPUNIT_ASSERT(valueItem != nullptr);
        CPPUNIT_ASSERT(valueItem->getTypes().count() > 1);
    }
    
    void testEmptyObject()
    {
        const char* json = R"({})";
        
        std::istringstream input(json);
        auto schema = parseJsonStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        CPPUNIT_ASSERT_EQUAL((size_t)0, objItem->getFields().size());
    }
    
    void testNullValues()
    {
        const char* json = R"({"name": null, "age": 30})";
        
        std::istringstream input(json);
        auto schema = parseJsonStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        CPPUNIT_ASSERT_EQUAL((size_t)2, objItem->getFields().size());
    }
    
    void testStreamInput()
    {
        const char* json = R"({"test": "value"})";
        
        std::istringstream input(json);
        auto schema = parseJsonStream(input);
        
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
    }
    
    void testMultipleFiles()
    {
        // This test demonstrates schema merging across files
        // In practice, this would use parseJsonFileWithFlags
        const char* json1 = R"({"name": "test", "age": 30})";
        const char* json2 = R"({"name": "test2", "city": "Austin"})";
        
        std::istringstream input1(json1);
        auto schema1 = parseJsonStream(input1);
        
        std::istringstream input2(json2);
        auto schema2 = parseJsonStream(input2);
        
        // Merge schemas
        mergeSchemaItems(schema1.get(), schema2.get());
        
        auto* objItem = dynamic_cast<ObjectItem*>(schema1.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        
        // Should have all three fields: name, age, city
        const auto& fields = objItem->getFields();
        CPPUNIT_ASSERT_EQUAL((size_t)3, fields.size());
    }
    
    void testNorootFlag()
    {
        // Test NDJSON format which should set needsNoroot flag
        const char* ndjson = R"({"id": 1}
{"id": 2}
{"id": 3})";
        
        std::istringstream input(ndjson);
        auto schema = parseJsonStream(input);
        
        // The schema itself doesn't track the NOROOT flag,
        // that's handled by parseJsonFileWithFlags, but we can
        // verify the schema structure is correct for NDJSON
        CPPUNIT_ASSERT(schema != nullptr);
        auto* objItem = dynamic_cast<ObjectItem*>(schema.get());
        CPPUNIT_ASSERT(objItem != nullptr);
        CPPUNIT_ASSERT_EQUAL((size_t)1, objItem->getFields().size());
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(Json2EclTests);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(Json2EclTests, "Json2EclTests");

#endif // _USE_CPPUNIT
