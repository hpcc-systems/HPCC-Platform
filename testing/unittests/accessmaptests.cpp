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
#include "AccessMapGenerator.hpp"
#include "EsdlAccessMapGenerator.hpp"
#include "unittests.hpp"

class EsdlAccessMapGeneratorTests : public CppUnit::TestFixture
{
    constexpr static const char* primaryScope = "primary";

    class MockReporter : public EsdlDefReporter
    {
    protected:
        virtual void reportSelf(Flags flag, const char* component, const char* level, const char* msg) const override {}
    };

    EsdlAccessMapScopeMapper scopeMapper;
    EsdlAccessMapLevelMapper levelMapper;
    Owned<EsdlDefReporter>   esdlReporter;

    CPPUNIT_TEST_SUITE( EsdlAccessMapGeneratorTests );
        CPPUNIT_TEST(testVariable_DollarBrace);
        CPPUNIT_TEST(testVariable_BraceDollar);
        CPPUNIT_TEST(testVariable_Dollar);
        CPPUNIT_TEST(testVariable_Brace);
    CPPUNIT_TEST_SUITE_END();
public:
    using AccessMap = MapStringTo<SecAccessFlags>;

    #define GENERATOR \
        Owned<AccessMap> accessMap(new AccessMap()); \
        EsdlAccessMapReporter reporter(*accessMap, esdlReporter.getLink()); \
        EsdlAccessMapGenerator generator(scopeMapper, levelMapper, reporter)
    
    #define GENERATE(test) \
        if (!generator.generateMap()) \
        { \
            fprintf(stdout, "\nTest(%s) map generation failure\n", test); \
            CPPUNIT_ASSERT(false); \
        }

    void testVariable_DollarBrace()
    {
        GENERATOR;
        generator.setVariable("foo", "Bar");
        generator.insertScope(primaryScope, "Allow${foo}:Read");
        GENERATE("variable-${}");

        evaluateAccessMap("variable-${}", *accessMap, {{"AllowBar", SecAccess_Read}});
    }

    void testVariable_BraceDollar()
    {
        GENERATOR;
        generator.setVariable("foo", "Bar");
        generator.insertScope(primaryScope, "Allow{$foo}:Read");
        GENERATE("variable-{$}");

        evaluateAccessMap("variable-{$}", *accessMap, {{"AllowBar", SecAccess_Read}});
    }

    void testVariable_Dollar()
    {
        GENERATOR;
        generator.setVariable("foo", "Bar");
        generator.insertScope(primaryScope, "Allow$foo:Read");
        GENERATE("variable-${}");

        evaluateAccessMap("variable-${}", *accessMap, {{"Allow$foo", SecAccess_Read}});
    }

    void testVariable_Brace()
    {
        GENERATOR;
        generator.setVariable("foo", "Bar");
        generator.insertScope(primaryScope, "Allow{foo}:Read");
        GENERATE("variable-{$}");

        evaluateAccessMap("variable-{$}", *accessMap, {{"Allow{foo}", SecAccess_Read}});
    }

    EsdlAccessMapGeneratorTests()
        : scopeMapper({ primaryScope })
        , esdlReporter(new MockReporter())
    {
    }

    void evaluateAccessMap(const char* test, AccessMap& actual, const std::map<std::string, SecAccessFlags>& expected)
    {
        bool pass = true;
        if (actual.ordinality() != expected.size())
        {
            fprintf(stdout, "\nTest(%s) expected %zu map entries but got %u.\n", test, expected.size(), actual.ordinality());
            pass = false;
        }
        std::set<std::string> matchedKeys;
        HashIterator it(actual);
        ForEach(it)
        {
            IMapping& cur = it.query();
            const char* key = (const char*)cur.getKey();
            SecAccessFlags actualFlags = (actual.getValue(key) ? *actual.getValue(key) : SecAccess_Unavailable);
            if (expected.count(key) == 0)
            {
                fprintf(stdout, "\nTest(%s) unexpected %s:%s\n", test, key, getSecAccessFlagName(actualFlags));
                pass = false;
            }
            else
            {
                SecAccessFlags expectedFlags = expected.find(key)->second;
                if (actualFlags != expectedFlags)
                {
                    fprintf(stdout, "\nTest(%s) feature %s expected flags %s but got %s.\n", test, key, getSecAccessFlagName(expectedFlags), getSecAccessFlagName(actualFlags));
                    pass = false;
                }
                matchedKeys.insert(key);
            }
        }
        if (matchedKeys.size() != expected.size())
        {
            for (auto& e : expected)
            {
                if (matchedKeys.find(e.first) == matchedKeys.end())
                {
                    fprintf(stdout, "\nTest(%s) expected  %s:%s", test, e.first.c_str(), getSecAccessFlagName(e.second));
                    pass = false;
                }
            }
        }
        CPPUNIT_ASSERT(pass);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( EsdlAccessMapGeneratorTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( EsdlAccessMapGeneratorTests, "esdlaccessmapgeneratortests" );

#endif // _USE_CPPUNIT
