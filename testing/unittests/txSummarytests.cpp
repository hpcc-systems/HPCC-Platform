/*##############################################################################

    Copyright (C) 2020 HPCC Systems®.

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

//#ifdef _USE_CPPUNIT
#include "unittests.hpp"
#include "txsummary.hpp"
#include "espcontext.hpp"




class TxSummaryTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(TxSummaryTests);
        CPPUNIT_TEST(testPath);
        CPPUNIT_TEST(testShortPath);
        CPPUNIT_TEST(testIllegalKeys);
        CPPUNIT_TEST(testTypes);
        CPPUNIT_TEST(testSet);
        CPPUNIT_TEST(testSizeClear);
        CPPUNIT_TEST(testProfile);
        CPPUNIT_TEST(testFilter);

    CPPUNIT_TEST_SUITE_END();

public:
    TxSummaryTests(){}

    void validateResults(StringBuffer& testResult, const char* expectedResult, const char* testName, const char* step=nullptr, bool dbgout=false)
    {
        // remove newline formatting if any
        testResult.stripChar('\n');
        testResult.stripChar(' ');
        if( !strieq(testResult.str(), expectedResult))
        {
            printf("Mismatch (%s %s): Test Result vs Expected Result\n", testName, step ? step : "");
            printf("test:\n%s\nexpected:\n%s\n", testResult.str(), expectedResult);
            fflush(stdout);
            throw MakeStringException(100, "Failed Test (%s %s)", testName, step ? step : "");
        } else if( dbgout ){
            printf("test:\n%s\nexpected:\n%s\n", testResult.str(), expectedResult);
        }
    }

    void testPath()
    {
        constexpr const char* resultJSON = R"!!({"app":{"global_id":"global-val","local_id":"local-val"},"root1":"root1-val","one":{"two":{"three":"123","four":"124"},"dos":{"three":"123"}}})!!";
        constexpr const char* resultText = R"!!(app.global_id=global-val;app.local_id=local-val;root1=root1-val;one.two.three=123;one.two.four=124;one.dos.three=123;)!!";
        const char* testName="testJsonPath";

        Owned<CTxSummary> tx = new CTxSummary();
        tx->append("app.global_id", "global-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("app.local_id", "local-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("root1", "root1-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("one.two.three", "123", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("one.two.four", "124", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("one.dos.three", "123", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        StringBuffer output;
        tx->serialize(output, LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultJSON, testName, "json");

        tx->serialize(output.clear(), LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultText, testName, "text");
    }

    void testShortPath()
    {
        constexpr const char* resultJSON = R"!!({"a":"a-val","b":{"c":"bc-val","d":"bd-val"},"e":"e-val"})!!";
        constexpr const char* resultText = R"!!(a=a-val;b.c=bc-val;b.d=bd-val;e=e-val;)!!";
        const char* testName="testJsonShortPath";

        Owned<CTxSummary> tx = new CTxSummary();
        tx->append("a", "a-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("b.c", "bc-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("b.d", "bd-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("e", "e-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        StringBuffer output;
        tx->serialize(output, LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultJSON, testName, "json");

        tx->serialize(output.clear(), LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultText, testName, "text");
    }

    void testTypes()
    {
        VStringBuffer resultJSON("{\"emptystr\":\"\",\"nullstr\":\"\",\"int\":%i,\"uint\":%u,\"uint64\":%" I64F "u,\"querytimer\":42,\"updatetimernew\":23,\"bool\":1}", INT_MAX, UINT_MAX, ULLONG_MAX );
        VStringBuffer resultText("emptystr;nullstr;int=%i;uint=%u;uint64=%" I64F "u;querytimer=42ms;updatetimernew=23ms;bool=1;", INT_MAX, UINT_MAX, ULLONG_MAX );
        const char* testName="testTypes";
        Owned<CTxSummary> tx = new CTxSummary();

        // String
        tx->append("emptystr", "", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        // we can't add nullptr when using the template append function
        tx->append("nullstr", "", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Int
        int val1 = INT_MAX;
        tx->append("int", val1, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Unsigned Int
        unsigned int val2 = UINT_MAX;
        tx->append("uint", val2, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Unsigned Int64
        unsigned __int64 val3 = ULLONG_MAX;
        tx->append("uint64", val3, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Cumulative Timer
        CumulativeTimer* t = tx->queryTimer("querytimer", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        t->add(23);
        tx->updateTimer("querytimer", 19, LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->updateTimer("updatetimernew", 23, LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->updateTimer("filteredoutbygroup", 5, LogMin, TXSUMMARY_GRP_CORE);

        // Boolean
        tx->append("bool", true, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        StringBuffer output;
        tx->serialize(output, LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultJSON.str(), testName, "json");

        tx->serialize(output.clear(), LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultText.str(), testName, "text");
    }

    void testIllegalKeys()
    {
        const char* testName="testIllegalKeys";
        bool result = true;
        Owned<CTxSummary> tx = new CTxSummary();

        result = tx->append(nullptr, "foobar", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed null key", testName);

        result = tx->append("", "foobar", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed empty key", testName);

        result = tx->append("app", "foobar", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        result = tx->append("app", "secondval", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed duplicate key: (app)", testName);

        // Fail due to 'app' present as a value
        // Can't append a duplicate key whose first path part 'app' that would resolve
        // to an object named 'app' holding a value named 'local'
        result = tx->append("app.local", "foobar", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed duplicate key: (app.local)", testName);

        result = tx->append("apple", "pie", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        result = tx->append("apple", "crisp", LogMin, TXSUMMARY_GRP_ENTERPRISE); // partial match on 'app', fail due to 'apple'
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed duplicate key: (apple)", testName);

        result = tx->append("pineapple.cake", "upside", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        result = tx->append("pineapple.cake", "down", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed duplicate hierarchical key: (pineapple.cake)", testName);

        result = tx->append(".rap", "foobar", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed malformed key with empty leading path part: (.rap)", testName);

        result = tx->append("cap.", "foobar", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed malformed key with empty trailing path part: (cap.)", testName);

        result = tx->append("bread..baker", "foobar", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        if(result)
            throw MakeStringException(100, "Failed Test (%s) - allowed malformed key with empty path part: (bread..baker)", testName);

        CumulativeTimer* timer = nullptr;
        try
        {
            // expected exception thrown because "app" is not a timer
            timer = tx->queryTimer("app", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        }
        catch(IException* e)
        {
            e->Release();
        }

        if(timer)
            throw MakeStringException(100, "Failed Test (%s) - string entry mistaken for CumulativeTimer", testName);
    }

    void testSet()
    {
        constexpr const char* result1 = R"!!({"one":99,"two":2,"three":3})!!";
        constexpr const char* result2 = R"!!({"one":1,"two":"2","three":"3"})!!";
        const char* testName="testSet";

        Owned<CTxSummary> tx = new CTxSummary();
        tx->append("one", 99u, LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("two", 2u, LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->updateTimer("three", 3, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        StringBuffer output;
        tx->serialize(output, LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, result1, testName, "before");

        tx->set("one", 1U, LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->set("two", "2", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        // Likely future update will disallow a set to change an entry
        // between timer and scalar types
        tx->set("three", "3", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        tx->serialize(output.clear(), LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, result2, testName, "after");
    }

    void testSizeClear()
    {
        constexpr const char* result1 = R"!!(size=6)!!";
        constexpr const char* result2 = R"!!(size=0)!!";
        const char* testName="testSizeClear";

        Owned<CTxSummary> tx = new CTxSummary();
        tx->append("a", "a-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("b.c", "bc-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("b.d", "bd-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("e", "e-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("x.y.z", 77, LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("x.y.a", 88U, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        VStringBuffer output("size=%" I64F "u", tx->size());
        validateResults(output, result1, testName, "first sizing");

        tx->clear();
        output.clear().appendf("size=%" I64F "u", tx->size());
        validateResults(output, result2, testName, "after clear");

        tx->append("a", "a-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("b.c", "bc-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("b.d", "bd-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("e", "e-val", LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("x.y.z", 77, LogMin, TXSUMMARY_GRP_ENTERPRISE);
        tx->append("x.y.a", 88U, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        output.clear().appendf("size=%" I64F "u", tx->size());
        validateResults(output, result1, testName, "second sizing");
    }

    void testFilter()
    {
        constexpr const char* resultOps1JsonMax = R"!!({"dev-str":"q","dev-int":1,"dev-uint":2,"dev-uint64":3,"dev-timer":19,"dev-bool":0,"max-only":1})!!";
        constexpr const char* resultOps1JsonMin = R"!!({"dev-str":"q","dev-int":1,"dev-uint":2,"dev-uint64":3,"dev-timer":19,"dev-bool":0})!!";
        constexpr const char* resultOps2JsonMax = R"!!({"ops-str":"q","ops-int":1,"ops-uint":2,"ops-uint64":3,"ops-timer":23,"ops-bool":0,"max-only":1})!!";
        constexpr const char* resultOps2JsonMin = R"!!({"ops-str":"q","ops-int":1,"ops-uint":2,"ops-uint64":3,"ops-timer":23,"ops-bool":0})!!";
        constexpr const char* resultOps1TextMax = R"!!(dev-str=q;dev-int=1;dev-uint=2;dev-uint64=3;dev-timer=19ms;dev-bool=0;max-only=1;)!!";
        constexpr const char* resultOps1TextMin = R"!!(dev-str=q;dev-int=1;dev-uint=2;dev-uint64=3;dev-timer=19ms;dev-bool=0;)!!";
        constexpr const char* resultOps2TextMax = R"!!(ops-str=q;ops-int=1;ops-uint=2;ops-uint64=3;ops-timer=23ms;ops-bool=0;max-only=1;)!!";
        constexpr const char* resultOps2TextMin = R"!!(ops-str=q;ops-int=1;ops-uint=2;ops-uint64=3;ops-timer=23ms;ops-bool=0;)!!";
        const char* testName="testFilter";

        Owned<CTxSummary> tx = new CTxSummary();
        // String
        tx->append("dev-str", "q", LogMin, TXSUMMARY_GRP_CORE);
        tx->append("ops-str", "q", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Int
        int val1 = 1;
        tx->append("dev-int", val1, LogMin, TXSUMMARY_GRP_CORE);
        tx->append("ops-int", val1, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Unsigned Int
        unsigned int val2 = 2;
        tx->append("dev-uint", val2, LogMin, TXSUMMARY_GRP_CORE);
        tx->append("ops-uint", val2, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Unsigned Int64
        unsigned __int64 val3 = 3;
        tx->append("dev-uint64", val3, LogMin, TXSUMMARY_GRP_CORE);
        tx->append("ops-uint64", val3, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Cumulative Timer
        // create with a call to query, then update
        CumulativeTimer* t = tx->queryTimer("dev-timer", LogMin, TXSUMMARY_GRP_CORE);
        tx->updateTimer("dev-timer", 19, LogMin, TXSUMMARY_GRP_CORE);
        // create with a call to update
        tx->updateTimer("ops-timer", 23, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // Boolean
        tx->append("dev-bool", false, LogMin, TXSUMMARY_GRP_CORE);
        tx->append("ops-bool", false, LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // LogMax, all serialization styles
        tx->append("max-only", true, LogMax, TXSUMMARY_OUT_TEXT | TXSUMMARY_OUT_JSON);

        StringBuffer output;
        tx->serialize(output, LogMax, TXSUMMARY_GRP_CORE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultOps1JsonMax, testName, "ops1 + json + max");

        output.clear();
        tx->serialize(output, LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultOps2JsonMax, testName, "ops2 + json + max");

        output.clear();
        tx->serialize(output, LogMin, TXSUMMARY_GRP_CORE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultOps1JsonMin, testName, "ops1 + json + min");

        output.clear();
        tx->serialize(output, LogMin, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultOps2JsonMin, testName, "ops2 + json + min");

        output.clear();
        tx->serialize(output, LogMax, TXSUMMARY_GRP_CORE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultOps1TextMax, testName, "ops1 + text + max");

        output.clear();
        tx->serialize(output, LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultOps2TextMax, testName, "ops2 + text + max");

        output.clear();
        tx->serialize(output, LogMin, TXSUMMARY_GRP_CORE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultOps1TextMin, testName, "ops1 + text + min");

        output.clear();
        tx->serialize(output, LogMin, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultOps2TextMin, testName, "ops2 + text + min");

    }

    class CTxSummaryProfileTest : public CTxSummaryProfileBase
    {
        public:
            virtual bool tailorSummary(IEspContext* ctx) override {return true;}
    };

    void testProfile()
    {
        constexpr const char* resultJson = R"!!({"user":"testuser","name4json":"tval","three":{"four":"three-four","name4all":"v4a"}})!!";
        constexpr const char* resultText = R"!!(user=testuser;name4text=tval;three.four=three-four;three.name4all=v4a;)!!";
        const char* testName="testProfile";

        Owned<CTxSummary> tx = new CTxSummary();
        Linked<ITxSummaryProfile> profile = new CTxSummaryProfileTest();
        tx->setProfile(profile);

        profile->addMap("ops2-json", {TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT, "name4text", true});
        profile->addMap("ops2-json", {TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON, "name4json", true});
        profile->addMap("three.ops2-all", {TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT | TXSUMMARY_OUT_JSON, "three.name4all", true});
        profile->addMap("ops1", {TXSUMMARY_GRP_CORE, TXSUMMARY_OUT_TEXT | TXSUMMARY_OUT_JSON, "ops1-noshow", true});

        tx->append("user", "testuser", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // test matching the second profile entry with the same name, but different output style
        tx->append("ops2-json", "tval", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // add hierarchy
        tx->append("three.four", "three-four", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // test matching entry for all output styles
        // test matching a hierarchical entry
        tx->append("three.ops2-all", "v4a", LogMin, TXSUMMARY_GRP_ENTERPRISE);

        // test matching entry for a group that isn't serialized
        // also tests correct JSON delimiting when last child of
        // object is filtered out from serialization
        tx->append("ops1", "ops1val", LogMin, TXSUMMARY_GRP_CORE);

        StringBuffer output;

        tx->serialize(output.clear(), LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_JSON);
        validateResults(output, resultJson, testName, "map flat name JSON out");

        tx->serialize(output.clear(), LogMax, TXSUMMARY_GRP_ENTERPRISE, TXSUMMARY_OUT_TEXT);
        validateResults(output, resultText, testName, "map flat name TEXT out");
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( TxSummaryTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( TxSummaryTests, "txsummary" );

//#endif // _USE_CPPUNIT
