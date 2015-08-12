/*##############################################################################

    Copyright (C) 2012 HPCC Systems®.

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

#ifdef _USE_URIPARSER
// =============================================================== URI parser
#include "uri.hpp"
class URITests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( URITests );
        CPPUNIT_TEST(testURIError);
        CPPUNIT_TEST(testURIUnknwon);
        CPPUNIT_TEST(testURILocal);
        CPPUNIT_TEST(testURIDali);
    CPPUNIT_TEST_SUITE_END();
    const IContextLogger &logctx;

    void test_uri(const char * str, bool shouldBeURI, URISchemeType scheme=URIScheme_error, const char * server=NULL, const char * path=NULL)
    {
        bool isURI = URI::isURI(str);
        ASSERT(isURI == shouldBeURI);
        if (!isURI)
            return;

        // Now, validate URI
        try
        {
            URI res(str);
            ASSERT(res.getScheme() == scheme);
            // No need to validate the rest
            if (scheme == URIScheme_error)
                return;
            StringBuffer response;
            res.appendServerStr(response);
            ASSERT(strcmp(response.str(), server) == 0);
            response.clear();
            res.appendPathStr(response);
            ASSERT(strcmp(response.str(), path) == 0);
        }
        catch (IException *e)
        {
            StringBuffer buf;
            logctx.CTXLOG("Exception: %s", e->errorMessage(buf).str());
            e->Release();
            ASSERT(false); // Check exception log
        }
    }

public:
    URITests() : logctx(queryDummyContextLogger()) {}

    void testURIError() {
        test_uri("You, shall not, pass!", false);
        test_uri("http://almost there...", false);
    }

    void testURIUnknwon() {
        test_uri("ftp://www.hpccsystems.com/", true);
        test_uri("gopher://www.hpccsystems.com/", true);
        test_uri("https://www.hpccsystems.com:443/", true);
        test_uri("http://user:passwd@www.hpccsystems.com:8080/my/path?is=full#of-stuff", true);
    }

    void testURILocal() {
        test_uri("file:///opt/HPCCSystems/examples/IMDB/ActorsInMovies.ecl", true, URIScheme_file, "", "/opt/HPCCSystems/examples/IMDB/ActorsInMovies.ecl");
    }

    void testURIDali() {
        // Dali file types
        test_uri("hpcc://mydali/path/to/file", true, URIScheme_hpcc, "mydali", "path/to/file");
        test_uri("hpcc://mydali/path/to/superfile?super", true, URIScheme_hpcc, "mydali", "path/to/superfile?super");
        test_uri("hpcc://mydali/path/to/superfile?super#subname", true, URIScheme_hpcc, "mydali", "path/to/superfile?super#subname");
        test_uri("hpcc://mydali/path/to/streamfile?stream", true, URIScheme_hpcc, "mydali", "path/to/streamfile?stream");
        test_uri("hpcc://mydali/path/to/streamfile?stream#047", true, URIScheme_hpcc, "mydali", "path/to/streamfile?stream#47");

        // Variations in Dali location
        test_uri("hpcc://mydali:7070/path/to/file", true, URIScheme_hpcc, "mydali:7070", "path/to/file");
        test_uri("hpcc://user@mydali:7070/path/to/file", true, URIScheme_hpcc, "user@mydali:7070", "path/to/file");
        test_uri("hpcc://user@mydali/path/to/file", true, URIScheme_hpcc, "user@mydali", "path/to/file");
        test_uri("hpcc://user:passwd@mydali:7070/path/to/file", true, URIScheme_hpcc, "user:passwd@mydali:7070", "path/to/file");
        test_uri("hpcc://user:passwd@mydali/path/to/file", true, URIScheme_hpcc, "user:passwd@mydali", "path/to/file");
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( URITests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( URITests, "URI" );

#endif // _USE_URIPARSER

#endif // _USE_CPPUNIT
