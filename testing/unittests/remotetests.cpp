/*##############################################################################

    Copyright (C) 2012 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#ifdef _USE_CPPUNIT
#include "jlib.hpp"
#include "jlog.hpp"

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>
#include <cppunit/extensions/HelperMacros.h>
// CPPUNIT_ASSERT is too slow, even when not matching failure
#define ASSERT(a) { if (!(a)) CPPUNIT_ASSERT(a); }

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
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( URITests, "URITests" );

#endif // _USE_URIPARSER

#endif // _USE_CPPUNIT
