/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "workunit.hpp"
#include "jlzw.hpp"
#include "jptree.hpp"

#include <cstring>

static constexpr const char * testQueryArchive = R"xml(<Archive><Query>OUTPUT('archive text');</Query></Archive>)xml";

static void getWuQueryText(const IConstWUQuery &query, StringAttr &text)
{
    StringAttrAdaptor adaptor(text);
    query.getQueryText(adaptor);
}

static void getWuQueryShortText(const IConstWUQuery &query, StringAttr &text)
{
    StringAttrAdaptor adaptor(text);
    query.getQueryShortText(adaptor);
}

class wuArchiveTests : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE( wuArchiveTests );
        CPPUNIT_TEST(testQueryArchiveStoredAsXmlText);
        CPPUNIT_TEST(testCompressedQueryArchiveReadsAsXmlText);
        CPPUNIT_TEST(testMissingQueryTextClearsOutput);
        CPPUNIT_TEST(testCorruptCompressedQueryArchiveClearsOutput);
    CPPUNIT_TEST_SUITE_END();

public:
    void testQueryArchiveStoredAsXmlText()
    {
        Owned<ILocalWorkUnit> wu = createLocalWorkUnit();
        Owned<IWUQuery> query = wu->updateQuery();

        query->setQueryText(testQueryArchive);

        IPropertyTree *queryTree = queryExtendedWU(wu)->queryPTree()->queryPropTree("Query");
        CPPUNIT_ASSERT(queryTree);
        CPPUNIT_ASSERT(!queryTree->isBinary("Text"));
        CPPUNIT_ASSERT_EQUAL_STR(testQueryArchive, queryTree->queryProp("Text"));

        StringAttr text;
        getWuQueryText(*query, text);
        CPPUNIT_ASSERT_EQUAL_STR(testQueryArchive, text.get());
    }

    void testCompressedQueryArchiveReadsAsXmlText()
    {
        Owned<ILocalWorkUnit> wu = createLocalWorkUnit();
        Owned<IWUQuery> query = wu->updateQuery();

        Owned<IPropertyTree> archive = createPTreeFromXMLString(testQueryArchive, ipt_caseInsensitive|ipt_lowmem);
        MemoryBuffer serialized;
        archive->serialize(serialized);

        IPropertyTree *queryTree = queryExtendedWU(wu)->queryPTree()->queryPropTree("Query");
        CPPUNIT_ASSERT(queryTree);
        queryTree->setPropBin("Text", serialized.length(), serialized.toByteArray(), COMPRESS_METHOD_LZ4HC);

        CPPUNIT_ASSERT(query->isArchive());

        StringAttr text;
        getWuQueryText(*query, text);
        CPPUNIT_ASSERT(strstr(text.get(), "<Archive") != nullptr);
        CPPUNIT_ASSERT(strstr(text.get(), "<Query>OUTPUT(&apos;archive text&apos;);</Query>") != nullptr);

        StringAttr shortText;
        getWuQueryShortText(*query, shortText);
        CPPUNIT_ASSERT_EQUAL_STR("OUTPUT('archive text');", shortText.get());
    }

    void testMissingQueryTextClearsOutput()
    {
        Owned<ILocalWorkUnit> wu = createLocalWorkUnit();
        Owned<IWUQuery> query = wu->updateQuery();

        StringAttr text("previous");
        getWuQueryText(*query, text);
        CPPUNIT_ASSERT(text.get() == nullptr);

        StringAttr shortText("previous");
        getWuQueryShortText(*query, shortText);
        CPPUNIT_ASSERT(shortText.get() == nullptr);
    }

    void testCorruptCompressedQueryArchiveClearsOutput()
    {
        Owned<ILocalWorkUnit> wu = createLocalWorkUnit();
        Owned<IWUQuery> query = wu->updateQuery();

        const char *corruptArchive = "not a serialized property tree";
        IPropertyTree *queryTree = queryExtendedWU(wu)->queryPTree()->queryPropTree("Query");
        CPPUNIT_ASSERT(queryTree);
        queryTree->setPropBin("Text", strlen(corruptArchive), corruptArchive, COMPRESS_METHOD_LZ4HC);

        CPPUNIT_ASSERT(!query->isArchive());

        StringAttr text("previous");
        getWuQueryText(*query, text);
        CPPUNIT_ASSERT(text.get() == nullptr);

        StringAttr shortText("previous");
        getWuQueryShortText(*query, shortText);
        CPPUNIT_ASSERT(shortText.get() == nullptr);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( wuArchiveTests );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( wuArchiveTests, "wu" );

#endif // _USE_CPPUNIT