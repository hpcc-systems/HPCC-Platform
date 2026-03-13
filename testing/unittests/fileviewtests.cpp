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

/*
 * fileview2 unit tests
 *
 * Tests for RowBlock, RowCache, and CardinalityElement/CardinalityMapping
 * components that can be exercised without full HPCC infrastructure.
 */

#ifdef _USE_CPPUNIT
#include "jliball.hpp"
#include "unittests.hpp"

#include "fvsource.ipp"
#include "fvtransform.ipp"

//---------------------------------------------------------------------------
// FixedRowBlock tests
//---------------------------------------------------------------------------

class FixedRowBlockTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(FixedRowBlockTest);
        CPPUNIT_TEST(testGetRow);
        CPPUNIT_TEST(testGetRowOutOfRange);
        CPPUNIT_TEST(testFetchRow);
        CPPUNIT_TEST(testFetchRowOutOfRange);
        CPPUNIT_TEST(testGetNextStoredOffset);
    CPPUNIT_TEST_SUITE_END();

public:
    void testGetRow()
    {
        // Create a buffer with 4 fixed-size rows of 8 bytes each
        MemoryBuffer mb;
        for (unsigned i = 0; i < 4; i++)
        {
            unsigned __int64 val = (i + 1) * 100;
            mb.append(sizeof(val), &val);
        }
        FixedRowBlock block(mb, 10, 0, 8); // start at row 10

        size32_t len;
        unsigned __int64 rowOffset;

        // Fetch row 10 (first row)
        const void * row = block.getRow(10, len, rowOffset);
        CPPUNIT_ASSERT(row != nullptr);
        CPPUNIT_ASSERT_EQUAL((size32_t)8, len);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)0, rowOffset);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)100, *(const unsigned __int64 *)row);

        // Fetch row 12 (third row)
        row = block.getRow(12, len, rowOffset);
        CPPUNIT_ASSERT(row != nullptr);
        CPPUNIT_ASSERT_EQUAL((size32_t)8, len);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)16, rowOffset);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)300, *(const unsigned __int64 *)row);

        // Fetch row 13 (last row)
        row = block.getRow(13, len, rowOffset);
        CPPUNIT_ASSERT(row != nullptr);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)400, *(const unsigned __int64 *)row);
    }

    void testGetRowOutOfRange()
    {
        MemoryBuffer mb;
        unsigned __int64 val = 42;
        mb.append(sizeof(val), &val);
        FixedRowBlock block(mb, 5, 0, 8);

        size32_t len;
        unsigned __int64 rowOffset;

        // Row before start
        CPPUNIT_ASSERT(block.getRow(4, len, rowOffset) == nullptr);
        // Row after end
        CPPUNIT_ASSERT(block.getRow(6, len, rowOffset) == nullptr);
    }

    void testFetchRow()
    {
        MemoryBuffer mb;
        for (unsigned i = 0; i < 3; i++)
        {
            unsigned __int64 val = (i + 1) * 10;
            mb.append(sizeof(val), &val);
        }
        FixedRowBlock block(mb, 0, 100, 8); // startOffset=100

        size32_t len;
        // Fetch at offset 100 (first row)
        const void * row = block.fetchRow(100, len);
        CPPUNIT_ASSERT(row != nullptr);
        CPPUNIT_ASSERT_EQUAL((size32_t)8, len);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)10, *(const unsigned __int64 *)row);

        // Fetch at offset 108 (second row)
        row = block.fetchRow(108, len);
        CPPUNIT_ASSERT(row != nullptr);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)20, *(const unsigned __int64 *)row);
    }

    void testFetchRowOutOfRange()
    {
        MemoryBuffer mb;
        unsigned __int64 val = 1;
        mb.append(sizeof(val), &val);
        FixedRowBlock block(mb, 0, 100, 8);

        size32_t len;
        // Before startOffset
        CPPUNIT_ASSERT(block.fetchRow(99, len) == nullptr);
        // Past end
        CPPUNIT_ASSERT(block.fetchRow(108, len) == nullptr);
    }

    void testGetNextStoredOffset()
    {
        MemoryBuffer mb;
        for (unsigned i = 0; i < 3; i++)
        {
            unsigned __int64 val = i;
            mb.append(sizeof(val), &val);
        }
        FixedRowBlock block(mb, 5, 200, 8);

        __int64 nextRow;
        offset_t nextOffset;
        block.getNextStoredOffset(nextRow, nextOffset);
        // 3 rows starting at row 5 => next = 8
        CPPUNIT_ASSERT_EQUAL((__int64)8, nextRow);
        // startOffset(200) + buffer.length(24)
        CPPUNIT_ASSERT_EQUAL((offset_t)224, nextOffset);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(FixedRowBlockTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(FixedRowBlockTest, "FixedRowBlockTest");

//---------------------------------------------------------------------------
// RowCache tests
//---------------------------------------------------------------------------

class RowCacheTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(RowCacheTest);
        CPPUNIT_TEST(testAddAndGet);
        CPPUNIT_TEST(testGetMiss);
        CPPUNIT_TEST(testEviction);
        CPPUNIT_TEST(testMultipleBlocks);
    CPPUNIT_TEST_SUITE_END();

    FixedRowBlock * createBlock(__int64 startRow, unsigned numRows, size32_t recSize)
    {
        MemoryBuffer mb;
        for (unsigned i = 0; i < numRows; i++)
        {
            unsigned __int64 val = startRow + i;
            mb.append(sizeof(val), &val);
        }
        return new FixedRowBlock(mb, startRow, startRow * recSize, recSize);
    }

public:
    void testAddAndGet()
    {
        RowCache cache;
        cache.addRowsOwn(createBlock(0, 10, 8));

        RowLocation loc;
        bool found = cache.getCacheRow(5, loc);
        CPPUNIT_ASSERT(found);
        CPPUNIT_ASSERT(loc.matchRow != nullptr);
        CPPUNIT_ASSERT_EQUAL((size32_t)8, loc.matchLength);
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)5, *(const unsigned __int64 *)loc.matchRow);
    }

    void testGetMiss()
    {
        RowCache cache;
        cache.addRowsOwn(createBlock(0, 10, 8));

        RowLocation loc;
        bool found = cache.getCacheRow(15, loc);
        CPPUNIT_ASSERT(!found);
    }

    void testEviction()
    {
        RowCache cache;
        // Add 21 blocks (exceeds MaxBlocksCached=20), should trigger eviction
        for (unsigned i = 0; i < 21; i++)
            cache.addRowsOwn(createBlock(i * 10, 10, 8));

        // Should still be able to find the most recent block
        RowLocation loc;
        bool found = cache.getCacheRow(205, loc);
        CPPUNIT_ASSERT(found);
        CPPUNIT_ASSERT(loc.matchRow != nullptr);
    }

    void testMultipleBlocks()
    {
        RowCache cache;
        cache.addRowsOwn(createBlock(0, 5, 8));
        cache.addRowsOwn(createBlock(5, 5, 8));
        cache.addRowsOwn(createBlock(10, 5, 8));

        RowLocation loc;
        // Row from first block
        CPPUNIT_ASSERT(cache.getCacheRow(2, loc));
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)2, *(const unsigned __int64 *)loc.matchRow);

        // Row from second block
        CPPUNIT_ASSERT(cache.getCacheRow(7, loc));
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)7, *(const unsigned __int64 *)loc.matchRow);

        // Row from third block
        CPPUNIT_ASSERT(cache.getCacheRow(12, loc));
        CPPUNIT_ASSERT_EQUAL((unsigned __int64)12, *(const unsigned __int64 *)loc.matchRow);
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(RowCacheTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(RowCacheTest, "RowCacheTest");

//---------------------------------------------------------------------------
// CardinalityElement / CardinalityMapping tests
//---------------------------------------------------------------------------

class CardinalityTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(CardinalityTest);
        CPPUNIT_TEST(testElementSingleValue);
        CPPUNIT_TEST(testElementRange);
        CPPUNIT_TEST(testElementUnbounded);
        CPPUNIT_TEST(testElementEmpty);
        CPPUNIT_TEST(testMappingColon);
        CPPUNIT_TEST(testMappingNoColon);
        CPPUNIT_TEST(testInvertedCardinality);
    CPPUNIT_TEST_SUITE_END();

public:
    void testElementSingleValue()
    {
        CardinalityElement elem;
        elem.init(1, "5");
        CPPUNIT_ASSERT_EQUAL((unsigned)5, elem.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)5, elem.max);
    }

    void testElementRange()
    {
        CardinalityElement elem;
        elem.init(4, "2..8");
        CPPUNIT_ASSERT_EQUAL((unsigned)2, elem.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)8, elem.max);
    }

    void testElementUnbounded()
    {
        CardinalityElement elem;
        elem.init(4, "1..M");
        CPPUNIT_ASSERT_EQUAL((unsigned)1, elem.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)(unsigned)CardinalityElement::Unbounded, elem.max);

        CardinalityElement elem2;
        elem2.init(1, "M");
        CPPUNIT_ASSERT_EQUAL((unsigned)0, elem2.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)(unsigned)CardinalityElement::Unbounded, elem2.max);
    }

    void testElementEmpty()
    {
        CardinalityElement elem;
        elem.init(0, "");
        CPPUNIT_ASSERT_EQUAL((unsigned)1, elem.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)1, elem.max);
    }

    void testMappingColon()
    {
        CardinalityMapping mapping("1:M");
        CPPUNIT_ASSERT_EQUAL((unsigned)1, mapping.primary.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)1, mapping.primary.max);
        CPPUNIT_ASSERT_EQUAL((unsigned)0, mapping.secondary.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)(unsigned)CardinalityElement::Unbounded, mapping.secondary.max);
    }

    void testMappingNoColon()
    {
        CardinalityMapping mapping("5");
        CPPUNIT_ASSERT_EQUAL((unsigned)1, mapping.primary.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)1, mapping.primary.max);
        CPPUNIT_ASSERT_EQUAL((unsigned)5, mapping.secondary.min);
        CPPUNIT_ASSERT_EQUAL((unsigned)5, mapping.secondary.max);
    }

    void testInvertedCardinality()
    {
        StringBuffer out;
        getInvertedCardinality(out, "1:M");
        CPPUNIT_ASSERT_EQUAL_STR("M:1", out.str());

        out.clear();
        getInvertedCardinality(out, "1..2:3..M");
        CPPUNIT_ASSERT_EQUAL_STR("3..M:1..2", out.str());
    }
};
CPPUNIT_TEST_SUITE_REGISTRATION(CardinalityTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(CardinalityTest, "CardinalityTest");

#endif // _USE_CPPUNIT
