/*##############################################################################


    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR getValue OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#include <initializer_list>
#include "jlib.hpp"
#include "jdebug.hpp"
#include "jsort.hpp"
#include "jexcept.hpp"
#include "rtlnewkey.hpp"
#include "eclrtl_imp.hpp"
#include "rtlrecord.hpp"
#include "rtlkey.hpp"
#include "rtlnewkey.hpp"
#include "rtlfield.hpp"
#include "rtldynfield.hpp"

#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

class InMemoryRows
{
public:
    InMemoryRows(size_t _countRows, const byte * * _rows, const RtlRecord & _record)
     : countRows(_countRows), rows(_rows), record(_record)
    {
    }

    const byte * queryRow(unsigned i) const { return rows[i]; }
    size_t numRows() const { return countRows; }
    const RtlRecord & queryRecord() const { return record; }

protected:
    size_t countRows;
    const byte * * rows;
    const RtlRecord & record;
};

//Always has an even number of transitions
//Null transitions can be used at the start and end of the ranges to map anything
//equality conditions repeat the transition
class InMemoryRowCursor : public ISourceRowCursor
{
public:
    InMemoryRowCursor(InMemoryRows & _source) : source(_source), seekRow(_source.queryRecord())
    {
    }

    virtual const byte * findNext(const RowCursor & search) override
    {
        size_t numRows = source.numRows();
        if (numRows == 0)
            return nullptr;

        size_t high = numRows;
        size_t low = 0; // Could be cur

        bool scanOnNext = false;
        if (cur != 0 && scanOnNext)
        {
            //MORE: The next match is likely to be close, so first of all look for a match in the next few rows
            //An always searching forwards, so can guarantee that it follows cur > low
        }
        //Find the value of low,high where all rows 0..low-1 are < search and rows low..max are >= search
        while (low<high)
        {
            size_t mid = low + (high - low) / 2;
            seekRow.setRow(source.queryRow(mid), search.numFilterFields());
            int rc = search.compareNext(seekRow);  // compare seekRow with the row we are hoping to find
            if (rc < 0)
                low = mid + 1;  // if this row is lower than the seek row, exclude mid from the potential positions
            else
                high = mid; // otherwise exclude all above mid from the potential positions.
        }
        cur = low;
        if (low == numRows)
            return nullptr;
        return source.queryRow(cur);
    }

    virtual const byte * next() override
    {
        cur++;
        if (cur == source.numRows())
            return nullptr;
        return source.queryRow(cur);
    }

    virtual void reset() override
    {
        cur = 0;
        seekRow.setRow(nullptr);
    }

protected:
    size_t cur = 0;
    InMemoryRows & source;
    RtlDynRow seekRow;
};

/*
class IStdException : extends std::exception
{
Owned<IException> jException;
public:
IStdException(IException *E) : jException(E) {};
};
*/

//Scan a set of rows to find the matches - used to check that the keyed operations are correct
class RowScanner
{
public:
    RowScanner(const RtlRecord & _info, RowFilter & _filter, const PointerArray & _rows) : rows(_rows), curRow(_info, nullptr), filter(_filter)
    {
    }

    bool first()
    {
        return resolveNext(0);
    }

    bool next()
    {
        return resolveNext(curIndex+1);
    }

    bool resolveNext(unsigned next)
    {
        while (next < rows.ordinality())
        {
            curRow.setRow(rows.item(next));
            if (filter.matches(curRow))
            {
                curIndex = next;
                return true;
            }
            next++;
        }
        curIndex = next;
        return false;
    }

    const RtlRow & queryRow() const { return curRow; }

protected:
    const PointerArray & rows;
    RtlDynRow curRow;
    RowFilter & filter;
    unsigned curIndex = 0;
};



static void addRange(IValueSet * set, const char * lower, const char * upper, size32_t subLength = MatchFullString)
{
    Owned<IValueTransition> lowerBound = lower ? set->createUtf8Transition(CMPge, rtlUtf8Length(strlen(lower), lower), lower, subLength) : nullptr;
    Owned<IValueTransition> upperBound = upper ? set->createUtf8Transition(CMPle, rtlUtf8Length(strlen(upper), upper), upper, subLength) : nullptr;
    set->addRange(lowerBound, upperBound);
};

class RawRowCompare : public ICompare
{
public:
    RawRowCompare(const RtlRecord & _record) : record(_record) {}

    virtual int docompare(const void * left,const void * right) const
    {
        return record.compare((const byte *)left, (const byte *)right);
    }

    const RtlRecord & record;
};

class ValueSetTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ValueSetTest);
        CPPUNIT_TEST(testBounds);
        CPPUNIT_TEST(testSubStrings);
        CPPUNIT_TEST(testKeyed2);
        CPPUNIT_TEST(testRange);
        CPPUNIT_TEST(testSerialize);
        CPPUNIT_TEST(testUnion);
        CPPUNIT_TEST(testExclude);
        CPPUNIT_TEST(testInverse);
        CPPUNIT_TEST(testIntersect);
        CPPUNIT_TEST(testStr2);
        CPPUNIT_TEST(testFilter);
        CPPUNIT_TEST(testKeyed1);
    CPPUNIT_TEST_SUITE_END();

protected:
    void checkSet(const IValueSet * set, const char * expected)
    {
        StringBuffer temp;
        set->serialize(temp);
        if (!streq(expected, temp))
            CPPUNIT_ASSERT_EQUAL(expected, temp.str());
    }

    void testRange(const RtlTypeInfo & type, const char * low, const char * high, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        addRange(set, low, high);
        checkSet(set, expected);
    }

    void testRangeStr1(const char * low, const char * high, const char * expected)
    {
        RtlStringTypeInfo str1(type_string, 1);
        testRange(str1, low, high, expected);
    }

    void testRangeInt2(const char * low, const char * high, const char * expected)
    {
        RtlIntTypeInfo int2(type_int, 2);
        testRange(int2, low, high, expected);
    }

    void testRangeStrX(const char * low, const char * high, const char * expected)
    {
        RtlStringTypeInfo type(type_string|RFTMunknownsize, 0);
        testRange(type, low, high, expected);
    }

    void testRangeUniX(const char * low, const char * high, const char * expected)
    {
        RtlUnicodeTypeInfo type(type_string|RFTMunknownsize, 0, "");
        testRange(type, low, high, expected);
    }

    void testRange()
    {
        testRangeStr1("A","Z","['A','Z']");
        testRangeStr1("X","z","['X','z']");
        testRangeStr1("Z","X","");
        testRangeStr1(nullptr, "z","(,'z']");
        testRangeStr1("Z", nullptr,"['Z',)");
        testRangeStr1("A", "A","['A']");
        testRangeStr1(nullptr, nullptr,"(,)");
        testRangeStr1("é", "é","['é']"); // Test utf8 translation
        testRangeStr1("AB", "ZX","['A','Z']");

        testRangeInt2("0","1","[0,1]");
        testRangeInt2("-1","1","[-1,1]");
        testRangeInt2("-32768","32767","[-32768,32767]");
        testRangeInt2("32768","0","[-32768,0]");

        testRangeStrX("A", "Z","['A','Z']");
        testRangeStrX("AB", "ZX","['AB','ZX']");
        testRangeStrX("éèabc", "ab","");
        testRangeStrX("a'b", "éèabc", "['a\\'b','éèabc']");

        testRangeUniX("A", "Z","['A','Z']");
        testRangeUniX("AB", "ZX","['AB','ZX']");
        testRangeUniX("éèabc", "ab","");
        testRangeUniX("a'b", "éèabc", "['a\\'b','éèabc']");
    }

    void testSerialize(const RtlTypeInfo & type, const char * filter, const char * expected = nullptr)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        checkSet(set, expected ? expected : filter);

        MemoryBuffer mb;
        set->serialize(mb);
        Owned<IValueSet> newset = createValueSet(type, mb);
        checkSet(newset, expected ? expected : filter);
        CPPUNIT_ASSERT(set->equals(*newset));

        StringBuffer s;
        set->serialize(s);
        Owned<IValueSet> newset2 = createValueSet(type);
        deserializeSet(*newset2, s);
        CPPUNIT_ASSERT(set->equals(*newset2));
    }
    void testUnion(const RtlTypeInfo & type, const char * filter, const char * next, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        deserializeSet(*set, next);
        checkSet(set, expected);

        //test the arguments in the opposite order
        Owned<IValueSet> set2 = createValueSet(type);
        deserializeSet(*set2, next);
        deserializeSet(*set2, filter);
        checkSet(set, expected);

        //test the arguments in the opposite order
        Owned<IValueSet> set3a = createValueSet(type);
        Owned<IValueSet> set3b = createValueSet(type);
        deserializeSet(*set3a, next);
        deserializeSet(*set3b, filter);
        set3a->unionSet(set3b);
        checkSet(set3a, expected);
    }
    void testSerialize()
    {
        RtlStringTypeInfo str1(type_string, 1);
        RtlIntTypeInfo int2(type_int, 2);
        RtlIntTypeInfo int8(type_int, 8);
        RtlSwapIntTypeInfo swapint2(type_int, 2);
        RtlSwapIntTypeInfo swapint8(type_int, 8);
        RtlKeyedIntTypeInfo keyedint8(type_keyedint, 8, &swapint8);
        RtlStringTypeInfo strx(type_string|RFTMunknownsize, 0);
        RtlUtf8TypeInfo utf8(type_utf8|RFTMunknownsize, 0, nullptr);
        testSerialize(int2, "[123]");
        testSerialize(int2, "(123,234]");
        testSerialize(int2, "[123,234)");
        testSerialize(int2, "(123,234)");
        testSerialize(int2, "(,234)");
        testSerialize(int2, "(128,)");
        testSerialize(int2, "(,)");
        testSerialize(int2, "(123,234),(456,567)");
        testSerialize(int2, "(456,567),(123,234)", "(123,234),(456,567)");

        testSerialize(swapint2, "[123]");
        testSerialize(swapint2, "(123,234]");
        testSerialize(swapint2, "[123,234)");
        testSerialize(swapint2, "(123,234)");
        testSerialize(swapint2, "(,234)");
        testSerialize(swapint2, "(128,)");
        testSerialize(swapint2, "(,)");
        testSerialize(swapint2, "(123,234),(456,567)");
        testSerialize(swapint2, "(456,567),(123,234)", "(123,234),(456,567)");

        testSerialize(int8, "[9223372036854775807]");
        testSerialize(int8, "[-9223372036854775808]");
        testSerialize(swapint8, "[9223372036854775807]");
        testSerialize(swapint8, "[-9223372036854775808]");
        testSerialize(keyedint8, "[10]");
        testSerialize(keyedint8, "[-10]");

        testSerialize(str1, "['A']");
        testSerialize(str1, "[',']");
        testSerialize(str1, "['\\'']");
        testSerialize(str1, "['\\u0041']", "['A']");
        testSerialize(str1, "['\\n']");
        testSerialize(utf8, "['\\u611b']", "['愛']");
        testSerialize(strx, "['\\'\\'','}']");

        testSerialize(str1, "[A]", "['A']");
        testSerialize(int2, "(123]", "");
        testSerialize(int2, "[123)", "");
        testSerialize(int2, "[1,0]", "");
    }

    void testUnion()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testSerialize(int2, "[3,5],[5,7]", "[3,7]");
        testSerialize(int2, "[3,5],(5,7]", "[3,7]");
        testSerialize(int2, "[3,5),[5,7]", "[3,7]");
        testSerialize(int2, "[3,5),(5,7]");
        testSerialize(int2, "[4],[4]", "[4]");
        testSerialize(int2, "[4,5],[4]", "[4,5]");
        testSerialize(int2, "[4,5],[5]", "[4,5]");

        testUnion(int2, "[3,5]", "[,1]", "(,1],[3,5]");
        testUnion(int2, "[3,5]", "[,3]", "(,5]");
        testUnion(int2, "[3,5]", "[,4]", "(,5]");
        testUnion(int2, "[3,5]", "[,]", "(,)");
        testUnion(int2, "[3,5]", "[1,)", "[1,)");
        testUnion(int2, "[3,5]", "[3,)", "[3,)");
        testUnion(int2, "[3,5]", "[5,)", "[3,)");
        testUnion(int2, "[3,5]", "(5,)", "[3,)");
        testUnion(int2, "[3,5]", "[6,)", "[3,5],[6,)");
        testUnion(int2, "[3,5]", "(6,)", "[3,5],(6,)");

        testUnion(int2, "[4,7],[12,15]", "[1,1]", "[1],[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,4)", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,4]", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,5)", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,7)", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,7]", "[1,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,8]", "[1,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,12)", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,12]", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,14]", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,15]", "[1,15]");
        testUnion(int2, "[4,7],[12,15]", "[1,16]", "[1,16]");
        testUnion(int2, "[4,7],[12,15]", "[1,17]", "[1,17]");
        testUnion(int2, "[4,7],[12,15]", "(4,5)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,7)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(4,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,4]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,5)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,7)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,14]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,15]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[4,16]", "[4,16]");
        testUnion(int2, "[4,7],[12,15]", "[4,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "(5,7)", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,14]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,15]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(5,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "(7,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,8)", "[4,8),[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,12]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,14]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,15]", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "(7,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "[7,7]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[7,8]", "[4,8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[7,12)", "[4,15]");
        testUnion(int2, "[4,7],[12,15]", "[7,17]", "[4,17]");
        testUnion(int2, "[4,7],[12,15]", "[8,8]", "[4,7],[8],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,12)", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,12]", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,14]", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,15]", "[4,7],[8,15]");
        testUnion(int2, "[4,7],[12,15]", "[8,16]", "[4,7],[8,16]");
        testUnion(int2, "[4,7],[12,15]", "[8,17]", "[4,7],[8,17]");
        testUnion(int2, "[4,7],[12,15]", "(8,11)", "[4,7],(8,11),[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,12)", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,12]", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,14]", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,15]", "[4,7],(8,15]");
        testUnion(int2, "[4,7],[12,15]", "(8,16]", "[4,7],(8,16]");
        testUnion(int2, "[4,7],[12,15]", "(8,17]", "[4,7],(8,17]");
        testUnion(int2, "[4,7],[12,15]", "(12,14]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(12,15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "(12,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "(12,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[12,12]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[12,14]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[12,15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[12,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "[12,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[14,14]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[14,15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[14,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "[14,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[15]", "[4,7],[12,15]");
        testUnion(int2, "[4,7],[12,15]", "[15,16]", "[4,7],[12,16]");
        testUnion(int2, "[4,7],[12,15]", "[15,17]", "[4,7],[12,17]");
        testUnion(int2, "[4,7],[12,15]", "[15,17)", "[4,7],[12,17)");
        testUnion(int2, "[4,7],[12,15]", "[16]", "[4,7],[12,15],[16]");
        testUnion(int2, "[4,7],[12,15]", "[16,17]", "[4,7],[12,15],[16,17]");
        testUnion(int2, "[4,7],[12,15]", "[17]", "[4,7],[12,15],[17]");

        testUnion(int2, "(4,7),(12,15)", "[1,1]", "[1],(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,4)", "[1,4),(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,4]", "[1,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,5)", "[1,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,7)", "[1,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,7]", "[1,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,8]", "[1,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,12)", "[1,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,12]", "[1,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,14]", "[1,15)");
        testUnion(int2, "(4,7),(12,15)", "[1,15]", "[1,15]");
        testUnion(int2, "(4,7),(12,15)", "[1,16]", "[1,16]");
        testUnion(int2, "(4,7),(12,15)", "[1,17]", "[1,17]");
        testUnion(int2, "(4,7),(12,15)", "(4,5)", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,7)", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,7]", "(4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,8]", "(4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,12)", "(4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(4,12]", "(4,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,4]", "[4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,5)", "[4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,7)", "[4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,7]", "[4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,8]", "[4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,12)", "[4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,12]", "[4,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,14]", "[4,15)");
        testUnion(int2, "(4,7),(12,15)", "[4,15]", "[4,15]");
        testUnion(int2, "(4,7),(12,15)", "[4,16]", "[4,16]");
        testUnion(int2, "(4,7),(12,15)", "[4,17]", "[4,17]");
        testUnion(int2, "(4,7),(12,15)", "(5,7)", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,7]", "(4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,8]", "(4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,12)", "(4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,12]", "(4,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,14]", "(4,15)");
        testUnion(int2, "(4,7),(12,15)", "(5,15]", "(4,15]");
        testUnion(int2, "(4,7),(12,15)", "(5,17]", "(4,17]");
        testUnion(int2, "(4,7),(12,15)", "(7,8]", "(4,7),(7,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,8)", "(4,7),(7,8),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,12)", "(4,7),(7,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,12]", "(4,7),(7,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,14]", "(4,7),(7,15)");
        testUnion(int2, "(4,7),(12,15)", "(7,15]", "(4,7),(7,15]");
        testUnion(int2, "(4,7),(12,15)", "(7,17]", "(4,7),(7,17]");
        testUnion(int2, "(4,7),(12,15)", "[7,7]", "(4,7],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[7,8]", "(4,8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[7,12)", "(4,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[7,17]", "(4,17]");
        testUnion(int2, "(4,7),(12,15)", "[8,8]", "(4,7),[8],(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,12)", "(4,7),[8,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,12]", "(4,7),[8,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,14]", "(4,7),[8,15)");
        testUnion(int2, "(4,7),(12,15)", "[8,15]", "(4,7),[8,15]");
        testUnion(int2, "(4,7),(12,15)", "[8,16]", "(4,7),[8,16]");
        testUnion(int2, "(4,7),(12,15)", "[8,17]", "(4,7),[8,17]");
        testUnion(int2, "(4,7),(12,15)", "(8,11)", "(4,7),(8,11),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,12)", "(4,7),(8,12),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,12]", "(4,7),(8,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,14]", "(4,7),(8,15)");
        testUnion(int2, "(4,7),(12,15)", "(8,15]", "(4,7),(8,15]");
        testUnion(int2, "(4,7),(12,15)", "(8,16]", "(4,7),(8,16]");
        testUnion(int2, "(4,7),(12,15)", "(8,17]", "(4,7),(8,17]");
        testUnion(int2, "(4,7),(12,15)", "(12,14]", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "(12,15]", "(4,7),(12,15]");
        testUnion(int2, "(4,7),(12,15)", "(12,16]", "(4,7),(12,16]");
        testUnion(int2, "(4,7),(12,15)", "(12,17]", "(4,7),(12,17]");
        testUnion(int2, "(4,7),(12,15)", "[12,12]", "(4,7),[12,15)");
        testUnion(int2, "(4,7),(12,15)", "[12,14]", "(4,7),[12,15)");
        testUnion(int2, "(4,7),(12,15)", "[12,15]", "(4,7),[12,15]");
        testUnion(int2, "(4,7),(12,15)", "[12,16]", "(4,7),[12,16]");
        testUnion(int2, "(4,7),(12,15)", "[12,17]", "(4,7),[12,17]");
        testUnion(int2, "(4,7),(12,15)", "[14,14]", "(4,7),(12,15)");
        testUnion(int2, "(4,7),(12,15)", "[14,15]", "(4,7),(12,15]");
        testUnion(int2, "(4,7),(12,15)", "[14,16]", "(4,7),(12,16]");
        testUnion(int2, "(4,7),(12,15)", "[14,17]", "(4,7),(12,17]");
        testUnion(int2, "(4,7),(12,15)", "[15]", "(4,7),(12,15]");
        testUnion(int2, "(4,7),(12,15)", "[15,16]", "(4,7),(12,16]");
        testUnion(int2, "(4,7),(12,15)", "(15,16]", "(4,7),(12,15),(15,16]");
        testUnion(int2, "(4,7),(12,15)", "[15,17]", "(4,7),(12,17]");
        testUnion(int2, "(4,7),(12,15)", "[15,17)", "(4,7),(12,17)");
        testUnion(int2, "(4,7),(12,15)", "[16]", "(4,7),(12,15),[16]");
        testUnion(int2, "(4,7),(12,15)", "[16,17]", "(4,7),(12,15),[16,17]");
        testUnion(int2, "(4,7),(12,15)", "[17]", "(4,7),(12,15),[17]");
    }

    void testExclude(const RtlTypeInfo & type, const char * filter, const char * next, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        Owned<IValueSet> set2 = createValueSet(type);
        deserializeSet(*set2, next);
        set->excludeSet(set2);
        checkSet(set, expected);
    }

    //Tests killRange which is used by excludeSet()
    void testExclude()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testExclude(int2, "[4]", "(4,5]", "[4]");


        testExclude(int2, "[4,7],[12,15]", "[1,1]", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,4)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,4]", "(4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,5)", "[5,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,7)", "[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,7]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(,)", "");
        testExclude(int2, "[4,7],[12,15]", "(,12]", "(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(6,)", "[4,6]");
        testExclude(int2, "[4,7],[12,15]", "[1,8]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,12)", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,12]", "(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,14]", "(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[1,15]", "");
        testExclude(int2, "[4,7],[12,15]", "[1,16]", "");
        testExclude(int2, "[4,7],[12,15]", "(4,5)", "[4],[5,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,7)", "[4],[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,7]", "[4],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,8]", "[4],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,12)", "[4],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(4,12]", "[4],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,4]", "(4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,5)", "[5,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,7)", "[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,7]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,8]", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,12)", "[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,12]", "(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,14]", "(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[4,15]", "");
        testExclude(int2, "[4,7],[12,15]", "[4,16]", "");
        testExclude(int2, "[4,7],[12,15]", "(5,7)", "[4,5],[7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,7]", "[4,5],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,8]", "[4,5],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,12)", "[4,5],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,12]", "[4,5],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,14]", "[4,5],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "(5,15)", "[4,5],[15]");
        testExclude(int2, "[4,7],[12,15]", "(5,15]", "[4,5]");
        testExclude(int2, "[4,7],[12,15]", "(5,17]", "[4,5]");
        testExclude(int2, "[4,7],[12,15]", "(7,8]", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,8)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,12)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,14]", "[4,7],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "(7,15]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "(7,17]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[7,7]", "[4,7),[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[7,8]", "[4,7),[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[7,12)", "[4,7),[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[7,17]", "[4,7)");
        testExclude(int2, "[4,7],[12,15]", "[8,8]", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,12)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,14]", "[4,7],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[8,15]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[8,16]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[8,17]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "(8,11)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(8,12)", "[4,7],[12,15]");
        testExclude(int2, "[4,7],[12,15]", "(8,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "(12,14]", "[4,7],[12],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "(12,15]", "[4,7],[12]");
        testExclude(int2, "[4,7],[12,15]", "(12,16]", "[4,7],[12]");
        testExclude(int2, "[4,7],[12,15]", "[12,12]", "[4,7],(12,15]");
        testExclude(int2, "[4,7],[12,15]", "[12,14]", "[4,7],(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[12,15]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[12,16]", "[4,7]");
        testExclude(int2, "[4,7],[12,15]", "[14,14]", "[4,7],[12,14),(14,15]");
        testExclude(int2, "[4,7],[12,15]", "[14,15]", "[4,7],[12,14)");
        testExclude(int2, "[4,7],[12,15]", "[14,16]", "[4,7],[12,14)");
        testExclude(int2, "[4,7],[12,15]", "[14,17]", "[4,7],[12,14)");
        testExclude(int2, "[4,7],[12,15]", "[15]", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[15,16]", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[15,17]", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[15,17)", "[4,7],[12,15)");
        testExclude(int2, "[4,7],[12,15]", "[16]", "[4,7],[12,15]");
        testExclude(int2, "(4,7),(12,15)", "[1,1]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,4)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,4]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,5)", "[5,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,7)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,7]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,8]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,12)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,12]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,14]", "(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[1,15]", "");
        testExclude(int2, "(4,7),(12,15)", "[1,16]", "");
        testExclude(int2, "(4,7),(12,15)", "(4,5)", "[5,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,7)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,7]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,8]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,12)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(4,12]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,4]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,5)", "[5,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,7)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,7]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,8]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,12)", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,12]", "(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,14]", "(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[4,15]", "");
        testExclude(int2, "(4,7),(12,15)", "(5,6)", "(4,5],[6,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,7)", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,7]", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,8]", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,12)", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,12]", "(4,5],(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,14]", "(4,5],(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(5,15]", "(4,5]");
        testExclude(int2, "(4,7),(12,15)", "(5,17]", "(4,5]");
        //return;
        testExclude(int2, "(4,7),(12,15)", "(7,8]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,8)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(7,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(7,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[7,7]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[7,8]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[7,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[7,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[8,8]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[8,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[8,16]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[8,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(8,11)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,12)", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(8,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(8,16]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(8,17]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(12,14]", "(4,7),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "(12,15]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "(12,16]", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[12,12]", "(4,7),(12,15)");
        testExclude(int2, "(4,7),(12,15)", "[13]", "(4,7),(12,13),(13,15)");
        testExclude(int2, "(4,7),(12,15)", "[12,14)", "(4,7),[14,15)");
        testExclude(int2, "(4,7),(12,15)", "[12,15)", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[12,16)", "(4,7)");
        testExclude(int2, "(4,7),(12,15)", "[14,14]", "(4,7),(12,14),(14,15)");
        testExclude(int2, "(4,7),(12,15)", "[14,15]", "(4,7),(12,14)");
        testExclude(int2, "(4,7),(12,15)", "[14,16]", "(4,7),(12,14)");
        testExclude(int2, "(4,7),(12,15)", "[15]", "(4,7),(12,15)");
    }

    void testInverse(const RtlTypeInfo & type, const char * filter, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        set->invertSet();
        checkSet(set, expected);
    }

    //Tests killRange which is used by excludeSet()
    void testInverse()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testInverse(int2, "[4]", "(,4),(4,)");
        testInverse(int2, "[4,5]", "(,4),(5,)");
        testInverse(int2, "(4,5)", "(,4],[5,)");
        testInverse(int2, "(,5)", "[5,)");
        testInverse(int2, "[6,)", "(,6)");
        testInverse(int2, "", "(,)");
        testInverse(int2, "(,)", "");
        testInverse(int2, "[4,5],(8,9),[12,14)", "(,4),(5,8],[9,12),[14,)");
    }

    void testIntersect(const RtlTypeInfo & type, const char * filter, const char * next, const char * expected)
    {
        Owned<IValueSet> set = createValueSet(type);
        deserializeSet(*set, filter);
        Owned<IValueSet> set2 = createValueSet(type);
        deserializeSet(*set2, next);
        set->intersectSet(set2);
        checkSet(set, expected);

        //Test the opposite way around
        Owned<IValueSet> seta = createValueSet(type);
        deserializeSet(*seta, filter);
        Owned<IValueSet> setb = createValueSet(type);
        deserializeSet(*setb, next);
        setb->intersectSet(seta);
        checkSet(set, expected);
    }

    void testIntersect()
    {
        RtlIntTypeInfo int2(type_int, 2);

        testIntersect(int2, "", "[1,2],(3,6)", "");
        testIntersect(int2, "(,)", "[1,2],(3,6)", "[1,2],(3,6)");
        testIntersect(int2, "(,)", "", "");
        testIntersect(int2, "", "", "");
        testIntersect(int2, "(,)", "(,)", "(,)");

        testIntersect(int2, "(3,7),[10,20]", "[2]", "");
        testIntersect(int2, "(3,7),[10,20]", "[3]", "");
        testIntersect(int2, "(3,7),[10,20]", "[4]", "[4]");
        testIntersect(int2, "(3,7),[10,20]", "[6]", "[6]");
        testIntersect(int2, "(3,7),[10,20]", "[7]", "");
        testIntersect(int2, "(3,7),[10,20]", "[10]", "[10]");
        testIntersect(int2, "(3,7),[10,20]", "[20]", "[20]");
        testIntersect(int2, "(3,7),[10,20]", "[21]", "");

        testIntersect(int2, "(3,7),[10,20]", "[2,3]", "");
        testIntersect(int2, "(3,7),[10,20]", "[2,5]", "(3,5]");
        testIntersect(int2, "(3,7),[10,20]", "[2,7]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[2,8]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[2,10]", "(3,7),[10]");
        testIntersect(int2, "(3,7),[10,20]", "[2,15]", "(3,7),[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[2,20]", "(3,7),[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[2,25]", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[3,3]", "");
        testIntersect(int2, "(3,7),[10,20]", "[3,5]", "(3,5]");
        testIntersect(int2, "(3,7),[10,20]", "[3,7]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[3,8]", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "[3,10]", "(3,7),[10]");
        testIntersect(int2, "(3,7),[10,20]", "[3,15]", "(3,7),[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[3,20]", "(3,7),[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[3,25]", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[5,7]", "[5,7)");
        testIntersect(int2, "(3,7),[10,20]", "[5,8]", "[5,7)");
        testIntersect(int2, "(3,7),[10,20]", "[5,10]", "[5,7),[10]");
        testIntersect(int2, "(3,7),[10,20]", "[5,15]", "[5,7),[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[5,20]", "[5,7),[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[5,25]", "[5,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[7,8]", "");
        testIntersect(int2, "(3,7),[10,20]", "[7,10]", "[10]");
        testIntersect(int2, "(3,7),[10,20]", "[7,15]", "[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[7,20]", "[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[7,25]", "[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[10,15]", "[10,15]");
        testIntersect(int2, "(3,7),[10,20]", "[10,20]", "[10,20]");
        testIntersect(int2, "(3,7),[10,20]", "[10,25]", "[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "[15,20]", "[15,20]");
        testIntersect(int2, "(3,7),[10,20]", "[15,25]", "[15,20]");

        testIntersect(int2, "(3,7),[10,20]", "[20,25]", "[20]");

        testIntersect(int2, "(3,7),[10,20]", "(2,3)", "");
        testIntersect(int2, "(3,7),[10,20]", "(2,5)", "(3,5)");
        testIntersect(int2, "(3,7),[10,20]", "(2,7)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(2,8)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(2,10)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(2,15)", "(3,7),[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(2,20)", "(3,7),[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(2,25)", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(3,5)", "(3,5)");
        testIntersect(int2, "(3,7),[10,20]", "(3,7)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(3,8)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(3,10)", "(3,7)");
        testIntersect(int2, "(3,7),[10,20]", "(3,15)", "(3,7),[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(3,20)", "(3,7),[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(3,25)", "(3,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(5,7)", "(5,7)");
        testIntersect(int2, "(3,7),[10,20]", "(5,8)", "(5,7)");
        testIntersect(int2, "(3,7),[10,20]", "(5,10)", "(5,7)");
        testIntersect(int2, "(3,7),[10,20]", "(5,15)", "(5,7),[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(5,20)", "(5,7),[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(5,25)", "(5,7),[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(7,8)", "");
        testIntersect(int2, "(3,7),[10,20]", "(7,10)", "");
        testIntersect(int2, "(3,7),[10,20]", "(7,15)", "[10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(7,20)", "[10,20)");
        testIntersect(int2, "(3,7),[10,20]", "(7,25)", "[10,20]");

        testIntersect(int2, "(3,7),[10,20]", "(10,15)", "(10,15)");
        testIntersect(int2, "(3,7),[10,20]", "(10,20)", "(10,20)");

        testIntersect(int2, "(3,7),[10,20]", "(15,20)", "(15,20)");
        testIntersect(int2, "(3,7),[10,20]", "(15,25)", "(15,20]");

        testIntersect(int2, "(3,7),[10,20]", "(20,25)", "");

        testIntersect(int2, "(3,5),[7,10),[15,20),(30,32),[37]", "(4,7],(9,12),[13,31],(36,)", "(4,5),[7],(9,10),[15,20),(30,31],[37]");
    }
    void testSubStrings()
    {
        RtlStringTypeInfo str3(type_string, 3);
        RtlStringTypeInfo str8(type_string, 8);
        RtlStringTypeInfo strx(type_string|RFTMunknownsize, 0);

        testSerialize(str3, "['a':1],", "['a  ':1]");
        testSerialize(str3, "['a':1,'b':1],", "['a  ':1,'b  ':1]");
        testSerialize(str3, "['a':1,'bx':2),", "['a  ':1,'bx ':2)");
        testSerialize(str3, "['aa':1],", "");
        testSerialize(str3, "['aaa':3],", "['aaa']");
        testSerialize(str3, "['aaa':4],", "['aaa']");
        testSerialize(str3, "['aa':1,'bc':1],", "('a  ':1,'b  ':1]");

        testUnion(str3, "['aaa']", "['aaa']", "['aaa']");
        testUnion(str3, "['a':1]", "['aaa']", "['a  ':1]");
        testUnion(str3, "['a':1]", "['aa':2]", "['a  ':1]");
        testUnion(str3, "['a':1]", "['b':1]", "['a  ':1],['b  ':1]");   //subranges are not merged
        testUnion(str3, "['a':1]", "['bb':2]", "['a  ':1],['bb ':2]");
        testUnion(str3, "['abc':2,'abz':2]", "['bbx':2,'bdz':2]", "('bb ':2,'bd ':2]");

        testIntersect(str3, "['a':1]", "['aaa']", "['aaa']");
        testIntersect(str3, "['a':1]", "['aa':2]", "['aa ':2]");
        testIntersect(str3, "['a':1]", "['b':1]", "");
        testIntersect(str3, "['a':1]", "['bb':2]", "");
        testIntersect(str3, "['ab':1,]", "[,'bb':2]", "('a  ':1,'bb ':2]");
        testIntersect(str3, "['ax':1,]", "[,'bx':1]", "('a  ':1,'b  ':1]");

        testExclude(str3, "['a':1]", "['aaa']", "['a  ':1,'aaa'),('aaa','a  ':1]");
        testExclude(str3, "['a':1]", "['aa':2]", "['a  ':1,'aa ':2),('aa ':2,'a  ':1]");
        testExclude(str3, "['a':1]", "['b':1]", "['a  ':1]");
        testExclude(str3, "['a':1]", "['bb':2]", "['a  ':1]");
        testExclude(str3, "['a':1,]", "[,'bb':2]", "('bb ':2,)");
        testExclude(str3, "['a':1,]", "[,'bbx':2]", "('bb ':2,)");
        testExclude(str3, "[,'ccx':2]", "['bbx':2,]", "(,'bb ':2]");
        testExclude(str3, "['aax':2,'ddx':2]", "['bbx':2,'ccx':2]", "('aa ':2,'bb ':2],('cc ':2,'dd ':2]");
        testExclude(str3, "[,]", "['bbx':2,'ccx':2]", "(,'bb ':2],('cc ':2,)");

        testUnion(str3, "['ab':2],['bc':2],['gh':2]", "['e':1]", "['ab ':2],['bc ':2],['e  ':1],['gh ':2]");
        testUnion(str8, "['KT19':4],['KT40':4],['KT3 ':4],['KT50':4]", "['KT2':3]","['KT19    ':4],['KT2     ':3],['KT3     ':4],['KT40    ':4],['KT50    ':4]");

        verifyRawSet("['MA':2]", strx, "+", { "\002\000\000\000MA", "\002\000\000\000MA" }, { 2 });
        verifyRawSet("['a  ':1],['b  ':1]", str3, "++", { "a  ", "a  ", "b  ", "b  " }, { 1, 1 });
        verifyRawSet("['a  ':1],['bb ':2]", str3, "++", { "a  ", "a  ", "bb ", "bb " }, { 1, 2 });
        verifyRawSet("('bb ':2,'bd ':2]", str3, "++", { "abc", "abz", "bbx", "bdz" }, { 2, 2 });
        verifyRawSet("(,'bb ':2]", str3, "+-", { nullptr, "ccx", "bbx", nullptr }, { 2, 2} );
        verifyRawSet("('aa ':2,'bb ':2],('cc ':2,'dd ':2]", str3, "+-", { "aax", "ddx", "bbx", "ccx"}, { 2, 2} );
        verifyRawSet("(,'bb ':2],('cc ':2,)", str3, "+-", { nullptr, nullptr, "bbx", "ccx" }, { 2, 2} );
    }
    void testStr2()
    {
        RtlStringTypeInfo str1(type_string, 1);
        Owned<IValueSet> az = createValueSet(str1);
        addRange(az, "A", "Z");
        checkSet(az, "['A','Z']");
        Owned<IValueSet> dj = createValueSet(str1);
        addRange(dj, "D", "J");
        checkSet(dj, "['D','J']");
        Owned<IValueSet> hz = createValueSet(str1);
        addRange(hz, "H", "Z");
        Owned<IValueSet> jk = createValueSet(str1);
        addRange(jk, "J", "K");
        Owned<IValueSet> kj = createValueSet(str1);
        addRange(kj, "K", "J");
        checkSet(kj, "");

    }
    // id:int2 extra:string padding:! name:string2
    // Keep in sorted order so they can be reused for index testing
    const char *testRows[6] = {
            "\001\000\004\000\000\000MARK!GH",
            "\002\000\000\000\000\000!AB",
            "\000\001\003\000\000\000FRY!JH",
            "\001\001\003\000\000\000MAR!AC",
            "\002\001\003\000\000\000MAS!JH",
            "\003\002\004\000\000\000MASK!JH",
    };

    const RtlIntTypeInfo int2 = RtlIntTypeInfo(type_int, 2);
    const RtlIntTypeInfo int4 = RtlIntTypeInfo(type_int, 4);
    const RtlStringTypeInfo str1 = RtlStringTypeInfo(type_string, 1);
    const RtlStringTypeInfo str2 = RtlStringTypeInfo(type_string, 2);
    const RtlStringTypeInfo str4 = RtlStringTypeInfo(type_string, 4);
    const RtlStringTypeInfo strx = RtlStringTypeInfo(type_string|RFTMunknownsize, 0);
    const RtlDataTypeInfo data4 = RtlDataTypeInfo(type_data, 4);
    const RtlFieldInfo id = RtlFieldInfo("id", nullptr, &int2);
    const RtlFieldInfo extra = RtlFieldInfo("extra", nullptr, &strx);
    const RtlFieldInfo padding = RtlFieldInfo("padding", nullptr, &str1);
    const RtlFieldInfo name = RtlFieldInfo("name", nullptr, &str2);
    const RtlFieldInfo * const fields[5] = { &id, &extra, &padding, &name, nullptr };
    const RtlRecordTypeInfo recordType = RtlRecordTypeInfo(type_record, 4, fields);
    const RtlRecord record = RtlRecord(recordType, true);

    void verifyFilter(const RtlRecord & searchRecord, IFieldFilter * filter)
    {
        StringBuffer str1, str2;
        filter->serialize(str1);
        Owned<IFieldFilter> clone1 = deserializeFieldFilter(searchRecord, str1);
        clone1->serialize(str2);

        MemoryBuffer mem1, mem2;
        filter->serialize(mem1);
        Owned<IFieldFilter> clone2 = deserializeFieldFilter(searchRecord, mem1);
        clone2->serialize(mem2);

        if (!streq(str1, str2))
            CPPUNIT_ASSERT_EQUAL(str1.str(), str2.str());

        if ((mem1.length() != mem2.length()) || memcmp(mem1.bytes(), mem2.bytes(), mem1.length()) != 0)
        {
            printf("Filter %s failed\n", str1.str());
            CPPUNIT_ASSERT_MESSAGE("Binary filter deserialize failed", false);
        }
    }

    void processFilter(RowFilter & cursor, const char * originalFilter, const RtlRecord & searchRecord)
    {
        const char * filter = originalFilter;
        while (filter && *filter)
        {
            StringBuffer next;
            const char * semi = strchr(filter, ';');
            if (semi)
            {
                next.append(semi-filter, filter);
                filter = semi+1;
            }
            else
            {
                next.append(filter);
                filter = nullptr;
            }

            IFieldFilter * fieldFilter = deserializeFieldFilter(searchRecord, next);
            assertex(fieldFilter);
            cursor.addFilter(*fieldFilter);
            verifyFilter(searchRecord, fieldFilter);
        }
    }

    //testFilter("id=[1,3];name=(,GH)", { false, true, false, false, false });
    void testFilter(RowFilter & cursor, const char * originalFilter, const std::initializer_list<bool> & expected)
    {
        const byte * * rows = reinterpret_cast<const byte * *>(testRows);
        RtlDynRow row(record, nullptr);
        assertex((expected.end() - expected.begin()) == (unsigned)_elements_in(testRows));
        const bool * curExpected = expected.begin();
        for (unsigned i= 0; i < _elements_in(testRows); i++)
        {
            row.setRow(rows[i]);
            if (cursor.matches(row) != curExpected[i])
            {
                StringBuffer actual;
                for (unsigned i=0; i<cursor.numFilterFields(); i++)
                    cursor.queryFilter(i).serialize(actual.append(","));
                printf("Failure to match row %u filter '%s' (%s)\n", i, originalFilter, actual.str());
                CPPUNIT_ASSERT_EQUAL(curExpected[i], cursor.matches(row));
            }
        }
    };

    void testFilter(const char * originalFilter, const std::initializer_list<bool> & expected)
    {
        RowFilter cursor;
        processFilter(cursor, originalFilter, record);

        testFilter(cursor, originalFilter, expected);
    }

    IValueSet * createRawSet(const RtlTypeInfo * fieldType, const char * key, const std::initializer_list<const char *> & transitions, const std::initializer_list<unsigned> & lengths)
    {
        RowFilter cursor;
        unsigned numTransitions = (transitions.end() - transitions.begin());
        unsigned numLengths = (lengths.end() - lengths.begin());
        assertex((numTransitions & 1) == 0);
        assertex(strlen(key) == numTransitions/2);
        assertex(numLengths == numTransitions/2);

        Owned<IValueSet> set = createValueSet(*fieldType);
        const unsigned * lens = lengths.begin();
        for (unsigned i=0; i < numTransitions; i+= 2)
        {
            const char * low = transitions.begin()[i];
            const char * high = transitions.begin()[i+1];

            if (key[i/2] == '+')
                set->addRawRangeEx(low, high, lens[i/2]);
            else
                set->killRawRangeEx(low, high, lens[i/2]);
        }
        return set.getClear();
    }

    void verifyRawSet(const char * expected, const RtlTypeInfo & fieldType, const char * key, const std::initializer_list<const char *> & transitions, const std::initializer_list<unsigned> & lengths)
    {
        Owned<IValueSet> set = createRawSet(&fieldType, key, transitions, lengths);
        StringBuffer actual;
        set->serialize(actual);
        if (!streq(expected, actual))
            CPPUNIT_ASSERT_EQUAL(expected, actual.str());
    }

    void testRawFilter(const char * fieldName, const char * key, const std::initializer_list<const char *> & transitions, const std::initializer_list<unsigned> & lengths, const std::initializer_list<bool> & expected)
    {
        unsigned fieldNum = record.getFieldNum(fieldName);
        assertex(fieldNum != (unsigned) -1);
        const RtlTypeInfo *fieldType = record.queryType(fieldNum);
        Owned<IValueSet> set = createRawSet(fieldType, key, transitions, lengths);

        IFieldFilter * filter = createFieldFilter(fieldNum, set);
        RowFilter cursor;
        cursor.addFilter(*filter);
        testFilter(cursor, "<raw>", expected);
    }

    void testFilter()
    {
        testFilter("", { true, true, true, true, true, true });
        testFilter("id=[1]", { true, false, false, false, false, false });
        testFilter("id=[1],[2],[4],[6],[12],[23],[255],[256],[300],[301],[320]", { true, true, true, false, false, false });
        testFilter("id=[1,2]", { true, true, false, false, false, false });
        testFilter("id=(1,2]", { false, true, false, false, false, false });
        testFilter("id=[1,3];name=(,GH)", { false, true, false, false, false, false });
        testFilter("id=[1,3];name=(,GH]", { true, true, false, false, false, false });
        testFilter("extra=['MAR','MAS']", { true, false, false, true, true, false });
        testFilter("extra=('MAR','MAS')", { true, false, false, false, false, false });
        testFilter("id=(,257]", { true, true, true, true, false, false });
        testFilter("extra:2=['MA']", { true, false, false, true, true, true });
        testFilter("extra:2=['  ']", { false, true, false, false, false, false });
        testFilter("extra:0=['XX']", { false, false, false, false, false, false });
        testFilter("extra:0=['']", { true, true, true, true, true, true });
        testFilter("name:1=['A']", { false, true, false, true, false, false });
        //Check substring ranges are correctly truncated
        testFilter("extra:3=['MARK','MAST']", { false, false, false, false, true, true });
        testFilter("name:1=['AB','JA']", { true, false, true, false, true, true });

        //New substring implementation
        testFilter("extra=['MA':2]", { true, false, false, true, true, true });
        testFilter("extra=['  ':2]", { false, true, false, false, false, false });
        testFilter("extra=['XX':0]", { false, false, false, false, false, false });
        testFilter("extra=['':0]", { true, true, true, true, true, true });
        testFilter("name=['A':1]", { false, true, false, true, false, false });
        //Check substring ranges are correctly truncated
        testFilter("extra=['MARK':3,'MAST':3]", { false, false, false, false, true, true });
        testFilter("name=['AB':1,'JA':1]", { true, false, true, false, true, true });
        testFilter("extra=[' ':1],['MA':2]", { true, true, false, true, true, true });
        testFilter("extra=['MARK':3,]", { false, false, false, false, true, true });
        testFilter("extra=[,'MARK':3]", { true, true, true, true, false, false });
        testFilter("extra=['FRA':2,]", { true, false, false, true, true, true });
        testFilter("extra=['FRA':2,'MARK':3]", { true, false, false, true, false, false });
        testFilter("extra=['FRA':2,],[,'MARK':3]", { true, true, true, true, true, true });
        testFilter("extra=['FRZ':2,'MARK':3]", { true, false, false, true, false, false });
        testFilter("extra=['MARK':3,],[,'FRZ':2]", { false, true, true, false, true, true });

        testRawFilter("extra", "+", { "\002\000\000\000MA", "\002\000\000\000MA" }, { 2 }, { true, false, false, true, true, true });
        testRawFilter("extra", "+", { "\004\000\000\000MARK", nullptr }, { 3 }, { false, false, false, false, true, true });
    }

    void testKeyed(const char * originalFilter, const char * expected)
    {
        const byte * * rows = reinterpret_cast<const byte * *>(testRows);
        RowFilter filter;
        processFilter(filter, originalFilter, record);

        InMemoryRows source(_elements_in(testRows), rows, record);
        InMemoryRowCursor sourceCursor(source); // could be created by source.createCursor()
        KeySearcher searcher(source.queryRecord(), filter, &sourceCursor);

        StringBuffer matches;
        while (searcher.next())
        {
            searcher.queryRow().lazyCalcOffsets(1);  // In unkeyed case we may not have calculated field 0 offset (though it is always going to be 0).
            matches.append(searcher.queryRow().getInt(0)).append("|");
        }

        if (!streq(matches, expected))
        {
            printf("Failure to match expected keyed filter '%s' (%s, %s)\n", originalFilter, expected, matches.str());
            CPPUNIT_ASSERT(streq(matches, expected));
        }
    }

    void testKeyed1()
    {
        testKeyed("extra=['MAR','MAS']", "1|257|258|");
        testKeyed("","1|2|256|257|258|515|");
        testKeyed("id=[1,2]","1|2|");
        testKeyed("id=[1],[256]","1|256|");
        testKeyed("id=[1],[256,280]","1|256|257|258|");
        testKeyed("id=[1],[256,280],[1000],[1023]","1|256|257|258|");
        testKeyed("id=[1],[2],[4],[6],[12],[23],[255],[256],[300],[301],[320]","1|2|256|");
        testKeyed("id!=[1],[256]","2|257|258|515|");
        testKeyed("id!=","1|2|256|257|258|515|");
        testKeyed("extra=['MAR','MAS']", "1|257|258|");
        testKeyed("extra=('MAR','MAS')", "1|");
        testKeyed("name=['AB','AC']", "2|257|");
    }

    void generateOrderedRows(PointerArray & rows, const RtlRecord & rowRecord)
    {
        //Generate rows with 3 fields.   Try and ensure:
        //  each trailing field starts with 0, non zero.
        //  the third field has significant distribution in the number of elements for each value of field2
        //  duplicates occur in the full keyed values.
        //  sometimes the next value in sequence happens to match a trailing filter condition e.g. field3=1
        //Last field First field x ranges from 0 to n1
        //Second field
        //Second field y ranges from 0 .. n2, and is included if (x + y) % 3 != 0 and (x + y) % 5 != 0
        //Third field is sparse from 0..n3.  m = ((x + y) % 11 ^2 + 1;  if (n3 + x *2 + y) % m = 0 then it is included
        unsigned n = 100000;
        unsigned f1 = 0;
        unsigned f2 = 0;
        unsigned f3 = 0;
        unsigned numf2 = 1;
        unsigned countf2 = 0;
        unsigned numf3 = 1;
        unsigned countf3 = 0;
        MemoryBuffer buff;
        for (unsigned i = 0; i < n; i++)
        {
            buff.setLength(0);
            MemoryBufferBuilder builder(buff, 0);
            size32_t offset = 0;
            offset = rowRecord.queryType(0)->buildInt(builder, offset, nullptr, f1);
            offset = rowRecord.queryType(1)->buildInt(builder, offset, nullptr, f2);
            offset = rowRecord.queryType(2)->buildInt(builder, offset, nullptr, f3);

            byte * row = new byte[offset];
            memcpy(row, buff.bufferBase(), offset);
            rows.append(row);

            if (++countf3 == numf3)
            {
                f2++;
                if (++countf2 == numf2)
                {
                    f1++;
                    f2 = i % 2;
                    numf2 = i % 23;
                    if (numf2 == 0)
                    {
                        f1++;
                        numf2 = (i % 21) + 1;
                    }
                    countf2 = 0;
                }

                f3 = i % 7;
                countf3 = 0;
                numf3 = i % 9;
                if (numf3 == 0)
                {
                    f3++;
                    numf3 = (i % 11) + 1;
                }
            }

            if (i % 5)
                f3++;
        }

        //Sort the rows - to allow different field types to be used.
        RawRowCompare compareRow(rowRecord);
        qsortvec(rows.getArray(), rows.ordinality(), compareRow);
    }

    void traceRow(const RtlRow & row)
    {
        row.lazyCalcOffsets(3);
        printf("%u %u %u", (unsigned)row.getInt(0), (unsigned)row.getInt(1), (unsigned)row.getInt(2));
    }

    const RtlFieldInfo f1 = RtlFieldInfo("f1", nullptr, &int2);
    const RtlFieldInfo f2 = RtlFieldInfo("f2", nullptr, &int2);
    const RtlFieldInfo f3 = RtlFieldInfo("f3", nullptr, &int2);
    const RtlFieldInfo * const testFields[4] = { &f1, &f2, &f3, nullptr };
    const RtlRecordTypeInfo testRecordType = RtlRecordTypeInfo(type_record, 6, testFields);
    const RtlRecord testRecord = RtlRecord(testRecordType, true);

    const RtlFieldInfo f1s = RtlFieldInfo("f1", nullptr, &strx);
    const RtlFieldInfo f2s = RtlFieldInfo("f2", nullptr, &str4);
    const RtlFieldInfo f3s = RtlFieldInfo("f3", nullptr, &str2);
    const RtlFieldInfo * const testFieldsB[4] = { &f1s, &f2s, &f3s, nullptr };
    const RtlRecordTypeInfo testRecordTypeB = RtlRecordTypeInfo(type_record, 10, testFieldsB);
    const RtlRecord testRecordB = RtlRecord(testRecordTypeB, true);

    void timeKeyedScan(const PointerArray & rows, const RtlRecord & searchRecord, const char * filterText)
    {
        RowFilter filter;
        processFilter(filter, filterText, searchRecord);

        CCycleTimer timeKeyed;
        unsigned countKeyed = 0;
        {
            InMemoryRows source(rows.ordinality(), (const byte * *)rows.getArray(), searchRecord);
            InMemoryRowCursor sourceCursor(source); // could be created by source.createCursor()
            KeySearcher searcher(source.queryRecord(), filter, &sourceCursor);

            while (searcher.next())
            {
                countKeyed++;
            }
        }
        unsigned __int64 keyedMs = timeKeyed.elapsedNs();

        CCycleTimer timeScan;
        unsigned countScan = 0;
        {
            RowScanner scanner(searchRecord, filter, rows);

            bool hasSearch = scanner.first();
            while (hasSearch)
            {
                countScan++;
                hasSearch = scanner.next();
            }
        }
        unsigned __int64 scanMs = timeScan.elapsedNs();
        CPPUNIT_ASSERT_EQUAL(countScan, countKeyed);

        printf("[%s] %u matches keyed(%" I64F "u) scan(%" I64F "u) (%.3f)\n", filterText, countScan, keyedMs, scanMs, (double)keyedMs/scanMs);
    }


    void testKeyed(const PointerArray & rows, const RtlRecord & searchRecord, const char * filterText)
    {
        RowFilter filter;
        processFilter(filter, filterText, searchRecord);

        InMemoryRows source(rows.ordinality(), (const byte * *)rows.getArray(), searchRecord);
        InMemoryRowCursor sourceCursor(source); // could be created by source.createCursor()
        KeySearcher searcher(source.queryRecord(), filter, &sourceCursor);
        RowScanner scanner(source.queryRecord(), filter, rows);

        unsigned count = 0;
        bool hasSearch = searcher.next();
        bool hasScan = scanner.first();
        while (hasSearch && hasScan)
        {
            count++;
            if (searchRecord.compare(searcher.queryRow().queryRow(), scanner.queryRow().queryRow()) != 0)
                break;

            hasSearch = searcher.next();
            hasScan = scanner.next();
        }
        if (hasSearch || hasScan)
        {
            printf("[%s] Keyed: ", filterText);
            if (hasSearch)
                traceRow(searcher.queryRow());
            else
                printf("<missing>");
            printf(" Scan: ");
            if (hasScan)
                traceRow(scanner.queryRow());
            else
                printf("<missing>");
            printf("\n");
            CPPUNIT_ASSERT_MESSAGE("Keyed search did not match scan", false);
        }
        else
        {
            const bool compareTiming = true;
            if (compareTiming)
                timeKeyedScan(rows, searchRecord, filterText);
            else
                printf("[%s] %u matches\n", filterText, count);
        }
    }


    void testKeyed2(const RtlRecord & record, bool testSubString)
    {
        PointerArray rows;
        generateOrderedRows(rows, record);

        testKeyed(rows, record, "");
        testKeyed(rows, record, "f1=[5]");
        testKeyed(rows, record, "f1=[0]");
        testKeyed(rows, record, "f2=[1]");
        testKeyed(rows, record, "f3=[1]");
        testKeyed(rows, record, "f3=[4]");
        testKeyed(rows, record, "f3=[1,3]");
        testKeyed(rows, record, "f3=[1],[2],[3]");
        testKeyed(rows, record, "f1=[21];f2=[20];f3=[4]");
        testKeyed(rows, record, "f1=[7];f3=[5]");
        testKeyed(rows, record, "f1=[7,];f3=[,5]");

        if (testSubString)
        {
            testKeyed(rows, record, "f1:1=['1']");
            testKeyed(rows, record, "f1:2=['10']");
            testKeyed(rows, record, "f1:30=['5']");
            testKeyed(rows, record, "f3:1=['1']");
            testKeyed(rows, record, "f3:2=['10']");
            testKeyed(rows, record, "f3:2=['123']");
            testKeyed(rows, record, "f3:2=['123','126']");

            //new format substring
            testKeyed(rows, record, "f1=['1':1]");
            testKeyed(rows, record, "f1=['10':2]");
            testKeyed(rows, record, "f1=['5':30]");
            testKeyed(rows, record, "f3=['1':1]");
            testKeyed(rows, record, "f3=['10':2]");
            testKeyed(rows, record, "f3=['123':2]");
            testKeyed(rows, record, "f3=['123','126':2]");
        }

        ForEachItemIn(i, rows)
            delete [] (byte *)rows.item(i);
    }

    void testKeyed2()
    {
        testKeyed2(testRecord, false);
        testKeyed2(testRecordB, true);
    }

    void appendHex(StringBuffer & target, size_t len, const void * data, bool lower)
    {
        for (unsigned i=0; i < len; i++)
            target.appendhex(((const byte *)data)[i], lower);
    }
    void testBound(const RtlTypeInfo & type, const char * original, unsigned subLength, bool lower, bool inclusive, size_t sizeExpected, const char * expected)
    {
        MemoryBuffer buff;
        MemoryBufferBuilder builder(buff, 0);
        type.buildUtf8(builder, 0, nullptr, rtlUtf8Length(strlen(original), original), original);
        byte result[1000];
        if (lower)
            type.setLowBound(result, (const byte *)buff.toByteArray(), subLength, inclusive);
        else
            type.setHighBound(result, (const byte *)buff.toByteArray(), subLength, inclusive);

        size_t size = type.size(result, nullptr);
        if ((size != sizeExpected) || memcmp(result, expected, size) != 0)
        {
            StringBuffer left, right;
            appendHex(left, sizeExpected, expected, true);
            appendHex(right, size, result, true);
            CPPUNIT_ASSERT_EQUAL(left.str(), right.str());
        }
    }

    void testBoundQstr(const RtlTypeInfo & type, const char * original, unsigned subLength, bool lower, bool inclusive, size_t sizeExpected, const char * expected)
    {
        size32_t tempLen;
        rtlDataAttr temp;
        rtlStrToQStrX(tempLen, temp.refstr(), sizeExpected, expected);
        testBound(type, original, subLength, lower, inclusive, rtlQStrSize(tempLen), temp.getstr());
    }

    void testBounds()
    {
        //Lower bounds
        testBound(str4, "X", MatchFullString, true, true, 4, "X   ");
        testBound(str4, "X", 4, true, true, 4, "X   ");
        testBound(str4, "X", 3, true, true, 4, "X  \x00");
        testBound(str4, "A", MatchFullString, true, false, 4, "A  !");
        testBound(str4, "A", 4, true, false, 4, "A  !");
        testBound(str4, "X", 3, true, false, 4, "X !\x00");
        //Upper bounds
        testBound(str4, "X", 4, false, true, 4, "X   ");
        testBound(str4, "X", 3, false, true, 4, "X  \xFF");
        testBound(str4, "A", 4, false, false, 4, "A  \x1F");
        testBound(str4, "X", 3, false, false, 4, "X \x1F\xFF");

        //Lower bounds
        testBound(data4, "X", MatchFullString, true, true, 4, "X\x00\x00\x00");
        testBound(data4, "X", 4, true, true, 4, "X\x00\x00\x00");
        testBound(data4, "X", 3, true, true, 4, "X\x00\x00\x00");
        testBound(data4, "A", MatchFullString, true, false, 4, "A\x00\x00\x01");
        testBound(data4, "A", 4, true, false, 4, "A\x00\x00\x01");
        testBound(data4, "X", 3, true, false, 4, "X\x00\x01\x00");
        //Upper bounds
        testBound(data4, "X", 4, false, true, 4, "X\x00\x00\x00");
        testBound(data4, "X", 3, false, true, 4, "X\x00\x00\xFF");
        testBound(data4, "A", 4, false, false, 4, "@\xFF\xFF\xFF");
        testBound(data4, "X", 3, false, false, 4, "W\xFF\xFF\xFF");

        //Horrible cases
        const RtlQStringTypeInfo qstr4(type_qstring, 4);
        testBoundQstr(qstr4, "AAAA", MatchFullString, true, true, 4, "AAAA");
        testBoundQstr(qstr4, "AAAA", MatchFullString, true, false, 4, "AAAB");
        testBoundQstr(qstr4, "AAAA", 4, true, true, 4, "AAAA");
        testBoundQstr(qstr4, "AAAA", 4, true, false, 4, "AAAB");
        testBoundQstr(qstr4, "AAA ", 3, true, false, 4, "AAB ");
        testBoundQstr(qstr4, "AAAA", 3, true, false, 4, "AAB ");
        testBoundQstr(qstr4, "AA_ ", 3, true, false, 4, "AB  ");
        testBoundQstr(qstr4, "A__ ", 3, true, false, 4, "B   ");
        testBoundQstr(qstr4, "AA_", 3, true, false, 4, "AB  ");
        testBoundQstr(qstr4, "A_X", 2, true, false, 4, "B   ");
        testBoundQstr(qstr4, "A_  ", 2, true, false, 4, "B   ");

        testBoundQstr(qstr4, "AAAA", MatchFullString, false, true, 4, "AAAA");
        testBoundQstr(qstr4, "AAAA", MatchFullString, false, false, 4, "AAA@");
        testBoundQstr(qstr4, "AAAA", 4, false, true, 4, "AAAA");
        testBoundQstr(qstr4, "AAAA", 4, false, false, 4, "AAA@");
        testBoundQstr(qstr4, "AAA ", 3, false, false, 4, "AA@_");
        testBoundQstr(qstr4, "AAAA", 3, false, false, 4, "AA@_");
        testBoundQstr(qstr4, "AA  ", 3, false, false, 4, "A@__");

        testBoundQstr(qstr4, "A   ", 3, false, false, 4, "@___");
        testBoundQstr(qstr4, "A X", 2, false, false, 4, "@___");
        testBoundQstr(qstr4, "A   ", 2, false, false, 4, "@___");

        testBoundQstr(qstr4, "A   ", 4, false, false, 4, "@___");
        testBoundQstr(qstr4, "A X ", 4, false, false, 4, "A W_");
    }


};

CPPUNIT_TEST_SUITE_REGISTRATION(ValueSetTest);
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION(ValueSetTest, "ValueSetTest");

#endif
