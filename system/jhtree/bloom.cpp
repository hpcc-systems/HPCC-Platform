/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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

#include "platform.h"
#include "jlib.hpp"
#include "bloom.hpp"
#include "math.h"

BloomFilter::BloomFilter(unsigned _cardinality, double _probability)
{
    unsigned cardinality = _cardinality ? _cardinality : 1;
    double probability = _probability >= 0.3 ? 0.3 : (_probability < 0.01 ? 0.01 : _probability);
    numBits = rtlRoundUp(-(cardinality*log(probability))/pow(log(2),2));
    unsigned tableSize = (numBits + 7) / 8;
    numBits = tableSize * 8;
    numHashes = round((numBits * log(2))/cardinality);
    table = (byte *) calloc(tableSize, 1);
}

BloomFilter::BloomFilter(unsigned _numHashes, unsigned _tableSize, byte *_table)
{
    numBits = _tableSize * 8;
    numHashes = _numHashes;
    table = _table;  // Note - takes ownership
}

BloomFilter::~BloomFilter()
{
    free(table);
}

void BloomFilter::add(hash64_t hash)
{
    uint32_t hash1 = hash >> 32;
    uint32_t hash2 = hash & 0xffffffff;
    for (unsigned i=0; i < numHashes; i++)
    {
        // Kirsch and Mitzenmacher technique (Harvard U)
        uint64_t bit = (hash1 + (i * hash2)) % numBits;
        uint64_t slot = bit / 8;
        unsigned shift = bit % 8;
        unsigned mask = 1 << shift;
        table[slot] |= mask;
    }
}

bool BloomFilter::test(hash64_t hash) const
{
    uint32_t hash1 = hash >> 32;
    uint32_t hash2 = hash & 0xffffffff;
    for (unsigned i=0; i < numHashes; i++)
    {
        // Kirsch and Mitzenmacher technique (Harvard U)
        uint64_t bit = (hash1 + (i * hash2)) % numBits;
        uint64_t slot = bit / 8;
        unsigned shift = bit % 8;
        unsigned mask = 1 << shift;
        if (!(table[slot] & mask))
            return false;
      }
    return true;
}

BloomBuilder::BloomBuilder(unsigned _maxHashes) : maxHashes(_maxHashes)
{
    isValid = true;
}

bool BloomBuilder::add(hash64_t val)
{
    if (isValid)
    {
        if (hashes.length()==maxHashes)
        {
            isValid = false;
            hashes.kill();
        }
        else
            hashes.append(val);
    }
    return isValid;
}

bool BloomBuilder::valid() const
{
    return isValid && hashes.length();
}

const BloomFilter * BloomBuilder::build(double probability) const
{
    if (!valid())
        return nullptr;
    BloomFilter *b = new BloomFilter(hashes.length(), probability);
    ForEachItemIn(idx, hashes)
    {
        b->add(hashes.item(idx));
    }
    return b;
}

#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class BloomTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BloomTest);
      CPPUNIT_TEST(testBloom);
    CPPUNIT_TEST_SUITE_END();

    const unsigned count = 1000000;
    void testBloom()
    {
        BloomBuilder b;
        for (unsigned val = 0; val < count; val++)
            b.add(rtlHash64Data(sizeof(val), &val, HASH64_INIT));
        Owned<const BloomFilter> f = b.build(0.01);
        unsigned falsePositives = 0;
        unsigned falseNegatives = 0;
        unsigned start = usTick();
        for (unsigned val = 0; val < count; val++)
        {
            if (!f->test(rtlHash64Data(sizeof(val), &val, HASH64_INIT)))
                falseNegatives++;
            if (f->test(rtlHash64Data(sizeof(val), &val, HASH64_INIT+1)))
                falsePositives++;
        }
        unsigned end = usTick();
        ASSERT(falseNegatives==0);
        DBGLOG("Bloom filter (%d, %d) gave %d false positives (%.02f %%) in %d uSec", f->queryNumHashes(), f->queryTableSize(), falsePositives, (falsePositives * 100.0)/count, end-start);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION( BloomTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( BloomTest, "BloomTest" );

#endif
