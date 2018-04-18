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
#include "jset.hpp"
#include "bloom.hpp"
#include "math.h"
#include "eclhelper.hpp"
#include "rtlrecord.hpp"

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

IndexBloomFilter::IndexBloomFilter(unsigned _numHashes, unsigned _tableSize, byte *_table, __uint64 _fields)
: BloomFilter(_numHashes, _tableSize, _table), fields(_fields)
{}

int IndexBloomFilter::compare(CInterface *const *_a, CInterface *const *_b)
{
    const IndexBloomFilter *a = static_cast<IndexBloomFilter *>(*_a);
    const IndexBloomFilter *b = static_cast<IndexBloomFilter *>(*_b);
    return a->fields - b->fields;
}

bool IndexBloomFilter::reject(const SegMonitorList &segs) const
{
    hash64_t hashval = HASH64_INIT;
    return getBloomHash(fields, segs, hashval) && !test(hashval);
}

extern bool getBloomHash(__int64 fields, const SegMonitorList &segs, hash64_t &hashval)
{
    while (fields)
    {
        unsigned f = ffsll(fields)-1;    // extract lowest 1 bit
        fields &= ~ (((__uint64) 1)<<f); // and clear it
        IKeySegmentMonitor *seg = segs.item(f);
        if (seg)
        {
            assertex(seg->getFieldIdx() == f);
            if (!seg->getBloomHash(hashval))
                return false;
        }
    }
    return true;
}

class RowHasher : public CInterfaceOf<IRowHasher>
{
public:
    RowHasher(const RtlRecord &_recInfo, __uint64 _fields);
    virtual hash64_t hash(const byte *row) const override;
    virtual bool isExact(const SegMonitorList &segs) const override;
    virtual __uint64 queryFields() const override { return fields; }
private:
    const RtlRecord &recInfo;
    const __uint64 fields;
};

class SimpleRowHasher : public RowHasher
{
public:
    SimpleRowHasher(const RtlRecord &_recInfo, __uint64 _fields, unsigned _offset, unsigned _length);
    virtual hash64_t hash(const byte *row) const override;
private:
    const unsigned offset;
    const unsigned length;
};

RowHasher::RowHasher(const RtlRecord &_recInfo, __uint64 _fields) : recInfo(_recInfo), fields(_fields)
{
}

hash64_t RowHasher::hash(const byte *row) const
{
    auto lfields = fields;
    hash64_t hashval = HASH64_INIT;
    // Assumes fixed size fields for now. Could probably optimize a bit
    while (lfields)
    {
        unsigned f = ffsll(lfields)-1;    // extract lowest 1 bit
        lfields &= ~ (((__uint64) 1)<<f); // and clear it
        hashval = rtlHash64Data(recInfo.queryType(f)->getMinSize(), row + recInfo.getFixedOffset(f), hashval);
    }
    return hashval;
}

bool RowHasher::isExact(const SegMonitorList &segs) const
{
    auto lfields = fields;
    // This will need reworking if/when non-fixed-size fields are supported (should actually become easier)
    while (lfields)
    {
        unsigned f = ffsll(lfields)-1;    // extract lowest 1 bit
        lfields &= ~ (((__uint64) 1)<<f); // and clear it
        if (!segs.isExact(recInfo.queryType(f)->getMinSize(), recInfo.getFixedOffset(f)))
            return false;
    }
    return true;
}

SimpleRowHasher::SimpleRowHasher(const RtlRecord &_recInfo, __uint64 _fields, unsigned _offset, unsigned _length)
: RowHasher(_recInfo, _fields), offset(_offset), length(_length)
{
}

hash64_t SimpleRowHasher::hash(const byte *row) const
{
    return rtlHash64Data(length, row + offset, HASH64_INIT);
}

// For cases where we know data is sorted

class jhtree_decl SortedBloomBuilder : public CInterfaceOf<IBloomBuilder>
{
public:
    SortedBloomBuilder(const IBloomBuilderInfo &_helper);
    SortedBloomBuilder(unsigned _maxHashes, double _probability);
    virtual const BloomFilter * build() const override;
    virtual bool add(hash64_t val) override;
    virtual unsigned queryCount() const override;
    virtual bool valid() const override;

protected:
    ArrayOf<hash64_t> hashes;
    const unsigned maxHashes;
    hash64_t lastHash = 0;
    const double probability = 0.0;
    bool isValid = true;
};

SortedBloomBuilder::SortedBloomBuilder(const IBloomBuilderInfo &helper)
: maxHashes(helper.getBloomLimit()),
  probability(helper.getBloomProbability())
{
    if (maxHashes==0 || !helper.getBloomEnabled())
        isValid = false;
}

SortedBloomBuilder::SortedBloomBuilder(unsigned _maxHashes, double _probability)
: maxHashes(_maxHashes),
  probability(_probability)
{
    if (maxHashes==0)
        isValid = false;
}

unsigned SortedBloomBuilder::queryCount() const
{
    return hashes.length();
}

bool SortedBloomBuilder::add(hash64_t hash)
{
    if (isValid && (hash != lastHash || !hashes.length()))
    {
        if (hashes.length()==maxHashes)
        {
            isValid = false;
            hashes.kill();
        }
        else
            hashes.append(hash);
        lastHash = hash;
    }
    return isValid;
}

bool SortedBloomBuilder::valid() const
{
    return isValid && hashes.length();
}

const BloomFilter * SortedBloomBuilder::build() const
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

// For cases where we do not know data is sorted - use a hash table to store the hashes

class jhtree_decl UnsortedBloomBuilder : public CInterfaceOf<IBloomBuilder>
{
public:
    UnsortedBloomBuilder(const IBloomBuilderInfo &_helper);
    UnsortedBloomBuilder(unsigned _maxHashes, double _probability);
    ~UnsortedBloomBuilder();
    virtual const BloomFilter * build() const override;
    virtual bool add(hash64_t val) override;
    virtual unsigned queryCount() const override;
    virtual bool valid() const override;

protected:
    hash64_t *hashes = nullptr;
    const unsigned maxHashes;
    const unsigned tableSize;
    unsigned tableCount = 0;
    const double probability = 0.0;
};


UnsortedBloomBuilder::UnsortedBloomBuilder(const IBloomBuilderInfo &helper)
: maxHashes(helper.getBloomLimit()),
  probability(helper.getBloomProbability()),
  tableSize(((helper.getBloomLimit()*4)/3)+1)
{
    if (tableSize && helper.getBloomEnabled())
    {
        hashes = (hash64_t *) calloc(sizeof(hash64_t), tableSize);
    }

}

UnsortedBloomBuilder::UnsortedBloomBuilder(unsigned _maxHashes, double _probability)
: maxHashes(_maxHashes),
  probability(_probability),
  tableSize(((_maxHashes*4)/3)+1)
{
    if (tableSize)
        hashes = (hash64_t *) calloc(sizeof(hash64_t), tableSize);
}

UnsortedBloomBuilder::~UnsortedBloomBuilder()
{
    free(hashes);
}

unsigned UnsortedBloomBuilder::queryCount() const
{
    return tableCount;
}

bool UnsortedBloomBuilder::add(hash64_t hash)
{
    if (hashes)
    {
        if (!hash)
        {
            // Something that genuinely hashes to zero cannot be handled - so just mark the builder as invalid
            free(hashes);
            hashes = nullptr;
            tableCount = 0;
        }
        else
        {
            unsigned pos = hash % tableSize;
            for (;;)
            {
                hash64_t val = hashes[pos];
                if (!val)
                    break;
                if (val== hash)
                    return true;
                pos++;
                if (pos == tableSize)
                    pos = 0;
            }
            if (tableCount==maxHashes)
            {
                free(hashes);
                hashes = nullptr;
                tableCount = 0;
            }
            else
            {
                hashes[pos] = hash;
                tableCount++;
            }
        }
    }
    return hashes != nullptr;
}

bool UnsortedBloomBuilder::valid() const
{
    return tableCount != 0;
}

const BloomFilter * UnsortedBloomBuilder::build() const
{
    if (!valid())
        return nullptr;
    BloomFilter *b = new BloomFilter(tableCount, probability);
    for (unsigned idx = 0; idx < tableSize; idx++)
    {
        hash64_t val = hashes[idx];
        if (val)
            b->add(val);
    }
    return b;
}

extern jhtree_decl IBloomBuilder *createBloomBuilder(const IBloomBuilderInfo &helper)
{
    __uint64 fields = helper.getBloomFields();
    if (!(fields & (fields+1)))   // only true if all the ones are at the lsb end...
        return new SortedBloomBuilder(helper);
    else
        return new UnsortedBloomBuilder(helper);
}

extern jhtree_decl IRowHasher *createRowHasher(const RtlRecord &recInfo, __uint64 fields)
{
    if (!(fields & (fields-1)))  // Only one bit set
    {
        unsigned field = ffsll(fields)-1;
        if (recInfo.isFixedOffset(field) && recInfo.queryType(field)->isFixedSize())
           return new SimpleRowHasher(recInfo, fields, recInfo.getFixedOffset(field), recInfo.queryType(field)->getMinSize()); // Specialize to speed up most common case
    }
    else if (!(fields & (fields+1)))   // only true if all the ones are at the lsb end...
    {
        unsigned lastField = ffsll(fields+1)-2;
        if (recInfo.isFixedOffset(lastField) && recInfo.queryType(lastField)->isFixedSize())
           return new SimpleRowHasher(recInfo, fields, 0, recInfo.queryType(lastField)->getMinSize()); // Specialize to speed up another common case - fixed-size block at start
    }
    return new RowHasher(recInfo, fields);
}


#ifdef _USE_CPPUNIT
#include "unittests.hpp"

class BloomTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BloomTest);
    CPPUNIT_TEST(testSortedBloom);
    CPPUNIT_TEST(testUnsortedBloom);
    CPPUNIT_TEST(testFailedSortedBloomBuilder);
    CPPUNIT_TEST(testFailedUnsortedBloomBuilder);
    CPPUNIT_TEST_SUITE_END();

    const unsigned count = 1000000;
    void testSortedBloom()
    {
        SortedBloomBuilder b(count, 0.01);
        for (unsigned val = 0; val < count; val++)
        {
            b.add(rtlHash64Data(sizeof(val), &val, HASH64_INIT));
            b.add(rtlHash64Data(sizeof(val), &val, HASH64_INIT));
        }
        Owned<const BloomFilter> f = b.build();
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

    void testUnsortedBloom()
    {
        UnsortedBloomBuilder b(count, 0.01);
        for (unsigned val = 0; val < count; val++)
            b.add(rtlHash64Data(sizeof(val), &val, HASH64_INIT));
        for (unsigned val = 0; val < count; val++)
            b.add(rtlHash64Data(sizeof(val), &val, HASH64_INIT));
        Owned<const BloomFilter> f = b.build();
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

    void testFailedSortedBloomBuilder()
    {
        SortedBloomBuilder b1(0, 0.01);
        ASSERT(!b1.valid())
        ASSERT(!b1.add(0))
        SortedBloomBuilder b2(1, 0.01);
        ASSERT(b2.add(1))
        ASSERT(!b2.add(2))
        SortedBloomBuilder b3(10, 0.01);
        ASSERT(b3.add(1))
        ASSERT(b3.add(0))  // ok to add 0 to sorted bloom tables
        ASSERT(b3.add(2))
    }

    void testFailedUnsortedBloomBuilder()
    {
        UnsortedBloomBuilder b1(0, 0.01);
        ASSERT(!b1.valid())
        ASSERT(!b1.add(0))
        UnsortedBloomBuilder b2(1, 0.01);
        ASSERT(b2.add(1))
        ASSERT(!b2.add(2))
        UnsortedBloomBuilder b3(10, 0.01);
        ASSERT(b3.add(1))
        ASSERT(!b3.add(0))   // Not ok to add hash value 0 to unsorted
        ASSERT(!b3.add(2))
    }


};

CPPUNIT_TEST_SUITE_REGISTRATION( BloomTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( BloomTest, "BloomTest" );

#endif
