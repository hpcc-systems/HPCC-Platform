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

#ifndef _BLOOM_INCL
#define _BLOOM_INCL

#include "jhtree.hpp"

/**
 *   A BloomFilter object is used to create or test a Bloom filter - this can be used to quickly determine whether a value has been added to the filter,
 *   giving some false positives but no false negatives.
 */

class jhtree_decl BloomFilter : public CInterface
{
public:
    /*
     * Create an empty bloom filter
     *
     * @param cardinality Expected number of values to be added. This will be used to determine the appropriate size and hash count
     * @param probability Desired probability of false positives. This will be used to determine the appropriate size and hash count
     */
    BloomFilter(unsigned cardinality, double probability=0.1);
    /*
     * Create a bloom filter from a previously-generated table. Parameters must batch those used when building the table.
     *
     * @param numHashes  Number of hashes to use for each lookup.
     * @param tableSize  Size (in bytes) of the table
     * @param table      Bloom table. Note that the BloomFilter object will take ownership of this memory, so it must be allocated on the heap.
     */
    BloomFilter(unsigned numHashes, unsigned tableSize, byte *table);
    /*
     * BloomFilter destructor
     */
    virtual ~BloomFilter();
    /*
     * Add a value to the filter
     *
     * @param hash   The hash of the value to be added
     */
    void add(hash64_t hash);
    /*
     * Test if a value has been added to the filter (with some potential for false-positives)
     *
     * @param hash   The hash of the value to be tested.
     * @return       False if the value is definitely not present, otherwise true.
     */
    bool test(hash64_t hash) const;
    /*
     * Add a value to the filter, by key
     *
     * @param len   The length of the key
     * @param val   The key data
     */
    inline void add(size32_t len, const void *val) { add(rtlHash64Data(len, val, HASH64_INIT)); }
    /*
     * Test if a value has been added to the filter (with some potential for false-positives), by key
     *
     * @param len   The length of the key
     * @param val   The key data
     * @return       False if the value is definitely not present, otherwise true.
     */
    inline bool test(size32_t len, const void *val) { return test(rtlHash64Data(len, val, HASH64_INIT)); }
    /*
     * Retrieve bloom table size
     *
     * @return       Size, in bytes.
     */
    inline unsigned queryTableSize() const { return numBits / 8; }
    /*
     * Retrieve bloom table hash count
     *
     * @return       Hash count.
     */
    inline unsigned queryNumHashes() const { return numHashes; }
    /*
     * Retrieve bloom table data
     *
     * @return       Table data.
     */
    inline const byte *queryTable() const { return table; }
protected:
    unsigned numBits;
    unsigned numHashes;
    byte *table;
};

class jhtree_decl BloomBuilder
{
public:
    BloomBuilder(unsigned _maxHashes = 1000000);
    const BloomFilter * build(double probability=0.1) const;
    bool add(hash64_t hash);
    inline bool add(size32_t len, const void *val) { return add(rtlHash64Data(len, val, HASH64_INIT)); }
    bool valid() const;
protected:
    ArrayOf<hash64_t> hashes;
    const unsigned maxHashes;
    bool isValid;
};
#endif
