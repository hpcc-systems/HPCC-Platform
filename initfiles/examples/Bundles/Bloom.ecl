EXPORT Bloom := MODULE,FORWARD
  IMPORT Std;
  EXPORT Bundle := MODULE(Std.BundleBase)
    EXPORT Name := 'Bloom';
    EXPORT Description := 'Bloom filter implementation, and example of ECL bundle layout';
    EXPORT Authors := ['Richard Chapman','Charles Kaminsky'];
    EXPORT License := 'http://www.apache.org/licenses/LICENSE-2.0';
    EXPORT Copyright := 'Copyright (C) 2013 HPCC Systems';
    EXPORT DependsOn := [];
    EXPORT Version := '1.0.0';
  END;

  /*
   * Create a bloom filter. The parameters will determine the size of the hash table used and
   * the number of hashes required to give the answer. Expect at times up to 7 hashes and
   * consequently 7 lookups per key. If this number of lookups degrades performance, use
   * forceNumHashes, but expect that the bloom filter table size will increase (how much
   * larger depends on the false positive probability).
   *
   * @param falsePositiveProbability   A value between 0.05 and 0.3 representing the desired probability of false positives
   * @param cardinality                The expected approximate number of values to be added to the bloom hash table
   * @param forceNumHashes             Optional parameter to force the number of hashes per lookup
   * @param forceNumBits               Optional parameter to force the number of bits in the hash table
   * @return                           A module exporting bloom filter helper attributes
   */

  EXPORT bloomFilter(UNSIGNED DECIMAL6_3 falsePositiveProbability,
                     UNSIGNED INTEGER8 cardinality,
                     UNSIGNED integer4 forceNumHashes = 0,
                     UNSIGNED integer4 forceNumBits = 0
                     ) := MODULE

    UNSIGNED DECIMAL6_3 fpProb  := IF (falsePositiveProbability >=0.3, 0.3,
                                       IF (falsePositiveProbability <=0.050, 0.05,
                                           falsePositiveProbability));
    UNSIGNED _numBits := IF (forceNumHashes = 0, ROUNDUP(-(cardinality*ln((REAL4) fpProb))/POWER(ln(2),2)), forceNumBits);

    /*
     * Return the actual size of the table used, calculated from the parameters to the module
     * @return Actual table size, in bytes
     */
    EXPORT UNSIGNED tableSize := (_numBits + 7) / 8;

    /*
     * Return the actual size of the table used, calculated from the parameters to the module
     * @return Actual table size, in bits
     */
    EXPORT UNSIGNED numBits := tableSize*8;

    /*
     * Return the actual number of hashes used, calculated from the parameters to the module
     * @return Actual number of hashes
     */
    EXPORT UNSIGNED numHashes := IF (forceNumHashes = 0, (numBits/cardinality)*ln(2), forceNumHashes);

    /*
     * The resulting bloom table
     * @return Bloom table
     */
    EXPORT bloomrec := RECORD
      DATA bits { maxlength(tablesize) };
    END;

    EXPORT TRANSFORM(bloomrec) addBloom(UNSIGNED4 hash1, UNSIGNED4 hash2, UNSIGNED4 _numhashes = numHashes, UNSIGNED _tablesize=tableSize) := BEGINC++
      byte * self = __self.ensureCapacity(_tablesize + sizeof(unsigned), NULL);
      if (*(unsigned *) self == 0)
      {
         *(unsigned *) self = _tablesize;
         memset(self+sizeof(unsigned), 0, _tablesize);
      }
      unsigned long long bit  = 0;
      unsigned long long slot = 0;
      unsigned int shift      = 0;
      unsigned int mask       = 0;
      unsigned int test       = 0;
      const int slotsize = 8;
      unsigned long long numbits = _tablesize * slotsize;
      byte * outbits = self + sizeof(unsigned);
      for (int i=0; i< _numhashes; i++)
      {
        // Kirsch and Mitzenmacher technique (Harvard U)
        bit = (hash1 + (i * hash2)) % numbits;
        slot = bit / slotsize;
        shift = bit % slotsize;
        mask = 1 << shift;
        outbits[slot] |= mask;
      }
      return _tablesize+sizeof(unsigned);
    ENDC++;

    EXPORT transform(bloomrec) addBloom64(UNSIGNED8 hashVal) := addBloom(hashVal >> 32, hashVal & 0xffffffff);

    TRANSFORM(bloomrec) _mergeBloom(bloomrec r, UNSIGNED _tablesize=tableSize) := BEGINC++
      if (!r || !__self.row())
        rtlFail(0, "Unexpected error in _mergeBloom");

      byte * self = __self.ensureCapacity(_tablesize + sizeof(unsigned), NULL);
      unsigned lenR = *(unsigned *) r;
      unsigned lenS = *(unsigned *) self;
      if (lenS != lenR || lenS != _tablesize)
        rtlFail(0, "Unexpected error in _mergeBloom");

      self += sizeof(unsigned);
      r += sizeof(unsigned);
      while (lenR--)
        *self++ |= *r++;
      return _tablesize+sizeof(unsigned);
    ENDC++;

    EXPORT TRANSFORM(bloomrec) mergeBloom(bloomrec r) := _mergeBloom(r);

    EXPORT BOOLEAN testBloom(DATA bits, unsigned4 hash1, unsigned4 hash2, unsigned4 _numhashes = numHashes) := BEGINC++
      #option pure
      const char *bitarray = (const char *) bits;
      unsigned long long bit  = 0;
      unsigned long long slot = 0;
      unsigned int shift      = 0;
      unsigned int mask       = 0;
      unsigned int test       = 0;

      const int slotsize = 8;
      unsigned long long numbits = lenBits * slotsize;

      bool retval = true;
      // Test each bit in the char array
      for (int i=0; i< _numhashes; i++)
      {
        // Kirsch and Mitzenmacher technique (Harvard U)
        bit =  (hash1 + (i * hash2)) % numbits;
        slot  = bit / 8;
        shift = bit % 8;
        mask = 1 << shift;
        test = bitarray[slot] & mask;
        // If a bit isn't on,
        // return false
        if (test == 0)
        {
          retval = false;
          break;
        }
      }
      return retval;
    ENDC++;

    EXPORT boolean testBloom64(DATA bits, unsigned8 hashVal) := testBloom(bits, hashVal >> 32, hashVal & 0xffffffff);
  END;

  EXPORT buildBloomFilter(UNSIGNED DECIMAL6_3 fpProb,
                          UNSIGNED INTEGER8 cardinality,
                          VIRTUAL DATASET ds, <?> ANY keyfields) := MODULE

    SHARED myBloomFilter := bloomFilter(fpProb, cardinality);
    SHARED myBloomRec := myBloomFilter.bloomrec;
    EXPORT UNSIGNED numBits := myBloomFilter.numBits;
    EXPORT UNSIGNED numHashes := myBloomFilter.numHashes;
    EXPORT UNSIGNED tableSize := myBloomFilter.tableSize;

    TRANSFORM(myBloomRec) addTransform(ds L) := myBloomFilter.addBloom64(hash64(L.<keyfields>));
    EXPORT buildDS := AGGREGATE(ds, myBloomRec, addTransform(LEFT), myBloomFilter.mergeBloom(ROWS(RIGHT)[NOBOUNDCHECK 2]));
    EXPORT buildbits := buildDS[1].bits;

    EXPORT lookupBits(DATA bloomFilterData, typeof(ds.<keyfields>) keyval) := FUNCTION
      RETURN myBloomFilter.testBloom64(bloomFilterData, HASH64(keyval));
    END;

    EXPORT lookup(STRING filename, typeof(ds.<keyfields>) keyval) := FUNCTION
      bloomFile := DATASET(filename, myBloomRec, FLAT);
      bloomFilterData := bloomFile[1].bits : ONCE;
      RETURN lookupBits(bloomFilterData, keyval);
    END;
  END;

  EXPORT __selfTest := MODULE
    SHARED testrec := RECORD
      STRING20 name;
    END;

    testdata := DATASET([{'Richard'}], testrec);
    theFilter := buildBloomFilter(0.3, 100, testdata, testdata.name);
    filterBits := theFilter.buildBits;

    EXPORT __selfTest := [
      // OUTPUT('NumBits is ' + theFilter.numBits + '\n');
      // OUTPUT('NumHashes is ' + theFilter.numHashes + '\n');
      ASSERT(theFilter.numBits = 256);
      ASSERT(theFilter.numHashes = 1);
      ASSERT(theFilter.lookupBits(filterBits, 'Richard') = TRUE);
      ASSERT(theFilter.lookupBits(filterBits, 'Lorraine') = FALSE)
    ];
  END;

END;