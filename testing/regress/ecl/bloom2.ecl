//A general purpose module that can be shared by all bloom implementations.

bloomFilter(UNSIGNED DECIMAL6_3 falsePositiveProbability,
                   UNSIGNED INTEGER8 cardinality,
                   unsigned integer4 forceNumHashes = 0,
                   unsigned integer4 forceNumBits = 0
                   ) := MODULE

    //  Number of hashes is calculated to optimize the fp probability and the numBits size.
    //    Expect at times up to 7 hashes and consequently 7 lookups per key.  
    //    If this number of lookups degrades performance, use describeBloomFilterSpecifyHashes.
    //    Expect a larger bloom filter.  How much larger depends on the false positive probability.
    
//    UNSIGNED DECIMAL6_3 fpProb  := if(falsePositiveProbability >=0.3 or falsePositiveProbability <=0.050
 //                                    ERROR('Bloom filter unsuitable for false positive probabilities outside range 0.5 to 0.3'),
  //                                    falsePositiveProbability);    
    UNSIGNED DECIMAL6_3 fpProb  := if(falsePositiveProbability >=0.3, 0.3,
                                     if (falsePositiveProbability <=0.050, 0.05,
                                       falsePositiveProbability));
    unsigned _numBits := if (forceNumHashes = 0, ROUNDUP(-(cardinality*ln((REAL4) fpProb))/POWER(ln(2),2)), forceNumBits);
    EXPORT unsigned tableSize := (_numBits + 7) / 8;
    EXPORT unsigned numBits := tableSize*8;
    EXPORT unsigned numHashes := if (forceNumHashes = 0, (numBits/cardinality)*ln(2), forceNumHashes);

    EXPORT bloomrec := 
      RECORD
        DATA bits { maxlength(tablesize) }; 
      END;

    EXPORT TRANSFORM(bloomrec) addBloom(unsigned4 hash1, unsigned4 hash2, unsigned4 _numhashes = numHashes, unsigned _tablesize=tableSize) := 
      BEGINC++ 
        byte * self = __self.ensureCapacity(_tablesize + sizeof(unsigned), NULL);
        if (*(unsigned *) self == 0) {
           *(unsigned *) self = _tablesize;
           memset(self+sizeof(unsigned), 0, _tablesize);
        }
        unsigned long long bit  = 0;
        unsigned long long slot = 0;
        unsigned int shift      = 0;
        unsigned int mask       = 0;
        const int slotsize = 8;
        unsigned long long numbits = _tablesize * slotsize;
        byte * outbits = self + sizeof(unsigned);
        for (unsigned i=0; i< _numhashes; i++) {
          // Kirsch and Mitzenmacher technique (Harvard U)
          bit   =  (hash1 + (i * hash2)) % numbits;
          slot  = bit / slotsize;
          shift = bit % slotsize;
          mask = 1 << shift;
          outbits[slot] |= mask;
        }
        return _tablesize+sizeof(unsigned);
      ENDC++;

    EXPORT transform(bloomrec) addBloom64(unsigned8 hashVal) := addBloom(hashVal >> 32, hashVal & 0xffffffff);

    TRANSFORM(bloomrec) _mergeBloom(bloomrec r, unsigned _tablesize=tableSize) := 
      BEGINC++
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

    export TRANSFORM(bloomrec) mergeBloom(bloomrec r) := _mergeBloom(r);

    shared boolean testBloom(DATA bits, unsigned4 hash1, unsigned4 hash2, unsigned4 _numhashes = numHashes) := 
      BEGINC++
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
        for (unsigned i=0; i< _numhashes; i++) {
            // Kirsch and Mitzenmacher technique (Harvard U)
            bit   =  (hash1 + (i * hash2)) % numbits;
            slot  = bit / 8;
            shift = bit % 8;
            mask = 1 << shift;
            test = bitarray[slot] & mask;
            // If a bit isn't on,
            // return false
            if (test == 0) {  
                retval = false;
                break;
            }
        }
        return retval;
      ENDC++;

    EXPORT boolean testBloom64(DATA bits, unsigned8 hashVal) := testBloom(bits, hashVal >> 32, hashVal & 0xffffffff);

END;      

buildBloomFilter(UNSIGNED DECIMAL6_3 fpProb,
                        UNSIGNED INTEGER8 cardinality, 
                        VIRTUAL DATASET ds, <?> ANY keyfields) := MODULE
    
    SHARED myBloomFilter := bloomFilter(fpProb, cardinality);
    SHARED myBloomRec := myBloomFilter.bloomrec;
    EXPORT unsigned numBits := myBloomFilter.numBits;
    EXPORT unsigned numHashes := myBloomFilter.numHashes;
    EXPORT unsigned tableSize := myBloomFilter.tableSize;

    transform(myBloomRec) addTransform(ds L) := myBloomFilter.addBloom64(hash64(L.<keyfields>));
    EXPORT buildDS := AGGREGATE(ds, myBloomRec, addTransform(LEFT), myBloomFilter.mergeBloom(ROWS(RIGHT)[NOBOUNDCHECK 2]));
    EXPORT buildbits := buildDS[1].bits;

    EXPORT lookupBits(DATA bloomFilterData, typeof(ds.<keyfields>) keyval) := FUNCTION
      return myBloomFilter.testBloom64(bloomFilterData, HASH64(keyval));
    END;
    
    EXPORT lookup(string filename, typeof(ds.<keyfields>) keyval) := FUNCTION
      bloomFile := DATASET(filename, myBloomRec, FLAT);
      bloomFilterData := bloomFile[1].bits : ONCE;
      return lookupBits(bloomFilterData, keyval);
    END;

END; // module

testrec := RECORD
    STRING20 name;
END;

testdata := DATASET([{'Richard'}], testrec);
theFilter := buildBloomFilter(0.3, 100, testdata, testdata.name);
filterBits := theFilter.buildBits;

'NumBits is '; theFilter.numBits; '\n';
'NumHashes is '; theFilter.numHashes; '\n';

theFilter.lookupBits(filterBits, 'Richard');
theFilter.lookupBits(filterBits, 'Lorraine');
