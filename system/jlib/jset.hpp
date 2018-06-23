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



#ifndef __JSET__
#define __JSET__

#include "jiface.hpp"

#if defined (_WIN32)
#include <intrin.h>
#endif

//Return the number of trailing zeros. Deliberately undefined if value == 0
inline unsigned countTrailingUnsetBits(unsigned value)
{
    dbgassertex(value != 0);
#if defined(__GNUC__)
    return __builtin_ctz(value);
#elif defined (_WIN32)
    unsigned long index;
    _BitScanForward(&index, value);
    return (unsigned)index;
#else
    unsigned mask = 1U;
    unsigned i;
    for (i=0; i < sizeof(unsigned)*8; i++)
    {
        if (value & mask)
            return i;
        mask = mask << 1;
    }
    return i;
#endif
}

inline unsigned countTrailingUnsetBits(unsigned __int64 value)
{
    dbgassertex(value != 0);
#if defined(__GNUC__)
    return __builtin_ctzll(value);
#elif defined (_WIN32)
    //There doesn't seem to be a 64bit version of _BitScanForward() generally available
    unsigned long index;
    if ((unsigned)value)
    {
        _BitScanForward(&index, (unsigned)value);
        return (unsigned)index;
    }
    else
    {
        _BitScanForward(&index, (unsigned)(value >> 32));
        return (unsigned)index+32;
    }
#else
    unsigned __int64 mask = 1U;
    unsigned i;
    for (i=0; i < sizeof(mask)*8; i++)
    {
        if (value & mask)
            return i;
        mask = mask << 1;
    }
    return i;
#endif
}

//Return the number of leading zeros. Deliberately undefined if value == 0
inline unsigned countLeadingUnsetBits(unsigned value)
{
    dbgassertex(value != 0);
#if defined(__GNUC__)
    return __builtin_clz(value);
#elif defined (_WIN32)
    unsigned long index;
    _BitScanReverse(&index, value);
    return (unsigned)((sizeof(unsigned)*8)-1 - index);
#else
    unsigned mask = 1U << ((sizeof(unsigned)*8)-1);
    unsigned i;
    for (i=0; i < sizeof(unsigned)*8; i++)
    {
        if (value & mask)
            return i;
        mask = mask >> 1;
    }
    return i;
#endif
}

//Return the number of bits including the first non-zero bit.  Undefined if value == 0
inline unsigned getMostSignificantBit(unsigned value)
{
    dbgassertex(value != 0);
#if defined(__GNUC__)
    return (sizeof(unsigned)*8) - __builtin_clz(value);
#elif defined (_WIN32)
    unsigned long index;
    _BitScanReverse(&index, value);
    return (unsigned)index+1;
#else
    unsigned mask = 1U << ((sizeof(unsigned)*8)-1);
    unsigned i;
    for (i=0; i < sizeof(unsigned)*8; i++)
    {
        if (value & mask)
            return sizeof(unsigned)*8-i;
        mask = mask >> 1;
    }
    return 0;
#endif
}

#if defined (_WIN32)
// These are standard in glibc
inline int ffsll(__uint64 i)
{
    return i ? countTrailingUnsetBits(i) + 1 : 0;
}

inline int ffs(unsigned i)
{
    return i ? countTrailingUnsetBits(i) + 1 : 0;
}
#endif

interface jlib_decl IBitSet : public IInterface 
{
    virtual void set(unsigned n,bool val=true)      = 0;
    virtual bool invert(unsigned n)                 = 0;            // returns inverted value
    virtual bool test(unsigned n)                   = 0;
    virtual bool testSet(unsigned n,bool val=true)  = 0;            // returns prev val
    virtual unsigned scan(unsigned from,bool tst)       = 0;        // returns index of first = val >= from
    virtual unsigned scanInvert(unsigned from,bool tst) = 0;        // like scan but inverts bit as well
    virtual void incl(unsigned lo, unsigned hi)     = 0;
    virtual void excl(unsigned lo, unsigned hi)     = 0;
    virtual void reset() = 0;
    virtual void serialize(MemoryBuffer &buffer) const = 0;
};

// type of underlying bit storage, exposed so thread-unsafe version can know boundaries
typedef unsigned bits_t;
enum { BitsPerItem = sizeof(bits_t) * 8 };


// Simple BitSet // 0 based, all intermediate items exist, operations threadsafe and atomic
extern jlib_decl IBitSet *createThreadSafeBitSet();
extern jlib_decl IBitSet *deserializeThreadSafeBitSet(MemoryBuffer &mb);

/* Not thread safe, but can be significantly faster than createThreadSafeBitSet
 * Client provides a fixed block of memory used for the bit set, threads must ensure they do not set bits
 * in parallel within the same bits_t space.
 * IOW, e.g. bits 0-sizeof(bits_t) must be set from only 1 thread at a time.
 */
extern jlib_decl IBitSet *createBitSet(size32_t memSize, const void *mem, bool reset=true);
// These forms allows the size of the bit set to be dynamic. No guarantees about threading.
extern jlib_decl IBitSet *createBitSet(unsigned initialBits);
extern jlib_decl IBitSet *createBitSet();
extern jlib_decl IBitSet *deserializeBitSet(MemoryBuffer &mb);
// returns number of bytes required to represent numBits in memory
extern jlib_decl size32_t getBitSetMemoryRequirement(unsigned numBits);




#endif
