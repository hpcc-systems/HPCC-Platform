/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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


#include "jlib.hpp"
#include "jset.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

//-----------------------------------------------------------------------

// Simple BitSet // 0 based all, intermediate items exist, operations threadsafe and atomic

class CBitSet : public CInterface, implements IBitSet
{
public:
    IMPLEMENT_IINTERFACE;
protected:
    //unsigned seems to be most efficient, and required for __builtin_ffs below
    typedef unsigned bits_t;
    enum { BitsPerItem = sizeof(bits_t) * 8 };
    ArrayOf<bits_t> bits;
    mutable CriticalSection crit;

public:
    CBitSet() { }
    CBitSet(MemoryBuffer &buffer)
    {
        deserialize(buffer);
    }
    void set(unsigned n,bool val) 
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        CriticalBlock block(crit);
        if (i>=bits.ordinality()) {
            if (!val)
                return; // don't bother
            while (i>bits.ordinality())
                bits.append(0);
            bits.append(t);
        }
        else {
            bits_t m=bits.item(i);
            if (val)
                m |= t;
            else 
                m &= ~t;
            bits.replace(m,i);
        }
    }
        
    bool invert(unsigned n) 
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        CriticalBlock block(crit);
        bool ret;
        if (i>=bits.ordinality()) {
            while (i>bits.ordinality())
                bits.append(0);
            bits.append(t);
            ret = true;
        }
        else {
            bits_t m=bits.item(i);
            ret = ((m&t)==0);
            if (ret)
                m |= t;
            else 
                m &= ~t;
            bits.replace(m,i);
        }
        return ret;
    }
        
    bool test(unsigned n) 
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        CriticalBlock block(crit);
        if (i<bits.ordinality()) {
            bits_t m=bits.item(i);
            if (m&t)
                return true;
        }
        return false;
    }
        
    bool testSet(unsigned n,bool val) 
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        CriticalBlock block(crit);
        bool ret;
        if (i>=bits.ordinality()) {
            ret = false;
            if (!val)
                return false; // don't bother
            while (i>bits.ordinality())
                bits.append(0);
            bits.append(t);
        }
        else {
            bits_t m=bits.item(i);
            ret = (m&t)!=0;
            if (val)
                m |= t;
            else 
                m &= ~t;
            bits.replace(m,i);
        }
        return ret;
    }

    unsigned _scan(unsigned from,bool tst,bool scninv)
    {
        bits_t noMatchMask=tst?0:(bits_t)-1;
        unsigned j=from%BitsPerItem;
        CriticalBlock block(crit);
        // returns index of first = val >= from
        unsigned n=bits.ordinality();
        unsigned i;
        for (i=from/BitsPerItem;i<n;i++) {
            bits_t m=bits.item(i);
            if (m!=noMatchMask)
            {
#if defined(__GNUC__)
                //Use the __builtin_ffs instead of a loop to find the first bit set/cleared
                bits_t testMask = m;
                if (j != 0)
                {
                    //Set all the bottom bits to the value we're not searching for
                    bits_t mask = (((bits_t)1)<<j)-1;
                    if (tst)
                        testMask &= ~mask;
                    else
                        testMask |= mask;

                    //May possibly match exactly - if so continue main loop
                    if (testMask==noMatchMask)
                    {
                        j = 0;
                        continue;
                    }
                }

                //Guaranteed a match at this point
                if (tst)
                {
                    //Returns one plus the index of the least significant 1-bit of testMask (testMask != 0)
                    unsigned pos = __builtin_ffs(testMask)-1;
                    if (scninv) {
                        bits_t t = ((bits_t)1)<<pos;
                        m &= ~t;
                        bits.replace(m,i);
                    }
                    return i*BitsPerItem+pos;
                }
                else
                {
                    //Same as above but invert the bitmask
                    unsigned pos = __builtin_ffs(~testMask)-1;
                    if (scninv) {
                        bits_t t = ((bits_t)1)<<pos;
                        m |= t;
                        bits.replace(m,i);
                    }
                    return i*BitsPerItem+pos;
                }
#else
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++) {
                    if (t&m) {
                        if (tst) {
                            if (scninv) {
                                m &= ~t;
                                bits.replace(m,i);
                            }
                            return i*BitsPerItem+j;
                        }
                    }
                    else {
                        if (!tst) {
                            if (scninv) {
                                m |= t;
                                bits.replace(m,i);
                            }
                            return i*BitsPerItem+j;
                        }
                    }
                    t <<= 1;
                }
#endif
            }
            j = 0;
        }
        if (tst) 
            return (unsigned)-1;
        unsigned ret = n*BitsPerItem;
        if (n*BitsPerItem<from)
            ret = from;
        if (scninv)
            set(ret,true);
        return ret;
    }

    unsigned scan(unsigned from,bool tst)
    {
        return _scan(from,tst,false);
    }

    unsigned scanInvert(unsigned from,bool tst) // like scan but inverts bit as well
    {
        return _scan(from,tst,true);
    }

    void _incl(unsigned lo, unsigned hi,bool val)
    {
        if (hi<lo)
            return;
        unsigned j=lo%BitsPerItem;
        unsigned nb=(hi-lo)+1;
        CriticalBlock block(crit);
        unsigned n=bits.ordinality();
        unsigned i;
        for (i=lo/BitsPerItem;i<n;i++) {
            bits_t m;
            if ((nb>=BitsPerItem)&&(j==0)) {
                m = i;
                nb -= BitsPerItem;
            }
            else {
                m=bits.item(i);
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++) {
                    if (val)
                        m |= t;
                    else
                        m &= ~t;
                    if (--nb==0)
                        break;
                    t <<= 1;
                }
            }
            bits.replace(m,i);
            if (nb==0)
                return;
            j = 0;
        }
        if (val) {
            while (nb>=BitsPerItem) {
                bits.append((bits_t)-1);
                nb-=BitsPerItem;
            }
            if (nb>0) {
                bits_t m=0;
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++) {
                    m |= t;
                    if (--nb==0)
                        break;
                    t <<= 1;
                }
                bits.append(m);
            }
        }
    }

    void incl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,true);
    }

    void excl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,false);
    }

    void reset()
    {
        CriticalBlock block(crit);
        bits.kill();
    }

    void serialize(MemoryBuffer &buffer) const
    {
        CriticalBlock block(crit);
        buffer.append(bits.ordinality());
        ForEachItemIn(b, bits)
            buffer.append(bits.item(b));
    }

    void deserialize(MemoryBuffer &buffer)
    {
        CriticalBlock block(crit);
        bits.kill();
        unsigned count;
        buffer.read(count);
        if (count)
        {
            bits.ensure(count);
            while (count--)
            {
                bits_t b;
                buffer.read(b);
                bits.append(b);
            }
        }
    }
};

extern jlib_decl IBitSet *createBitSet()
{
    return new CBitSet();
}

extern jlib_decl IBitSet *deserializeIBitSet(MemoryBuffer &mb)
{
    return new CBitSet(mb);
}



