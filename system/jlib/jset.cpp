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


#include "jlib.hpp"
#include "jset.hpp"
#include "jmutex.hpp"
#include "jexcept.hpp"

//-----------------------------------------------------------------------

// NB: The CBitSet*Helper's are primarily avoid the need for virtuals in the implementations
class CBitSetArrayHelper
{
protected:
    ArrayOf<bits_t> bits;

    inline bits_t getBits(unsigned i) const { return bits.item(i); }
    inline void setBits(unsigned i, bits_t m)
    {
        bits.replace(m, i);
    }
    inline void addBitSet(bits_t m)
    {
        bits.append(m);
    }
    inline unsigned getWidth() const { return bits.ordinality(); }
};

class CBitSetMemoryHelper
{
protected:
    bits_t *mem;
    unsigned bitSetUnits;
    MemoryBuffer mb; // Used if mem not provided, also implies expansion allowed
    bool fixedMemory;

    CBitSetMemoryHelper()
    {
        fixedMemory = false;
        bitSetUnits = 0;
        mem = NULL;
    }
    inline bits_t getBits(unsigned i) const { return mem[i]; }
    inline void setBits(unsigned i, bits_t m) { mem[i] = m; }
    inline void addBitSet(bits_t m)
    {
        if (fixedMemory)
            throw MakeStringException(-1, "CBitSet with fixed mem cannot expand");
        mb.append(m);
        mem = (bits_t *)mb.bufferBase();
        ++bitSetUnits;
    }
    inline unsigned getWidth() const { return bitSetUnits; }
};

template <class BITSETHELPER>
class CBitSetBase : public BITSETHELPER, public CSimpleInterfaceOf<IBitSet>
{
protected:
    typedef BITSETHELPER PARENT;
    using PARENT::getWidth;
    using PARENT::getBits;
    using PARENT::setBits;
    using PARENT::addBitSet;

    unsigned _scan(unsigned from, bool tst, bool scninv)
    {
        bits_t noMatchMask=tst?0:(bits_t)-1;
        unsigned j=from%BitsPerItem;
        // returns index of first = val >= from
        unsigned n=getWidth();
        unsigned i = from/BitsPerItem;
        if (j != 0 && i < n)
        {
            bits_t m = getBits(i);
            if (m!=noMatchMask)
            {
                //Evaluate (testMask = tst ? m : ~m); without using a conditional.
                //After this bits will be set wherever we want a match
                bits_t testMask = m ^ noMatchMask;

                //Clear all the bottom bits
                bits_t mask = (((bits_t)1)<<j)-1;
                testMask &= ~mask;

                //May possibly be empty now ignored bits are removed - if so continue main loop
                if (testMask != 0)
                {
                    //Guaranteed a match at this point
                    //Returns one the index of the least significant 1-bit of testMask
                    //(testMask != 0) since that has been checked above (noMatchMask == 0)
                    unsigned pos = countTrailingUnsetBits(testMask);
                    if (scninv)
                    {
                        bits_t t = ((bits_t)1)<<pos;
                        m ^= t;
                        setBits(i, m);
                    }
                    return i*BitsPerItem+pos;
                }
            }
            j = 0;
            i++;
        }
        for (;i<n;i++)
        {
            bits_t m = getBits(i);
            if (m!=noMatchMask)
            {
                //Evaluate (testMask = tst ? m : ~m); without using a conditional.
                //After this bits will be set wherever we want a match
                bits_t testMask = m ^ noMatchMask;

                //Guaranteed a match at this point
                //Returns one the index of the least significant 1-bit of testMask
                //(testMask != 0) since that has been checked above (noMatchMask == 0)
                unsigned pos = countTrailingUnsetBits(testMask);
                if (scninv)
                {
                    bits_t t = ((bits_t)1)<<pos;
                    m ^= t;
                    setBits(i, m);
                }
                return i*BitsPerItem+pos;
            }
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
    void _incl(unsigned lo, unsigned hi, bool val)
    {
        if (hi<lo)
            return;
        unsigned j=lo%BitsPerItem;
        unsigned nb=(hi-lo)+1;
        unsigned n=getWidth();
        unsigned i=lo/BitsPerItem;
        if (n<=i)
        {
            if (!val)
                return;
            while (n < i)
            {
                addBitSet(0);
                ++n;
            }
            if (j>0)
            {
                bits_t m = 0;
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++)
                {
                    m |= t;
                    if (--nb==0)
                        break;
                    t <<= 1;
                }
                addBitSet(m);
            }
            if (nb==0)
                return;
            j = 0;
        }
        else
        {
            for (;i<n;i++)
            {
                bits_t m;
                if ((nb>=BitsPerItem)&&(j==0))
                {
                    if (val)
                        m = (bits_t)-1;
                    else
                        m = 0;
                    nb -= BitsPerItem;
                }
                else
                {
                    m = getBits(i);
                    bits_t t = ((bits_t)1)<<j;
                    for (;j<BitsPerItem;j++)
                    {
                        if (val)
                            m |= t;
                        else
                            m &= ~t;
                        if (--nb==0)
                            break;
                        t <<= 1;
                    }
                }
                setBits(i, m);
                if (nb==0)
                    return;
                j = 0;
            }
        }
        if (val)
        {
            while (nb>=BitsPerItem)
            {
                addBitSet((bits_t)-1);
                nb -= BitsPerItem;
            }
            if (nb>0)
            {
                bits_t m=0;
                bits_t t = ((bits_t)1)<<j;
                for (;j<BitsPerItem;j++)
                {
                    m |= t;
                    if (--nb==0)
                        break;
                    t <<= 1;
                }
                addBitSet(m);
            }
        }
    }
public:
// IBitSet impl.
    virtual void set(unsigned n, bool val)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i = n/BitsPerItem;
        if (i>=getWidth())
        {
            if (!val)
                return; // don't bother
            while (i>getWidth())
                addBitSet(0);
            addBitSet(t);
        }
        else
        {
            bits_t m = getBits(i);
            if (val)
                m |= t;
            else
                m &= ~t;
            setBits(i, m);
        }
    }
    virtual bool invert(unsigned n)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        bool ret;
        if (i>=getWidth())
        {
            while (i>getWidth())
                addBitSet(0);
            addBitSet(t);
            ret = true;
        }
        else
        {
            bits_t m = getBits(i);
            ret = 0 == (m&t);
            if (ret)
                m |= t;
            else
                m &= ~t;
            setBits(i, m);
        }
        return ret;
    }
    virtual bool test(unsigned n)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        if (i<getWidth())
        {
            bits_t m = getBits(i);
            if (m&t)
                return true;
        }
        return false;
    }
    virtual bool testSet(unsigned n, bool val)
    {
        bits_t t=((bits_t)1)<<(n%BitsPerItem);
        unsigned i=n/BitsPerItem;
        if (i>=getWidth())
        {
            if (val)
            {
                while (i>getWidth())
                    addBitSet(0);
                addBitSet(t);
            }
            return false;
        }
        else
        {
            bits_t m = getBits(i);
            if (m&t)
            {
                if (!val)
                    setBits(i, m & ~t);
                return true;
            }
            else
            {
                if (val)
                    setBits(i, m | t);
                return false;
            }
        }
    }
    virtual unsigned scan(unsigned from,bool tst)
    {
        return _scan(from,tst,false);
    }
    virtual unsigned scanInvert(unsigned from,bool tst) // like scan but inverts bit as well
    {
        return _scan(from,tst,true);
    }
    virtual void incl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,true);
    }
    virtual void excl(unsigned lo, unsigned hi)
    {
        _incl(lo,hi,false);
    }
};

size32_t getBitSetMemoryRequirement(unsigned numBits)
{
    unsigned bitSetUnits = (numBits + (BitsPerItem-1)) / BitsPerItem;
    return bitSetUnits * sizeof(bits_t);
}

// Simple BitSet // 0 based all, intermediate items exist, operations threadsafe and atomic
class CBitSetThreadSafe : public CBitSetBase<CBitSetArrayHelper>
{
    typedef CBitSetBase<CBitSetArrayHelper> PARENT;
    mutable CriticalSection crit;
    void deserialize(MemoryBuffer &buffer)
    {
        CriticalBlock block(crit);
        bits.kill();
        unsigned count;
        buffer.read(count);
        if (count)
        {
            bits.ensureCapacity(count);
            while (count--)
            {
                bits_t b;
                buffer.read(b);
                bits.append(b);
            }
        }
    }

public:
    CBitSetThreadSafe()
    {
    }
    CBitSetThreadSafe(MemoryBuffer &buffer)
    {
        deserialize(buffer);
    }
// IBitSet overloads
    virtual void set(unsigned n, bool val)
    {
        CriticalBlock block(crit);
        PARENT::set(n, val);
    }
    virtual bool invert(unsigned n)
    {
        CriticalBlock block(crit);
        return PARENT::invert(n);
    }
    virtual bool test(unsigned n)
    {
        CriticalBlock block(crit);
        return PARENT::test(n);
    }
    virtual bool testSet(unsigned n, bool val)
    {
        CriticalBlock block(crit);
        return PARENT::testSet(n, val);
    }
    virtual unsigned scan(unsigned from, bool tst)
    {
        CriticalBlock block(crit);
        return _scan(from,tst,false);
    }
    virtual unsigned scanInvert(unsigned from, bool tst) // like scan but inverts bit as well
    {
        CriticalBlock block(crit);
        return _scan(from,tst,true);
    }
    virtual void incl(unsigned lo, unsigned hi)
    {
        CriticalBlock block(crit);
        _incl(lo,hi,true);
    }
    virtual void excl(unsigned lo, unsigned hi)
    {
        CriticalBlock block(crit);
        _incl(lo,hi,false);
    }
    virtual void reset()
    {
        CriticalBlock block(crit);
        bits.kill();
    }
    virtual void serialize(MemoryBuffer &buffer) const
    {
        CriticalBlock block(crit);
        buffer.append(bits.ordinality());
        ForEachItemIn(b, bits)
            buffer.append(bits.item(b));
    }
};

extern jlib_decl IBitSet *createThreadSafeBitSet()
{
    return new CBitSetThreadSafe();
}


class CBitSet : public CBitSetBase<CBitSetMemoryHelper>
{
    void deserialize(MemoryBuffer &buffer)
    {
        unsigned count;
        buffer.read(count);
        if (count)
        {
            unsigned bitSets = count/BitsPerItem;
            bitSetUnits = bitSets;
            mem = (bits_t *)mb.reserveTruncate(bitSets*sizeof(bits_t));
        }
        else
        {
            bitSetUnits = 0;
            mem = NULL;
        }
        fixedMemory = false;
    }
public:
    CBitSet()
    {
       // In this form, bitSetUnits and mem will be updated when addBitSet expands mb
    }
    CBitSet(size32_t memSz, const void *_mem, bool reset)
    {
        bitSetUnits = memSz*sizeof(byte) / sizeof(bits_t);
        mem = (bits_t *)_mem;
        if (reset)
            memset(mem, 0, bitSetUnits*sizeof(bits_t));
        fixedMemory = true;
    }
    CBitSet(unsigned initialBits)
    {
        size32_t memRequired = getBitSetMemoryRequirement(initialBits);
        mb.appendBytes(0, memRequired);
        mem = (bits_t *)mb.bufferBase();
        bitSetUnits = memRequired/sizeof(bits_t);
        fixedMemory = false;
    }
    CBitSet(MemoryBuffer &buffer)
    {
        deserialize(buffer);
    }
    virtual void reset()
    {
        memset(mem, 0, sizeof(bits_t)*bitSetUnits);
    }
    virtual void serialize(MemoryBuffer &buffer) const
    {
        buffer.append((unsigned)(BitsPerItem*bitSetUnits));
        buffer.append(bitSetUnits*sizeof(bits_t), mem);
    }
};

extern jlib_decl IBitSet *createBitSet(unsigned maxBits, const void *mem, bool reset)
{
    return new CBitSet(maxBits, mem, reset);
}

extern jlib_decl IBitSet *createBitSet(unsigned initialBits)
{
    return new CBitSet(initialBits);
}

extern jlib_decl IBitSet *createBitSet()
{
    return new CBitSet();
}


// NB: Doubt you'd want to interchange, but serialization formats are compatible
extern jlib_decl IBitSet *deserializeThreadSafeBitSet(MemoryBuffer &mb)
{
    return new CBitSetThreadSafe(mb);
}

extern jlib_decl IBitSet *deserializeBitSet(MemoryBuffer &mb)
{
    return new CBitSet(mb);
}



