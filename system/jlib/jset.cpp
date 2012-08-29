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
    UnsignedArray bits;
    mutable CriticalSection crit;

public:
    CBitSet() { }
    CBitSet(MemoryBuffer &buffer)
    {
        deserialize(buffer);
    }
    void set(unsigned n,bool val) 
    {
        CriticalBlock block(crit);
        unsigned t=1<<(n%32);
        unsigned i=n/32;
        if (i>=bits.ordinality()) {
            if (!val)
                return; // don't bother
            while (i>bits.ordinality())
                bits.append(0);
            bits.append(t);
        }
        else {
            unsigned m=bits.item(i);
            if (val)
                m |= t;
            else 
                m &= ~t;
            bits.replace(m,i);
        }
    }
        
    bool invert(unsigned n) 
    {
        CriticalBlock block(crit);
        bool ret;
        unsigned t=1<<(n%32);
        unsigned i=n/32;
        if (i>=bits.ordinality()) {
            while (i>bits.ordinality())
                bits.append(0);
            bits.append(t);
            ret = true;
        }
        else {
            unsigned m=bits.item(i);
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
        CriticalBlock block(crit);
        unsigned t=1<<(n%32);
        unsigned i=n/32;
        if (i<bits.ordinality()) {
            unsigned m=bits.item(i);
            if (m&t)
                return true;
        }
        return false;
    }
        
    bool testSet(unsigned n,bool val) 
    {
        CriticalBlock block(crit);
        bool ret;
        unsigned t=1<<(n%32);
        unsigned i=n/32;
        if (i>=bits.ordinality()) {
            ret = false;
            if (!val)
                return false; // don't bother
            while (i>bits.ordinality())
                bits.append(0);
            bits.append(t);
        }
        else {
            unsigned m=bits.item(i);
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
        CriticalBlock block(crit);
        // returns index of first = val >= from
        unsigned full=tst?0:0xffffffff;
        unsigned j=from%32;
        unsigned n=bits.ordinality();
        unsigned i;
        for (i=from/32;i<n;i++) {
            unsigned m=bits.item(i);
            if (m!=full) {
                unsigned t = 1<<j;
                for (;j<32;j++) {
                    if (t&m) {
                        if (tst) {
                            if (scninv) {
                                m &= ~t;
                                bits.replace(m,i);
                            }
                            return i*32+j;
                        }
                    }
                    else {
                        if (!tst) {
                            if (scninv) {
                                m |= t;
                                bits.replace(m,i);
                            }
                            return i*32+j;
                        }
                    }
                    t <<= 1;
                }
            }
            j = 0;
        }
        if (tst) 
            return (unsigned)-1;
        unsigned ret = n*32;
        if (n*32<from)
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
        CriticalBlock block(crit);
        if (hi<lo)
            return;
        unsigned j=lo%32;
        unsigned n=bits.ordinality();
        unsigned nb=(hi-lo)+1;
        unsigned i;
        for (i=lo/32;i<n;i++) {
            unsigned m;
            if ((nb>=32)&&(j==0)) {
                m = i;
                nb -= 32;
            }
            else {
                m=bits.item(i);
                unsigned t = 1<<j;
                for (;j<32;j++) {
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
            while (nb>=32) {
                bits.append(0xffffffff);
                nb-=32;
            }
            if (nb>0) {
                unsigned m=0;
                unsigned t = 1<<j;
                for (;j<32;j++) {
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
                unsigned b;
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



