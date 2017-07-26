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
#include "jsort.hpp"
#include "jexcept.hpp"
#include "rtlkey.hpp"
#include "rtlkey2.hpp"
#include "eclrtl_imp.hpp"
#include "rtlrecord.hpp"

#define KSM_SET             0x01
#define KSM_WILD            0x02
#define KSM_LITTLE_ENDIAN   0x04
#define KSM_SIGNED          0x08
#define KSM_VAROFFSET       0x10
#define KSM_TRANSLATED      0x20



class CKeySegmentMonitor : implements IKeySegmentMonitor, public CInterface
{
protected:
    size32_t size;
    size32_t offset;
    unsigned hash;

public:
    IMPLEMENT_IINTERFACE;
    CKeySegmentMonitor(unsigned _offset, unsigned _size);
    CKeySegmentMonitor(MemoryBuffer &mb)
    {
        mb.read(size).read(offset).read(hash);
    }

    virtual bool matchesBuffer(const void * rawRow) const = 0;
    virtual bool matches(const RtlRow * rawRow) const
    {
        return matchesBuffer(rawRow->queryRow());
    }

    virtual bool increment(void *keyval) const;
    virtual unsigned getOffset() const { return offset; }
    virtual unsigned getSize() const { return size; }
    virtual IKeySegmentMonitor * split(unsigned splitSize) { throwUnexpected(); } // not required in most cases
    virtual bool isWild() const { return false; }
    virtual bool isEmpty() const { return false; }
    virtual void *queryValue() const { return NULL; }
    virtual bool isSigned() const { return false; }
    virtual bool isLittleEndian() const { return false; }
    virtual unsigned numFieldsRequired() const { return 0; }  // Fixed offset segmonitors don't care about field numbers

    virtual int docompare(const void * l, const void * r) const
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        return memcmp(lptr, rptr, size);
    }
    virtual int docompareraw(const void *l, const void *r) const
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        return memcmp(lptr, rptr, size);
    }
    virtual int docompareraw(const RtlRow *l, const RtlRow *r) const
    {
        return docompareraw(l->queryRow(), r->queryRow());
    }

    virtual bool equivalentTo(const IKeySegmentMonitor &other) const 
    {
        return offset==other.getOffset() 
            && size==other.getSize() 
            && isSigned()==other.isSigned() 
            && isLittleEndian()==other.isLittleEndian();
    }

    virtual unsigned queryHashCode() const
    {
        return hash;
    }

    virtual bool setOffset(unsigned _offset)
    {
        offset = _offset;
        return true;
    }

    virtual void setHigh(void *keyval) const;

    virtual bool isSimple() const
    {
        return false; // err on the side of caution
    }

    virtual void copy(void * l, const void * r) const
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        memcpy(lptr, rptr, size);
    }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const
    {
        KeySegmentMonitorSerializeType typ = serializeType();
        assertex(typ!=KSMST_none);
        return mb.append((byte)typ).append(size).append(offset).append(hash);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const = 0;

};

class CDummyKeySegmentMonitor : public CKeySegmentMonitor
{
    bool lisSigned;
    bool lisLittleEndian;

public:
    CDummyKeySegmentMonitor(unsigned _offset, unsigned _size, bool _isSigned, bool _isLittleEndian)
        : CKeySegmentMonitor(_offset, _size), lisSigned(_isSigned), lisLittleEndian(_isLittleEndian)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) lisSigned);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) lisLittleEndian);
    }

    CDummyKeySegmentMonitor(MemoryBuffer &mb)
        : CKeySegmentMonitor(mb)
    {
        mb.read(lisSigned).read(lisLittleEndian);
    }

    virtual IKeySegmentMonitor *clone() const
    {
        return new CDummyKeySegmentMonitor(offset, size, lisSigned, lisLittleEndian);
    }


    virtual void setLow(void *keyval) const { throwUnexpected(); }
    virtual void endRange(void *keyval) const { throwUnexpected(); }
    virtual bool matchesBuffer(const void *keyval) const { throwUnexpected(); }
    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *with) const { throwUnexpected(); }
    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const { throwUnexpected(); }
    virtual unsigned getFlags() const 
    {
        unsigned ret = 0;
        if (lisLittleEndian)
            ret |= KSM_LITTLE_ENDIAN;
        if (lisSigned)
            ret |= KSM_SIGNED;
        return ret;
    }
    virtual bool isSigned() const { return lisSigned; }
    virtual bool isLittleEndian() const { return lisLittleEndian; }
    virtual bool isWellKeyed() const { throwUnexpected(); }
    virtual bool isOptional() const { return true; }
    virtual bool isSimple() const { return true; }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const
    {
        return CKeySegmentMonitor::serialize(mb).append(lisSigned).append(lisLittleEndian);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const { return KSMST_DUMMYKEYSEGMENTMONITOR; } 


};

class CWildKeySegmentMonitor : public CKeySegmentMonitor
{
public:
    CWildKeySegmentMonitor(unsigned _offset, unsigned _size);
    CWildKeySegmentMonitor(MemoryBuffer &mb)
        : CKeySegmentMonitor(mb)
    {
    }

    virtual IKeySegmentMonitor * split(unsigned splitSize);
    virtual bool matchesBuffer(const void *keyval) const;
    virtual int docompare(const void *,const void *) const;
    virtual int docompareraw(const void *,const void *) const;
    virtual void setLow(void *keyval) const;
    virtual void endRange(void *keyval) const;
    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *with) const;
    virtual bool isWild() const { return true; }
    virtual bool isWellKeyed() const { return false; }
    virtual unsigned getFlags() const { return KSM_WILD; }
    virtual bool isOptional() const { return true; }
    virtual IKeySegmentMonitor *clone() const;
    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const { throwUnexpected(); }
    virtual KeySegmentMonitorSerializeType serializeType() const { return KSMST_WILDKEYSEGMENTMONITOR; }

};

class CSetKeySegmentMonitor : public CKeySegmentMonitor
{
private:
    Owned<IStringSet> set;
    mutable CriticalSection cacheCrit;
    mutable bool lastCompareResult;
    mutable bool hasCompareResult;
    mutable char *lastCompareValue;
    bool optional;

public:
    CSetKeySegmentMonitor(bool _optional, IStringSet *set, unsigned _offset, unsigned _size);
    CSetKeySegmentMonitor(MemoryBuffer &mb)
        : CKeySegmentMonitor(mb)
    {
        lastCompareResult = false;
        hasCompareResult = false;
        set.setown(deserializeStringSet(mb));
        mb.read(optional);
        lastCompareValue = new char[size];
    }
    ~CSetKeySegmentMonitor();

// IKeySegmentMonitor
    virtual bool increment(void *keyval) const;
    virtual void setLow(void *keyval) const;
    virtual bool matchesBuffer(const void *keyval) const;
    virtual void endRange(void *keyval) const;
    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *next) const { return NULL; }
    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const;
    virtual unsigned getFlags() const { return KSM_SET; }
    virtual bool isEmpty() const { return set->isEmptySet(); }
    virtual bool isWellKeyed() const;
    virtual bool isOptional() const { return optional; }
    virtual bool isSimple() const { return true; }
    virtual bool isSigned() const { return set->isSigned(); }
    virtual bool isLittleEndian() const { return !set->isBigEndian(); }
    virtual IKeySegmentMonitor *clone() const;

    virtual int docompare(const void * l, const void * r) const
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        return set->memcmp(lptr, rptr, size);
    }

    virtual int docompareraw(const void *l, const void *r) const
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        return set->memcmp(lptr, rptr, size);
    }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const
    {
        CKeySegmentMonitor::serialize(mb);
        set->serialize(mb);
        return mb.append(optional);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const { return KSMST_SETKEYSEGMENTMONITOR; }

};

CKeySegmentMonitor::CKeySegmentMonitor(unsigned _offset, unsigned _size)
{
    size = _size;
    offset = _offset;
    hash = 123456;
    hash = hashc((unsigned char *) &offset, sizeof(offset), hash);
    hash = hashc((unsigned char *) &size, sizeof(size), hash);
}

bool CKeySegmentMonitor::increment(void *bufptr) const
{
    char *ptr = ((char *) bufptr) + offset;
    int i = size;
    while (i--)
    {
        ptr[i]++;
        if (ptr[i]!=0)
            return true;
    }
    return false;
}

void CKeySegmentMonitor::setHigh(void *bufptr) const
{ 
    // NOTE - effectively whenever this is called we are treating the segmonitor as if it was a wild one
    char *ptr = ((char *) bufptr) + offset;
    memset(ptr, 0xff, size); 
}


CWildKeySegmentMonitor::CWildKeySegmentMonitor(unsigned _offset, unsigned _size)
    : CKeySegmentMonitor(_offset, _size)
{
}

IKeySegmentMonitor * CWildKeySegmentMonitor::split(unsigned splitSize)
{
    // Modifies current as well as creating a new one - use with care!
    assert(!IsShared());
    if(splitSize >= size)
        return NULL;
    unsigned splitOffset = offset;
    offset += splitSize;
    size -= splitSize;
    return new CWildKeySegmentMonitor(splitOffset, splitSize);
}

IKeySegmentMonitor *CWildKeySegmentMonitor::clone() const
{   
    return new CWildKeySegmentMonitor(offset, size);
}

bool CWildKeySegmentMonitor::matchesBuffer(const void *keyval) const
{ 
    return true;
}

int CWildKeySegmentMonitor::docompare(const void *l, const void *r) const
{
    return 0;
}

int CWildKeySegmentMonitor::docompareraw(const void *l, const void *r) const
{
    return 0;
}

void CWildKeySegmentMonitor::setLow(void *bufptr) const
{ 
    char *ptr = ((char *) bufptr) + offset;
    memset(ptr, 0, size); 
}

void CWildKeySegmentMonitor::endRange(void *bufptr) const
{ 
    char *ptr = ((char *) bufptr) + offset;
    memset(ptr, 0xff, size); 
}

IKeySegmentMonitor *CWildKeySegmentMonitor::merge(IKeySegmentMonitor *next) const
{
    if (next->isWild())
    {
        assertex(offset + size == next->getOffset());
        return new CWildKeySegmentMonitor(offset, next->getSize()+size);
    }
    else
        return NULL;
}

CSetKeySegmentMonitor::CSetKeySegmentMonitor(bool _optional, IStringSet *_set, unsigned _offset, unsigned _size)
    : set(_set), CKeySegmentMonitor(_offset, _size)
{
    lastCompareValue = new char[_size];
    hasCompareResult = false;
    lastCompareResult = false;
    optional = _optional;
    hash =  FNV_32_HASHONE_VALUE(hash, (byte) set->isSigned());
    hash =  FNV_32_HASHONE_VALUE(hash, (byte) !set->isBigEndian());
}

CSetKeySegmentMonitor::~CSetKeySegmentMonitor()
{
    delete [] lastCompareValue;
}

IKeySegmentMonitor *CSetKeySegmentMonitor::clone() const 
{
    return new CSetKeySegmentMonitor(optional, set.getLink(), offset, size);
}

IKeySegmentMonitor *CSetKeySegmentMonitor::combine(const IKeySegmentMonitor *with) const
{
    assertex(equivalentTo(*with)); // note - badly named - does not mean the condition is equivalent, only the field being compared
    const CSetKeySegmentMonitor *withSet = QUERYINTERFACE(with, const CSetKeySegmentMonitor);
    if (!withSet)
        return with->combine(this); // let the simpler segmonitor do the work
    Owned<IStringSet> resultSet = set->intersectSet(withSet->set);
    return createKeySegmentMonitor(optional, resultSet.getClear(), offset, size);
}

bool CSetKeySegmentMonitor::increment(void *bufptr) const
{
    char *ptr = ((char *) bufptr) + offset;
    bool ok = set->increment(ptr);
    if (ok)
    {
        unsigned nextTransition;
        bool res = set->inRange(ptr, nextTransition);
        if (!res)
        {
            if (-1 == nextTransition) return false;
            set->getTransitionValue(ptr, nextTransition);
        }
    }
    return ok;
}

void CSetKeySegmentMonitor::setLow(void *bufptr) const
{
    char *ptr = ((char *) bufptr) + offset;
    if (set->transitionCount())
        set->getTransitionValue(ptr, 0);
    else
        memset(ptr, 0, size); // MORE - should really trap earlier
}

void CSetKeySegmentMonitor::endRange(void *bufptr) const
{
    char *ptr = ((char *) bufptr) + offset;
    unsigned nextTransition;
    bool res = set->inRange(ptr, nextTransition);
    assertex(res);
    verifyex(set->getTransitionValue(ptr, nextTransition));
}

bool CSetKeySegmentMonitor::matchesBuffer(const void *bufptr) const
{
    // MORE - should investigate sometime how much benefit we get from this caching...

    char *ptr = ((char *) bufptr) + offset;
    CriticalBlock b(cacheCrit);
    if (hasCompareResult && 0 == memcmp(lastCompareValue, ptr, size))
        return lastCompareResult;
    lastCompareResult = set->inRange(ptr);
    memcpy(lastCompareValue, ptr, size);
    hasCompareResult = true;
    return lastCompareResult;
}

bool CSetKeySegmentMonitor::isWellKeyed() const
{
    // This check determines whether or not keyed, opt considers this field to be keyed.
    // The goal is to allow sets but not ranges, slightly complicated by the fact that adjacent values in a set turn into ranges.
    return set->numValues() < 50;
}

class CSingleKeySegmentMonitorBase : public CKeySegmentMonitor
{
protected:
    void *val;
    bool optional;
public:
    CSingleKeySegmentMonitorBase(bool _optional, const void *_val, unsigned _offset, unsigned _size)
        : CKeySegmentMonitor(_offset, _size)
    {
        if (_val)
        {
            val = malloc(_size);
            memcpy(val, _val, _size);
        }
        else
            val = NULL;
        optional = _optional;
    }
    CSingleKeySegmentMonitorBase(bool _optional, unsigned _offset, const void *_val1, unsigned _val1size, const void *_val2, unsigned _val2size)
        : CKeySegmentMonitor(_offset, _val1size+_val2size)
    {
        val = malloc(size);
        memcpy(val, _val1, _val1size);
        memcpy(((char *) val)+_val1size, _val2, _val2size);
        optional = _optional;
    }
    CSingleKeySegmentMonitorBase(bool _optional, unsigned _offset, unsigned _size)
        : CKeySegmentMonitor(_offset, _size)
    {
        val = NULL;
        optional = _optional;
    }

    CSingleKeySegmentMonitorBase(MemoryBuffer &mb)
        : CKeySegmentMonitor(mb)
    {
        bool hasval;
        mb.read(hasval);
        if (hasval) {
            val = malloc(size);
            memcpy(val,mb.readDirect(size),size);
        }
        else
            val = NULL;
        mb.read(optional);
    }

    ~CSingleKeySegmentMonitorBase()
    {
        free(val);
    }

// IKeySegmentMonitor
    virtual bool increment(void *bufptr) const
    {
        // Set to next permitted value above current
        if (docompare(bufptr, ((char *) val)-offset) < 0)
        {
            char *ptr = ((char *) bufptr) + offset;
            memcpy(ptr, val, size);
            return true;
        }
        else
            return false;
    }
    virtual void setLow(void *bufptr) const
    {
        // Set to lowest permitted value
        char *ptr = ((char *) bufptr) + offset;
        memcpy(ptr, val, size);
    }
    virtual bool matchesBuffer(const void *bufptr) const
    {
        // Is current a permitted value?
        char *ptr = ((char *) bufptr) + offset;
        return memcmp(ptr, val, size) == 0;
    }

    virtual void endRange(void *bufptr) const
    {
        // Set to last permitted value in the range that includes current (which is asserted to be valid)
#ifdef DEBUG
        assertex(matchesBuffer(bufptr));
#endif
    }

    virtual void *queryValue() const
    {
        return val; 
    }
    virtual bool isWellKeyed() const { return true; }
    virtual bool isOptional() const { return optional; }
    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *next) const { return NULL; }
    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const { throwUnexpected(); };
    virtual bool isSimple() const { return true; }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const override
    {
        CKeySegmentMonitor::serialize(mb);
        if (val) 
            mb.append((bool)true).append(size,val);
        else
            mb.append((bool)false);
        return mb.append(optional);
    }


};

class CSingleKeySegmentMonitor : public CSingleKeySegmentMonitorBase
{
public:
    CSingleKeySegmentMonitor(bool _optional, const void *_val, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
    }
    CSingleKeySegmentMonitor(bool _optional, unsigned _offset, const void *_val1, unsigned _val1size, const void *_val2, unsigned _val2size)
        : CSingleKeySegmentMonitorBase(_optional, _offset, _val1, _val1size, _val2, _val2size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
    }

    CSingleKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const
    {
        return new CSingleKeySegmentMonitor(optional, val, offset, size);
    }

    virtual unsigned getFlags() const
    {
        return 0;
    }

    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *_next) const
    {
        CSingleKeySegmentMonitor *next = QUERYINTERFACE(_next, CSingleKeySegmentMonitor);
        if(next)
        {
            void *nextval = next->queryValue();
            assertex(nextval != NULL && offset + size == next->getOffset());
            return new CSingleKeySegmentMonitor(optional, offset, val, size, nextval, next->getSize());
        }
        else
            return NULL;
    }

    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const
    {
        assertex(equivalentTo(*with)); // note - badly named - does not mean the condition is equivalent, only the field being compared
        // result is either clone of myself, or emptySet
        if (with->matchesBuffer(val))  // MORE - this looks wrong! Only works with offset=0 ??
            return clone();
        else
            return createEmptyKeySegmentMonitor(optional, offset, size);
    
    }

    virtual IKeySegmentMonitor * split(unsigned splitSize)
    {
        if(splitSize >= size)
            return NULL;
        unsigned splitOffset = offset;
        offset += splitSize;
        size -= splitSize;
        Owned<IKeySegmentMonitor> ret = new CSingleKeySegmentMonitor(optional, val, splitOffset, splitSize);
        void * newval = malloc(size);
        memcpy(newval, static_cast<byte *>(val)+splitSize, size);
        free(val);
        val = newval;
        return ret.getClear();
    }

    virtual bool isSigned() const { return false; }
    virtual bool isLittleEndian() const { return false; }

    virtual KeySegmentMonitorSerializeType serializeType() const { return KSMST_SINGLEKEYSEGMENTMONITOR; }
};

class CSingleBigSignedKeySegmentMonitor : public CSingleKeySegmentMonitorBase
{
public:
    CSingleBigSignedKeySegmentMonitor(bool _optional, const void *_val, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
    }

    CSingleBigSignedKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const
    {
        return new CSingleBigSignedKeySegmentMonitor(optional, val, offset, size);
    }

    virtual unsigned getFlags() const
    {
        return KSM_SIGNED;
    }

    virtual int docompare(const void *l, const void *r) const
    {
        return memcmpbigsigned(((char *) l) + offset, ((char *) r) + offset, size);
    }

    virtual int docompareraw(const void *l, const void *r) const
    {
        return memcmpbigsigned(((char *) l) + offset, ((char *) r) + offset, size);
    }
    virtual bool isSigned() const { return true; }
    virtual bool isLittleEndian() const { return false; }
    virtual KeySegmentMonitorSerializeType serializeType() const { return KSMST_SINGLEBIGSIGNEDKEYSEGMENTMONITOR; }
};

class CSingleLittleSignedKeySegmentMonitor : public CSingleKeySegmentMonitorBase
{
public:
    CSingleLittleSignedKeySegmentMonitor(bool _optional, const void *_val, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
    }

    CSingleLittleSignedKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const
    {
        return new CSingleLittleSignedKeySegmentMonitor(optional, val, offset, size);
    }

    virtual unsigned getFlags() const
    {
        return KSM_LITTLE_ENDIAN | KSM_SIGNED;
    }

    virtual int docompare(const void *l, const void *r) const
    {
        return memcmplittlesigned(((char *) l) + offset, ((char *) r) + offset, size);
    }

    virtual int docompareraw(const void *l, const void *r) const
    {
        return memcmplittlesigned(((char *) l) + offset, ((char *) r) + offset, size);
    }
    virtual bool isSigned() const { return true; }
    virtual bool isLittleEndian() const { return true; }
    virtual KeySegmentMonitorSerializeType serializeType() const { return KSMST_SINGLELITTLESIGNEDKEYSEGMENTMONITOR; }
};

class CSingleLittleKeySegmentMonitor : public CSingleKeySegmentMonitorBase
{
public:
    CSingleLittleKeySegmentMonitor(bool _optional, const void *_val, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
    }

    CSingleLittleKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const
    {
        return new CSingleLittleKeySegmentMonitor(optional, val, offset, size);
    }

    virtual unsigned getFlags() const
    {
        return KSM_LITTLE_ENDIAN;
    }

    virtual int docompare(const void *l, const void *r) const
    {
        return memcmplittleunsigned(((char *) l) + offset, ((char *) r) + offset, size);
    }

    virtual int docompareraw(const void *l, const void *r) const
    {
        return memcmplittleunsigned(((char *) l) + offset, ((char *) r) + offset, size);
    }
    virtual bool isSigned() const { return false; }
    virtual bool isLittleEndian() const { return true; }
    virtual KeySegmentMonitorSerializeType serializeType() const { return KSMST_CSINGLELITTLEKEYSEGMENTMONITOR; }
};


class CIndirectKeySegmentMonitor : implements IKeySegmentMonitor, public CInterface
{
protected:
    unsigned hash;

public:
    CIndirectKeySegmentMonitor(IKeySegmentMonitor * _base, unsigned _offset) 
    {
        base.setown(_base); 
        offset = _offset; 
        hash = hashc((unsigned char *) &offset, sizeof(offset), base->queryHashCode());

    }

    CIndirectKeySegmentMonitor(MemoryBuffer &mb)
    {
        mb.read(offset).read(hash);
        base.setown(deserializeKeySegmentMonitor(mb));
    }

    IMPLEMENT_IINTERFACE

    virtual bool increment(void *keyval) const
    {
        return base->increment((byte *)keyval + offset);
    }
    virtual void setLow(void *keyval) const
    {
        base->setLow((byte *)keyval + offset);
    }
    virtual void setHigh(void *keyval) const
    {
        base->setHigh((byte *)keyval + offset);
    }
    virtual void endRange(void *keyval) const
    {
        base->endRange((byte *)keyval + offset);
    }
    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *with) const { return NULL; }          // MORE?
    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const { throwUnexpected(); }
    virtual IKeySegmentMonitor * split(unsigned splitSize) { return NULL; } // not required in most cases
    virtual bool isWild() const                             { return base->isWild(); }
    virtual unsigned getOffset() const                      { return offset; }
    virtual unsigned getSize() const                        { return base->getSize(); }
    virtual void *queryValue() const                        { return NULL; }
    virtual bool isEmpty() const                            { return base->isEmpty(); }
    virtual bool equivalentTo(const IKeySegmentMonitor &other) const   { return false; }        // MORE?
    virtual bool isSigned() const                           { return base->isSigned(); }
    virtual bool isLittleEndian() const                     { return base->isLittleEndian(); }
    virtual bool isWellKeyed() const                        { return base->isWellKeyed(); }
    virtual bool isOptional() const                         { return base->isOptional(); }
    virtual unsigned numFieldsRequired() const              { return base->numFieldsRequired(); }

    virtual unsigned queryHashCode() const
    {
        return hash;
    }

    virtual bool setOffset(unsigned _offset) { return false; }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const
    {
        KeySegmentMonitorSerializeType typ = serializeType();
        assertex(typ!=KSMST_none);
        mb.append((byte)typ).append(offset).append(hash);
        return base->serialize(mb);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const = 0;


protected:
    Owned<IKeySegmentMonitor> base;
    unsigned offset;
};

// The base monitor provided to this segment monitor is constructed with offsets of 0
// offset refers to where the field would be in an "expanded" version of the record (all variable size - and thus unkeyed - fields being assumed null)

class CNewVarOffsetKeySegmentMonitor : public CIndirectKeySegmentMonitor
{
public:
    CNewVarOffsetKeySegmentMonitor(IKeySegmentMonitor * _base, unsigned _offset, unsigned _fieldIdx)
    : CIndirectKeySegmentMonitor(_base, _offset), fieldIdx(_fieldIdx)
    {
    }

    CNewVarOffsetKeySegmentMonitor(MemoryBuffer &mb)
    : CIndirectKeySegmentMonitor(mb)
    {
        mb.read(fieldIdx);
    }

    virtual bool matches(const RtlRow *keyval) const override
    {
        return base->matchesBuffer(getSegmentBase(keyval));
    }

    virtual unsigned numFieldsRequired() const override { return fieldIdx+1; }

    // We can't presently build in-memory indexes for varoffset fields, as we don't have a way to
    // translate the offsets in the lhs (keybuffer) side of the compare.
    // I suppose if we wanted to lift that we could pass in RtlRow for both parameters?
    // For now, these methods can never be called

    virtual bool matchesBuffer(const void *keyval) const override
    {
        throwUnexpected();
    }
    virtual int docompare(const void * expandedLeft, const void * rawRight) const override
    {
        throwUnexpected();
    }
    virtual void copy(void * expandedRow, const void * rawRight) const override
    {
        throwUnexpected();
    }
    virtual int docompareraw(const void * left, const void * right) const override
    {
        throwUnexpected();
    }

    virtual bool isSimple() const override
    {
        return false;  // Does not support in-memory indexes
    }

    virtual unsigned getFlags() const                       { return KSM_VAROFFSET; }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const override
    {
        return CIndirectKeySegmentMonitor::serialize(mb).append(fieldIdx);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const override
    {
        if ((base->serializeType()==KSMST_none))
            return KSMST_none;
        return KSMST_VAROFFSETKEYSEGMENTMONITOR;
    }


    virtual IKeySegmentMonitor *clone() const override
    {
        return NULL; // MORE - can probably be done now
    }

protected:
    const void *getSegmentBase(const RtlRow *inRec) const
    {
        return ((const byte *) inRec->queryRow()) + inRec->getOffset(fieldIdx);
    }

    unsigned fieldIdx;
};



class CTranslatedKeySegmentMonitor : public CIndirectKeySegmentMonitor
{
public:
    CTranslatedKeySegmentMonitor(IKeySegmentMonitor * _base, unsigned _offset, IKeySegmentFormatTranslator * _formatTranslator) : CIndirectKeySegmentMonitor(_base, _offset) 
    { 
        formatTranslator.setown(_formatTranslator);
        size = base->getSize();
        unsigned formatHash = formatTranslator->queryHashCode();
        hash = hashc((unsigned char *)&formatHash, sizeof(formatHash), hash);
    }

    CTranslatedKeySegmentMonitor(MemoryBuffer &mb)
        : CIndirectKeySegmentMonitor(mb)
    {
        throwUnexpected();
    }

    virtual IKeySegmentMonitor *clone() const
    {
        return NULL;
    }

    virtual bool matchesBuffer(const void *keyval) const
    {
        void *expandedLeft = alloca(size);
        formatTranslator->extractField(expandedLeft, keyval);
        return base->matchesBuffer(expandedLeft);
    }
    virtual int docompare(const void * left,const void * right) const
    {
        void *expandedRight = alloca(size);
        formatTranslator->extractField(expandedRight, right);
        return base->docompare((const byte *)left + offset, expandedRight);
    }

    virtual void copy(void * left,const void * right) const
    {
        void *expandedRight = alloca(size);
        formatTranslator->extractField(expandedRight, right);
        base->copy((byte *)left + offset, expandedRight);
    }

    virtual int docompareraw(const void * left,const void * right) const
    {
        void *expandedLeft = alloca(size);
        void *expandedRight = alloca(size);
        formatTranslator->extractField(expandedLeft, left);
        formatTranslator->extractField(expandedRight, right);
        return base->docompare(expandedLeft, expandedRight);
    }

    virtual bool matches(const RtlRow *keyval) const
    {
        return matchesBuffer(keyval->queryRow());
    }
    virtual int docompare(const void * left,const RtlRow * right) const
    {
        return docompare(left, right->queryRow());
    }

    virtual void copy(void * left,const RtlRow * right) const
    {
        copy(left, right->queryRow());
    }

    virtual int docompareraw(const RtlRow * left,const RtlRow * right) const
    {
        return docompareraw(left->queryRow(), right->queryRow());
    }

    virtual bool isSimple() const
    {
        return false; // No way to serialize/persist at present
    }

    virtual unsigned getFlags() const                       { return KSM_TRANSLATED; }

    virtual KeySegmentMonitorSerializeType serializeType() const
    {
        return KSMST_none;
    }

protected:
    Owned<IKeySegmentFormatTranslator> formatTranslator;
    size_t size;
};

class COverrideableKeySegmentMonitor : public IOverrideableKeySegmentMonitor, public CInterface
{
    const void *overridden;
    unsigned hash;

public:
    IMPLEMENT_IINTERFACE

    COverrideableKeySegmentMonitor(IKeySegmentMonitor * _base) 
    {
        base.setown(_base); 
        overridden = NULL;
        hash = base->queryHashCode();
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 123); 
    }

    COverrideableKeySegmentMonitor(MemoryBuffer &mb) 
    {
        mb.read(hash);
        base.setown(deserializeKeySegmentMonitor(mb)); 
        overridden = NULL;
    }

    virtual void setOverrideBuffer(const void *ptr)
    {
        overridden = ptr;
    }

    virtual unsigned queryHashCode() const
    {
        return hash;
    }

    virtual bool matchesBuffer(const void *keyval) const
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            return memcmp((char *) keyval+offset, (char *) overridden+offset, base->getSize()) == 0;
        }
        else
            return base->matchesBuffer(keyval);
    }
    virtual bool matches(const RtlRow *keyval) const
    {
        return matchesBuffer(keyval->queryRow());
    }

    virtual bool increment(void *keyval) const
    {
        if (overridden)
        {
            // Set to next permitted value above current
            unsigned offset = base->getOffset();
            if (memcmp((char *) keyval+offset, (char *) overridden+offset, base->getSize()) < 0)
            {
                memcpy((char *) keyval+offset, (char *) overridden+offset, base->getSize());
                return true;
            }
            return false;
        }
        else
            return base->increment(keyval);
    }
    virtual void setLow(void *keyval) const
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            memcpy((char *) keyval+offset, (char *) overridden+offset, base->getSize());
        }
        else
            base->setLow(keyval);
    }
    virtual void setHigh(void *keyval) const
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            memcpy((char *) keyval+offset, (char *) overridden+offset, base->getSize());
        }
        else
            base->setHigh(keyval);
    }
    virtual void endRange(void *keyval) const
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            memcpy((char *) keyval+offset, (char *) overridden+offset, base->getSize());
        }
        base->endRange(keyval);
    }
    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *with) const { return NULL; }          // MORE?
    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const { throwUnexpected(); }
    virtual IKeySegmentMonitor * split(unsigned splitSize) { return NULL; } // not required in most cases
    virtual bool isWild() const                             { return overridden ? false : base->isWild(); }
    virtual unsigned getOffset() const                      { return base->getOffset(); }
    virtual unsigned getSize() const                        { return base->getSize(); }
    virtual void *queryValue() const                        { return NULL; }
//  virtual unsigned getFlags() const                       { return base->getFlags(); }
    virtual bool isEmpty() const                            { return base->isEmpty(); }
    virtual bool equivalentTo(const IKeySegmentMonitor &other) const   { return false; }        // MORE?
    virtual bool isSigned() const                           { return base->isSigned(); }
    virtual bool isLittleEndian() const                     { return base->isLittleEndian(); }
    virtual bool isWellKeyed() const                        { return overridden ? true : base->isWellKeyed(); }
    virtual bool isOptional() const                         { return base->isOptional(); }
    virtual unsigned numFieldsRequired() const              { return base->numFieldsRequired(); }

    virtual int docompare(const void * expandedLeft, const void *rawRight) const
    {
        return base->docompare(expandedLeft, rawRight);
    }
    virtual void copy(void * expandedRow, const void *rawRight) const
    {
        base->copy(expandedRow, rawRight); // MORE - is this right?
    }
    virtual int docompareraw(const void * left, const void * right) const
    {
        return base->docompare(left, right);
    }

    virtual int docompare(const void * expandedLeft, const RtlRow *rawRight) const
    {
        return base->docompare(expandedLeft, rawRight);
    }
    virtual void copy(void * expandedRow, const RtlRow *rawRight) const
    {
        base->copy(expandedRow, rawRight); // MORE - is this right?
    }
    virtual int docompareraw(const RtlRow * left, const RtlRow * right) const
    {
        return base->docompare(left, right);
    }
virtual bool setOffset(unsigned _offset)
    {
        throwUnexpected();
    }
    virtual bool isSimple() const
    {
        return base->isSimple();
    }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const
    {
        KeySegmentMonitorSerializeType subtyp = base->serializeType();
        assertex(subtyp!=KSMST_none);
        mb.append((byte)KSMST_OVERRIDEABLEKEYSEGMENTMONITOR).append(hash);
        return base->serialize(mb);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const
    {
        if ((base->serializeType()==KSMST_none)||overridden)   // don't think we can support overridden (TBD revisit)
            return KSMST_none;
        return KSMST_OVERRIDEABLEKEYSEGMENTMONITOR;
    }

    virtual IKeySegmentMonitor *clone() const
    {
        return NULL;
    }

protected:
    Owned<IKeySegmentMonitor> base;
};


ECLRTL_API IStringSet *createRtlStringSet(size32_t size)
{
    return createStringSet(size);
}

ECLRTL_API IStringSet *createRtlStringSetEx(size32_t size, bool bigEndian, bool isSigned)
{
    return createStringSet(size, bigEndian, isSigned);
}

ECLRTL_API IStringSet * rtlUnionSet(IStringSet * lhs, IStringSet * rhs)
{
    if (lhs->isEmptySet())
        return LINK(rhs);
    else if (lhs->isFullSet())
        return LINK(lhs);

    if (rhs->isEmptySet())
        return LINK(lhs);
    else if (rhs->isFullSet())
        return LINK(rhs);

    return lhs->unionSet(rhs);
}

ECLRTL_API IStringSet * rtlIntersectSet(IStringSet * lhs, IStringSet * rhs)
{
    if (lhs->isFullSet())
        return LINK(rhs);
    else if (lhs->isEmptySet())
        return LINK(lhs);

    if (rhs->isFullSet())
        return LINK(lhs);
    else if (rhs->isEmptySet())
        return LINK(rhs);

    return lhs->intersectSet(rhs);
}


IKeySegmentMonitor *createKeySegmentMonitor(bool optional, IStringSet *set, unsigned _offset, unsigned _size)
{
    if (!set)
        return new CWildKeySegmentMonitor(_offset, _size);

    Owned<IStringSet> removeSet = set; // make sure set is released if optimized out.
    if (set->isSingleValue())
    {
        void *data = alloca(_size);
        set->getTransitionValue(data, 0);
        if (set->isSigned())
        {
            if (set->isBigEndian())
                return createSingleBigSignedKeySegmentMonitor(optional, _offset, _size, data);
            else
                return createSingleLittleSignedKeySegmentMonitor(optional, _offset, _size, data);
        }
        else
        {
            if (set->isBigEndian())
                return createSingleKeySegmentMonitor(optional, _offset, _size, data);
            else
                return createSingleLittleKeySegmentMonitor(optional, _offset, _size, data);
        }
    }
    else if (set->isFullSet())
        return new CWildKeySegmentMonitor(_offset, _size);
    else
        return new CSetKeySegmentMonitor(optional, removeSet.getClear(), _offset, _size);
}

ECLRTL_API IStringSet *createRtlStringValue(size32_t size, const char * value)
{
    IStringSet * set = createStringSet(size);
    set->addRange(value, value);
    return set;
}

IKeySegmentMonitor *createWildKeySegmentMonitor(unsigned _offset, unsigned _size)
{
    return new CWildKeySegmentMonitor(_offset, _size);
}

IKeySegmentMonitor *createEmptyKeySegmentMonitor(bool optional, unsigned _offset, unsigned _size)
{
    return new CSetKeySegmentMonitor(optional, createStringSet(_size), _offset, _size);
}

ECLRTL_API IKeySegmentMonitor *createSingleKeySegmentMonitor(bool optional, unsigned offset, unsigned size, const void * value)
{
    return new CSingleKeySegmentMonitor(optional, value, offset, size);
}

ECLRTL_API IOverrideableKeySegmentMonitor *createOverrideableKeySegmentMonitor(IKeySegmentMonitor *base)
{
    return new COverrideableKeySegmentMonitor(base);
}

ECLRTL_API IKeySegmentMonitor *createSingleBigSignedKeySegmentMonitor(bool optional, unsigned offset, unsigned size, const void * value)
{
    return new CSingleBigSignedKeySegmentMonitor(optional, value, offset, size);
}

ECLRTL_API IKeySegmentMonitor *createSingleLittleSignedKeySegmentMonitor(bool optional, unsigned offset, unsigned size, const void * value)
{
    // MORE - common int sizes 1,2,4 (8?) might be better done with dedicated subclasses
    return new CSingleLittleSignedKeySegmentMonitor(optional, value, offset, size);
}

ECLRTL_API IKeySegmentMonitor *createSingleLittleKeySegmentMonitor(bool optional, unsigned offset, unsigned size, const void * value)
{
    // MORE - common int sizes 1,2,4 (8?) might be better done with dedicated subclasses
    return new CSingleLittleKeySegmentMonitor(optional, value, offset, size);
}

ECLRTL_API IKeySegmentMonitor *createDummyKeySegmentMonitor(unsigned _offset, unsigned _size, bool isSigned, bool isLittleEndian)
{
    if (isSigned)
        if (isLittleEndian)
            return new CSingleLittleSignedKeySegmentMonitor(false, NULL, _offset, _size);
        else
            return new CSingleBigSignedKeySegmentMonitor(false, NULL, _offset, _size);
    else
        if (isLittleEndian)
            return new CSingleLittleKeySegmentMonitor(false, NULL, _offset, _size);
        else
            return new CSingleKeySegmentMonitor(false, NULL, _offset, _size);
        
//  return new CDummyKeySegmentMonitor(_offset, _size, isSigned, isLittleEndian);
}

ECLRTL_API IKeySegmentMonitor *createNewVarOffsetKeySegmentMonitor(IKeySegmentMonitor * base, unsigned offset, unsigned fieldIdx)
{
    return new CNewVarOffsetKeySegmentMonitor(base, offset, fieldIdx);
}

ECLRTL_API IKeySegmentMonitor *createTranslatedKeySegmentMonitor(IKeySegmentMonitor * base, unsigned offset, IKeySegmentFormatTranslator * translator)
{
    return new CTranslatedKeySegmentMonitor(base, offset, translator);
}

ECLRTL_API IKeySegmentMonitor *deserializeKeySegmentMonitor(MemoryBuffer &mb)
{
    byte typ;
    mb.read(typ);
    switch ((KeySegmentMonitorSerializeType)typ) {
        case KSMST_WILDKEYSEGMENTMONITOR:
            return new CWildKeySegmentMonitor(mb);
        case KSMST_SETKEYSEGMENTMONITOR:
            return new CSetKeySegmentMonitor(mb);
        case KSMST_SINGLEKEYSEGMENTMONITOR:
            return new CSingleKeySegmentMonitor(mb);
        case KSMST_SINGLEBIGSIGNEDKEYSEGMENTMONITOR:
            return new CSingleBigSignedKeySegmentMonitor(mb);
        case KSMST_SINGLELITTLESIGNEDKEYSEGMENTMONITOR:
            return new CSingleLittleSignedKeySegmentMonitor(mb);
        case KSMST_CSINGLELITTLEKEYSEGMENTMONITOR:
            return new CSingleLittleKeySegmentMonitor(mb);
        case KSMST_DUMMYKEYSEGMENTMONITOR:
            return new CDummyKeySegmentMonitor(mb);
        case KSMST_VAROFFSETKEYSEGMENTMONITOR:
            return new CNewVarOffsetKeySegmentMonitor(mb);
        case KSMST_OVERRIDEABLEKEYSEGMENTMONITOR:
            return new COverrideableKeySegmentMonitor(mb);
    }
    return NULL; // up to caller to check
}

enum StringSetSerializeType
{
    SSST_none,
    SSST_BIGUNSIGNEDSTRINGSET,
    SSST_BIGSIGNEDSTRINGSET,
    SSST_LITTLEUNSIGNEDSTRINGSET,
    SSST_LITTLESIGNEDSTRINGSET,
    SSST_max
};

ECLRTL_API int memcmpbigsigned(const void *l, const void *r, unsigned size)
{
    signed int diff = ((signed char *) l)[0]-((signed char *) r)[0];
    if (diff)
        return diff;
    for(unsigned i = 1; i < size; i++)
    {
        diff = ((unsigned char *) l)[i]-((unsigned char *) r)[i];
        if (diff)
            return diff;
    }
    return 0;
}

ECLRTL_API int memcmplittleunsigned(const void *l, const void *r, unsigned size)
{
    while (size)
    {
        size--;
        int diff = ((unsigned char *) l)[size]-((unsigned char *) r)[size];
        if (diff)
            return diff;
    }
    return 0;
}

ECLRTL_API int memcmplittlesigned(const void *l, const void *r, unsigned size)
{
    size--;
    signed int diff = ((signed char *) l)[size]-((signed char *) r)[size];
    if (diff)
        return diff;
    while (size)
    {
        size--;
        diff = ((unsigned char *) l)[size]-((unsigned char *) r)[size];
        if (diff)
            return diff;
    }
    return 0;
}

class CStringSet : implements IStringSet, public CInterface
{
protected:
    size32_t size;
    IArrayOf<ITransition> transitions;

    IStringSet *unionOrIntersect(IStringSet *r, bool isUnion);

    virtual CStringSet *createEmptySet() = 0;
    virtual bool decrement(void *val) const = 0;
    virtual bool increment(void *val) const = 0;
    virtual int memcmp(const void *val1, const void *val2, size32_t size) const = 0;
    virtual unsigned getCardinality(const void *val1, const void *val2, size32_t size) const = 0;
    virtual void memset(void *ptr, int val, size32_t size) const = 0;
    virtual bool isLowVal(const void *val) const = 0;
    virtual bool isHighVal(const void *val) const = 0;
    bool oneless(const void *l, const void *r) const;

    void addTransitionAt(const void *val, bool state, unsigned pos);
    void appendTransition(ITransition *t);

public:
    IMPLEMENT_IINTERFACE;
    CStringSet(size32_t size);
    CStringSet(MemoryBuffer &mb);

// IStringSet
    virtual void addRange(const void *loval, const void *hival);
    virtual void addAll();
    virtual ITransition *queryTransition(unsigned idx);
    virtual bool getTransitionValue(void *value, unsigned idx);
    virtual void killRange(const void *loval, const void *hival);
    virtual bool inRange(const void *val) const;
    virtual bool inRange(const void *val, unsigned &transition) const;
    virtual size32_t getSize() { return size; };
    virtual void reset();
    virtual unsigned transitionCount();
    virtual IStringSet *invertSet();
    virtual IStringSet *unionSet(IStringSet *);
    virtual IStringSet *intersectSet(IStringSet *);
    virtual const char *describe(StringBuffer &ret);
    virtual bool isEmptySet() const { return transitions.length()==0; }
    virtual bool isFullSet() const 
    { 
        return transitions.length()==2 && 
            isLowVal(transitions.item(0).getValue()) &&
            isHighVal(transitions.item(1).getValue());
    }
    virtual bool isSingleValue() const
    {
        return transitions.length()==2 && 
            memcmp(transitions.item(0).getValue(), transitions.item(1).getValue(), size) == 0;
    }

    virtual unsigned numValues() const
    {
        unsigned ret = 0;
        unsigned idx = 0;
        while (transitions.isItem(idx+1))
        {
            unsigned thisrange = getCardinality(transitions.item(idx).getValue(), transitions.item(idx+1).getValue(), size);
            if (thisrange + ret < ret)
                return (unsigned) -1;
            ret += thisrange;
            idx += 2;
        }
        return ret;
    }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const
    {
        StringSetSerializeType typ = serializeType();
        assertex(typ!=SSST_none);
        mb.append((byte)typ).append(size).append(transitions.ordinality());
        ForEachItemIn(i,transitions) {
            transitions.item(i).serialize(size,mb);
        }
        return mb;
    }

    virtual StringSetSerializeType serializeType() const = 0;
};

class CBigUnsignedStringSet : public CStringSet
{
protected:
    virtual CStringSet *createEmptySet()
    {
        return new CBigUnsignedStringSet(size);
    }

    virtual bool increment(void *_val) const
    {
        unsigned char *val = (unsigned char *)_val;
        int i = size;
        while (i--)
        {
            val[i]++;
            if (val[i]!=0)
                return true;
        }
        return false;
    }

    virtual bool decrement(void *_val) const
    {
        unsigned char *val = (unsigned char *)_val;
        int i = size;
        while (i--)
        {
            val[i]--;
            if ((unsigned char)val[i]!=0xff)
                return true;
        }
        return false;
    }

    virtual int memcmp(const void *val1, const void *val2, size32_t size) const
    {
        return ::memcmp(val1, val2, size);
    }

    virtual void memset(void *ptr, int val, size32_t size) const
    {
        ::memset(ptr, val, size);
    }

    virtual unsigned getCardinality(const void *val1, const void *val2, size32_t size) const
    {
        unsigned char *p1 = (unsigned char *) val1;
        unsigned char *p2 = (unsigned char *) val2;
        unsigned ret = 1;
        unsigned mult = 1;
        while (size--)
        {
            unsigned diff = p2[size] - p1[size];
            if (diff)
            {
                if (!mult) 
                    return (unsigned) -1;
                else
                    ret += diff * mult;
            }
            if (mult*256 < mult)
                mult = 0;
            else
                mult *= 256;
        }
        return ret;
    }

    virtual bool isHighVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        for (unsigned i = 0; i < size; i++)
            if (vval[i] != 0xff)
                return false;
        return true;
    }

    virtual bool isLowVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        for (unsigned i = 0; i < size; i++)
            if (vval[i] != 0x00)
                return false;
        return true;
    }

    virtual bool isSigned() const { return false; }
    virtual bool isBigEndian() const { return true; }


    virtual StringSetSerializeType serializeType() const
    {
        return SSST_BIGUNSIGNEDSTRINGSET;
    }

public:
    CBigUnsignedStringSet(unsigned size) : CStringSet(size) {}
    CBigUnsignedStringSet(MemoryBuffer &mb) : CStringSet(mb) {}


};

class CBigSignedStringSet : public CBigUnsignedStringSet
{
protected:
    virtual CStringSet *createEmptySet()
    {
        return new CBigSignedStringSet(size);
    }

    // increment and decrement are same as unsigned

    virtual int memcmp(const void *val1, const void *val2, size32_t size) const
    {
        return ::memcmpbigsigned(val1, val2, size);
    }

    virtual void memset(void *ptr, int val, size32_t size) const
    {
        ::memset(ptr, val, size);
        switch(val)
        {
        case 0:
            *(unsigned char *) ptr = 0x80;
            break;
        case 0xff:
            *(unsigned char *) ptr = 0x7f;
            break;
        default:
            throwUnexpected();
        }
    }

    virtual bool isHighVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        if (vval[0] != 0x7f)
            return false;
        for (unsigned i = 1; i < size; i++)
            if (vval[i] != 0xff)
                return false;
        return true;
    }

    virtual bool isLowVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        if (vval[0] != 0x80)
            return false;
        for (unsigned i = 1; i < size; i++)
            if (vval[i] != 0x00)
                return false;
        return true;
    }

    virtual bool isSigned() const { return true; }
    virtual bool isBigEndian() const { return true; }

    virtual StringSetSerializeType serializeType() const
    {
        return SSST_BIGSIGNEDSTRINGSET;
    }
public:
    CBigSignedStringSet(unsigned size) : CBigUnsignedStringSet(size) {}
    CBigSignedStringSet(MemoryBuffer &mb) : CBigUnsignedStringSet(mb) {}
};

class CLittleUnsignedStringSet : public CStringSet
{
protected:
    virtual CStringSet *createEmptySet()
    {
        return new CLittleUnsignedStringSet(size);
    }

    virtual bool increment(void *_val) const
    {
        unsigned char *val = (unsigned char *)_val;
        unsigned i = 0;
        while (i < size)
        {
            val[i]++;
            if (val[i]!=0)
                return true;
            i++;
        }
        return false;
    }

    virtual unsigned getCardinality(const void *val1, const void *val2, size32_t size) const
    {
        unsigned char *p1 = (unsigned char *) val1;
        unsigned char *p2 = (unsigned char *) val2;
        unsigned ret = 1;
        unsigned mult = 1;
        unsigned i = 0;
        while (i < size)
        {
            unsigned diff = p2[i] - p1[i];
            if (diff)
            {
                if (!mult) 
                    return (unsigned) -1;
                else
                    ret += diff * mult;
            }
            if (mult*256 < mult)
                mult = 0;
            else
                mult *= 256;
            i++;
        }
        return ret;
    }

    virtual bool decrement(void *_val) const
    {
        unsigned char *val = (unsigned char *)_val;
        unsigned i = 0;
        while (i < size)
        {
            val[i]--;
            if ((unsigned char)val[i]!=0xff)
                return true;
            i++;
        }
        return false;
    }

    virtual int memcmp(const void *val1, const void *val2, size32_t size) const
    {
        return ::memcmplittleunsigned(val1, val2, size);
    }

    virtual void memset(void *ptr, int val, size32_t size) const
    {
        ::memset(ptr, val, size);
    }

    virtual bool isHighVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        for (unsigned i = 0; i < size; i++)
            if (vval[i] != 0xff)
                return false;
        return true;
    }

    virtual bool isLowVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        for (unsigned i = 0; i < size; i++)
            if (vval[i] != 0x00)
                return false;
        return true;
    }

    virtual bool isSigned() const { return false; }
    virtual bool isBigEndian() const { return false; }

    virtual StringSetSerializeType serializeType() const
    {
        return SSST_LITTLEUNSIGNEDSTRINGSET;
    }

public:
    CLittleUnsignedStringSet(unsigned size) : CStringSet(size) {}
    CLittleUnsignedStringSet(MemoryBuffer &mb) : CStringSet(mb) {}
};

class CLittleSignedStringSet : public CLittleUnsignedStringSet
{
protected:
    virtual CStringSet *createEmptySet()
    {
        return new CLittleSignedStringSet(size);
    }

    // increment and decrement are same as unsigned

    virtual int memcmp(const void *val1, const void *val2, size32_t size) const
    {
        return ::memcmplittlesigned(val1, val2, size);
    }

    virtual void memset(void *ptr, int val, size32_t size) const
    {
        if (size > 1)
            ::memset(ptr, val, size);
        unsigned char *pptr = (unsigned char *) ptr;
        switch(val)
        {
        case 0:
            pptr[size-1] = 0x80;
            break;
        case 0xff:
            pptr[size-1] = 0x7f;
            break;
        default:
            throwUnexpected();
        }
    }

    virtual bool isHighVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        if (vval[size-1] != 0x7f)
            return false;
        for (unsigned i = 0; i < size-1; i++)
            if (vval[i] != 0xff)
                return false;
        return true;
    }

    virtual bool isLowVal(const void *val) const
    {
        const unsigned char *vval = (const unsigned char *) val;
        if (vval[size-1] != 0x80)
            return false;
        for (unsigned i = 0; i < size-1; i++)
            if (vval[i] != 0x00)
                return false;
        return true;
    }

    virtual bool isSigned() const { return true; }
    virtual bool isBigEndian() const { return false; }

    virtual StringSetSerializeType serializeType() const
    {
        return SSST_LITTLESIGNEDSTRINGSET;
    }

public:
    CLittleSignedStringSet(unsigned size) : CLittleUnsignedStringSet(size) {}
    CLittleSignedStringSet(MemoryBuffer &mb) : CLittleUnsignedStringSet(mb) {}
};


class CTransition : implements ITransition, public CInterface
{
private:
    bool state;                 // note: should move before ITransition to pack better in 64bit
    const void *val;

public:
    IMPLEMENT_IINTERFACE;

    CTransition(const void *_val, bool _state)
    {
        val = _val;
        state = _state;
    }

    CTransition(MemoryBuffer &mb,size32_t size)
    {
        mb.read(state);
        val = malloc(size);
        memcpy((void *)val,mb.readDirect(size),size);
    }

    ~CTransition() { free((void *) val); }

// ITransition

    bool getState() const { return state; }
    const void *getValue() const { return val; }

    MemoryBuffer &serialize(size32_t size, MemoryBuffer &mb) const
    {
        mb.append(state);
        memcpy(mb.reserve(size),val,size);
        return mb;
    }

    bool canSerialize() const  { return true; }

};

//======================================================================================


CStringSet::CStringSet(size32_t _size)
{
    size = _size;
}

CStringSet::CStringSet(MemoryBuffer &mb)
{
    mb.read(size);
    unsigned n;
    mb.read(n);
    while(n--) 
        transitions.append(*new CTransition(mb,size));
}


void CStringSet::reset()
{
    transitions.kill();
}

bool CStringSet::oneless(const void *l, const void *r) const
{
    // MORE - would be more efficient to make this virtual like the memcmp...
    void *t = alloca(size);
    memcpy(t, r, size);
    decrement(t);
    return memcmp(l, t, size)==0;
}

unsigned CStringSet::transitionCount()
{
    return transitions.ordinality();
}

void CStringSet::addTransitionAt(const void *val, bool state, unsigned pos)
{
    void *newval = malloc(size);
    memcpy(newval, val, size);
    transitions.add(* new CTransition(newval, state), pos);
}

void CStringSet::appendTransition(ITransition *t)
{
    if (t->getState() && transitions.length())
    {
        unsigned lastidx = transitions.length()-1;
        ITransition &prev = transitions.item(lastidx);
        assertex(prev.getState()==!t->getState());
        if (oneless(prev.getValue(), t->getValue()))
        {
            transitions.remove(lastidx);
            t->Release();
            return;
        }
    }
    transitions.append(*t);
}

void CStringSet::addRange(const void *loval, const void *hival)
{
    if (!loval)
    {
        void *x = alloca(size);
        memset(x, 0, size);
        loval = x;
    }
    if (!hival)
    {
        void *x = alloca(size);
        memset(x, 0xff, size);
        hival = x;
    }
    if (memcmp(loval, hival, size) > 0)
        return;

    unsigned idx;
    bool inset = false;
    int b = transitions.ordinality();
    if (!b)
    {
        addTransitionAt(loval, true, 0);
        addTransitionAt(hival, false, 1);
        return;
    }
    else
    {
        // binchop to find last transition > val...
        unsigned int a = 0;
        int rc;
        while ((int)a<b)
        {
            int i = a+(b+1-a)/2;
            rc = memcmp(loval, transitions.item(i-1).getValue(), size);
            if (rc>0)
                a = i;
            else
                b = i-1;
        }
        if (a>0)
        {
            idx = a;
            ITransition &t = transitions.item(idx-1);
            if(!t.getState())
            {
                if (oneless(t.getValue(), loval)) 
                    transitions.remove(--idx);
                else
                    addTransitionAt(loval, true, idx++);
            }
            else
                inset = true;
        }
        else
        {
            addTransitionAt(loval, true, 0);
            idx = 1;
        }
    }

    while (transitions.isItem(idx))
    {
        ITransition &t = transitions.item(idx);
        int diff = memcmp(t.getValue(), hival, size);
        if (diff <= 0)
        {
            inset = t.getState();
            transitions.remove(idx);
        }
        else
            break;
    }

    if (!inset)
    {
        if (transitions.isItem(idx))
        {
            ITransition &t = transitions.item(idx);
            assertex(t.getState());
            if (oneless(hival, t.getValue()))
            {
                transitions.remove(idx);
                return;
            }
        }
        addTransitionAt(hival, false, idx);
    }
}

void CStringSet::killRange(const void *loval, const void *hival)
{
    if (!loval)
    {
        void *x = alloca(size);
        memset(x, 0, size);
        loval = x;
    }
    if (!hival)
    {
        void *x = alloca(size);
        memset(x, 0xff, size);
        hival = x;
    }
    assertex(memcmp(loval, hival, size) <= 0);
    bool inset = false;
    ForEachItemIn(idx, transitions)
    {
        ITransition &t = transitions.item(idx);
        int diff = memcmp(t.getValue(), loval, size);
        if (diff < 0)
            inset = t.getState();
        else
            break;
    }
    if (inset)
    {
        void *nlo = alloca(size);
        memcpy(nlo, loval, size);
        decrement(nlo);
        addTransitionAt(nlo, false, idx++);
    }
    while (transitions.isItem(idx))
    {
        ITransition &t = transitions.item(idx);
        int diff = memcmp(t.getValue(), hival, size);
        if (diff <= 0)
        {
            inset = t.getState();
            transitions.remove(idx);
        }
        else
            break;
    }
    if (inset)
    {
        void *nhi = alloca(size);
        memcpy(nhi, hival, size);
        increment(nhi);
        addTransitionAt(nhi, true, idx);
    }
}

void CStringSet::addAll()
{
    reset();
    void *val = alloca(size);
    memset(val, 0, size);
    addTransitionAt(val, true, 0);
    memset(val, 0xff, size);
    addTransitionAt(val, false, 1);
}

const char *CStringSet::describe(StringBuffer &ret)
{
    ret.append('[');
    ForEachItemIn(idx, transitions)
    {
        ITransition &t = transitions.item(idx);
        if (t.getState())
        {
            if (idx)
                ret.append(',');
        }
        else
            ret.append("..");
        appendURL(&ret, (char *) t.getValue(), size, true);
    }
    ret.append(']');
    return ret.str();
}

bool CStringSet::inRange(const void *val) const
{
    unsigned nextTransition;
    return inRange(val, nextTransition);
}

bool CStringSet::inRange(const void *val, unsigned &nextTransition) const
{
    int b = transitions.ordinality();
    if (!b)
    {
        nextTransition = (unsigned) -1; 
        return false; 
    }
    else if (b >= 4)
    {
        // binchop to find last transition >= val...
        unsigned int a = 0;
        int rc;
        while ((int)a<b)
        {
            int i = a+(b+1-a)/2;
            rc = memcmp(val, transitions.item(i-1).getValue(), size);
            if (rc>=0)
                a = i;
            else
                b = i-1;
        }
        if (a>0)
        {
            nextTransition = (a>=transitions.ordinality())? (unsigned) -1: a; // a is first transition that is > val
            a--;
            if (transitions.item(a).getState())
                return true;
            if (memcmp(val, transitions.item(a).getValue(), size)==0)
            {
                nextTransition = a;
                return true;
            }
            return false;

        }
        else
        {
            nextTransition = 0;
            return false;
        }
    }
    else
    {
        bool inset = false;
        ForEachItemIn(idx, transitions)
        {
            ITransition &t = transitions.item(idx);
            int diff = memcmp(t.getValue(), val, size);
            if (t.getState())
            {
                if (diff <= 0)
                    inset = true;
                if (diff == 0)
                {
                    idx++;
                    break;
                }
                else if (diff > 0)
                    break;
            }
            else
            {
                if (diff >= 0)
                    break;
                if (diff < 0)
                    inset = false;
            }
        }
        nextTransition = (idx>=transitions.ordinality())? (unsigned) -1: idx;
        return inset;
    }
}

IStringSet *CStringSet::unionOrIntersect(IStringSet *r, bool isUnion)
{
    bool inA = false;
    bool inB = false;
    bool state = false;
    assertex(r->getSize()==size);
    int idxA = 0;
    int idxB = 0;
    ITransition *tA = queryTransition(idxA);
    ITransition *tB = r->queryTransition(idxB);
    CStringSet *result = createEmptySet();
    for (;;)
    {
        int diff;
        if (tA == NULL)
        {
            if (tB == NULL)
                break;
            else
                diff = 1;
        }
        else if (tB == NULL)
            diff = -1;
        else
            diff = memcmp(tA->getValue(), tB->getValue(), size);
        ITransition *t = NULL;
        if (!diff)
        {
            diff = (int) tB->getState() - (int) tA->getState(); // leading edge sorts before trailing edge for intersect...
            if (isUnion)
                diff = -diff;                                   // trailing edge sorts before leading edge for union...
        }
        if (diff <= 0)
        {
            inA = tA->getState();
            t = tA;
            idxA++;
            tA = queryTransition(idxA);
        }
        if (diff >= 0)
        {
            inB = tB->getState();
            t = tB;
            idxB++;
            tB = r->queryTransition(idxB);
        }
        bool newState;
        if (isUnion)
            newState = inA || inB;
        else
            newState = inA && inB;
        if (newState != state)
        {
            state = newState;
            t->Link();
            result->appendTransition(t);
        }
    }
    return result;
}

IStringSet *CStringSet::invertSet()
{
    CStringSet *result = createEmptySet();
    result->addAll();
    bool inset = false;
    void *loval = alloca(size);
    void *hival = alloca(size);
    memset(loval, 0, size);
    ForEachItemIn(idx, transitions)
    {
        ITransition &t = transitions.item(idx);
        assertex(t.getState() == !inset);
        if (inset)
        {
            memcpy(hival, t.getValue(), size);
            result->killRange(loval, hival);
        }
        else
            memcpy(loval, t.getValue(), size);
        inset = t.getState();
    }
    if (inset)
    {
        memset(hival, 0xff, size);
        result->killRange(loval, hival);
    }
    return result;
}

IStringSet *CStringSet::unionSet(IStringSet *other)
{
    return unionOrIntersect(other, true);
}

IStringSet *CStringSet::intersectSet(IStringSet *other)
{
    return unionOrIntersect(other, false);
}

ITransition *CStringSet::queryTransition(unsigned int idx)
{
    if (transitions.isItem(idx)) 
    {
        ITransition *t = &transitions.item(idx);
        return t;
    }
    else
        return NULL;
}

bool CStringSet::getTransitionValue(void *value, unsigned int idx)
{
    if (idx == (unsigned) -1 || idx >= transitions.ordinality()) return false;
    ITransition &t = transitions.item(idx);
    memcpy(value, t.getValue(), size);
    return true;
}

IStringSet *createStringSet(size32_t size)
{
    return new CBigUnsignedStringSet(size);
}

IStringSet *createStringSet(size32_t size, bool bigEndian, bool isSigned)
{
    if (bigEndian)
    {
        if (isSigned)
            return new CBigSignedStringSet(size);
        else
            return new CBigUnsignedStringSet(size);
    }
    else
    {
        if (isSigned)
            return new CLittleSignedStringSet(size);
        else
            return new CLittleUnsignedStringSet(size);
    }
}

ECLRTL_API IStringSet *deserializeStringSet(MemoryBuffer &mb)
{
    byte typ;
    mb.read(typ);
    switch((StringSetSerializeType)typ) {
    case SSST_BIGUNSIGNEDSTRINGSET:
        return new CBigUnsignedStringSet(mb);
    case SSST_BIGSIGNEDSTRINGSET:
        return new CBigSignedStringSet(mb);
    case SSST_LITTLEUNSIGNEDSTRINGSET:
        return new CLittleUnsignedStringSet(mb);
    case SSST_LITTLESIGNEDSTRINGSET:
        return new CLittleSignedStringSet(mb);
    }
    return NULL; // up to caller to check
};


#ifdef _USE_CPPUNIT
#include <cppunit/extensions/HelperMacros.h>

/*
class IStdException : extends std::exception
{
    Owned<IException> jException;
public:
    IStdException(IException *E) : jException(E) {};
};
*/

class SegmentMonitorTest : public CppUnit::TestFixture  
{
    CPPUNIT_TEST_SUITE( SegmentMonitorTest  );
        CPPUNIT_TEST(testOptional);
        CPPUNIT_TEST(testCombine);
    CPPUNIT_TEST_SUITE_END();

protected:

    void testOptional()
    {
        Owned<IKeySegmentMonitor> wild0_20 = createWildKeySegmentMonitor(0, 20);
        Owned<IKeySegmentMonitor> wild10_10 = createWildKeySegmentMonitor(10,10);
        Owned<IStringSet> abcdef = createStringSet(10);
        abcdef->addRange("ABCDEFGHIJ", "ABCDEFGHIJ");
        Owned<IKeySegmentMonitor> opt0_20 = createSingleKeySegmentMonitor(true, 0,20, "abcdefghijklmnopqrst");
        Owned<IKeySegmentMonitor> opt20_10 = createKeySegmentMonitor(true, LINK(abcdef), 20, 10);
        Owned<IKeySegmentMonitor> opt30_10 = createSingleKeySegmentMonitor(true, 30, 10, "KLMNOPQRST");
        Owned<IKeySegmentMonitor> nonOpt0_10 = createSingleKeySegmentMonitor(false, 0,10, "abcdefghij");
        Owned<IKeySegmentMonitor> nonOpt0_20 = createSingleKeySegmentMonitor(false, 0,20, "abcdefghijklmnopqrst");
        Owned<IKeySegmentMonitor> nonOpt20_10 = createKeySegmentMonitor(false, LINK(abcdef), 20, 10);
        Owned<IKeySegmentMonitor> nonOpt30_10 = createSingleKeySegmentMonitor(false, 30, 10, "KLMNOPQRST");
        CPPUNIT_ASSERT(wild0_20->isOptional());
        CPPUNIT_ASSERT(opt20_10->isOptional());
        CPPUNIT_ASSERT(opt30_10->isOptional());
        CPPUNIT_ASSERT(!nonOpt0_10->isOptional());
        CPPUNIT_ASSERT(!nonOpt0_20->isOptional());
        CPPUNIT_ASSERT(!nonOpt20_10->isOptional());
        CPPUNIT_ASSERT(!nonOpt30_10->isOptional());

#if 0
        IKeySegmentMonitorArray segments;
        segments.append(*LINK(wild0_20));
        segments.append(*LINK(opt20_10));
        
        CPPUNIT_ASSERT(segments.ordinality() == 1);
        CPPUNIT_ASSERT(segments.item(0).isWild());
        CPPUNIT_ASSERT(segments.item(0).getOffset() == 0);
        CPPUNIT_ASSERT(segments.item(0).getSize() == 30);

        segments.kill();
        segments.append(*LINK(wild0_20));
        segments.append(*LINK(opt20_10));
        segments.append(*LINK(nonOpt30_10));
        CPPUNIT_ASSERT(segments.ordinality() == 2);
        CPPUNIT_ASSERT(segments.item(0).isWild());
        CPPUNIT_ASSERT(segments.item(0).getOffset() == 0);
        CPPUNIT_ASSERT(segments.item(0).getSize() == 30);
        CPPUNIT_ASSERT(!segments.item(1).isWild());
        CPPUNIT_ASSERT(segments.item(1).getOffset() == 30);
        CPPUNIT_ASSERT(segments.item(1).getSize() == 10);

        segments.kill();
        segments.append(*LINK(nonOpt0_20));
        segments.append(*LINK(opt20_10));
        segments.append(*LINK(nonOpt30_10));
        CPPUNIT_ASSERT(segments.ordinality() == 3);
        CPPUNIT_ASSERT(!segments.item(1).isWild());
        CPPUNIT_ASSERT(segments.item(1).getOffset() == 20);
        CPPUNIT_ASSERT(segments.item(1).getSize() == 10);

        segments.kill();
        segments.append(*LINK(nonOpt0_10));
        segments.append(*LINK(wild10_10));
        segments.append(*LINK(opt20_10));
        segments.append(*LINK(nonOpt30_10));
        CPPUNIT_ASSERT(segments.ordinality() == 3);
        CPPUNIT_ASSERT(!segments.item(0).isWild());
        CPPUNIT_ASSERT(segments.item(1).isWild());
        CPPUNIT_ASSERT(segments.item(1).getOffset() == 10);
        CPPUNIT_ASSERT(segments.item(1).getSize() == 20);

        segments.kill();
        segments.append(*LINK(opt0_20));
        segments.append(*LINK(opt20_10));
        segments.append(*LINK(nonOpt30_10));
        CPPUNIT_ASSERT(segments.ordinality() == 3);
        CPPUNIT_ASSERT(!segments.item(0).isWild());
        CPPUNIT_ASSERT(!segments.item(1).isWild());
        CPPUNIT_ASSERT(segments.item(1).getOffset() == 20);
        CPPUNIT_ASSERT(segments.item(1).getSize() == 10);
#endif
    }

    void testCombine()
    {
        Owned<IStringSet> az = createStringSet(1);
        az->addRange("A", "Z");
        Owned<IStringSet> dj = createStringSet(1);
        dj->addRange("D", "J");
        Owned<IStringSet> hz = createStringSet(1);
        hz->addRange("H", "Z");
        Owned<IStringSet> jk = createStringSet(1);
        jk->addRange("J", "K");

        Owned<IKeySegmentMonitor> segA = createSingleKeySegmentMonitor(true, 0,1, "A");
        Owned<IKeySegmentMonitor> segA2 = createSingleKeySegmentMonitor(true, 0,1, "A");
        Owned<IKeySegmentMonitor> segJ = createSingleKeySegmentMonitor(true, 0,1, "J");
        Owned<IKeySegmentMonitor> segAZ = createKeySegmentMonitor(true, az.getLink(), 0,1);
        Owned<IKeySegmentMonitor> segDJ = createKeySegmentMonitor(true, dj.getLink(), 0,1);
        Owned<IKeySegmentMonitor> segHZ = createKeySegmentMonitor(true, hz.getLink(), 0,1);
        Owned<IKeySegmentMonitor> segJK = createKeySegmentMonitor(true, jk.getLink(), 0,1);

        Owned<IKeySegmentMonitor> result;
        result.setown(segA->combine(segA2));
        CPPUNIT_ASSERT(!result->isEmpty());
        CPPUNIT_ASSERT(result->matchesBuffer("A"));

        result.setown(segA->combine(segJ));
        CPPUNIT_ASSERT(result->isEmpty());

        result.setown(segA->combine(segAZ));
        CPPUNIT_ASSERT(!result->isEmpty());
        CPPUNIT_ASSERT(result->matchesBuffer("A"));
        CPPUNIT_ASSERT(!result->matchesBuffer("B"));

        result.setown(segAZ->combine(segDJ));
        CPPUNIT_ASSERT(!result->isEmpty());
        CPPUNIT_ASSERT(!result->matchesBuffer("C"));
        CPPUNIT_ASSERT(result->matchesBuffer("D"));
        CPPUNIT_ASSERT(result->matchesBuffer("J"));
        CPPUNIT_ASSERT(!result->matchesBuffer("K"));

        result.setown(segHZ->combine(segDJ));
        CPPUNIT_ASSERT(!result->isEmpty());
        CPPUNIT_ASSERT(!result->matchesBuffer("G"));
        CPPUNIT_ASSERT(result->matchesBuffer("H"));
        CPPUNIT_ASSERT(result->matchesBuffer("J"));
        CPPUNIT_ASSERT(!result->matchesBuffer("K"));

    }
};

CPPUNIT_TEST_SUITE_REGISTRATION( SegmentMonitorTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( SegmentMonitorTest, "SegmentMonitorTest" );

#endif
