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
#include "rtlnewkey.hpp"

class CKeySegmentMonitor : implements IKeySegmentMonitor, public CInterface
{
protected:
    size32_t size;
    size32_t offset;
    unsigned fieldIdx;
    unsigned hash;

public:
    IMPLEMENT_IINTERFACE;
    CKeySegmentMonitor(unsigned _fieldIdx, unsigned _offset, unsigned _size);
    CKeySegmentMonitor(MemoryBuffer &mb)
    {
        mb.read(size).read(offset).read(fieldIdx).read(hash);
    }

    virtual bool matchesBuffer(const void * rawRow) const override = 0;
    virtual bool matches(const RtlRow * rawRow) const override
    {
        return matchesBuffer(rawRow->queryRow());
    }

    virtual bool increment(void *keyval) const override;
    virtual unsigned getFieldIdx() const override { return fieldIdx; }
    virtual unsigned getOffset() const override { return offset; }
    virtual unsigned getSize() const override { return size; }
    virtual bool isWild() const override { return false; }
    virtual bool isEmpty() const override { return false; }
    virtual bool isSigned() const override { return false; }
    virtual bool isLittleEndian() const override { return false; }
    virtual unsigned numFieldsRequired() const override { return 0; }  // Should rename to queryFieldIdx or similar

    virtual int docompare(const void * l, const void * r) const override
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        return memcmp(lptr, rptr, size);
    }

    virtual bool equivalentTo(const IKeySegmentMonitor &other) const override
    {
        return offset==other.getOffset() 
            && size==other.getSize() 
            && isSigned()==other.isSigned() 
            && isLittleEndian()==other.isLittleEndian();
    }

    virtual unsigned queryHashCode() const override
    {
        return hash;
    }

    virtual bool setOffset(unsigned _offset) override
    {
        offset = _offset;
        return true;
    }

    virtual void setHigh(void *keyval) const override;

    virtual void copy(void * l, const void * r) const override
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        memcpy(lptr, rptr, size);
    }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const override
    {
        KeySegmentMonitorSerializeType typ = serializeType();
        assertex(typ!=KSMST_none);
        return mb.append((byte)typ).append(size).append(offset).append(hash);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const override = 0;
};

class CWildKeySegmentMonitor : public CKeySegmentMonitor
{
public:
    CWildKeySegmentMonitor(unsigned _fieldIdx, unsigned _offset, unsigned _size);
    CWildKeySegmentMonitor(MemoryBuffer &mb)
        : CKeySegmentMonitor(mb)
    {
    }
    virtual bool matchesBuffer(const void *keyval) const override;
    virtual int docompare(const void *,const void *) const override;
    virtual void setLow(void *keyval) const override;
    virtual void endRange(void *keyval) const override;
    virtual bool isWild() const override { return true; }
    virtual bool isSimple() const override { return true; }
    virtual bool isWellKeyed() const override { return false; }
    virtual bool isOptional() const override { return true; }
    virtual IKeySegmentMonitor *clone() const override;
    virtual KeySegmentMonitorSerializeType serializeType() const override { return KSMST_WILDKEYSEGMENTMONITOR; }
};

class CSetKeySegmentMonitor : public CKeySegmentMonitor
{
private:
    Owned<IStringSet> set;
    bool optional;

public:
    CSetKeySegmentMonitor(bool _optional, IStringSet *set, unsigned _fieldIdx, unsigned _offset, unsigned _size);
    CSetKeySegmentMonitor(MemoryBuffer &mb)
        : CKeySegmentMonitor(mb)
    {
        set.setown(deserializeStringSet(mb));
        mb.read(optional);
    }

// IKeySegmentMonitor
    virtual bool increment(void *keyval) const override;
    virtual void setLow(void *keyval) const override;
    virtual bool matchesBuffer(const void *keyval) const override;
    virtual void endRange(void *keyval) const override;
    virtual bool isEmpty() const override { return set->isEmptySet(); }
    virtual bool isWellKeyed() const override;
    virtual bool isOptional() const override { return optional; }
    virtual bool isSimple() const override { return true; }
    virtual bool isSigned() const override { return set->isSigned(); }
    virtual bool isLittleEndian() const override { return !set->isBigEndian(); }
    virtual IKeySegmentMonitor *clone() const override;

    virtual int docompare(const void * l, const void * r) const override
    {
        char *lptr = ((char *) l) + offset;
        char *rptr = ((char *) r) + offset;
        return set->memcmp(lptr, rptr, size);
    }

    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const override
    {
        CKeySegmentMonitor::serialize(mb);
        set->serialize(mb);
        return mb.append(optional);
    }

    virtual KeySegmentMonitorSerializeType serializeType() const override { return KSMST_SETKEYSEGMENTMONITOR; }
};

CKeySegmentMonitor::CKeySegmentMonitor(unsigned _fieldIdx, unsigned _offset, unsigned _size)
{
    size = _size;
    offset = _offset;
    fieldIdx = _fieldIdx;
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


CWildKeySegmentMonitor::CWildKeySegmentMonitor(unsigned _fieldIdx, unsigned _offset, unsigned _size)
    : CKeySegmentMonitor(_fieldIdx, _offset, _size)
{
}

IKeySegmentMonitor *CWildKeySegmentMonitor::clone() const
{   
    return new CWildKeySegmentMonitor(fieldIdx, offset, size);
}

bool CWildKeySegmentMonitor::matchesBuffer(const void *keyval) const
{ 
    return true;
}

int CWildKeySegmentMonitor::docompare(const void *l, const void *r) const
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

CSetKeySegmentMonitor::CSetKeySegmentMonitor(bool _optional, IStringSet *_set, unsigned _fieldIdx, unsigned _offset, unsigned _size)
    : set(_set), CKeySegmentMonitor(_fieldIdx, _offset, _size)
{
    optional = _optional;
    hash =  FNV_32_HASHONE_VALUE(hash, (byte) set->isSigned());
    hash =  FNV_32_HASHONE_VALUE(hash, (byte) !set->isBigEndian());
}

IKeySegmentMonitor *CSetKeySegmentMonitor::clone() const 
{
    return new CSetKeySegmentMonitor(optional, set.getLink(), fieldIdx, offset, size);
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
    char *ptr = ((char *) bufptr) + offset;
    return set->inRange(ptr);
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
    CSingleKeySegmentMonitorBase(bool _optional, const void *_val, unsigned _fieldIdx, unsigned _offset, unsigned _size)
        : CKeySegmentMonitor(_fieldIdx, _offset, _size)
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
    CSingleKeySegmentMonitorBase(bool _optional, unsigned _fieldIdx, unsigned _offset, unsigned _size)
        : CKeySegmentMonitor(_fieldIdx, _offset, _size)
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
    virtual bool increment(void *bufptr) const override
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
    virtual void setLow(void *bufptr) const override
    {
        // Set to lowest permitted value
        char *ptr = ((char *) bufptr) + offset;
        memcpy(ptr, val, size);
    }
    virtual bool matchesBuffer(const void *bufptr) const override
    {
        // Is current a permitted value?
        char *ptr = ((char *) bufptr) + offset;
        return memcmp(ptr, val, size) == 0;
    }

    virtual void endRange(void *bufptr) const override
    {
        // Set to last permitted value in the range that includes current (which is asserted to be valid)
#ifdef DEBUG
        assertex(matchesBuffer(bufptr));
#endif
    }

    virtual bool isWellKeyed() const override { return true; }
    virtual bool isOptional() const override { return optional; }
    virtual bool isSimple() const override { return true; }

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
    CSingleKeySegmentMonitor(bool _optional, const void *_val, unsigned _fieldIdx, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _fieldIdx, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
    }
    CSingleKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const override
    {
        return new CSingleKeySegmentMonitor(optional, val, fieldIdx, offset, size);
    }

    virtual bool isSigned() const override { return false; }
    virtual bool isLittleEndian() const override { return false; }

    virtual KeySegmentMonitorSerializeType serializeType() const override { return KSMST_SINGLEKEYSEGMENTMONITOR; }
};

class CSingleBigSignedKeySegmentMonitor : public CSingleKeySegmentMonitorBase
{
public:
    CSingleBigSignedKeySegmentMonitor(bool _optional, const void *_val, unsigned _fieldIdx, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _fieldIdx, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
    }

    CSingleBigSignedKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const override
    {
        return new CSingleBigSignedKeySegmentMonitor(optional, val, fieldIdx, offset, size);
    }

    virtual int docompare(const void *l, const void *r) const override
    {
        return memcmpbigsigned(((char *) l) + offset, ((char *) r) + offset, size);
    }

    virtual bool isSigned() const override { return true; }
    virtual bool isLittleEndian() const override { return false; }
    virtual KeySegmentMonitorSerializeType serializeType() const override { return KSMST_SINGLEBIGSIGNEDKEYSEGMENTMONITOR; }
};

class CSingleLittleSignedKeySegmentMonitor : public CSingleKeySegmentMonitorBase
{
public:
    CSingleLittleSignedKeySegmentMonitor(bool _optional, const void *_val, unsigned _fieldIdx, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _fieldIdx, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
    }

    CSingleLittleSignedKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const override
    {
        return new CSingleLittleSignedKeySegmentMonitor(optional, val, fieldIdx, offset, size);
    }

    virtual int docompare(const void *l, const void *r) const
    {
        return memcmplittlesigned(((char *) l) + offset, ((char *) r) + offset, size);
    }

    virtual bool isSigned() const override { return true; }
    virtual bool isLittleEndian() const override { return true; }
    virtual KeySegmentMonitorSerializeType serializeType() const override { return KSMST_SINGLELITTLESIGNEDKEYSEGMENTMONITOR; }
};

class CSingleLittleKeySegmentMonitor : public CSingleKeySegmentMonitorBase
{
public:
    CSingleLittleKeySegmentMonitor(bool _optional, const void *_val, unsigned _fieldIdx, unsigned _offset, unsigned _size)
        : CSingleKeySegmentMonitorBase(_optional, _val, _fieldIdx, _offset, _size)
    {
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 0);
        hash = FNV_32_HASHONE_VALUE(hash, (byte) 1);
    }

    CSingleLittleKeySegmentMonitor(MemoryBuffer &mb)
        : CSingleKeySegmentMonitorBase(mb)
    {
    }

    virtual IKeySegmentMonitor *clone() const override
    {
        return new CSingleLittleKeySegmentMonitor(optional, val, fieldIdx, offset, size);
    }

    virtual int docompare(const void *l, const void *r) const override
    {
        return memcmplittleunsigned(((char *) l) + offset, ((char *) r) + offset, size);
    }

    virtual bool isSigned() const override { return false; }
    virtual bool isLittleEndian() const override { return true; }
    virtual KeySegmentMonitorSerializeType serializeType() const override { return KSMST_CSINGLELITTLEKEYSEGMENTMONITOR; }
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

    virtual void setOverrideBuffer(const void *ptr) override
    {
        overridden = ptr;
    }

    virtual unsigned queryHashCode() const override
    {
        return hash;
    }

    virtual bool matchesBuffer(const void *keyval) const override
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            return memcmp((char *) keyval+offset, (char *) overridden+offset, base->getSize()) == 0;
        }
        else
            return base->matchesBuffer(keyval);
    }
    virtual bool matches(const RtlRow *keyval) const override
    {
        return matchesBuffer(keyval->queryRow());
    }

    virtual bool increment(void *keyval) const override
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
    virtual void setLow(void *keyval) const override
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            memcpy((char *) keyval+offset, (char *) overridden+offset, base->getSize());
        }
        else
            base->setLow(keyval);
    }
    virtual void setHigh(void *keyval) const override
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            memcpy((char *) keyval+offset, (char *) overridden+offset, base->getSize());
        }
        else
            base->setHigh(keyval);
    }
    virtual void endRange(void *keyval) const override
    {
        if (overridden)
        {
            unsigned offset = base->getOffset();
            memcpy((char *) keyval+offset, (char *) overridden+offset, base->getSize());
        }
        base->endRange(keyval);
    }
    virtual void copy(void * expandedRow, const void *rawRight) const override
    {
        base->copy(expandedRow, rawRight);
    }

    virtual bool isWild() const override                             { return overridden ? false : base->isWild(); }
    virtual unsigned getFieldIdx() const override                    { return base->getFieldIdx(); }
    virtual unsigned getOffset() const override                      { return base->getOffset(); }
    virtual unsigned getSize() const override                        { return base->getSize(); }
    virtual bool isEmpty() const override                            { return base->isEmpty(); }
    virtual bool isSigned() const override                           { return base->isSigned(); }
    virtual bool isLittleEndian() const override                     { return base->isLittleEndian(); }
    virtual bool isWellKeyed() const override                        { return overridden ? true : base->isWellKeyed(); }
    virtual bool isOptional() const override                         { return base->isOptional(); }
    virtual unsigned numFieldsRequired() const override              { return base->numFieldsRequired(); }
    virtual bool isSimple() const override                           { return base->isSimple();  }

    virtual bool equivalentTo(const IKeySegmentMonitor &other) const override { throwUnexpected(); }
    virtual int docompare(const void * expandedLeft, const void *rawRight) const override { throwUnexpected(); }
    virtual bool setOffset(unsigned _offset) override { throwUnexpected(); }
    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const override { throwUnexpected(); }
    virtual KeySegmentMonitorSerializeType serializeType() const override { throwUnexpected(); }
    virtual IKeySegmentMonitor *clone() const override { throwUnexpected(); }

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


IKeySegmentMonitor *createKeySegmentMonitor(bool optional, IStringSet *set, unsigned _fieldIdx, unsigned _offset, unsigned _size)
{
    if (!set)
        return new CWildKeySegmentMonitor(_fieldIdx, _offset, _size);

    Owned<IStringSet> removeSet = set; // make sure set is released if optimized out.
    if (set->isSingleValue())
    {
        void *data = alloca(_size);
        set->getTransitionValue(data, 0);
        if (set->isSigned())
        {
            if (set->isBigEndian())
                return createSingleBigSignedKeySegmentMonitor(optional, _fieldIdx, _offset, _size, data);
            else
                return createSingleLittleSignedKeySegmentMonitor(optional, _fieldIdx, _offset, _size, data);
        }
        else
        {
            if (set->isBigEndian())
                return createSingleKeySegmentMonitor(optional, _fieldIdx, _offset, _size, data);
            else
                return createSingleLittleKeySegmentMonitor(optional, _fieldIdx, _offset, _size, data);
        }
    }
    else if (set->isFullSet())
        return new CWildKeySegmentMonitor(_fieldIdx, _offset, _size);
    else
        return new CSetKeySegmentMonitor(optional, removeSet.getClear(), _fieldIdx, _offset, _size);
}

ECLRTL_API IStringSet *createRtlStringValue(size32_t size, const char * value)
{
    IStringSet * set = createStringSet(size);
    set->addRange(value, value);
    return set;
}

IKeySegmentMonitor *createWildKeySegmentMonitor(unsigned _fieldIdx, unsigned _offset, unsigned _size)
{
    return new CWildKeySegmentMonitor(_fieldIdx, _offset, _size);
}

IKeySegmentMonitor *createEmptyKeySegmentMonitor(bool optional, unsigned _fieldIdx, unsigned _offset, unsigned _size)
{
    return new CSetKeySegmentMonitor(optional, createStringSet(_size), _fieldIdx, _offset, _size);
}

ECLRTL_API IKeySegmentMonitor *createSingleKeySegmentMonitor(bool optional, unsigned _fieldIdx, unsigned offset, unsigned size, const void * value)
{
    return new CSingleKeySegmentMonitor(optional, value, _fieldIdx, offset, size);
}

ECLRTL_API IOverrideableKeySegmentMonitor *createOverrideableKeySegmentMonitor(IKeySegmentMonitor *base)
{
    return new COverrideableKeySegmentMonitor(base);
}

ECLRTL_API IKeySegmentMonitor *createSingleBigSignedKeySegmentMonitor(bool optional, unsigned fieldIdx, unsigned offset, unsigned size, const void * value)
{
    return new CSingleBigSignedKeySegmentMonitor(optional, value, fieldIdx, offset, size);
}

ECLRTL_API IKeySegmentMonitor *createSingleLittleSignedKeySegmentMonitor(bool optional, unsigned fieldIdx, unsigned offset, unsigned size, const void * value)
{
    // MORE - common int sizes 1,2,4 (8?) might be better done with dedicated subclasses
    return new CSingleLittleSignedKeySegmentMonitor(optional, value, fieldIdx, offset, size);
}

ECLRTL_API IKeySegmentMonitor *createSingleLittleKeySegmentMonitor(bool optional, unsigned fieldIdx, unsigned offset, unsigned size, const void * value)
{
    // MORE - common int sizes 1,2,4 (8?) might be better done with dedicated subclasses
    return new CSingleLittleKeySegmentMonitor(optional, value, fieldIdx, offset, size);
}

ECLRTL_API IKeySegmentMonitor *createDummyKeySegmentMonitor(unsigned _fieldIdx, unsigned _offset, unsigned _size, bool isSigned, bool isLittleEndian)
{
    if (isSigned)
        if (isLittleEndian)
            return new CSingleLittleSignedKeySegmentMonitor(false, NULL, _fieldIdx, _offset, _size);
        else
            return new CSingleBigSignedKeySegmentMonitor(false, NULL, _fieldIdx, _offset, _size);
    else
        if (isLittleEndian)
            return new CSingleLittleKeySegmentMonitor(false, NULL, _fieldIdx, _offset, _size);
        else
            return new CSingleKeySegmentMonitor(false, NULL, _fieldIdx, _offset, _size);
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

//---------------------------------------------------------------------------------------------------------------------

class LegacySetCreator : implements ISetCreator
{
public:
    LegacySetCreator(IStringSet & _set, size32_t _minRecordSize, const RtlTypeInfo * _fieldType)
    : set(_set), minRecordSize(_minRecordSize), fieldType(_fieldType) {}

    virtual void addRange(TransitionMask lowerMask, const StringBuffer & lowerString, TransitionMask upperMask, const StringBuffer & upperString) override
    {
        MemoryBufferBuilder lobuilder(lobuffer.clear(), minRecordSize);
        fieldType->buildUtf8(lobuilder, 0, nullptr, lowerString.length(), lowerString.str());

        MemoryBufferBuilder hibuilder(hibuffer.clear(), minRecordSize);
        fieldType->buildUtf8(hibuilder, 0, nullptr, upperString.length(), upperString.str());

        set.addRange(lobuffer.toByteArray(), hibuffer.toByteArray());
        if (!(lowerMask & CMPeq))
            set.killRange(lobuffer.toByteArray(), lobuffer.toByteArray());
        if (!(upperMask & CMPeq))
            set.killRange(hibuffer.toByteArray(), hibuffer.toByteArray());
    }

protected:
    IStringSet & set;
    const RtlTypeInfo *fieldType;
    size32_t minRecordSize;
    MemoryBuffer lobuffer;
    MemoryBuffer hibuffer;
};

void deserializeSet(IStringSet & set, size32_t minRecordSize, const RtlTypeInfo * fieldType, const char * filter)
{
    LegacySetCreator creator(set, minRecordSize, fieldType);
    deserializeSet(creator, filter);
}

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
    CPPUNIT_TEST_SUITE_END();

protected:

    void testOptional()
    {
        Owned<IKeySegmentMonitor> wild0_20 = createWildKeySegmentMonitor(0, 0, 20);
        Owned<IKeySegmentMonitor> wild10_10 = createWildKeySegmentMonitor(1, 10,10);
        Owned<IStringSet> abcdef = createStringSet(10);
        abcdef->addRange("ABCDEFGHIJ", "ABCDEFGHIJ");
        Owned<IKeySegmentMonitor> opt0_20 = createSingleKeySegmentMonitor(true, 0, 0,20, "abcdefghijklmnopqrst");
        Owned<IKeySegmentMonitor> opt20_10 = createKeySegmentMonitor(true, LINK(abcdef), 1, 20, 10);
        Owned<IKeySegmentMonitor> opt30_10 = createSingleKeySegmentMonitor(true, 2, 30, 10, "KLMNOPQRST");
        Owned<IKeySegmentMonitor> nonOpt0_10 = createSingleKeySegmentMonitor(false, 0, 0,10, "abcdefghij");
        Owned<IKeySegmentMonitor> nonOpt0_20 = createSingleKeySegmentMonitor(false, 0, 0,20, "abcdefghijklmnopqrst");
        Owned<IKeySegmentMonitor> nonOpt20_10 = createKeySegmentMonitor(false, LINK(abcdef), 1, 20, 10);
        Owned<IKeySegmentMonitor> nonOpt30_10 = createSingleKeySegmentMonitor(false, 2, 30, 10, "KLMNOPQRST");
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
};

CPPUNIT_TEST_SUITE_REGISTRATION( SegmentMonitorTest );
CPPUNIT_TEST_SUITE_NAMED_REGISTRATION( SegmentMonitorTest, "SegmentMonitorTest" );

#endif
