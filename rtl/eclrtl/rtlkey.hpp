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

#ifndef RTLKEY_INCL
#define RTLKEY_INCL
#include "eclrtl.hpp"

enum KeySegmentMonitorSerializeType
{
    KSMST_none,                  // can't serialize
    KSMST_WILDKEYSEGMENTMONITOR,
    KSMST_SETKEYSEGMENTMONITOR,
    KSMST_SINGLEKEYSEGMENTMONITOR,
    KSMST_SINGLEBIGSIGNEDKEYSEGMENTMONITOR,
    KSMST_SINGLELITTLESIGNEDKEYSEGMENTMONITOR,
    KSMST_CSINGLELITTLEKEYSEGMENTMONITOR,
    KSMST_DUMMYKEYSEGMENTMONITOR,
    KSMST_OVERRIDEABLEKEYSEGMENTMONITOR,
    KSMST_VAROFFSETKEYSEGMENTMONITOR,
    KSMST_max
};

interface ITransition : extends IInterface
{
    virtual bool getState() const = 0;
    virtual const void *getValue() const = 0;
    virtual MemoryBuffer &serialize(size32_t size, MemoryBuffer &buffer) const = 0;
};

interface IStringSet : public IInterface
{
    virtual void addRange(const void *loval, const void *hival) = 0;
    virtual void addAll() = 0;
    virtual ITransition *queryTransition(unsigned idx) = 0;
    virtual bool getTransitionValue(void *value, unsigned idx) = 0;
    virtual void killRange(const void *loval, const void *hival) = 0;
    virtual bool inRange(const void *val) const = 0;
    virtual bool inRange(const void *val, unsigned &transition) const = 0;
    virtual size32_t getSize() = 0;
    virtual void reset() = 0;
    virtual unsigned transitionCount() = 0;
    virtual const char *describe(StringBuffer &ret) = 0;
    virtual IStringSet *invertSet() = 0;
    virtual IStringSet *unionSet(IStringSet *) = 0;
    virtual IStringSet *intersectSet(IStringSet *) = 0;
    virtual bool isEmptySet() const = 0;
    virtual bool isFullSet() const = 0;
    virtual bool isSingleValue() const = 0;
    virtual bool isSigned() const = 0;
    virtual bool isBigEndian() const = 0;
    virtual unsigned numValues() const = 0;

    virtual int memcmp(const void *val1, const void *val2, size32_t size) const = 0; 
    virtual bool decrement(void *val) const = 0;
    virtual bool increment(void *val) const = 0;
    virtual MemoryBuffer &serialize(MemoryBuffer &buffer) const = 0;
};

ECLRTL_API IStringSet *createStringSet(size32_t size);
ECLRTL_API IStringSet *createStringSet(size32_t size, bool bigEndian, bool isSigned);
ECLRTL_API IStringSet *deserializeStringSet(MemoryBuffer &mb);

ECLRTL_API int memcmpbigsigned(const void *l, const void *r, unsigned size);
ECLRTL_API int memcmplittleunsigned(const void *l, const void *r, unsigned size);
ECLRTL_API int memcmplittlesigned(const void *l, const void *r, unsigned size);

class RtlRow;

// In the parameter names below, "expanded" means it's a pointer to a "key buffer" value, while
// "raw" means it's a pointer to a disk record. For indexes, the two are the same, but for
// disk files where we may be using segmonitors as a way to serialize remote filters and to build on-the-fly indexes,
// the distinction is significant.
// To support varoffset segmonitors, all "raw" values are passed as const RtlRow *. We do not support varoffset segmonitors in
// on-the-fly in-memory indexes, meaning that the functions taking "expanded" params do not need to support offset translation
// and the varoffset segmonitor does not implement these functions.

interface IKeySegmentMonitor : public IInterface
{
public:
    virtual bool increment(void * expandedRow) const = 0;
    virtual void setLow(void * expandedRow) const = 0;
    virtual void endRange(void * expandedRow) const = 0;
    virtual bool matchesBuffer(const void * expandedRow) const = 0;
    virtual bool matches(const RtlRow * rawRow) const = 0;
    virtual IKeySegmentMonitor *merge(IKeySegmentMonitor *with) const = 0;  // merge with adjacent, if possible
    virtual IKeySegmentMonitor *combine(const IKeySegmentMonitor *with) const = 0; // combine with overlapping (compulsory)
    virtual IKeySegmentMonitor * split(unsigned splitSize) = 0;
    virtual bool isWild() const = 0;
    virtual unsigned getOffset() const = 0;
    virtual unsigned getSize() const = 0;
    virtual void *queryValue() const = 0;
    virtual int docompare(const void * expandedLeft, const void * rawRight) const = 0;
//  virtual unsigned getFlags() const = 0;
    virtual bool isEmpty() const = 0;
    virtual bool equivalentTo(const IKeySegmentMonitor &other) const = 0;
    virtual bool isSigned() const = 0;
    virtual bool isLittleEndian() const = 0;
    virtual int docompareraw(const void * rawLeft, const void * rawRight) const = 0; // NOTE - no RtlRow version since only used for in-memory index builds
    virtual unsigned queryHashCode() const = 0;
    virtual bool isWellKeyed() const = 0;
    virtual bool setOffset(unsigned _offset) = 0;
    virtual bool isOptional() const = 0;

    virtual void setHigh(void * expandedRow) const = 0;
    virtual bool isSimple() const = 0;
    virtual void copy(void *expandedRow, const void *rawRow) const = 0;
    virtual MemoryBuffer &serialize(MemoryBuffer &mb) const = 0;
    virtual KeySegmentMonitorSerializeType serializeType() const = 0;
    virtual IKeySegmentMonitor *clone() const = 0;
    virtual unsigned numFieldsRequired() const = 0;
};

interface IOverrideableKeySegmentMonitor  : public IKeySegmentMonitor
{
    virtual void setOverrideBuffer(const void *ptr) = 0;
};

interface IBlobProvider
{
    virtual byte * lookupBlob(unsigned __int64 id) = 0;         // return reference, not freed by code generator, can dispose once transform() has returned.
};

interface IBlobCreator
{
    virtual unsigned __int64 createBlob(size32_t _size, const void * _ptr) = 0;
};

interface IIndexReadContext
{
public:
    virtual void append(IKeySegmentMonitor *segment) = 0;
    virtual unsigned ordinality() const = 0;
    virtual IKeySegmentMonitor *item(unsigned idx) const = 0;
    virtual void setMergeBarrier(unsigned offset) = 0;
};

interface IKeySegmentOffsetTranslator : public IInterface
{
    virtual const void * getSegmentBase(const void * row) const = 0;
    virtual const char * queryFactoryName() const = 0;
};

interface IKeySegmentFormatTranslator : public IInterface
{
    virtual void extractField(void * target, const void * row) const = 0;
    virtual const char * queryFactoryName() const = 0;
    virtual unsigned queryHashCode() const = 0;
};

ECLRTL_API IStringSet *createRtlStringSet(size32_t size);
ECLRTL_API IStringSet *createRtlStringSetEx(size32_t size, bool bigEndian, bool isSigned);
ECLRTL_API IStringSet *createRtlStringValue(size32_t size, const char * value);

//Following can optionally return either of their arguments as the result.
ECLRTL_API IStringSet *rtlUnionSet(IStringSet * lhs, IStringSet * rhs);
ECLRTL_API IStringSet *rtlIntersectSet(IStringSet * lhs, IStringSet * rhs);

ECLRTL_API IKeySegmentMonitor *createKeySegmentMonitor(bool optional, IStringSet *set, unsigned _offset, unsigned _size);
ECLRTL_API IKeySegmentMonitor *createEmptyKeySegmentMonitor(bool optional, unsigned _offset, unsigned _size);
ECLRTL_API IKeySegmentMonitor *createWildKeySegmentMonitor(unsigned _offset, unsigned _size);
ECLRTL_API IKeySegmentMonitor *createDummyKeySegmentMonitor(unsigned _offset, unsigned _size, bool isSigned, bool isLittleEndian);
ECLRTL_API IKeySegmentMonitor *createSingleKeySegmentMonitor(bool optional, unsigned _offset, unsigned _size, const void * value);
ECLRTL_API IOverrideableKeySegmentMonitor *createOverrideableKeySegmentMonitor(IKeySegmentMonitor *base);
ECLRTL_API IKeySegmentMonitor *createSingleBigSignedKeySegmentMonitor(bool optional, unsigned offset, unsigned size, const void * value);
ECLRTL_API IKeySegmentMonitor *createSingleLittleSignedKeySegmentMonitor(bool optional, unsigned offset, unsigned size, const void * value);
ECLRTL_API IKeySegmentMonitor *createSingleLittleKeySegmentMonitor(bool optional, unsigned offset, unsigned size, const void * value);

class RtlRecord;
//takes over ownership of base
ECLRTL_API IKeySegmentMonitor *createNewVarOffsetKeySegmentMonitor(IKeySegmentMonitor * base, unsigned offset, unsigned fieldIdx);

//takes over ownership of both arguments
ECLRTL_API IKeySegmentMonitor *createTranslatedKeySegmentMonitor(IKeySegmentMonitor * base, unsigned offset, IKeySegmentFormatTranslator * translator);

ECLRTL_API IKeySegmentMonitor *deserializeKeySegmentMonitor(MemoryBuffer &mb);

#endif
