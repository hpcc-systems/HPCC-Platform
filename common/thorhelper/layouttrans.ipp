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

#include "layouttrans.hpp"

typedef UnsignedArray Size32Array;

class RLTFailure : public IRecordLayoutTranslator::Failure
{
public:
    RLTFailure(Code _code) : code(_code) {}
    virtual Code queryCode() const { return code; }
    virtual StringBuffer & getDetail(StringBuffer & out) const { return out.append(detail); }
    RLTFailure * append(char const * str) { detail.append(str); return this; }
    RLTFailure * append(unsigned n) { detail.append(n); return this; }
    RLTFailure * appendScopeDesc(char const * scope);
    RLTFailure * appendFieldName(char const * scope, IDefRecordElement const * field);
private:
    Code code;
    StringBuffer detail;
};

class FieldSearcher
{
public:
    FieldSearcher(IDefRecordElement const * elem);
    bool search(IAtom * search, unsigned & pos) const;

private:
    MapBetween<IAtom *, IAtom *, unsigned, unsigned> tab;
};

class FieldMapping : public CInterface
{
public:
    typedef enum { None, Simple, ChildDataset } Type;
    typedef CIArrayOf<FieldMapping> List;

    FieldMapping(Type _type, IDefRecordElement const * _diskRecord, unsigned _diskFieldNum, bool _diskFieldKeyed, IDefRecordElement const * _activityRecord, unsigned _activityFieldNum, bool _activityFieldKeyed)
        : type(_type), diskRecord(_diskRecord), diskFieldNum(_diskFieldNum), activityRecord(_activityRecord), activityFieldNum(_activityFieldNum), diskFieldKeyed(_diskFieldKeyed), activityFieldKeyed(_activityFieldKeyed) {}
    List & queryChildMappings() { return childMappings; }
    Type queryType() const { return type; }
    char const * queryDiskFieldName() const { return diskRecord->queryChild(diskFieldNum)->queryName()->str(); }
    unsigned queryDiskFieldNum() const { return diskFieldNum; }
    size32_t queryDiskFieldSize() const { return diskRecord->queryChild(diskFieldNum)->queryType()->getSize(); }
    bool isDiskFieldFpos() const { return ((type != ChildDataset) && !diskFieldKeyed && (diskFieldNum == diskRecord->numChildren()-1) && diskRecord->queryChild(diskFieldNum)->queryType()->isInteger()); }
    bool isDiskFieldSet() const { return (diskRecord->queryChild(diskFieldNum)->queryType()->getTypeCode() == type_set); }
    char const * queryActivityFieldName() const { return activityRecord->queryChild(activityFieldNum)->queryName()->str(); }
    unsigned queryActivityFieldNum() const { return activityFieldNum; }
    size32_t queryActivityFieldSize() const { return activityRecord->queryChild(activityFieldNum)->queryType()->getSize(); }
    bool isActivityFieldFpos() const { return ((type == Simple) && !activityFieldKeyed && (activityFieldNum == activityRecord->numChildren()-1) && activityRecord->queryChild(activityFieldNum)->queryType()->isInteger()); }
    List const & queryChildMappings() const { return childMappings; }

private:
    Type type;
    IDefRecordElement const * diskRecord;
    IDefRecordElement const * activityRecord;
    unsigned diskFieldNum;
    unsigned activityFieldNum;
    bool diskFieldKeyed;
    bool activityFieldKeyed;
    List childMappings;
};

class MappingLevel
{
public:
    MappingLevel(FieldMapping::List & _mappings);
    MappingLevel(MappingLevel * parent, char const * name, FieldMapping::List & _mappings);
    void calculateMappings(IDefRecordElement const * diskRecord, unsigned numKeyedDisk, IDefRecordElement const * activityRecord, unsigned numKeyedActivity);
    void attemptMapping(IDefRecordElement const * diskRecord, unsigned diskFieldNum, bool diskFieldKeyed, IDefRecordElement const * activityRecord, unsigned activityFieldNum, bool activityFieldKeyed);

private:
    void checkField(IDefRecordElement const * record, unsigned num, char const * label);
    void queryCheckFieldChild(IDefRecordElement const * field, char const * label, IDefRecordElement const * & child, IDefRecordElement const * & blob);

private:
    bool topLevel;
    StringBuffer scope;
    FieldMapping::List & mappings;
};

class CRecordLayoutTranslator;

class ExpandedSegmentMonitorList : public CInterface, public IRecordLayoutTranslator::SegmentMonitorContext
{
public:
    ExpandedSegmentMonitorList(CRecordLayoutTranslator * _owner) : owner(_owner) {}
    IMPLEMENT_IINTERFACE;
    virtual void setMergeBarrier(unsigned barrierOffset);
    virtual void append(IKeySegmentMonitor * monitor);
    virtual bool isLastSegmentWild() const { return false; }
    virtual unsigned ordinality() const { return monitors.ordinality(); }
    virtual IKeySegmentMonitor * item(unsigned i) const { return &monitors.item(i); }
    virtual void reset() { monitors.kill(); }
private:
    CRecordLayoutTranslator * owner;
    IArrayOf<IKeySegmentMonitor> monitors;
};

class FieldCopy;

class RowTransformer : public CInterface
{
public:
    RowTransformer() {}
    RowTransformer(unsigned & seq, FieldMapping::List const & mappings) { build(seq, mappings); }
    void build(unsigned & seq, FieldMapping::List const & mappings);
    void transform(IRecordLayoutTranslator::RowTransformContext * ctx, byte const * in, size32_t inSize, size32_t & inOffset, IMemoryBlock & out, size32_t & outOffset) const;
    void getFposOut(IRecordLayoutTranslator::RowTransformContext const * ctx, offset_t & fpos) const;
    void createRowTransformContext(IRecordLayoutTranslator::RowTransformContext * ctx) const;

private:
    class RowRecord : public CInterface
    {
    public:
        RowRecord() : relOffset(0), relBase(0), size(0), followOn(false), childMappings(NULL), toFpos(false), fromFpos(false) {}
        RowRecord & setVals(size32_t _relOffset, unsigned _relBase, size32_t _size, bool _followOn) { relOffset = _relOffset; relBase = _relBase; size = _size; followOn = _followOn; return *this; }
        RowRecord & setChildMappings(FieldMapping::List const * _mappings) { childMappings = _mappings; return *this; }
        RowRecord & setFpos(bool to, bool from) { toFpos = to; fromFpos = from; return *this; }
        size32_t queryRelOffset() const { return relOffset; }
        unsigned queryRelBase() const { return relBase; }
        size32_t querySize() const { return size; }
        bool queryFollowOn() const { return followOn; }
        bool isToFpos() const { return toFpos; }
        bool isFromFpos() const { return fromFpos; }
        FieldMapping::List const * queryChildMappings() const { return childMappings; }
    private:
        size32_t relOffset;
        unsigned relBase;
        size32_t size;
        bool followOn;
        FieldMapping::List const * childMappings;
        bool toFpos;
        bool fromFpos;
    };

    RowRecord & ensureItem(CIArrayOf<RowRecord> & arr, unsigned pos);
    void createRowRecord(FieldMapping const & mapping, CIArrayOf<RowRecord> & records, size32_t diskOffset, unsigned numVarFields, bool & prevActivityField, unsigned & prevActivityFieldNum);
    void analyseMappings(FieldMapping::List const & mappings, CIArrayOf<RowRecord> & records);
    void generateSimpleCopy(unsigned & seq, RowRecord const & record);
    void generateCopyToFpos(RowRecord const & record);
    void generateCopyFromFpos(RowRecord const & record);
    void generateCopies(unsigned & seq, CIArrayOf<RowRecord> const & records);

private:
    unsigned sequence;
    size32_t finalFixedSize;
    Size32Array diskVarFieldRelOffsets;
    Size32Array diskVarFieldLenDisplacements;
    CIArrayOf<FieldCopy> copies;
    bool keepFpos;
    bool copyToFpos;
    size32_t copyToFposRelOffset;
    unsigned copyToFposRelBase;
    size32_t copyToFposSize;
};

class FieldCopy : public CInterface
{
public:
    FieldCopy(unsigned seq, size32_t _relOffset, unsigned _relBase) : sequence(seq), relOffset(_relOffset), relBase(_relBase), fixedSize(0) {}
    void addFixedSize(size32_t size) { fixedSize += size; }
    void addVarField(unsigned base) { varFields.append(base); }
    void setChildTransformer(RowTransformer * _transformer) { assertex(!childTransformer); childTransformer.setown(_transformer); }
    RowTransformer const * queryChildTransformer() const { return childTransformer; }
    void copy(IRecordLayoutTranslator::RowTransformContext * ctx, IMemoryBlock & out, size32_t & outOffset) const;
private:
    unsigned sequence;
    size32_t relOffset;
    unsigned relBase;
    size32_t fixedSize;
    UnsignedArray varFields;
    Owned<RowTransformer> childTransformer;
};

class CRecordLayoutTranslator : public IRecordLayoutTranslator, public CInterface
{
public:
    CRecordLayoutTranslator(IDefRecordMeta const * _diskMeta, IDefRecordMeta const * _activityMeta);
    ~CRecordLayoutTranslator() { delete [] activityKeySizes; }
    IMPLEMENT_IINTERFACE;
    virtual bool querySuccess() const { return !failure; }
    virtual Failure const & queryFailure() const { return *failure; }
    virtual void checkSizes(char const * filename, size32_t activitySize, size32_t diskSize) const;
    virtual bool queryKeysTransformed() const { return keysTransformed; }
    virtual SegmentMonitorContext * getSegmentMonitorContext() { return new ExpandedSegmentMonitorList(this); }
    virtual void createDiskSegmentMonitors(SegmentMonitorContext const & in, IIndexReadContext & out);
    virtual RowTransformContext * getRowTransformContext();
    virtual size32_t transformRow(RowTransformContext * ctx, byte const * in, size32_t inSize, IMemoryBlock & out, offset_t & fpos) const;
#ifdef DEBUG_HELPERS_REQUIRED
    virtual StringBuffer & getMappingsAsString(StringBuffer & out) const;
#endif

private:
    void calculateActivityKeySizes();
    void calculateKeysTransformed();

private:
    friend class ExpandedSegmentMonitorList;

    Linked<IDefRecordMeta> diskMeta;
    Linked<IDefRecordMeta> activityMeta;
    bool success;
    Owned<Failure> failure;
    FieldMapping::List mappings;
    size32_t * activityKeySizes;
    unsigned numKeyedDisk;
    unsigned numKeyedActivity;
    RowTransformer transformer;
    unsigned numTransformers;
    bool keysTransformed;
};

class CacheKey
{
public:
    CacheKey(size32_t _s1, void const * _d1, size32_t _s2, void const * _d2);
    unsigned getHash() const { return hashval; }
    bool operator==(CacheKey const & other) const { return((s1 == other.s1) && (s2 == other.s2) && (memcmp(d1, other.d1, s1) == 0) && (memcmp(d2, other.d2, s2) == 0)); }
private:
    size32_t s1;
    byte const * d1;
    size32_t s2;
    byte const * d2;
    unsigned hashval;
};

class CacheValue
{
public:
    CacheValue(size32_t s1, void const * d1, size32_t s2, void const * d2, IRecordLayoutTranslator * _trans);
    CacheKey const & queryKey() const { return key; }
    IRecordLayoutTranslator * getTranslator() const { return trans.getLink(); }
private:
    MemoryAttr b1;
    MemoryAttr b2;
    CacheKey key;
    Owned<IRecordLayoutTranslator> trans;
};

typedef SuperHashTableOf<CacheValue, CacheKey> CacheTable;

class CRecordLayoutTranslatorCache : public CacheTable, public IRecordLayoutTranslatorCache
{
public:
    IMPLEMENT_IINTERFACE;
    virtual ~CRecordLayoutTranslatorCache() { releaseAll(); }
    virtual IRecordLayoutTranslator * get(size32_t diskMetaSize, void const  * diskMetaData, size32_t activityMetaSize, void const * activityMetaData, IDefRecordMeta const * activityMeta = NULL);
    virtual unsigned count() const { return CacheTable::count(); }

private:
    virtual void     onAdd(void * et) {}
    virtual void     onRemove(void * et) { delete static_cast<CacheValue *>(et); }
    virtual unsigned getHashFromElement(const void * et) const { return static_cast<CacheValue const *>(et)->queryKey().getHash(); }
    virtual unsigned getHashFromFindParam(const void * fp) const { return static_cast<CacheKey const *>(fp)->getHash(); }
    virtual const void * getFindParam(const void * et) const { return &(static_cast<CacheValue const *>(et)->queryKey()); }
    virtual bool     matchesFindParam(const void * et, const void *key, unsigned fphash) const { return static_cast<CacheValue const *>(et)->queryKey() == *static_cast<CacheKey const *>(key); }
    virtual bool     matchesElement(const void *et, const void *searchET) const { return static_cast<CacheValue const *>(et)->queryKey() == static_cast<CacheValue const *>(searchET)->queryKey(); }
};
