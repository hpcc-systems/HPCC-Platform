/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
 */

#ifndef ECLHELPER_HPP
#define ECLHELPER_HPP

/*
This file contains interface definitions for the meta-information, dataset processing an activities.
It should only contain pure interface definitions or inline functions.

A note on naming conventions:
  getXXX implies that the returned value should be released by the caller
  queryXXX implies that it should not

Methods named getXXX returning const char * from generated code will return a value that MAY need releasing (via roxiemem)
or that may be constants. Callers should always call roxiemem::ReleaseRoxieRow on the returned value - this will do nothing
if the supplied pointer was not from the roxiemem heap. Usually an OwnedRoxieString is the easiest way to achieve this.

*/

#include "jscm.hpp"
#ifndef CHEAP_UCHAR_DEF
#ifdef _USE_ICU
#include "unicode/utf.h"
#else
typedef unsigned short UChar;
#endif
#endif
#include "rtlconst.hpp"

//Should be incremented whenever the virtuals in the context or a helper are changed, so
//that a work unit can't be rerun.  Try as hard as possible to retain compatibility.
#define ACTIVITY_INTERFACE_VERSION      652
#define MIN_ACTIVITY_INTERFACE_VERSION  650             //minimum value that is compatible with current interface

typedef unsigned char byte;

#ifndef I64C
#ifdef _WIN32
#define I64C(n) n##i64
#else
#define I64C(n) n##LL
#endif
#endif

#ifdef _WIN32
typedef size_t memsize_t;
#endif

typedef unsigned __int64 __uint64;
typedef __uint64 offset_t;

interface IOutputMetaData;
interface ICodeContext;
interface IAtom;
interface IException;
interface IFieldFilter;
interface IThorDiskCallback;

class MemoryBuffer;
class StringBuffer;
class rtlRowBuilder;
class Decimal;
struct RtlFieldInfo;
struct RtlTypeInfo;

#ifndef ICOMPARE_DEFINED
#define ICOMPARE_DEFINED
struct ICompare
{
    virtual int docompare(const void *,const void *) const =0;
protected:
    virtual ~ICompare() {}
};
#endif

#ifndef ICOMPAREEQ_DEFINED
#define ICOMPAREEQ_DEFINED
struct ICompareEq
{
    virtual bool match(const void *,const void *) const =0;
protected:
    virtual ~ICompareEq() {}
};
#endif


#ifndef IRANGECOMPARE_DEFINED
#define IRANGECOMPARE_DEFINED
struct IRangeCompare
{
    virtual int docompare(const void * left,const void * right, unsigned numFields) const =0;
    virtual unsigned maxFields() = 0;
protected:
    virtual ~IRangeCompare() {}
};
#endif

interface INaryCompareEq
{
    virtual bool match(unsigned _num, const void * * _rows) const = 0;
};

interface IEngineRowAllocator;

interface IRowBuilder
{
    virtual byte * ensureCapacity(size32_t required, const char * fieldName) = 0;
protected:
    virtual byte * createSelf() = 0;
    virtual void reportMissingRow() const = 0;
public:
    virtual IEngineRowAllocator *queryAllocator() const = 0;
};

class ARowBuilder : public IRowBuilder
{
public:
#ifdef _DEBUG
    inline byte * row() const { if (!self) reportMissingRow(); return self; }
#else
    inline byte * row() const { return self; }
#endif
    inline byte * getSelf()
    {
        if (self)
            return self;
        return createSelf();
    }

protected:
    inline ARowBuilder() { self = NULL; }

protected:
    byte * self;                                //This is embedded as a member to avoid overhead of a virtual call
};


#define COMMON_NEWTHOR_FUNCTIONS \
    virtual void Link() const = 0;      \
    virtual bool Release() const = 0;

#ifndef IRECORDSIZE_DEFINED     // also in jio.hpp
#define IRECORDSIZE_DEFINED
interface IRecordSize : public IInterface 
{
    virtual size32_t getRecordSize(const void *rec) = 0;
    virtual size32_t getFixedSize() const = 0;
    virtual size32_t getMinRecordSize() const = 0;
    inline bool isFixedSize()      const { return getFixedSize()!=0; }
    inline bool isVariableSize()   const { return getFixedSize()==0; }
};
#endif

interface IXmlWriter : public IInterface
{
public:
    virtual void outputQuoted(const char *text) = 0;
    virtual void outputString(unsigned len, const char *field, const char *fieldname) = 0;
    virtual void outputBool(bool field, const char *fieldname) = 0;
    virtual void outputData(unsigned len, const void *field, const char *fieldname) = 0;
    virtual void outputInt(__int64 field, unsigned size, const char *fieldname) = 0;
    virtual void outputUInt(unsigned __int64 field, unsigned size, const char *fieldname) = 0;
    virtual void outputReal(double field, const char *fieldname) = 0;
    virtual void outputDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) = 0;
    virtual void outputUDecimal(const void *field, unsigned size, unsigned precision, const char *fieldname) = 0;
    virtual void outputUnicode(unsigned len, const UChar *field, const char *fieldname) = 0;
    virtual void outputQString(unsigned len, const char *field, const char *fieldname) = 0;
    virtual void outputBeginDataset(const char *dsname, bool nestChildren) = 0;
    virtual void outputEndDataset(const char *dsname) = 0;
    virtual void outputBeginNested(const char *fieldname, bool nestChildren) = 0;
    virtual void outputEndNested(const char *fieldname) = 0;
    virtual void outputSetAll() = 0;
    virtual void outputUtf8(unsigned len, const char *field, const char *fieldname) = 0;
    virtual void outputBeginArray(const char *fieldname) = 0;
    virtual void outputEndArray(const char *fieldname) = 0;
    virtual void outputInlineXml(const char *text) = 0; //for appending raw xml content
    virtual void outputXmlns(const char *name, const char *uri) = 0;
    inline void outputCString(const char *field, const char *fieldname) { outputString((size32_t)strlen(field), field, fieldname); }
};

interface IFieldProcessor : public IInterface
{
public:
    virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field) = 0;
    virtual void processBool(bool value, const RtlFieldInfo * field) = 0;
    virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field) = 0;
    virtual void processInt(__int64 value, const RtlFieldInfo * field) = 0;
    virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field) = 0;
    virtual void processReal(double value, const RtlFieldInfo * field) = 0;
    virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field) = 0;
    virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field) = 0;
    virtual void processUnicode(unsigned len, const UChar *value, const RtlFieldInfo * field) = 0;
    virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field) = 0;
    virtual void processUtf8(unsigned len, const char *value, const RtlFieldInfo * field) = 0;
    inline  void processCString(const char *value, const RtlFieldInfo * field) { processString((size32_t)strlen(value), value, field); }

//The following are used process the structured fields
    virtual bool processBeginSet(const RtlFieldInfo * field, unsigned elements, bool isAll, const byte *data) = 0;
    virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned rows) = 0;
    virtual bool processBeginRow(const RtlFieldInfo * field) = 0;           // either in a dataset, or nested
    virtual void processEndSet(const RtlFieldInfo * field) = 0;
    virtual void processEndDataset(const RtlFieldInfo * field) = 0;
    virtual void processEndRow(const RtlFieldInfo * field) = 0;
};

class RtlDynamicRowBuilder;

interface IFieldSource : public IInterface
{
public:
    virtual bool getBooleanResult(const RtlFieldInfo *field) = 0;
    virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result) = 0;
    virtual double getRealResult(const RtlFieldInfo *field) = 0;
    virtual __int64 getSignedResult(const RtlFieldInfo *field) = 0;
    virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field) = 0;
    virtual void getStringResult(const RtlFieldInfo *field, size32_t &len, char * &result) = 0;
    virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result) = 0;
    virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result) = 0;
    virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value) = 0;

    //The following are used process the structured fields
    virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll) = 0;
    virtual void processBeginDataset(const RtlFieldInfo * field) = 0;
    virtual void processBeginRow(const RtlFieldInfo * field) = 0;
    virtual bool processNextSet(const RtlFieldInfo * field) = 0;
    virtual bool processNextRow(const RtlFieldInfo * field) = 0;
    virtual void processEndSet(const RtlFieldInfo * field) = 0;
    virtual void processEndDataset(const RtlFieldInfo * field) = 0;
    virtual void processEndRow(const RtlFieldInfo * field) = 0;
};

// Functions for processing rows - creating, serializing, destroying etc.
interface IOutputRowSerializer;
interface IOutputRowDeserializer;

class CRuntimeStatisticCollection;
interface IEngineRowAllocator : extends IInterface
{
    virtual const byte * * createRowset(unsigned _numItems) = 0;
    virtual const byte * * linkRowset(const byte * * rowset) = 0;
    virtual void releaseRowset(unsigned count, const byte * * rowset) = 0;
    virtual const byte * * appendRowOwn(const byte * * rowset, unsigned newRowCount, void * row) = 0;
    virtual const byte * * reallocRows(const byte * * rowset, unsigned oldRowCount, unsigned newRowCount) = 0;

    virtual void * createRow() = 0;
    virtual void releaseRow(const void * row) = 0;
    virtual void * linkRow(const void * row) = 0;

//Used for dynamically sizing rows.
    virtual void * createRow(size32_t & allocatedSize) = 0;
    virtual void * createRow(size32_t initialSize, size32_t & allocatedSize) = 0;
    virtual void * resizeRow(size32_t newSize, void * row, size32_t & size) = 0;            //NB: size is updated with the new size
    virtual void * finalizeRow(size32_t newSize, void * row, size32_t oldSize) = 0;

    virtual IOutputMetaData * queryOutputMeta() = 0;
    virtual unsigned queryActivityId() const = 0;
    virtual StringBuffer &getId(StringBuffer &) = 0;
    virtual IOutputRowSerializer *createDiskSerializer(ICodeContext *ctx = NULL) = 0;
    virtual IOutputRowDeserializer *createDiskDeserializer(ICodeContext *ctx) = 0;
    virtual IOutputRowSerializer *createInternalSerializer(ICodeContext *ctx = NULL) = 0;
    virtual IOutputRowDeserializer *createInternalDeserializer(ICodeContext *ctx) = 0;
    virtual IEngineRowAllocator *createChildRowAllocator(const RtlTypeInfo *childtype) = 0;

    virtual void gatherStats(CRuntimeStatisticCollection & stats) = 0;
    virtual void releaseAllRows() = 0; // Only valid on unique allocators, use with extreme care
};

interface IRowSerializerTarget
{
    virtual void put(size32_t len, const void * ptr) = 0;
    virtual size32_t beginNested(size32_t count) = 0;
    virtual void endNested(size32_t position) = 0;
};

interface IRowDeserializerSource
{
    virtual const byte * peek(size32_t maxLen) = 0;     // try and ensure up to maxSize bytes are available.
    virtual offset_t beginNested() = 0;
    virtual bool finishedNested(offset_t & pos) = 0;

    virtual size32_t read(size32_t len, void * ptr) = 0;
    virtual size32_t readSize() = 0;
    virtual size32_t readPackedInt(void * ptr) = 0;
    virtual size32_t readUtf8(ARowBuilder & target, size32_t offset, size32_t fixedSize, size32_t len) = 0;
    virtual size32_t readVStr(ARowBuilder & target, size32_t offset, size32_t fixedSize) = 0;
    virtual size32_t readVUni(ARowBuilder & target, size32_t offset, size32_t fixedSize) = 0;

    //Generally called from a prefetcher rather than a deserializer, but could be called from a deserializer that also removed fields.
    virtual void skip(size32_t size) = 0;
    virtual void skipPackedInt() = 0;
    virtual void skipUtf8(size32_t len) = 0;
    virtual void skipVStr() = 0;
    virtual void skipVUni() = 0;
};

interface IRowPrefetcherSource : extends IRowDeserializerSource
{
    virtual const byte * querySelf() = 0; // What is the address of the start of the row being prefetched (or nested dataset)
    virtual void noteStartChild() = 0;  // start of reading a child row - ensure that querySelf() refers to the child row
    virtual void noteFinishChild() = 0; // called when finished reading a child row.
};

interface IOutputRowSerializer : public IInterface
{
public:
    virtual void serialize(IRowSerializerTarget & out, const byte * self) = 0;
};

interface IOutputRowDeserializer : public IInterface
{
public:
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in) = 0;
};

interface ISourceRowPrefetcher : public IInterface
{
public:
    virtual void readAhead(IRowPrefetcherSource & in) = 0;
};

//This version number covers adding new functions into the metadata interface, and the serialized field/type information
#define OUTPUTMETADATA_VERSION              2

const char xpathCompoundSeparatorChar = (char)1;

//fieldType is a compound field....

// NOTE - do not change these values - they are stored in serialized type structures e.g. in index metadata

enum RtlFieldTypeMask
{
    RFTMkind                = 0x000000ff,                   // values are defined in rtlconst.hpp
    RFTMunsigned            = 0x00000100,                   // numeric types only.
    RFTMebcdic              = 0x00000100,                   // strings only
    RFTMlinkcounted         = 0x00000200,                   // datasets, rows and records only.  But possibly strings etc. later...
    RFTMislastbitfield      = 0x00000200,                   // is last bitfield.
    
    RFTMunknownsize         = 0x00000400,                   // if set, the field is unknown size - and length is the maximum length

    RFTMalien               = 0x00000800,                   // this is the physical format of a user defined type, if unknown size we can't calculate it
    RFTMhasnonscalarxpath   = 0x00001000,                   // field xpath contains multiple node, and is not therefore usable for naming scalar fields
    RFTMbiased              = 0x00002000,                   // type is stored with a bias

    RFTMinifblock           = 0x00004000,                   // on fields only, field is inside an ifblock (not presently generated)
    RFTMdynamic             = 0x00008000,                   // Reserved for use by RtlRecord to indicate structure needs to be deleted
    RFTMispayloadfield      = 0x00010000,                   // on fields only, field is in payload portion of index or dictionary

    // These flags are used in the serialized info only
    RFTMserializerFlags     = 0x01f00000,                   // Mask to remove from saved values
    RFTMhasChildType        = 0x00100000,                   // type has child type
    RFTMhasLocale           = 0x00200000,                   // type has locale
    RFTMhasFields           = 0x00400000,                   // type has fields
    RFTMhasXpath            = 0x00800000,                   // field has xpath
    RFTMhasInitializer      = 0x01000000,                   // field has initializer
    RFTMhasVirtualInitializer= 0x02000000,                   // field has virtual value for the initializer

    RFTMcontainsunknown     = 0x10000000,                   // contains a field of unknown type that we can't process properly
    RFTMcannotinterpret     = 0x20000000,                   // cannot interpret the record using the field type information
    RFTMhasxmlattr          = 0x40000000,                   // if specified, then xml output includes an attribute (recursive)
    RFTMnoserialize         = 0x80000000,                   // cannot serialize this typeinfo structure (contains aliens or other nasties)

    RFTMinherited           = (RFTMcontainsunknown|RFTMcannotinterpret|RFTMhasxmlattr|RFTMnoserialize)    // These flags are recursively set on any parent records too
};

//MORE: Can we provide any more useful information about ifblocks  E.g., a pseudo field?  We can add later if actually useful.
interface IRtlFieldTypeSerializer;
interface IRtlFieldTypeDeserializer;

//Interface used to get field information.  Separate from RtlTypeInfo for clarity and to ensure the vmt comes first.
interface RtlITypeInfo
{
    virtual size32_t size(const byte * self, const byte * selfrow) const = 0;
    virtual size32_t process(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IFieldProcessor & target) const = 0;  // returns the size
    virtual size32_t toXML(const byte * self, const byte * selfrow, const RtlFieldInfo * field, IXmlWriter & out) const = 0;

    virtual const char * queryLocale() const = 0;
    virtual const RtlFieldInfo * const * queryFields() const = 0;               // null terminated list
    virtual const RtlTypeInfo * queryChildType() const = 0;
    virtual const IFieldFilter * queryFilter() const = 0;

    virtual size32_t build(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, IFieldSource &source) const = 0;
    virtual size32_t buildNull(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field) const = 0;
    virtual size32_t buildString(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *result) const = 0;
    virtual size32_t buildUtf8(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, size32_t len, const char *result) const = 0;
    virtual size32_t buildInt(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, __int64 val) const = 0;
    virtual size32_t buildReal(ARowBuilder &builder, size32_t offset, const RtlFieldInfo *field, double val) const = 0;

    virtual void getString(size32_t & resultLen, char * & result, const void * ptr) const = 0;
    virtual void getUtf8(size32_t & resultLen, char * & result, const void * ptr) const = 0;
    virtual __int64 getInt(const void * ptr) const = 0;
    virtual double getReal(const void * ptr) const = 0;
    virtual size32_t getMinSize() const = 0;
    virtual size32_t deserialize(ARowBuilder & rowBuilder, IRowDeserializerSource & in, size32_t offset) const = 0;
    virtual void readAhead(IRowPrefetcherSource & in) const = 0;
    virtual int compare(const byte * left, const byte * right) const = 0;
    virtual unsigned hash(const byte * left, unsigned inHash) const = 0;
    virtual int compareRange(size32_t lenLeft, const byte * left, size32_t lenRight, const byte * right) const = 0;    // do not overload compare name - can affect vmt order.  NOT sure this is a great name
    virtual void setLowBound(void * buffer, const byte * value, size32_t subLength, bool inclusive) const = 0;
    virtual void setHighBound(void * buffer, const byte * value, size32_t subLength, bool inclusive) const = 0;
protected:
    ~RtlITypeInfo() = default;  // we can't use a virtual destructor as we want constexpr constructors.
};


//The core interface for the field meta information.
struct RtlTypeInfo : public RtlITypeInfo
{
    constexpr inline RtlTypeInfo(unsigned _fieldType, unsigned _length) : fieldType(_fieldType), length(_length) {}

// Some inline helper functions to avoid having to interpret the flags.
    inline bool canSerialize() const { return (fieldType & RFTMnoserialize) == 0; }
    inline bool isEbcdic() const { return (fieldType & RFTMebcdic) != 0; }
    inline bool isFixedSize() const { return (fieldType & RFTMunknownsize) == 0; }
    inline bool isLinkCounted() const { return (fieldType & RFTMlinkcounted) != 0; }
    inline bool isSigned() const { return (fieldType & RFTMunsigned) == 0; }
    inline bool isUnsigned() const { return (fieldType & RFTMunsigned) != 0; }
    inline bool isBlob() const { return getType() == type_blob; }
    inline bool canInterpret() const { return (fieldType & RFTMcannotinterpret) == 0; }
    inline unsigned getDecimalDigits() const { return (length & 0xffff); }
    inline unsigned getDecimalPrecision() const { return (length >> 16); }
    inline unsigned getBitfieldIntSize() const { return (length & 0xff); }
    inline unsigned getBitfieldNumBits() const { return (length >> 8) & 0xff; }
    inline unsigned getBitfieldShift() const { return (length >> 16) & 0xff; }
    inline unsigned getType() const { return (fieldType & RFTMkind); }
    virtual bool isScalar() const = 0;
    virtual bool isNumeric() const = 0;
    virtual bool canTruncate() const = 0;
    virtual bool canExtend(char &) const = 0;
    virtual bool canMemCmp() const = 0;

    virtual void doCleanup() const = 0; // delete any special cached variables.
    virtual void doDelete() const = 0;  // Used in place of virtual destructor to allow constexpr constructors.
    virtual bool equivalent(const RtlTypeInfo *other) const = 0;
public:
    unsigned fieldType;
    unsigned length;                // for bitfield (int-size, # bits, bitoffset) << 16
                                    // for decimal, numdigits | precision << 16
                                    // if RFTMunknownsize then maxlength (records) [maxcount(datasets)]
protected:
    ~RtlTypeInfo() = default;
};


//These values are used as special values for the initializer to implement different functionality
enum RtlVirtualType
{
    FNoInitializer,                 // 0 means no initialiser - not a special virtual initialiser
    FVirtualFilePosition,
    FVirtualLocalFilePosition,
    FVirtualFilename,
    FVirtualRowSize,
    FVirtualLimit = 256
};

static const char * const PVirtualFilePosition = (const char *)(memsize_t)FVirtualFilePosition;
static const char * const PVirtualLocalFilePosition = (const char *)(memsize_t)FVirtualLocalFilePosition;
static const char * const PVirtualFilename = (const char *)(memsize_t)FVirtualFilename;
static const char * const PVirtualRowSize = (const char *)(memsize_t)FVirtualRowSize;

inline bool isVirtualInitializer(const void * initializer) { return initializer && (memsize_t)initializer < FVirtualLimit; }
inline byte getVirtualInitializer(const void * initializer) { return (byte)(memsize_t)initializer; }

typedef IThorDiskCallback IVirtualFieldCallback;

//Core struct used for representing meta for a field.  Effectively used as an interface.
struct RtlFieldInfo
{
    constexpr inline RtlFieldInfo(const char * _name, const char * _xpath, const RtlTypeInfo * _type, unsigned _flags = 0, const char *_initializer = NULL)
    : name(_name), xpath(_xpath), type(_type), initializer(_initializer), flags(_type->fieldType | _flags) {}

    const char * name;
    const char * xpath;
    const RtlTypeInfo * type;
    const void *initializer;
    unsigned flags;

    inline bool hasNonScalarXpath() const { return (flags & RFTMhasnonscalarxpath) != 0; }

    inline bool omitable() const
    {
        return (flags & RFTMinifblock)!=0;
    }
    inline bool isFixedSize() const 
    { 
        return type->isFixedSize();
    }
    inline size32_t size(const byte * self, const byte * selfrow) const 
    { 
        return type->size(self, selfrow); 
    }
    inline size32_t build(ARowBuilder &builder, size32_t offset, IFieldSource & source) const
    {
        return type->build(builder, offset, this, source);
    }
    inline size32_t process(const byte * self, const byte * selfrow, IFieldProcessor & target) const 
    {
        return type->process(self, selfrow, this, target);
    }
    inline size32_t toXML(const byte * self, const byte * selfrow, IXmlWriter & target) const 
    {
        return type->toXML(self, selfrow, this, target);
    }
    bool equivalent(const RtlFieldInfo *to) const;
    const char *queryXPath() const
    {
        return xpath ? xpath : name;
    }
};

enum
{
    MDFgrouped              = 0x0001,
    MDFhasxml               = 0x0002,
    MDFneeddestruct         = 0x0004,
    MDFneedserializedisk    = 0x0008,
    MDFunknownmaxlength     = 0x0010,               // max length couldn't be determined from the record structure
    MDFhasserialize         = 0x0020,
    MDFneedserializeinternal= 0x0040,
    MDFdiskmatchesinternal  = 0x0080,

    MDFneedserializemask    = (MDFneedserializedisk|MDFneedserializeinternal),
};

interface IIndirectMemberVisitor
{
    virtual void visitRowset(size32_t count, const byte * * rows) = 0;
    virtual void visitRow(const byte * row) = 0;
    //MORE: add new functions if anything else is implemented out of line (e.g., strings)
};

class RtlRecord;

interface IOutputMetaData : public IRecordSize
{
    inline bool isGrouped()                 { return (getMetaFlags() & MDFgrouped) != 0; }
    inline bool hasXML()                    { return (getMetaFlags() & MDFhasxml) != 0; }

    virtual void toXML(const byte * self, IXmlWriter & out) = 0;
    virtual unsigned getVersion() const = 0;
    virtual unsigned getMetaFlags() = 0;
    virtual const RtlTypeInfo * queryTypeInfo() const = 0;

    virtual void destruct(byte * self) = 0;

    virtual IOutputMetaData * querySerializedDiskMeta() = 0;
    virtual IOutputRowSerializer * createDiskSerializer(ICodeContext * ctx, unsigned activityId) = 0;        // ctx is currently allowed to be NULL
    virtual IOutputRowDeserializer * createDiskDeserializer(ICodeContext * ctx, unsigned activityId) = 0;
    virtual ISourceRowPrefetcher * createDiskPrefetcher() = 0;

    virtual IOutputRowSerializer * createInternalSerializer(ICodeContext * ctx, unsigned activityId) = 0;        // ctx is currently allowed to be NULL
    virtual IOutputRowDeserializer * createInternalDeserializer(ICodeContext * ctx, unsigned activityId) = 0;

    virtual void process(const byte * self, IFieldProcessor & target, unsigned from, unsigned to) = 0;           // from and to are *hints* for the range of fields to call through with
    virtual void walkIndirectMembers(const byte * self, IIndirectMemberVisitor & visitor) = 0;
    virtual IOutputMetaData * queryChildMeta(unsigned i) = 0;
    virtual const RtlRecord &queryRecordAccessor(bool expand) const = 0;
};


#ifndef IROWSTREAM_DEFINED
#define IROWSTREAM_DEFINED
interface IRowStream : extends IInterface 
{
    virtual const void *nextRow()=0;                      // rows returned must be freed
    virtual void stop() = 0;                              // after stop called NULL is returned

    inline const void *ungroupedNextRow()
    {
        const void *ret = nextRow();
        if (!ret)
            ret = nextRow();
        return ret;
    }
};
#endif

interface ITypedRowStream : extends IRowStream 
{
    virtual IOutputMetaData * queryOutputMeta() const = 0;

    inline bool isGrouped() { return queryOutputMeta()->isGrouped(); }
};


interface ISetToXmlTransformer
{
    virtual void toXML(bool isAll, size32_t len, const byte * self, IXmlWriter & out) = 0;
};

enum
{
    XWFtrim         = 0x0001,
    XWFopt          = 0x0002,
    XWFnoindent     = 0x0004,
    XWFexpandempty  = 0x0008
};


#ifndef IHASH_DEFINED       // may be defined already elsewhere
#define IHASH_DEFINED
interface IHash
{
    virtual unsigned hash(const void *data)=0;
protected:
    virtual ~IHash() {}
};
#endif

interface IXmlToRowTransformer;
interface ICsvToRowTransformer;
interface IThorDiskCallback;
interface IThorIndexCallback;
interface IIndexReadContext;                    // this is misnamed!
interface IBlobProvider;
interface IBlobCreator;

//IResourceContext: Frozen unless major version upgrade.
interface IResourceContext
{
    virtual const char *loadResource(unsigned id) = 0;
};

//Provided by engine=>can extent
interface IEclGraphResults : public IInterface
{
    virtual void getLinkedResult(unsigned & count, const byte * * & ret, unsigned id) = 0;
    virtual void getDictionaryResult(size32_t & tcount, const byte * * & tgt, unsigned id) = 0;
    virtual const void * getLinkedRowResult(unsigned id) = 0;
};

//Provided by engine=>can extent
//Results are returned so helpers can store a reference and be thread-safe.
interface IThorChildGraph : public IInterface
{
    virtual IEclGraphResults * evaluate(unsigned parentExtractSize, const byte * parentExtract) = 0;
};

interface ISectionTimer : public IInterface
{
    virtual unsigned __int64 getStartCycles() = 0;
    virtual void noteSectionTime(unsigned __int64 startCycles) = 0;
};

//NB: New methods must always be added at the end of this interface to retain backward compatibility
interface IContextLogger;
interface IDebuggableContext;
interface IDistributedFileTransaction;
interface IUserDescriptor;
interface IHThorArg;
interface IHThorHashLookupInfo;
interface IEngineContext;
interface IWorkUnit;

interface ICodeContext : public IResourceContext
{
    // Fetching interim results from workunit/query context

    virtual bool getResultBool(const char * name, unsigned sequence) = 0;
    virtual void getResultData(unsigned & tlen, void * & tgt, const char * name, unsigned sequence) = 0;
    virtual void getResultDecimal(unsigned tlen, int precision, bool isSigned, void * tgt, const char * stepname, unsigned sequence) = 0;
    virtual void getResultDictionary(size32_t & tcount, const byte * * & tgt, IEngineRowAllocator * _rowAllocator, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer, IHThorHashLookupInfo * hasher) = 0;
    virtual void getResultRaw(unsigned & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) = 0;
    virtual void getResultSet(bool & isAll, size32_t & tlen, void * & tgt, const char * name, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) = 0;
    virtual __int64 getResultInt(const char * name, unsigned sequence) = 0;
    virtual double getResultReal(const char * name, unsigned sequence) = 0;
    virtual void getResultRowset(size32_t & tcount, const byte * * & tgt, const char * name, unsigned sequence, IEngineRowAllocator * _rowAllocator, bool isGrouped, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) = 0;
    virtual void getResultString(unsigned & tlen, char * & tgt, const char * name, unsigned sequence) = 0;
    virtual void getResultStringF(unsigned tlen, char * tgt, const char * name, unsigned sequence) = 0;
    virtual void getResultUnicode(unsigned & tlen, UChar * & tgt, const char * name, unsigned sequence) = 0;
    virtual char *getResultVarString(const char * name, unsigned sequence) = 0;
    virtual UChar *getResultVarUnicode(const char * name, unsigned sequence) = 0;

    // Writing results to workunit/query context/output

    virtual void setResultBool(const char *name, unsigned sequence, bool value) = 0;
    virtual void setResultData(const char *name, unsigned sequence, int len, const void * data) = 0;
    virtual void setResultDecimal(const char * stepname, unsigned sequence, int len, int precision, bool isSigned, const void *val) = 0; 
    virtual void setResultInt(const char *name, unsigned sequence, __int64 value, unsigned size) = 0;
    virtual void setResultRaw(const char *name, unsigned sequence, int len, const void * data) = 0;
    virtual void setResultReal(const char * stepname, unsigned sequence, double value) = 0;
    virtual void setResultSet(const char *name, unsigned sequence, bool isAll, size32_t len, const void * data, ISetToXmlTransformer * transformer) = 0;
    virtual void setResultString(const char *name, unsigned sequence, int len, const char * str) = 0;
    virtual void setResultUInt(const char *name, unsigned sequence, unsigned __int64 value, unsigned size) = 0;
    virtual void setResultUnicode(const char *name, unsigned sequence, int len, UChar const * str) = 0;
    virtual void setResultVarString(const char * name, unsigned sequence, const char * value) = 0;
    virtual void setResultVarUnicode(const char * name, unsigned sequence, UChar const * value) = 0;

    // Checking persists etc are up to date

    virtual unsigned getResultHash(const char * name, unsigned sequence) = 0;
    virtual unsigned __int64 getDatasetHash(const char * name, unsigned __int64 crc) = 0;

    // Fetching various environment information, typically accessed via std.system

    virtual char *getClusterName() = 0; // caller frees return string.
    virtual char *getEnv(const char *name, const char *defaultValue) const = 0;
    virtual char *getGroupName() = 0; // caller frees return string.
    virtual char *getJobName() = 0; // caller frees return string.
    virtual char *getJobOwner() = 0; // caller frees return string.
    virtual unsigned getNodeNum() = 0;
    virtual unsigned getNodes() = 0;
    virtual char *getOS() = 0; // caller frees return string
    virtual char *getPlatform() = 0; // caller frees return string.
    virtual unsigned getPriority() const = 0;
    virtual char *getWuid() = 0; // caller frees return string.

    // Exception handling

    virtual void addWuException(const char * text, unsigned code, unsigned severity, const char * source) = 0;
    virtual void addWuAssertFailure(unsigned code, const char * text, const char * filename, unsigned lineno, unsigned column, bool isAbort) = 0;

    // File resolution etc

    virtual char * getExpandLogicalName(const char * logicalName) = 0;
    virtual unsigned __int64 getFileOffset(const char *logicalPart) = 0;
    virtual char *getFilePart(const char *logicalPart, bool create=false) = 0; // caller frees return string.
    virtual IDistributedFileTransaction *querySuperFileTransaction() = 0;
    virtual IUserDescriptor *queryUserDescriptor() = 0;

    // Graphs, child queries etc

    virtual void executeGraph(const char * graphName, bool realThor, size32_t parentExtractSize, const void * parentExtract) = 0;
    virtual unsigned getGraphLoopCounter() const = 0;
    virtual IThorChildGraph * resolveChildQuery(__int64 activityId, IHThorArg * colocal) = 0;
    virtual IEclGraphResults * resolveLocalQuery(__int64 activityId) = 0;

    // Logging etc

    virtual unsigned logString(const char *text) const = 0;
    virtual const IContextLogger &queryContextLogger() const = 0;
    virtual IDebuggableContext *queryDebugContext() const = 0;

    // Memory management

    virtual IEngineRowAllocator * getRowAllocator(IOutputMetaData * meta, unsigned activityId) const = 0;
    virtual const char * cloneVString(const char *str) const = 0;
    virtual const char * cloneVString(size32_t len, const char *str) const = 0;

    // Called from generated code for FROMXML/TOXML

    virtual const void * fromXml(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) = 0;
    virtual void getRowXML(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) = 0;

    // Miscellaneous

    virtual void getExternalResultRaw(unsigned & tlen, void * & tgt, const char * wuid, const char * stepname, unsigned sequence, IXmlToRowTransformer * xmlTransformer, ICsvToRowTransformer * csvTransformer) = 0;    // shouldn't really be here, but it broke thor.
    virtual char * queryIndexMetaData(char const * lfn, char const * xpath) = 0;
    virtual IEngineContext *queryEngineContext() = 0;
    virtual char *getDaliServers() = 0;
    virtual IWorkUnit *updateWorkUnit() const = 0;

    virtual const void * fromJson(IEngineRowAllocator * _rowAllocator, size32_t len, const char * utf8, IXmlToRowTransformer * xmlTransformer, bool stripWhitespace) = 0;
    virtual void getRowJSON(size32_t & lenResult, char * & result, IOutputMetaData & info, const void * row, unsigned flags) = 0;
    virtual unsigned getExternalResultHash(const char * wuid, const char * name, unsigned sequence) = 0;
    virtual ISectionTimer * registerTimer(unsigned activityId, const char * name) = 0;
    virtual IEngineRowAllocator * getRowAllocatorEx(IOutputMetaData * meta, unsigned activityId, unsigned flags) const = 0;
    virtual void addWuExceptionEx(const char * text, unsigned code, unsigned severity, unsigned audience, const char * source) = 0;
};


//Provided by engine=>can extend
interface IFilePositionProvider : extends IInterface
{
    virtual unsigned __int64 getFilePosition(const void * row) = 0;
    virtual unsigned __int64 getLocalFilePosition(const void * row) = 0;
};

typedef size32_t (*rowTransformFunction)(ARowBuilder & rowBuilder, const byte * src);

interface IColumnProvider;
//Provided by engine=>can extend
interface IColumnProviderIterator : extends IInterface
{
    virtual IColumnProvider * first() = 0;
    virtual IColumnProvider * next() = 0;
};

//Provided by engine=>can extend
interface IColumnProvider : extends IInterface
{
    virtual bool        getBool(const char * path) = 0;
    virtual void        getData(size32_t len, void * text, const char * path) = 0;
    virtual void        getDataX(size32_t & len, void * & text, const char * path) = 0;
    virtual __int64     getInt(const char * path) = 0;
    virtual void        getQString(size32_t len, char * text, const char * path) = 0;
    virtual void        getString(size32_t len, char * text, const char * path) = 0;
    virtual void        getStringX(size32_t & len, char * & text, const char * path) = 0;
    virtual void        getUnicodeX(size32_t & len, UChar * & text, const char * path) = 0;
    virtual bool        getIsSetAll(const char * path) = 0;
    virtual IColumnProviderIterator * getChildIterator(const char * path) = 0;
    virtual void        getUtf8X(size32_t & len, char * & text, const char * path) = 0;

//v2 interface - allowing default values.  Different names are used to ensure the vmts stay in the correct order.
    virtual bool        readBool(const char * path, bool _default) = 0;
    virtual void        readData(size32_t len, void * text, const char * path, size32_t _lenDefault, const void * _default) = 0;
    virtual void        readDataX(size32_t & len, void * & text, const char * path, size32_t _lenDefault, const void * _default) = 0;
    virtual __int64     readInt(const char * path, __int64 _default) = 0;
    virtual void        readQString(size32_t len, char * text, const char * path, size32_t _lenDefault, const char * _default) = 0;
    virtual void        readString(size32_t len, char * text, const char * path, size32_t _lenDefault, const char * _default) = 0;
    virtual void        readStringX(size32_t & len, char * & text, const char * path, size32_t _lenDefault, const char * _default) = 0;
    virtual void        readUnicodeX(size32_t & len, UChar * & text, const char * path, size32_t _lenDefault, const UChar * _default) = 0;
    virtual bool        readIsSetAll(const char * path, bool _default) = 0;
    virtual void        readUtf8X(size32_t & len, char * & text, const char * path, size32_t _lenDefault, const char * _default) = 0;

//V3 
    virtual void        getDataRaw(size32_t len, void * text, const char * path) = 0;
    virtual void        getDataRawX(size32_t & len, void * & text, const char * path) = 0;
    virtual void        readDataRaw(size32_t len, void * text, const char * path, size32_t _lenDefault, const void * _default) = 0;
    virtual void        readDataRawX(size32_t & len, void * & text, const char * path, size32_t _lenDefault, const void * _default) = 0;

//V4
    virtual __uint64    getUInt(const char * path) = 0;
    virtual __uint64    readUInt(const char * path, __uint64 _default) = 0;

    virtual const char *readRaw(const char * path, size32_t &sz) const { return nullptr; }
};

//Member - can extend if new accessor function defined.
//      IHThorXmlReadArg
interface IXmlToRowTransformer : public IInterface
{
    virtual size32_t        transform(ARowBuilder & rowBuilder, IColumnProvider * row, IThorDiskCallback * callback) = 0;
    virtual IOutputMetaData * queryRecordSize() = 0;
};


interface ICsvParameters;
//Frozen - used in too many places.
interface ICsvToRowTransformer : public IInterface
{
    virtual unsigned getMaxColumns() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned * srcLen, const char * * src, unsigned __int64 _fpos) = 0;
    virtual ICsvParameters * queryCsvParameters() = 0;
    virtual IOutputMetaData * queryRecordSize() = 0;
};



// Activity index: Class name = s/TAK(.*)/IHThor$1Arg/, with $1 using camel case
enum ThorActivityKind
{   
    //This list cannot be reordered - unless all workunits are invalidated...
    TAKnone,
    TAKsubgraph,
    TAKdiskwrite, 
    TAKsort, 
    TAKdedup, 
    TAKfilter, 
    TAKsplit, 
    TAKproject, 
    TAKrollup,
    TAKiterate,
    TAKaggregate,
    TAKhashaggregate,
    TAKfirstn,
    TAKsample,
    TAKdegroup,
    TAKgroup,
    TAKworkunitwrite,
    TAKfunnel,
    TAKapply,
    TAKhashdistribute,
    TAKhashdedup,
    TAKnormalize,
    TAKremoteresult,
    TAKpull,
    TAKnormalizechild,
    TAKchilddataset,
    TAKselectn,
    TAKenth,
    TAKif,
    TAKnull,
    TAKdistribution,
    TAKcountproject,
    TAKchoosesets,
    TAKpiperead,
    TAKpipewrite,
    TAKcsvwrite,
    TAKpipethrough,
    TAKindexwrite,
    TAKchoosesetsenth,
    TAKchoosesetslast,
    TAKfetch,
    TAKworkunitread,
    TAKthroughaggregate,
    TAKspill,
    TAKcase,
    TAKlimit,
    TAKcsvfetch,
    TAKxmlwrite,
    TAKparse,
    TAKsideeffect,
    TAKtopn,
    TAKmerge,
    TAKxmlfetch,
    TAKxmlparse,
    TAKkeyeddistribute,
    TAKsoap_rowdataset,     // a source activity
    TAKsoap_rowaction,      // source and sink activity
    TAKsoap_datasetdataset,     // a through activity
    TAKsoap_datasetaction,      // sink activity
    TAKkeydiff,
    TAKkeypatch,
    TAKsequential,
    TAKparallel,
    TAKchilditerator,
    TAKdatasetresult,
    TAKrowresult,
    TAKchildif,             // condition inside a child query
    TAKpartition,
    TAKlocalgraph,
    TAKifaction,
    TAKemptyaction,
    TAKdiskread,                // records one at a time. (filter+project)
    TAKdisknormalize,           // same, but normalize a child dataset (filter+project)
    TAKdiskaggregate,           // non-grouped aggregate of dataset, or normalized dataset (filter/project input)
    TAKdiskcount,               // non-grouped count of dataset (not child), (may filter input)
    TAKdiskgroupaggregate,      // grouped aggregate on dataset (filter) (may work on project of input)
    TAKdiskexists,              // non-grouped count of dataset (not child), (may filter input)
    TAKindexread,
    TAKindexnormalize,
    TAKindexaggregate,
    TAKindexcount,
    TAKindexgroupaggregate,
    TAKindexexists,
    TAKchildread,
    TAKchildnormalize,
    TAKchildaggregate,
    TAKchildcount,
    TAKchildgroupaggregate,
    TAKchildexists,
    TAKskiplimit,
    TAKchildthroughnormalize,
    TAKcsvread,
    TAKxmlread,
    TAKlocalresultread,
    TAKlocalresultwrite,
    TAKcombine,
    TAKregroup,
    TAKrollupgroup,
    TAKcombinegroup,
    TAKlocalresultspill,
    TAKsimpleaction,
    TAKloopcount,
    TAKlooprow,
    TAKloopdataset,
    TAKchildcase,
    TAKremotegraph,
    TAKlibrarycall,
    TAKlocalstreamread,
    TAKprocess,
    TAKgraphloop,
    TAKparallelgraphloop,
    TAKgraphloopresultread,
    TAKgraphloopresultwrite,
    TAKgrouped,
    TAKsorted,
    TAKdistributed,
    TAKnwayjoin,
    TAKnwaymerge,
    TAKnwaymergejoin,
    TAKnwayinput,                       // for variable selections from a static list
    TAKnwaygraphloopresultread,
    TAKnwayselect,
    TAKnonempty,
    TAKcreaterowlimit,
    TAKexistsaggregate,
    TAKcountaggregate,
    TAKprefetchproject, 
    TAKprefetchcountproject, 
    TAKfiltergroup,
    TAKmemoryspillread,
    TAKmemoryspillwrite,
    TAKmemoryspillsplit,
    TAKsection,
    TAKlinkedrawiterator,
    TAKnormalizelinkedchild,
    TAKfilterproject,
    TAKcatch,
    TAKskipcatch,
    TAKcreaterowcatch,
    TAKsectioninput,
    TAKcaseaction,
    TAKwhen_dataset,
    TAKwhen_action,
    TAKsubsort,
    TAKindexgroupexists,
    TAKindexgroupcount,
    TAKhashdistributemerge,
    TAKhttp_rowdataset,     // a source activity
    TAKinlinetable,
    TAKstreamediterator,
    TAKexternalsource,
    TAKexternalsink,
    TAKexternalprocess,
    TAKdictionaryworkunitwrite,
    TAKdictionaryresultwrite,
    //Joins
    TAKjoin,
    TAKhashjoin,
    TAKlookupjoin,
    TAKselfjoin,
    TAKkeyedjoin,
    TAKalljoin,
    TAKsmartjoin,
    TAKunknownjoin1, // place holders to make it easy to insert new join kinds
    TAKunknownjoin2,
    TAKunknownjoin3,
    TAKjoinlight,           // lightweight, local, presorted join.
    TAKselfjoinlight,
    TAKlastjoin,
    //Denormalize
    TAKdenormalize,
    TAKhashdenormalize,
    TAKlookupdenormalize,
    TAKselfdenormalize,
    TAKkeyeddenormalize,
    TAKalldenormalize,
    TAKsmartdenormalize,
    TAKunknowndenormalize1,
    TAKunknowndenormalize2,
    TAKunknowndenormalize3,
    TAKlastdenormalize,
    //DenormalizeGroup
    TAKdenormalizegroup,
    TAKhashdenormalizegroup,
    TAKlookupdenormalizegroup,
    TAKselfdenormalizegroup,
    TAKkeyeddenormalizegroup,
    TAKalldenormalizegroup,
    TAKsmartdenormalizegroup,
    TAKunknowndenormalizegroup1,
    TAKunknowndenormalizegroup2,
    TAKunknowndenormalizegroup3,
    TAKlastdenormalizegroup,
    TAKjsonwrite,
    TAKjsonread,
    TAKtrace,
    TAKquantile,
    TAKjsonfetch,
    TAKspillread,
    TAKspillwrite,
    TAKnwaydistribute,
    TAKnewdiskread,  // This activity will eventually have a refactored interface, currently a placeholder

    TAKlast
};

inline bool isSimpleJoin(ThorActivityKind kind) { return (kind >= TAKjoin) && (kind <= TAKlastjoin); }
inline bool isDenormalizeJoin(ThorActivityKind kind) { return (kind >= TAKdenormalize) && (kind <= TAKlastdenormalize); }
inline bool isDenormalizeGroupJoin(ThorActivityKind kind) { return (kind >= TAKdenormalizegroup) && (kind <= TAKlastdenormalizegroup); }

struct ISortKeySerializer
{
    virtual size32_t keyToRecord(ARowBuilder & rowBuilder, const void * _key, size32_t & recordSize) = 0;       // both return size of key!
    virtual size32_t recordToKey(ARowBuilder & rowBuilder, const void * _record, size32_t & recordSize) = 0;        // record size in 3rd parameter
    virtual IOutputMetaData * queryRecordSize() = 0;
    virtual ICompare * queryCompareKey() = 0;
    virtual ICompare * queryCompareKeyRow() = 0;
};


struct CFieldOffsetSize 
{ 
    size32_t offset;
    size32_t size; 
};

//Derived=>Frozen unless major version upgrade.
interface IHThorArg : public IInterface
{
    virtual IOutputMetaData * queryOutputMeta() = 0;

    virtual void onCreate(ICodeContext * ctx, IHThorArg * colocalParent, MemoryBuffer * serializedCreate) = 0;
    virtual void serializeCreateContext(MemoryBuffer & out) = 0;
    virtual void onStart(const byte * parentExtract, MemoryBuffer * serializedStart) = 0;
    virtual void serializeStartContext(MemoryBuffer & out) = 0;
};

typedef IHThorArg * (*EclHelperFactory)();

//flags for thor disk access
enum 
{
//General disk access flags
    TDXtemporary        = 0x0001,
    TDXgrouped          = 0x0002,
    TDXcompress         = 0x0004,
    TDXvarfilename      = 0x0008,       // filename is dependent on the context.
    TDXupdateaccessed   = 0x0010,
    TDXdynamicfilename  = 0x0020,
    TDXjobtemp          = 0x0040,       // stay around while a wu is being executed.

//disk read flags
    TDRoptional         = 0x00000100,
    TDRunsorted         = 0x00000200,
    TDRorderedmerge     = 0x00000400,       // for aggregate variants only
    TDRusexmlcontents   = 0x00000800,       // xml reading.  Are the contents <> of an attribute used?
    TDRpreload          = 0x00001000,       // also present in the graph.
    TDRkeyed            = 0x00002000,       // is anything keyed?
    TDRxmlnoroot        = 0x00004000,       // xml without a surrounding root tag.
    TDRcountkeyedlimit  = 0x00008000,
    TDRkeyedlimitskips  = 0x00010000,
    TDRlimitskips       = 0x00020000,
    //unused              0x00040000,
    TDRaggregateexists  = 0x00080000,       // only aggregate is exists()
    TDRgroupmonitors    = 0x00100000,       // are segement monitors created for all group by conditions.
    TDRlimitcreates     = 0x00200000,
    TDRkeyedlimitcreates= 0x00400000,
    TDRunfilteredcount  = 0x00800000,       // count/aggregegate doesn't have an additional filter
    TDRfilenamecallback = 0x01000000,
    TDRtransformvirtual = 0x02000000,       // transform uses a virtual field.
    TDRdynformatoptions = 0x04000000,
    TDRinvariantfilename= 0x08000000,       // filename is non constant but has the same value for the whole query

//disk write flags
    TDWextend           = 0x0100,
    TDWoverwrite        = 0x0200,
    TDWpersist          = 0x0400,
    TDWnoreplicate      = 0x0800,
    TDWbackup           = 0x1000,
    TDWowned            = 0x2000,       // a file which should stay around even after the wu completes, but be deleted when wu is.
    TDWresult           = 0x4000,       // a result sent to disk
    TDWupdate           = 0x10000,      // only rebuild if inputs have changed.
    TDWnewcompress      = 0x20000,      // new compressed format - only specified on output
    TDWnooverwrite      = 0x40000,
    TDWupdatecrc        = 0x80000,      // has format crc
    TDWexpires          = 0x100000,
    TDWrestricted       = 0x200000,
};

//flags for thor index read
enum
{
    TIRsorted           = 0x00000001,
    TIRnofilter         = 0x00000002,
    TIRpreload          = 0x00000004,
    TIRoptional         = 0x00000008,
    TIRcountkeyedlimit  = 0x00000010,
    TIRkeyedlimitskips  = 0x00000020,
    TIRlimitskips       = 0x00000040,
    TIRstepleadequality = 0x00000080,               // all filters before the first stepping field are equalities
    TIRaggregateexists  = 0x00000100,               // only aggregate is exists()
    TIRgroupmonitors    = 0x00000200,               // are segment monitors created for all group by conditions.
    TIRlimitcreates     = 0x00000400,
    TIRkeyedlimitcreates= 0x00000800,
    TIRvarfilename      = 0x00001000,               // filename is dependent on the context.
    TIRdynamicfilename  = 0x00002000,
    TIRunfilteredtransform = 0x00004000,
    TIRorderedmerge     = 0x00008000,
    TIRunordered        = 0x00010000,
    TIRnewfilters       = 0x00020000,               // Uses new style field filters
    TIRusesblob         = 0x00040000,               // Uses blob in the transform/projected row
    TIRinvariantfilename= 0x00080000,               // filename is non constant but has the same value for the whole query
};

//flags for thor index write
enum 
{
    TIWoverwrite        = 0x0001,
    TIWbackup           = 0x0002,
    TIWunused           = 0x0004,       // no longer used
    TIWvarfilename      = 0x0008,       // filename is dependant on the context.
    TIWsmall            = 0x0010,
    TIWupdate           = 0x0020,
    TIWlocal            = 0x0040,       // i.e., no tlk
    TIWrowcompress      = 0x0080,
    TIWnolzwcompress    = 0x0100,
    TIWnooverwrite      = 0x0200,
    TIWupdatecrc        = 0x0400,
    TIWhaswidth         = 0x0800,
    TIWexpires          = 0x1000,
    TIWmaxlength        = 0x2000,       // explicit maxlength
    TIWrestricted       = 0x200000,     // value matches the equivalent TDW flag (same value as TDW* caused problems as some index related code uses TDW* flags)
};

//flags for thor dataset/temp tables
enum
{
    TTFnoconstant        = 0x0001,      // default flags is zero
    TTFdistributed       = 0x0002,
    TTFfiltered          = 0x0004,
};

struct IBloomBuilderInfo
{
    virtual bool getBloomEnabled() const = 0;
    virtual __uint64 getBloomFields() const = 0;
    virtual unsigned getBloomLimit() const = 0;
    virtual double getBloomProbability() const = 0;
};

struct IHThorIndexWriteArg : public IHThorArg
{
    virtual const char * getFileName() = 0;
    virtual int getSequence() = 0;
    virtual IOutputMetaData * queryDiskRecordSize() = 0;
    virtual const char * queryRecordECL() = 0;
    virtual unsigned getFlags() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src, IBlobCreator * blobs, unsigned __int64 & filepos) = 0;   //NB: returns size
    virtual const char * getDistributeIndexName() = 0;
    virtual unsigned getKeyedSize() = 0;
    virtual unsigned getExpiryDays() = 0;
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) = 0;
    virtual unsigned getFormatCrc() = 0;
    virtual const char * getCluster(unsigned idx) = 0;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) = 0;
    virtual bool getIndexMeta(size32_t & lenName, char * & name, size32_t & lenValue, char * & value, unsigned idx) = 0;
    virtual unsigned getWidth() = 0;                // only guaranteed present if TIWhaswidth defined
    virtual ICompare * queryCompare() = 0;          // only guaranteed present if TIWhaswidth defined
    virtual unsigned getMaxKeySize() = 0;
    virtual const IBloomBuilderInfo * const *queryBloomInfo() const = 0;
    virtual __uint64 getPartitionFieldMask() const = 0;
};

struct IHThorFirstNArg : public IHThorArg
{
    virtual __int64 getLimit() = 0;
    virtual __int64 numToSkip() = 0;
    virtual bool preserveGrouping() = 0;
};

struct IHThorChooseSetsArg : public IHThorArg
{
    virtual unsigned getNumSets() = 0;
    virtual unsigned getRecordAction(const void * _self) = 0;
    virtual bool setCounts(unsigned * data) = 0;
};

struct IHThorChooseSetsExArg : public IHThorArg
{
    virtual unsigned getNumSets() = 0;
    virtual unsigned getCategory(const void * _self) = 0;
    virtual void getLimits(__int64 * counts) = 0;
};


struct IHThorDiskWriteArg : public IHThorArg
{
    virtual const char * getFileName() = 0;
    virtual int getSequence() = 0;
    virtual IOutputMetaData * queryDiskRecordSize() = 0;
    virtual const char * queryRecordECL() = 0;
    virtual unsigned getFlags() = 0;
    virtual unsigned getTempUsageCount() = 0;
    virtual unsigned getExpiryDays() = 0;
    virtual void getUpdateCRCs(unsigned & eclCRC, unsigned __int64 & totalCRC) = 0;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) = 0;
    virtual unsigned getFormatCrc() = 0;
    virtual const char * getCluster(unsigned idx) = 0;
};

struct IHThorFilterArg : public IHThorArg
{
    virtual bool isValid(const void * _left) = 0;
    virtual bool canMatchAny() = 0;
};

struct IHThorFilterGroupArg : public IHThorArg
{
    virtual bool isValid(unsigned _num, const void * * _rows) = 0;
    virtual bool canMatchAny() = 0;
};

struct IHThorGroupArg : public IHThorArg
{
    virtual bool isSameGroup(const void * _left, const void * _right) = 0;
};

struct IHThorDegroupArg : public IHThorArg
{
};

typedef IHThorGroupArg IHThorGroupedArg;

//Typedefed=>Be careful about extending
struct IHThorIterateArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual size32_t createDefault(ARowBuilder & rowBuilder) = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned __int64 counter) = 0;
};
typedef IHThorIterateArg IHThorGroupIterateArg;

struct IHThorProcessArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual IOutputMetaData * queryRightRecordSize() = 0;
    virtual size32_t createInitialRight(ARowBuilder & rowBuilder) = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, ARowBuilder & rightBuilder, const void * _left, const void * _right, unsigned __int64 counter) = 0;
};

struct IHThorProjectArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left) = 0;
};

struct IHThorCountProjectArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, unsigned __int64 _counter) = 0;
};

struct IHThorNormalizeArg : public IHThorArg
{
    virtual unsigned numExpandedRows(const void * _self) = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, unsigned whichCount) = 0;
};

struct IHThorSelectNArg : public IHThorArg
{
    virtual unsigned __int64 getRowToSelect() = 0;
    virtual size32_t createDefault(ARowBuilder & rowBuilder) = 0;
};

enum
{
    TQFfirst            = 0x0001,       // default flags is zero
    TQFlast             = 0x0002,
    TQFsorted           = 0x0004,
    TQFlocalsorted      = 0x0008,
    TQFhasscore         = 0x0010,
    TQFhasrange         = 0x0020,
    TQFhasskew          = 0x0040,
    TQFdedup            = 0x0080,
    TQFunstable         = 0x0100,
    TQFvariabledivisions= 0x0200,       // num divisions is not a constant
    TQFneedtransform    = 0x0400,       // if not set the records are returned as-is
};

struct IHThorQuantileArg : public IHThorArg
{
    virtual unsigned getFlags() = 0;
    virtual unsigned __int64 getNumDivisions() = 0;
    virtual double getSkew() = 0;
    virtual ICompare * queryCompare() = 0;
    virtual size32_t createDefault(ARowBuilder & rowBuilder) = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, unsigned __int64 _counter) = 0;
    virtual unsigned __int64 getScore(const void * _left) = 0;
    virtual void getRange(bool & isAll, size32_t & tlen, void * & tgt) = 0;
};

struct IHThorCombineArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned _num, const void * * _rows) = 0;
};

struct IHThorCombineGroupArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, unsigned _num, const void * * _rows) = 0;
};

struct IHThorRollupGroupArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned _num, const void * * _rows) = 0;
};

typedef IHThorArg IHThorRegroupArg;

//Following is compatible with all interfaces
typedef IHThorArg IHThorNullArg;

struct IHThorActionArg : public IHThorArg
{
    virtual void action() = 0;
};
typedef IHThorActionArg IHThorSideEffectArg;

const int WhenDefaultId = 0;
const int WhenSuccessId = -1;
const int WhenFailureId = -2;
const int WhenParallelId = -3;
const int WhenBeforeId = -4;

typedef IHThorNullArg IHThorWhenActionArg;

struct IHThorLimitArg : public IHThorArg
{
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) = 0;
};

struct IHThorCatchArg : public IHThorArg
{
    virtual unsigned getFlags() = 0;
    virtual bool isHandler(IException * e) = 0;
    virtual void onExceptionCaught() = 0;
    virtual size32_t transformOnExceptionCaught(ARowBuilder & rowBuilder, IException * e) = 0;
};

struct IHThorSplitArg : public IHThorArg
{
    virtual unsigned numBranches() = 0;
    virtual bool isBalanced() = 0;
};

struct IHThorSpillExtra : public IInterface
{
    //fill in functions here if we need any more...
};

struct IHThorSpillArg : public IHThorDiskWriteArg
{
};


//Member=>New accessor function if derived.
interface INormalizeChildIterator : public IInterface
{
    virtual byte * first(const void * parentRecord) = 0;
    virtual byte * next() = 0;
};


struct IHThorNormalizeChildArg : public IHThorArg
{
    virtual INormalizeChildIterator * queryIterator() = 0;
    virtual IOutputMetaData * queryChildRecordSize() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * left, const void * right, unsigned counter) = 0;
};

struct IHThorNormalizeLinkedChildArg : public IHThorArg
{
      virtual byte * first(const void * parentRecord) = 0;
      virtual byte * next() = 0;
};


struct IHThorChildIteratorArg : public IHThorArg
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder) = 0;
};


struct IHThorRawIteratorArg : public IHThorArg
{
    virtual void queryDataset(size32_t & len, const void * & data) = 0;
};


struct IHThorLinkedRawIteratorArg : public IHThorArg
{
    virtual const byte * next() = 0;
};


enum {
    RFrolledismatchleft = 0x00001,      // Is the value of left passed to matches() the result of the rollup?
};

struct IHThorRollupArg : public IHThorArg
{
    virtual unsigned getFlags() = 0;
    virtual bool matches(const void * _left, const void * _right) = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right) = 0;
};

enum
{
    HDFwholerecord   = 0x0001,
    HDFcompareall    = 0x0002,
    HDFkeepleft      = 0x0004,
    HDFkeepbest      = 0x0008
};

struct IHThorDedupArg : public IHThorArg
{
    inline bool compareAll() { return (getFlags() & HDFcompareall) != 0; }
    inline bool keepLeft() { return (getFlags() & HDFkeepleft) != 0; }
    inline bool keepBest() { return (getFlags() & HDFkeepbest) != 0; }
    virtual bool matches(const void * _left, const void * _right) = 0;
    virtual unsigned numToKeep() = 0;
    virtual ICompare * queryComparePrimary() = 0;           // used to break global dedup into chunks
    virtual unsigned getFlags() = 0;
    virtual ICompare * queryCompareBest() = 0;
};

enum
{
    TAForderedmerge     = 0x00000001,
};

//Group Aggregate, Normalize Aggregate
struct IHThorRowAggregator 
{
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) = 0;      // has to be called because of conditional counts/sums etc.
    virtual size32_t processFirst(ARowBuilder & rowBuilder, const void * src) = 0;
    virtual size32_t processNext(ARowBuilder & rowBuilder, const void * src) = 0;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) = 0;        //only call if transform called at least once on src.
};

struct IHThorAggregateArg : public IHThorArg, public IHThorRowAggregator
{
    virtual unsigned getAggregateFlags() = 0;
};

struct IHThorThroughAggregateExtra : public IInterface
{
    virtual void sendResult(const void * self) = 0;
    virtual IOutputMetaData * queryAggregateRecordSize() = 0;
};

struct IHThorThroughAggregateArg : public IHThorAggregateArg, public IHThorThroughAggregateExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

interface IDistributionTable;
struct IHThorDistributionArg : public IHThorArg
{
    virtual void clearAggregate(IDistributionTable * * target) = 0;
    virtual void destruct(IDistributionTable * * target) = 0;
    virtual void process(IDistributionTable * * target, const void * src) = 0;
    virtual void serialize(IDistributionTable * * self, MemoryBuffer & out) = 0;
    virtual void merge(IDistributionTable * * self, MemoryBuffer & in) = 0;

    virtual IOutputMetaData * queryInternalRecordSize() = 0;
    virtual void gatherResult(IDistributionTable * * self, StringBuffer & temp) = 0;
    virtual void sendResult(size32_t length, const char * text) = 0;
};

struct IHThorGroupAggregateArg : public IHThorAggregateArg
{
    //use extra base class if inserting functions here
};

struct IHThorHashAggregateExtra : public IInterface 
{
    virtual IHash * queryHash() = 0;                    
    virtual ICompare * queryCompareElements() = 0;      // expect to docompare(const void * element1, const void * element2);
    virtual ICompare * queryCompareRowElement() = 0;    // expect to docompare(const void * row, const void * element);
    virtual IHash * queryHashElement() = 0;                 
};

struct IHThorHashAggregateArg : public IHThorAggregateArg, public IHThorHashAggregateExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorInlineTableArg : public IHThorArg
{
    virtual size32_t getRow(ARowBuilder & rowBuilder, __uint64 row) = 0;
    virtual __uint64 numRows() = 0;
    virtual unsigned getFlags() = 0;
};

struct IHThorSampleArg : public IHThorArg
{
    virtual unsigned getProportion() = 0;
    virtual unsigned getSampleNumber() = 0;
};

struct IHThorEnthArg : public IHThorArg
{
    virtual unsigned __int64 getProportionNumerator() = 0;
    virtual unsigned __int64 getProportionDenominator() = 0;
    virtual unsigned getSampleNumber() = 0;
};

struct IHThorFunnelArg : public IHThorArg
{
    virtual bool isOrdered() = 0;
    virtual bool pullSequentially() = 0;
};

struct IHThorNonEmptyArg : public IHThorArg
{
};

struct IHThorMergeArg : public IHThorArg
{
    virtual ICompare * queryCompare() = 0;
    virtual ISortKeySerializer * querySerialize() = 0;
    virtual ICompare * queryCompareKey() = 0;
    virtual ICompare * queryCompareRowKey() = 0;
    virtual bool dedup() = 0;
};

struct IHThorRemoteResultArg : public IHThorArg
{
    virtual void sendResult(const void * self) = 0;
    virtual int getSequence() = 0;
};

struct IHThorApplyArg : public IHThorArg
{
    virtual void apply(const void * src) = 0;
    virtual int getSequence() = 0;
    virtual void start() = 0;
    virtual void end() = 0;
};

enum
{
    TAFconstant         = 0x0001,

    TAFstable           = 0x0002,
    TAFunstable         = 0x0004,
    TAFspill            = 0x0008,
    TAFparallel         = 0x0010,
};

struct IHThorSortArg : public IHThorArg
{
    virtual const char * getSortedFilename()=0;
    virtual IOutputMetaData * querySortedRecordSize()=0;
    virtual ICompare * queryCompare()=0;
    virtual ICompare * queryCompareLeftRight()=0;
    virtual ISortKeySerializer * querySerialize() = 0;
    virtual unsigned __int64 getThreshold() = 0;                                // limit to size of dataset on a node. (0=default)
    virtual double getSkew() = 0;
    virtual bool hasManyRecords() = 0;
    virtual double getTargetSkew() = 0;
    virtual ICompare * queryCompareSerializedRow()=0;                           // null if row already serialized, or if compare not available
    virtual unsigned getAlgorithmFlags() = 0;
    virtual const char * getAlgorithm() = 0;
};

typedef IHThorSortArg IHThorSortedArg;

struct IHThorTopNExtra : public IInterface
{
    virtual __int64 getLimit() = 0;
    virtual bool hasBest() = 0;
    virtual int compareBest(const void * _left) = 0;
};

struct IHThorTopNArg : public IHThorSortArg, public IHThorTopNExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorSubSortExtra : public IInterface
{
    virtual bool isSameGroup(const void * _left, const void * _right) = 0;
};

struct IHThorSubSortArg : public IHThorSortArg, public IHThorSubSortExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

// JoinFlags
enum { 
    JFleftouter                  = 0x00000001,
    JFrightouter                 = 0x00000002,
    JFexclude                    = 0x00000004,
    JFleftonly  =JFleftouter|JFexclude,
    JFrightonly =JFrightouter|JFexclude,
    JFtypemask  =JFleftouter|JFrightouter|JFexclude,
    JFfirst                      = 0x00000008,
    JFfirstleft                  = 0x00000010,
    JFfirstright                 = 0x00000020,
    JFpartitionright             = 0x00000040,
    JFtransformMaySkip           = 0x00000080,
    JFfetchMayFilter             = 0x00000100,
    JFmatchAbortLimitSkips       = 0x00000200,
    JFonfail                     = 0x00000400,
    JFindexoptional              = 0x00000800,
    JFslidingmatch               = 0x00001000,
    JFextractjoinfields          = 0x00002000,
    JFmatchrequired              = 0x00004000,
    JFmanylookup                 = 0x00008000,
    JFparallel                   = 0x00010000,
    JFsequential                 = 0x00020000,
    JFcountmatchabortlimit       = 0x00080000,
    JFreorderable                = 0x00100000,
    JFtransformmatchesleft       = 0x00200000,
    JFvarindexfilename           = 0x00400000,
    JFdynamicindexfilename       = 0x00800000,
    JFlimitedprefixjoin          = 0x01000000,
    JFindexfromactivity          = 0x02000000,
    JFleftSortedLocally          = 0x04000000,
    JFrightSortedLocally         = 0x08000000,
    JFsmart                      = 0x10000000,
    JFunstable                   = 0x20000000, // can sorts be unstable?
    JFnevermatchself             = 0x40000000, // for a self join can a record match itself
    JFnewfilters                 = 0x80000000, // using FieldFilters not segmonitors
};

// FetchFlags
enum {
    FFdatafileoptional           = 0x0001,
    FFvarfilename                = 0x0002,
    FFdynamicfilename            = 0x0004,
    FFinvariantfilename          = 0x0008,
};  

// JoinTransformFlags
enum {
    JTFmatchedleft           = 0x0001,
    JTFmatchedright          = 0x0002
};

struct IHThorAnyJoinBaseArg : public IHThorArg
{
    virtual bool match(const void * _left, const void * _right) = 0;
    virtual size32_t createDefaultLeft(ARowBuilder & rowBuilder) = 0;
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder) = 0;
    virtual unsigned getJoinFlags() = 0;
    virtual unsigned getKeepLimit() = 0;
    virtual unsigned getMatchAbortLimit() = 0;
    virtual void onMatchAbortLimitExceeded() = 0;

//Join:
//Denormalize
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _count, unsigned _flags) = 0;
//Denormalize group
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, const void * _right, unsigned _numRows, const void * * _rows, unsigned _flags) = 0;

    inline bool isLeftAlreadyLocallySorted() { return (getJoinFlags() & JFleftSortedLocally) != 0; }
    inline bool isRightAlreadyLocallySorted() { return (getJoinFlags() & JFrightSortedLocally) != 0; }
};


struct IHThorJoinBaseArg : public IHThorAnyJoinBaseArg
{
    virtual ICompare * queryCompareRight()=0;
    virtual ICompare * queryCompareLeft()=0;
    virtual bool isLeftAlreadySorted() = 0;
    virtual bool isRightAlreadySorted() = 0;
    virtual ICompare * queryCompareLeftRight()=0;
    virtual ISortKeySerializer * querySerializeLeft() = 0;
    virtual ISortKeySerializer * querySerializeRight() = 0;
    virtual unsigned __int64 getThreshold() = 0;                                // limit to size of dataset on a node. (0=default)
    virtual double getSkew() = 0;
    virtual unsigned getJoinLimit() = 0;                                        // if a key joins more than this limit no records are output (0 = no limit)
    virtual double getTargetSkew() = 0;
    virtual ICompare * queryCompareLeftRightLower() = 0;
    virtual ICompare * queryCompareLeftRightUpper() = 0;
    virtual ICompare * queryPrefixCompare() = 0;

    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _left, const void * _right, IException * e, unsigned flags) = 0;
    virtual ICompare * queryCompareLeftKeyRightRow()=0;                         // compare serialized left key with right row
    virtual ICompare * queryCompareRightKeyLeftRow()=0;                         // as above if partition right selected
};

struct IHThorFetchContext
{
    virtual unsigned __int64 extractPosition(const void * _right) = 0;  // Gets file position value from rhs row
    virtual const char * getFileName() = 0;                 // Returns filename of raw file fpos'es refer into
    virtual IOutputMetaData * queryDiskRecordSize() = 0;            // Expected layout
    virtual IOutputMetaData * queryProjectedDiskRecordSize() = 0;   // Projected layout
    virtual unsigned getFetchFlags() = 0;
    virtual unsigned getDiskFormatCrc() = 0;
    virtual unsigned getProjectedFormatCrc() = 0;
    virtual void getFileEncryptKey(size32_t & keyLen, void * & key) = 0;
};

struct IHThorKeyedJoinBaseArg : public IHThorArg
{
    // For the data going to the indexRead remote activity:
    virtual size32_t extractIndexReadFields(ARowBuilder & rowBuilder, const void * _input) = 0;
    virtual IOutputMetaData * queryIndexReadInputRecordSize() = 0;
    virtual bool leftCanMatch(const void * inputRow) = 0;

    // Inside the indexRead remote activity:
    virtual const char * getIndexFileName() = 0;
    virtual IOutputMetaData * queryIndexRecordSize() = 0;            // Expected layout
    virtual IOutputMetaData * queryProjectedIndexRecordSize() = 0;   // Projected layout
    virtual void createSegmentMonitors(IIndexReadContext *ctx, const void *lhs) = 0;
    virtual bool indexReadMatch(const void * indexRow, const void * inputRow, IBlobProvider * blobs) = 0;
    virtual unsigned getJoinLimit() = 0;                                        // if a key joins more than this limit no records are output (0 = no limit)
    virtual unsigned getKeepLimit() = 0;                                        // limit to number of matches that are kept (0 = no limit)
    virtual unsigned getIndexFormatCrc() = 0;
    virtual unsigned getProjectedIndexFormatCrc() = 0;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) = 0;

    // For the data going to the fetch remote activity:
    virtual size32_t extractFetchFields(ARowBuilder & rowBuilder, const void * _input) = 0;
    virtual IOutputMetaData * queryFetchInputRecordSize() = 0;

    // Inside the fetch remote activity
    virtual bool fetchMatch(const void * diskRow, const void * inputRow) = 0;
    virtual size32_t extractJoinFields(ARowBuilder & rowBuilder, const void * diskRowOr, IBlobProvider * blobs) = 0;
    virtual IOutputMetaData * queryJoinFieldsRecordSize() = 0;

    // Back at the server
    virtual size32_t createDefaultRight(ARowBuilder & rowBuilder) = 0;
    virtual unsigned getJoinFlags() = 0;
    virtual bool diskAccessRequired() = 0;                  // if false, all transform values can be fulfilled from the key, which is passed as right.
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
    virtual unsigned __int64 getSkipLimit() = 0;
    virtual unsigned getMatchAbortLimit() = 0;
    virtual void onMatchAbortLimitExceeded() = 0;

    //keyedFpos is always 0
    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * _dummyRight, const void * _origRow, unsigned __int64 keyedFpos, IException * e) = 0;
//Join:
//Denormalize:
    //The _filepos field is used for full keyed join to pass in the fileposition.  It should really use the disk IThorDiskCallback interface.
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned __int64 keyedFpos, unsigned counter) = 0;
//Denormalize group:
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _joinFields, const void * _origRow, unsigned _numRows, const void * * _rows) = 0;

    inline bool hasNewSegmentMonitors()                     { return (getJoinFlags() & JFnewfilters) != 0; }

};

struct IHThorKeyedJoinArg : public IHThorKeyedJoinBaseArg, public IHThorFetchContext
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorJoinArg : public IHThorJoinBaseArg
{
};
typedef IHThorJoinArg IHThorDenormalizeArg;

typedef IHThorAnyJoinBaseArg IHThorAllJoinArg;

// Used for hash and lookup joins.
struct IHThorHashJoinExtra : public IInterface
{
    virtual IHash * queryHashLeft()=0;
    virtual IHash * queryHashRight()=0;
};

struct IHThorHashJoinArg : public IHThorJoinArg, public IHThorHashJoinExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

typedef IHThorHashJoinArg IHThorHashDenormalizeArg;
typedef IHThorHashJoinArg IHThorHashDenormalizeGroupArg;

enum
{
    KDFvarindexfilename     = 0x00000001,
    KDFdynamicindexfilename = 0x00000002,
    KDFnewfilters           = 0x00000004,
};

struct IHThorKeyedDistributeArg : public IHThorArg
{
    // Inside the indexRead remote activity:
    virtual const char * getIndexFileName() = 0;
    virtual IOutputMetaData * queryIndexRecordSize() = 0; //Excluding fpos and sequence
    virtual void createSegmentMonitors(IIndexReadContext *ctx, const void *lhs) = 0;
    virtual unsigned getFlags() = 0;
    virtual ICompare * queryCompareRowKey() = 0;
    virtual unsigned getFormatCrc() = 0;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) = 0;
    inline bool hasNewSegmentMonitors() { return (getFlags() & KDFnewfilters) != 0; }
};


struct IHThorFetchBaseArg : public IHThorArg, public IHThorFetchContext
{
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
    inline  bool transformNeedsRhs()       { return queryExtractedSize() != nullptr; }
    virtual size32_t extractJoinFields(ARowBuilder & rowBuilder, const void * _right) = 0;
    virtual bool extractAllJoinFields() = 0;
    virtual IOutputMetaData * queryExtractedSize() = 0;
};

struct IHThorBinFetchExtra : public IInterface
{
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _raw, const void * _key, unsigned __int64 _fpos) = 0;
};

struct IHThorFetchArg : public IHThorFetchBaseArg, public IHThorBinFetchExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

enum 
{
    POFextend           = 0x0001,
    POFgrouped          = 0x0002,
    POFmaxsize          = 0x0004,
};

struct IHThorWorkUnitWriteArg : public IHThorArg
{
    virtual int getSequence() = 0;
    virtual void serializeXml(const byte * self, IXmlWriter & out) = 0;
    virtual const char * queryName() = 0;
    virtual unsigned getFlags() = 0;
    virtual unsigned getMaxSize() = 0; // size in Mb
};

struct IHThorXmlWorkunitWriteArg : public IHThorWorkUnitWriteArg
{
    //Use a base class to add any functions here
};

struct IHThorHashDistributeArg : public IHThorArg
{
    virtual IHash    * queryHash()=0;
    virtual bool       isPulled()=0;
    virtual double     getSkew()=0;             // iff queryHash returns NULL
    virtual double     getTargetSkew()=0;
    virtual ICompare * queryMergeCompare()=0;       // iff TAKhasdistributemerge
};

enum
{
    SDFisall   = 0x0001,
};


struct IHThorNWayDistributeArg : public IHThorArg
{
    virtual unsigned   getFlags()=0;
    virtual bool       include(const byte * left, unsigned targetNode) = 0;
    inline bool        isAll() { return (getFlags() & SDFisall) != 0; }
};

struct IHThorHashDedupArg : public IHThorArg
{
    virtual ICompare * queryCompare()=0;
    virtual IHash    * queryHash()=0;
    virtual IOutputMetaData * queryKeySize() = 0;
    virtual size32_t recordToKey(ARowBuilder & rowBuilder, const void * _record) = 0;
    virtual ICompare * queryKeyCompare()=0;
    virtual unsigned getFlags() = 0;
    virtual IHash    * queryKeyHash()=0;
    virtual ICompare * queryRowKeyCompare()=0; // lhs is a row, rhs is a key
    virtual ICompare * queryCompareBest()=0;
    inline bool compareAll() { return (getFlags() & HDFcompareall) != 0; }
    inline bool keepLeft() { return (getFlags() & HDFkeepleft) != 0; }
    inline bool keepBest() { return (getFlags() & HDFkeepbest) != 0; }
};

struct IHThorHashMinusArg : public IHThorArg
{
    virtual ICompare * queryCompareLeft()=0;
    virtual ICompare * queryCompareRight()=0;
    virtual ICompare * queryCompareLeftRight()=0;
    virtual IHash    * queryHashLeft()=0;
    virtual IHash    * queryHashRight()=0;
};


struct IHThorIfArg : public IHThorArg
{
    virtual bool getCondition() = 0;
};

struct IHThorCaseArg : public IHThorArg
{
    virtual unsigned getBranch() = 0;
};


struct IHThorSequentialArg : public IHThorArg
{
    virtual unsigned numBranches() = 0;
};

struct IHThorParallelArg : public IHThorArg
{
    virtual unsigned numBranches() = 0;
};

enum 
{
    KDPoverwrite            = 0x0001,
    KDPtransform            = 0x0002,
    KDPvaroutputname        = 0x0004,
    KDPnooverwrite          = 0x0008,
    KDPexpires              = 0x0010,
};

struct IHThorKeyDiffArg : public IHThorArg
{
    virtual unsigned getFlags() = 0;
    virtual const char * getOriginalName() = 0;
    virtual const char * getUpdatedName() = 0;
    virtual const char * getOutputName() = 0;
    virtual int getSequence() = 0;
    virtual unsigned getExpiryDays() = 0;
};

struct IHThorKeyPatchArg : public IHThorArg
{
    virtual unsigned getFlags() = 0;
    virtual const char * getOriginalName() = 0;           // may be null
    virtual const char * getPatchName() = 0;
    virtual const char * getOutputName() = 0;
    virtual int getSequence() = 0;
    virtual unsigned getExpiryDays() = 0;
};


struct IHThorWorkunitReadArg : public IHThorArg
{
    virtual const char * queryName() = 0;
    virtual int querySequence() = 0;
    virtual const char * getWUID() = 0;
    virtual ICsvToRowTransformer * queryCsvTransformer() = 0;
    virtual IXmlToRowTransformer * queryXmlTransformer() = 0;
};

struct IHThorLocalResultReadArg : public IHThorArg
{
    virtual unsigned querySequence() = 0;
};

struct IHThorLocalResultWriteArg : public IHThorArg
{
    virtual unsigned querySequence() = 0;
    virtual bool usedOutsideGraph() = 0;
};

struct IHThorGraphLoopResultReadArg : public IHThorArg
{
    virtual unsigned querySequence() = 0;
};

struct IHThorGraphLoopResultWriteArg : public IHThorArg
{
};

typedef IHThorLocalResultWriteArg IHThorLocalResultSpillArg;

//-- Csv --

struct ICsvParameters
{
    enum
    {
        defaultQuote =        0x0001,
        defaultSeparate =     0x0002,
        defaultTerminate =    0x0004,
        hasUnicode =          0x0008,
        singleHeaderFooter =  0x0010,
        preserveWhitespace =  0x0020,
        manyHeaderFooter =    0x0040,
        defaultEscape =       0x0080,
    }; // flags values
    virtual unsigned     getFlags() = 0;
    virtual bool         queryEBCDIC() = 0;
    virtual const char * getHeader()              { return NULL; }
    virtual unsigned     queryHeaderLen() = 0;
    virtual size32_t     queryMaxSize() = 0;
    virtual const char * getQuote(unsigned idx) = 0;
    virtual const char * getSeparator(unsigned idx) = 0;
    virtual const char * getTerminator(unsigned idx) = 0;
    virtual const char * getEscape(unsigned idx) = 0;
    virtual const char * getFooter()              { return NULL; }
};

struct ITypedOutputStream
{
public:
    virtual void writeReal(double value) = 0;
    virtual void writeSigned(__int64 value) = 0;
    virtual void writeString(size32_t len, const char * data) = 0;
    virtual void writeUnicode(size32_t len, const UChar * data) = 0;
    virtual void writeUnsigned(unsigned __int64 value) = 0;
    virtual void writeUtf8(size32_t len, const char * data) = 0;
};

struct IHThorCsvWriteExtra : public IInterface
{
    virtual ICsvParameters * queryCsvParameters() = 0;
    virtual void writeRow(const byte * self, ITypedOutputStream * out) = 0;
};


struct IHThorCsvWriteArg : public IHThorDiskWriteArg, public IHThorCsvWriteExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};


struct IHThorCsvFetchExtra: public IInterface
{
    virtual unsigned getMaxColumns() = 0;
    virtual ICsvParameters * queryCsvParameters() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned * lenLeft, const char * * dataLeft, const void * _key, unsigned __int64 _fpos) = 0;
};

struct IHThorCsvFetchArg : public IHThorFetchBaseArg, public IHThorCsvFetchExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

//-- Xml --

struct IHThorXmlParseArg : public IHThorArg
{
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * left, IColumnProvider * parsed) = 0;
    virtual const char * getXmlIteratorPath() = 0;
    virtual void getSearchText(size32_t & retLen, char * & retText, const void * _self) = 0;
    virtual bool searchTextNeedsFree() = 0;
    virtual bool requiresContents() = 0;
};

struct IHThorXmlFetchExtra : public IInterface
{
    virtual size32_t transform(ARowBuilder & rowBuilder, IColumnProvider * rowLeft, const void * right, unsigned __int64 _fpos) = 0;
    virtual bool requiresContents() = 0;
};

struct IHThorXmlFetchArg : public IHThorFetchBaseArg, public IHThorXmlFetchExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

//Simple xml generation...
struct IHThorXmlWriteExtra : public IInterface
{
    virtual void toXML(const byte * self, IXmlWriter & out) = 0;
    virtual const char * getXmlIteratorPath() = 0;
    virtual const char * getHeader() = 0;
    virtual const char * getFooter() = 0;
    virtual unsigned getXmlFlags() = 0;
};

struct IHThorXmlWriteArg : public IHThorDiskWriteArg, public IHThorXmlWriteExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};


//-- PIPE access functions --

enum 
{
    TPFwritecsvtopipe       = 0x0001,
    TPFwritexmltopipe       = 0x0002,
    TPFreadcsvfrompipe      = 0x0004,
    TPFreadxmlfrompipe      = 0x0008,
    TPFreadusexmlcontents   = 0x0010,
    TPFreadnoroot           = 0x0020,
    TPFwritenoroot          = 0x0040,

    TPFrecreateeachrow      = 0x0100,
    TPFgroupeachrow         = 0x0200,
    TPFnofail               = 0x0400,
};


struct IHThorPipeReadArg : public IHThorArg
{
    virtual const char * getPipeProgram() = 0;
    virtual IOutputMetaData * queryDiskRecordSize() = 0;                // currently matches queryOutputMeta()
    virtual unsigned getPipeFlags() = 0;
    virtual ICsvToRowTransformer * queryCsvTransformer() = 0;
    virtual IXmlToRowTransformer * queryXmlTransformer() = 0;
    virtual const char * getXmlIteratorPath() = 0;
};

struct IHThorPipeWriteArg : public IHThorArg
{
    virtual const char * getPipeProgram() = 0;
    virtual int getSequence() = 0;
    virtual IOutputMetaData * queryDiskRecordSize() = 0;
    virtual char * getNameFromRow(const void * _self) = 0;
    virtual bool recreateEachRow() = 0;
    virtual unsigned getPipeFlags() = 0;
    virtual IHThorCsvWriteExtra * queryCsvOutput() = 0;
    virtual IHThorXmlWriteExtra * queryXmlOutput() = 0;
};

struct IHThorPipeThroughArg : public IHThorArg
{
    virtual const char * getPipeProgram() = 0;
    virtual char * getNameFromRow(const void * _self) = 0;
    virtual bool recreateEachRow() = 0;
    virtual unsigned getPipeFlags() = 0;
    virtual IHThorCsvWriteExtra * queryCsvOutput() = 0;
    virtual IHThorXmlWriteExtra * queryXmlOutput() = 0;
    virtual ICsvToRowTransformer * queryCsvTransformer() = 0;
    virtual IXmlToRowTransformer * queryXmlTransformer() = 0;
    virtual const char * getXmlIteratorPath() = 0;
};


//-- SOAP --

enum
{
    SOAPFgroup          = 0x000001,
    SOAPFonfail         = 0x000002,
    SOAPFlog            = 0x000004,
    SOAPFtrim           = 0x000008,
    SOAPFliteral        = 0x000010,
    SOAPFnamespace      = 0x000020,
    SOAPFencoding       = 0x000040,
    SOAPFpreserveSpace  = 0x000080,
    SOAPFlogmin         = 0x000100,
    SOAPFlogusermsg     = 0x000200,
    SOAPFhttpheaders    = 0x000400,
    SOAPFusescontents   = 0x000800,
    SOAPFmarkupinfo     = 0x001000,
    SOAPFxpathhints     = 0x002000,
    SOAPFnoroot         = 0x004000,
    SOAPFjson           = 0x008000,
    SOAPFxml            = 0x010000
};

enum
{
};

struct IHThorWebServiceCallActionArg : public IHThorArg
{
    virtual const char * getHosts() = 0;
    virtual const char * getService() = 0;

//writing to the soap service.
    virtual void toXML(const byte * self, IXmlWriter & out) = 0;
    virtual const char * getHeader() = 0;
    virtual const char * getFooter() = 0;
    virtual unsigned getFlags() = 0;
    virtual unsigned numParallelThreads() = 0;
    virtual unsigned numRecordsPerBatch() = 0;
    virtual int numRetries() = 0;
    virtual double getTimeout() = 0;
    virtual double getTimeLimit() = 0;
    virtual const char * getSoapAction() = 0;
    virtual const char * getNamespaceName() = 0;
    virtual const char * getNamespaceVar() = 0;
    virtual const char * getHttpHeaderName() = 0;
    virtual const char * getHttpHeaderValue() = 0;
    virtual const char * getProxyAddress() = 0;
    virtual const char * getAcceptType() = 0;
    virtual const char * getHttpHeaders() = 0;

    virtual IXmlToRowTransformer * queryInputTransformer() = 0;
    virtual const char * getInputIteratorPath() = 0;
    virtual size32_t onFailTransform(ARowBuilder & rowBuilder, const void * left, IException * e) = 0;
    virtual void getLogText(size32_t & lenText, char * & text, const void * left) = 0;  // iff SOAPFlogusermsg set
    virtual const char * getXpathHintsXml() = 0; //iff SOAPFxpathhints
    virtual const char * getRequestHeader() = 0;
    virtual const char * getRequestFooter() = 0;
};
typedef IHThorWebServiceCallActionArg IHThorSoapActionArg ;
typedef IHThorWebServiceCallActionArg IHThorHttpActionArg ;

class IHThorWebServiceCallArg : public IHThorWebServiceCallActionArg
{
};
typedef IHThorWebServiceCallArg IHThorSoapCallArg ;
typedef IHThorWebServiceCallArg IHThorHttpCallArg ;


typedef IHThorNullArg IHThorDatasetResultArg;
typedef IHThorNullArg IHThorRowResultArg;

//-- Parsing --

interface IMatchedResults
{
public:
    virtual bool getMatched(unsigned idx) = 0;
    virtual size32_t getMatchLength(unsigned idx) = 0;
    virtual size32_t getMatchPosition(unsigned idx) = 0;
    virtual void getMatchText(size32_t & outlen, char * & out, unsigned idx) = 0;
    virtual void getMatchUnicode(size32_t & outlen, UChar * & out, unsigned idx) = 0;
    virtual byte * queryRootResult() = 0;
    virtual byte * queryMatchRow(unsigned idx) = 0;
    virtual void getMatchUtf8(size32_t & outlen, char * & out, unsigned idx) = 0;
};


interface IProductionCallback
{
public:
    virtual void getText(size32_t & outlen, char * & out, unsigned idx) = 0;
    virtual void getUnicode(size32_t & outlen, UChar * & out, unsigned idx) = 0;
    virtual byte * queryResult(unsigned idx) = 0;
    virtual void getUtf8(size32_t & outlen, char * & out, unsigned idx) = 0;
};

interface IMatchWalker;
interface IValidator
{
};

interface IStringValidator : public IValidator
{
public:
    virtual bool isValid(size32_t len, const char * text) = 0;
};

interface IUnicodeValidator : public IValidator
{
public:
    virtual bool isValid(size32_t len, const UChar * text) = 0;
};

interface INlpHelper
{
public:
    virtual IValidator * queryValidator(unsigned idx) = 0;
};

struct IHThorParseArg : public IHThorArg
{
    enum { PFgroup = 1, PFparallel=2 };
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * left, IMatchedResults * results, IMatchWalker * walker) = 0;
    virtual void getSearchText(size32_t & retLen, char * & retText, const void * _self) = 0;
    virtual bool searchTextNeedsFree() = 0;
    virtual void queryCompiled(IResourceContext *ctx, size32_t & retLen, const void * & retData) = 0;
    virtual INlpHelper * queryHelper() = 0;
    virtual unsigned getFlags() = 0;
    virtual IOutputMetaData * queryProductionMeta(unsigned id) = 0;
    virtual size32_t executeProduction(ARowBuilder & rowBuilder, unsigned id, IProductionCallback * input) = 0;
};

//------------------------- New interfaces for compound source activities -------------------------

enum { DISTANCE_EXACT_MATCH = 0x80000000 };
interface IDistanceCalculator
{
    //returns which field has the difference, and distance in the 1st paramater.  Returns DISTANCE_EXACT_MATCH if all fields match.
    virtual unsigned getDistance(unsigned __int64 & distance, const void * before, const void * after, unsigned numFields) const =0;
protected:
    virtual ~IDistanceCalculator() {}
};


interface ISteppingMeta
{
    virtual unsigned getNumFields() = 0;
    virtual const CFieldOffsetSize * queryFields() = 0;     // order output by this activity (for merge/join = merge list)
    virtual IRangeCompare * queryCompare() = 0;             // NULL if can use memcmp to compare the fields?
    virtual IDistanceCalculator * queryDistance() = 0;
};

//These were commoned up, but really they are completely different - so keep them separate
interface IThorDiskCallback : extends IFilePositionProvider
{
    virtual const char * queryLogicalFilename(const void * row) = 0;
    virtual const byte * lookupBlob(unsigned __int64 id) = 0;         // return reference, not freed by code generator, can dispose once transform() has returned.
};

interface IThorIndexCallback : extends IInterface
{
    virtual const byte * lookupBlob(unsigned __int64 id) = 0;         // return reference, not freed by code generator, can dispose once transform() has returned.
};

enum
{
    SSFalwaysfilter     = 0x0001,
    SSFhaspriority      = 0x0002,
    SSFhasprefetch      = 0x0004,
    SSFisjoin           = 0x0008,
};

interface IHThorSteppedSourceExtra
{
    virtual unsigned getSteppedFlags() = 0;
    virtual double getPriority() = 0;
    virtual unsigned getPrefetchSize() = 0;
};


// Read, Normalize, Aggregate, Count, GroupAggregate, NormalizeAggregate
// any activity could theoretically have its (top-level) input filtered by segement monitors, 
// so included below, but TAKchildXXX won't in practice.  Filters are merged into the transform
// where-ever possible because that improves the scope for cse.
struct IHThorCompoundBaseArg : public IHThorArg
{
    virtual bool canMatchAny() = 0;
    virtual void createSegmentMonitors(IIndexReadContext *ctx) = 0;
    virtual bool canMatch(const void * row) = 0;
    virtual bool hasMatchFilter() = 0;
};

struct IHThorIndexReadBaseArg : extends IHThorCompoundBaseArg
{
    virtual const char * getFileName() = 0;
    virtual IOutputMetaData * queryDiskRecordSize() = 0;            // Expected layout
    virtual IOutputMetaData * queryProjectedDiskRecordSize() = 0;   // Projected layout
    virtual unsigned getFlags() = 0;
    virtual unsigned getProjectedFormatCrc() = 0;                   // Corresponding to projectedDiskRecordSize
    virtual unsigned getDiskFormatCrc() = 0;                        // Corresponding to diskRecordSize
    virtual void setCallback(IThorIndexCallback * callback) = 0;
    virtual bool getIndexLayout(size32_t & _retLen, void * & _retData) = 0;

    inline bool hasSegmentMonitors()                        { return (getFlags() & TIRnofilter) == 0; }
    inline bool hasNewSegmentMonitors()                     { return (getFlags() & TIRnewfilters) != 0; }
    virtual IHThorSteppedSourceExtra *querySteppingExtra() = 0;
};

struct IHThorDiskReadBaseArg : extends IHThorCompoundBaseArg
{
    virtual const char * getFileName() = 0;
    virtual IOutputMetaData * queryDiskRecordSize() = 0;            // Expected layout
    virtual IOutputMetaData * queryProjectedDiskRecordSize() = 0;   // Projected layout
    virtual unsigned getFlags() = 0;
    virtual unsigned getDiskFormatCrc() = 0;
    virtual unsigned getProjectedFormatCrc() = 0;
    virtual void getEncryptKey(size32_t & keyLen, void * & key) = 0;
    virtual void setCallback(IThorDiskCallback * callback) = 0;

    inline bool hasSegmentMonitors()                        { return (getFlags() & TDRkeyed) != 0; }
};


//New prototype interface for reading any format file through the same interface
//liable to change at any point.
struct IHThorNewDiskReadBaseArg : extends IHThorDiskReadBaseArg
{
    virtual const char * queryFormat() = 0;
    virtual void getFormatOptions(IXmlWriter & options) = 0;
    virtual void getFormatDynOptions(IXmlWriter & options) = 0;
};

//The following are mixin classes added to one of the activity base interfaces above.
// common between Read, Normalize
struct IHThorCompoundExtra : public IInterface
{
    virtual unsigned __int64 getChooseNLimit() = 0;
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
};

struct IHThorSourceCountLimit : public IInterface
{
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
    virtual unsigned __int64 getKeyedLimit() = 0;
    virtual void onKeyedLimitExceeded() = 0;
};

struct IHThorSourceLimitTransformExtra : public IInterface
{
    virtual size32_t transformOnLimitExceeded(ARowBuilder & rowBuilder) = 0;
    virtual size32_t transformOnKeyedLimitExceeded(ARowBuilder & rowBuilder) = 0;
};

//Read
struct IHThorCompoundReadExtra : public IHThorCompoundExtra
{
    virtual bool needTransform() = 0;
    virtual bool transformMayFilter() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * src) = 0;
    virtual unsigned __int64 getKeyedLimit() = 0;
    virtual void onKeyedLimitExceeded() = 0;
//v2 - stepping added
    virtual ISteppingMeta * queryRawSteppingMeta() = 0;
    virtual ISteppingMeta * queryProjectedSteppingMeta() = 0;
    virtual void mapOutputToInput(ARowBuilder & rowBuilder, const void * projectedRow, unsigned numFields) = 0;
    virtual size32_t unfilteredTransform(ARowBuilder & rowBuilder, const void * src) = 0;       // only valid if TIRunmatchedtransform is set
};

//Normalize
struct IHThorCompoundNormalizeExtra : public IHThorCompoundExtra
{
    virtual bool first(const void * src) = 0;
    virtual bool next() = 0;                            //NB: src from transformFirst() must stay in memory while transformNext() is being called.
    virtual size32_t transform(ARowBuilder & rowBuilder) = 0;
    virtual unsigned __int64 getKeyedLimit() = 0;
    virtual void onKeyedLimitExceeded() = 0;
};


//Aggregate
struct IHThorCompoundAggregateExtra : public IInterface
{
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) = 0;
    virtual bool processedAnyRows() = 0;
    virtual void processRow(ARowBuilder & rowBuilder, const void * src) = 0;
    virtual void processRows(ARowBuilder & rowBuilder, size32_t srcLen, const void * src) = 0;
    virtual size32_t mergeAggregate(ARowBuilder & rowBuilder, const void * src) = 0;        //only call if transform called at least once on src.
};

//Count
struct IHThorCompoundCountExtra : public IInterface
{
    virtual bool hasFilter() = 0;
    virtual size32_t numValid(const void * src) = 0;
    virtual size32_t numValid(size32_t srcLen, const void * src) = 0;
    virtual unsigned __int64 getChooseNLimit() = 0;
};

//NormalizeAggregate
struct IHThorGroupAggregateCallback : public IInterface
{
    virtual void processRow(const void * src) = 0;
};

struct IHThorCompoundGroupAggregateExtra : implements IHThorHashAggregateExtra, implements IHThorRowAggregator
{
    virtual void processRow(const void * src, IHThorGroupAggregateCallback * callback) = 0;
    virtual void processRows(size32_t srcLen, const void * src, IHThorGroupAggregateCallback * callback) = 0;
    virtual bool createGroupSegmentMonitors(IIndexReadContext *ctx) = 0;

    //Only applicable to index count variants.
    virtual size32_t initialiseCountGrouping(ARowBuilder & rowBuilder, const void * src) = 0;
    virtual size32_t processCountGrouping(ARowBuilder & rowBuilder, unsigned __int64 count) = 0;
    virtual unsigned getGroupingMaxField() = 0;
};

//------------------------- Concrete definitions -------------------------

struct IHThorIndexReadArg : extends IHThorIndexReadBaseArg, extends IHThorSourceLimitTransformExtra, extends IHThorCompoundReadExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorIndexNormalizeArg : extends IHThorIndexReadBaseArg, extends IHThorSourceLimitTransformExtra, extends IHThorCompoundNormalizeExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorIndexAggregateArg : extends IHThorIndexReadBaseArg, extends IHThorCompoundAggregateExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorIndexCountArg : extends IHThorIndexReadBaseArg, extends IHThorCompoundCountExtra, extends IHThorSourceCountLimit
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorIndexGroupAggregateArg : extends IHThorIndexReadBaseArg, extends IHThorCompoundGroupAggregateExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};


struct IHThorDiskReadArg : extends IHThorDiskReadBaseArg, extends IHThorSourceLimitTransformExtra, extends IHThorCompoundReadExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorDiskNormalizeArg : extends IHThorDiskReadBaseArg, extends IHThorSourceLimitTransformExtra, extends IHThorCompoundNormalizeExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorDiskAggregateArg : extends IHThorDiskReadBaseArg, extends IHThorCompoundAggregateExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorDiskCountArg : extends IHThorDiskReadBaseArg, extends IHThorCompoundCountExtra, extends IHThorSourceCountLimit
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorDiskGroupAggregateArg : extends IHThorDiskReadBaseArg, extends IHThorCompoundGroupAggregateExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};


struct IHThorNewDiskReadArg : extends IHThorNewDiskReadBaseArg, extends IHThorSourceLimitTransformExtra, extends IHThorCompoundReadExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};


struct IHThorCsvReadArg: public IHThorDiskReadBaseArg
{
    virtual unsigned getMaxColumns() = 0;
    virtual ICsvParameters * queryCsvParameters() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned * srcLen, const char * * src) = 0;
    virtual unsigned __int64 getChooseNLimit() = 0;
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
};

struct IHThorXmlReadArg: public IHThorDiskReadBaseArg
{
    virtual IXmlToRowTransformer * queryTransformer() = 0;
    virtual const char * getXmlIteratorPath() = 0;
    virtual unsigned __int64 getChooseNLimit() = 0;
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
};

typedef unsigned thor_loop_counter_t;
struct IHThorLoopArg : public IHThorArg
{
    enum { 
        LFparallel = 1,
        LFcounter = 2,
        LFfiltered = 4,
        LFnewloopagain = 8,
    };
    virtual unsigned getFlags() = 0;
    virtual bool sendToLoop(unsigned counter, const void * in) = 0;         // does the input row go to output or round the loop?
    virtual unsigned numIterations() = 0;                   // 0 if using loopAgain() instead.
    virtual bool loopAgain(unsigned counter, unsigned numRows, const void * * _rows)    = 0;
    virtual void createParentExtract(rtlRowBuilder & builder) = 0;
    virtual unsigned defaultParallelIterations() = 0;
    //If new loop again is set the following should be used instead of loopAgain
    virtual bool loopFirstTime() = 0;
    virtual unsigned loopAgainResult() = 0;  // which result contains the indication of whether to loop again?
};


struct IHThorGraphLoopArg : public IHThorArg
{
    enum { 
        GLFparallel = 1,
        GLFcounter = 2,
    };
    virtual unsigned getFlags() = 0;
    virtual unsigned numIterations() = 0;
    virtual void createParentExtract(rtlRowBuilder & builder) = 0;
};


struct IHThorRemoteArg : public IHThorArg
{
    virtual void createParentExtract(rtlRowBuilder & builder) = 0;
    virtual unsigned __int64 getRowLimit() = 0;
    virtual void onLimitExceeded() = 0;
};

struct IHThorLibraryCallArg : public IHThorArg
{
    virtual void createParentExtract(rtlRowBuilder & builder) = 0;
    using IHThorArg::queryOutputMeta;
    virtual IOutputMetaData * queryOutputMeta(unsigned whichOutput) = 0;
    virtual char * getLibraryName() = 0;
};

//------ Child varieties ------
// Child versions are defined separately because 
// i) not all versions are availble, 
// ii) they will never use segment monitors
// iii) transforms don't have row passed in.
// iv) Never any need to merge aggregates

//Normalize
struct IHThorChildNormalizeArg : public IHThorArg
{
    virtual bool first() = 0;
    virtual bool next() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder) = 0;
};

//Aggregate
struct IHThorChildAggregateArg : public IHThorArg
{
    virtual size32_t clearAggregate(ARowBuilder & rowBuilder) = 0;
    virtual void processRows(ARowBuilder & rowBuilder) = 0;
};

//NormalizedAggregate
//NB: The child may actually be a grandchild/great-grand child, so need to store some sort of current state in the hash table
struct IHThorChildGroupAggregateBaseArg : public IHThorArg
{
    virtual void processRows(IHThorGroupAggregateCallback * tc) = 0;
};

struct IHThorChildGroupAggregateArg : extends IHThorChildGroupAggregateBaseArg, extends IHThorHashAggregateExtra, implements IHThorRowAggregator
{
    COMMON_NEWTHOR_FUNCTIONS
};


struct IHThorChildThroughNormalizeBaseArg : public IHThorArg
{
};

struct IHThorChildThroughNormalizeArg : public IHThorChildThroughNormalizeBaseArg, extends IHThorCompoundNormalizeExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

//------------------------- Smart stepping activities -------------------------

//Does it make any sense to support these globally in thor?

struct IHThorNWayInputArg : public IHThorArg
{
    virtual void getInputSelection(bool & isAll, size32_t & tlen, void * & tgt) = 0;
};

struct IHThorNWayGraphLoopResultReadArg : public IHThorArg
{
    virtual void getInputSelection(bool & isAll, size32_t & tlen, void * & tgt) = 0;
    virtual bool isGrouped() const = 0;
};

struct IHThorNWayMergeExtra : public IInterface
{
    virtual ISteppingMeta * querySteppingMeta() = 0;
};

struct IHThorNWayMergeArg : extends IHThorMergeArg, extends IHThorNWayMergeExtra
{
    COMMON_NEWTHOR_FUNCTIONS
};

struct IHThorNWaySelectArg : public IHThorArg
{
    virtual unsigned getInputIndex() = 0;
};


//Notes:
//Join condition has an equality part, and an optional range part.  It can merge or transform to generate output
//if (transforming, or has a range) then mergeOrder is the join condition, otherwise it can be larger.
//Stepping information is generated for all fields in the merge order.
struct IHThorNWayMergeJoinArg : public IHThorArg
{
    enum
    {
        MJFinner            = 0x00000000,
        MJFleftonly         = 0x00000001,
        MJFmofn             = 0x00000002,
        MJFleftouter        = 0x00000003,
        MJFkindmask         = 0x0000000F,
        MJFtransform        = 0x00000010,
        MJFdedup            = 0x00000020,
        MJFhasrange         = 0x00000040,               // join condition has range component
        MJFstepped          = 0x00000080,               // ensure that all inputs support stepping.
        MJFhasdistance      = 0x00000100,
        MJFassertsorted     = 0x00000200,
        MJFglobalcompare    = 0x00000400,
        MJFhasclearlow      = 0x00000800,
        MJFhaspartition     = 0x00001000,

    //top bits may be used for temporary flags to test out optimizations - set using INTERNAL(0xnnnnnn)
    };

    virtual unsigned getJoinFlags() = 0;

    virtual ISteppingMeta * querySteppingMeta() = 0;    // meta for 
    virtual IOutputMetaData * queryInputMeta() = 0;
    virtual unsigned numEqualFields() = 0;
    virtual unsigned numOrderFields() = 0;              // how many fields output is ordered by
    virtual ICompare * queryMergeCompare()=0;           // same as querySteepingMeta()->queryCompare(#orderFields)
    virtual ICompare * queryEqualCompare()=0;           // same as querySteppingMeta()->queryCompare(#equalFields);
    virtual ICompareEq * queryEqualCompareEq()=0;       // same as querySteppingMeta()->queryCompare(#equalFields) == 0;
    virtual ICompareEq * queryNonSteppedCompare() = 0;  // non-stepped exact (range) comparison, return NULL if none;
                                                        // requires cross product to be calculated really, so not goof for simple merge join

//For range comparison
    virtual void adjustRangeValue(ARowBuilder & rowBuilder, const void * input, __int64 delta) = 0;     // implementation must ensure field doesn't go -ve.
    virtual unsigned __int64 extractRangeValue(const void * input) = 0;             // distance is assumed to be unsigned, code generator must bias if not true.
    virtual __int64 maxRightBeforeLeft() = 0;
    virtual __int64 maxLeftBeforeRight() = 0;

//MJFtransform
    virtual size32_t transform(ARowBuilder & rowBuilder, unsigned _num, const void * * _rows) = 0;

//MJFleftonly helper
    virtual bool createNextJoinValue(ARowBuilder & rowBuilder, const void * _value) = 0;

//MJFmofn helper
    virtual unsigned getMinMatches() = 0;
    virtual unsigned getMaxMatches() = 0;

//merge join function for comparing all rows. 
    virtual INaryCompareEq * queryGlobalCompare() = 0;  // for merge join, guarded by flag MJFglobalcompare is set.
    virtual size32_t createLowInputRow(ARowBuilder & rowBuilder) = 0;
    virtual ICompareEq * queryPartitionCompareEq()=0;       // only present if MJFhaspartition is defined
};


enum
{
    PPFparallel                 = 0x0001,
};

struct IHThorPrefetchProjectArg : public IHThorArg 
{
    virtual bool canFilter() = 0;
    virtual bool canMatchAny() = 0;
    virtual unsigned getFlags() = 0;
    virtual unsigned getLookahead() = 0;
    virtual IThorChildGraph *queryChild() = 0;
    virtual bool preTransform(rtlRowBuilder & extract, const void * _left, unsigned __int64 _counter) = 0; // returns false if left can be skipped
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, IEclGraphResults * results, unsigned __int64 _counter) = 0;
};


//Combination of filter and [count] project
struct IHThorFilterProjectArg : public IHThorArg
{
    virtual bool canFilter() = 0;
    virtual bool canMatchAny() = 0;
    virtual size32_t transform(ARowBuilder & rowBuilder, const void * _left, unsigned __int64 _counter) = 0;
};

enum {
    TSFprivate              = 0x0001,       //contents can't be seen 
    TSFdynamicDescription   = 0x0002,       //has a get description that depends on the context (otherwise blank)
};

struct IHThorSectionArg : public IHThorArg
{
    virtual unsigned getFlags() = 0;
    virtual void getDescription(size32_t & _retLen, char * & _retData) = 0;
};

struct IHThorSectionInputArg : public IHThorArg
{
    virtual unsigned getFlags() = 0;
};


struct IHThorStreamedIteratorArg : public IHThorArg
{
    virtual IRowStream * createInput() = 0;
};


//------------------------- Dictionary stuff -------------------------

interface IHThorHashLookupInfo
{
    virtual IHash * queryHash() = 0;
    virtual ICompare * queryCompare() = 0;
    virtual IHash * queryHashLookup() = 0;
    virtual ICompare * queryCompareLookup() = 0;
};


struct IHThorDictionaryWorkUnitWriteArg : public IHThorArg
{
    virtual int getSequence() = 0;
    virtual const char * queryName() = 0;
    virtual unsigned getFlags() = 0;
    virtual IHThorHashLookupInfo * queryHashLookupInfo() = 0;
};

struct IHThorDictionaryResultWriteArg : public IHThorArg
{
    virtual unsigned querySequence() = 0;
    virtual bool usedOutsideGraph() = 0;
    virtual IHThorHashLookupInfo * queryHashLookupInfo() = 0;
};

struct IHThorTraceArg : public IHThorArg
{
    virtual bool isValid(const void * _left) = 0;
    virtual bool canMatchAny() = 0;
    virtual unsigned getKeepLimit() = 0;
    virtual unsigned getSample() = 0;
    virtual unsigned getSkip() = 0;
    virtual const char *getName() = 0;
};

//This interface is passed as an implicit parameter to the embed activity factory.  It allows the activity to determine
//if it is being executed in a child query, is stranded and other useful information.
interface IThorActivityContext
{
public:
    virtual bool isLocal() const = 0;
    virtual unsigned numSlaves() const = 0;
    virtual unsigned numStrands() const = 0;
    virtual unsigned querySlave() const = 0; // 0 based 0..numSlaves-1
    virtual unsigned queryStrand() const = 0; // 0 based 0..numStrands-1
};

//MORE: How does is this extended to support onStart/onCreate
//MORE: How is this extended to allow multiple outputs
interface IHThorExternalArg : public IHThorArg
{
    virtual IRowStream * createOutput(IThorActivityContext * activityContext) = 0;
    virtual void execute(IThorActivityContext * activityContext) = 0;
    virtual void setInput(unsigned whichInput, IRowStream * input) = 0;
};

/*
interface IPropertyTree;
interface IThorExternalRowProcessor : public IInterface
{
    virtual void onCreate(ICodeContext * ctx, IPropertyTree * graph) = 0;
    virtual void addInput(unsigned idx, ITypedRowStream * input) = 0;
    virtual IRowStream * createOutput(unsigned idx) = 0;
    virtual void start() = 0;
    virtual void execute() = 0;
    virtual void stop() = 0;
    virtual void reset() = 0;
    virtual void onDestroy() = 0;
};


struct IHThorExternalArg : public IHThorArg
{
    virtual IThorExternalRowProcessor * createProcessor() = 0;
};
*/

//------------------------- Other stuff -------------------------

struct IRemoteConnection;

struct IGlobalCodeContext
{
    virtual ICodeContext * queryCodeContext() = 0;
    virtual void fail(int, const char *) = 0;  

    virtual bool isResult(const char * name, unsigned sequence) = 0;
    virtual unsigned getWorkflowId() = 0;
    virtual void doNotify(char const * name, char const * text) = 0;

    virtual int queryLastFailCode() = 0;
    virtual void getLastFailMessage(size32_t & outLen, char * & outStr, const char * tag) = 0;
    virtual bool fileExists(const char * filename) = 0;
    virtual void deleteFile(const char * logicalName) = 0;

    virtual void selectCluster(const char * cluster) = 0;
    virtual void restoreCluster() = 0;

    virtual void setWorkflowCondition(bool value) = 0;
    virtual void returnPersistVersion(const char * logicalName, unsigned eclCRC, unsigned __int64 allCRC, bool isFile) = 0;
    virtual void setResultDataset(const char * name, unsigned sequence, size32_t len, const void *val, unsigned numRows, bool extend) = 0;
    virtual void getEventName(size32_t & outLen, char * & outStr) = 0;
    virtual void getEventExtra(size32_t & outLen, char * & outStr, const char * tag) = 0;
    virtual void doNotify(char const * name, char const * text, const char * target) = 0;
};


struct IEclProcess : public IInterface
{
    virtual int perform(IGlobalCodeContext * gctx, unsigned wfid) = 0;
    virtual unsigned getActivityVersion() const = 0;
};


//------------------------------------------------------------------------------------------------

inline bool isLocalFpos(unsigned __int64 rp) { return (rp & I64C(0x8000000000000000)) != 0; }
inline unsigned getLocalFposPart(unsigned __int64 rp) { return (unsigned) ((rp >> 48) & 0x7fff); }
inline unsigned __int64 getLocalFposOffset(unsigned __int64 rp) { return rp & I64C(0xffffffffffff); }
inline unsigned __int64 makeLocalFposOffset(unsigned part, unsigned __int64 offset) 
{ 
    return (I64C(0x8000000000000000) | ((unsigned __int64)(part) << 48) | (offset));
}
static inline unsigned rtlMin(unsigned a, unsigned b)       { return a < b ? a : b; }
static inline unsigned rtlMax(unsigned a, unsigned b)       { return a > b ? a : b; }


class XmlChildIterator
{
public:
    inline XmlChildIterator() {};
    inline ~XmlChildIterator() {};
    inline void initOwn(IColumnProviderIterator * _iter) { cur.clear(); iter.setown(_iter); }
    inline IColumnProvider * first() { if (iter) cur.set(iter->first()); return cur; }
    inline IColumnProvider * next() { if (iter) cur.set(iter->next()); return cur; }
protected:
    Owned<IColumnProviderIterator> iter;
    Owned<IColumnProvider> cur;
};

#ifdef STARTQUERY_EXPORTS
#define STARTQUERY_API DECL_EXPORT
#else
#define STARTQUERY_API DECL_IMPORT
#endif

int STARTQUERY_API start_query(int argc, const char *argv[]);

#endif
