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

#ifndef rtlds_imp_hpp
#define rtlds_imp_hpp

#include "eclhelper.hpp"
#include "eclrtl_imp.hpp"

//These interfaces aren't always used (because of speed), although may be sensible to switch to them if they prove useful
//either for hiding the implementations, or because I can then write general library functions.

interface IRtlDatasetSimpleCursor
{
    virtual const byte * first() = 0;
    virtual const byte * next() = 0;
};

interface IRtlDatasetCursor : public IRtlDatasetSimpleCursor
{
    virtual const byte * get() = 0;
    virtual bool isValid() = 0;
    virtual const byte * select(unsigned idx) = 0;
};

//---------------------------------------------------------------------------

interface IResourceContext;
typedef size32_t (*DefaultRowCreator)(ARowBuilder & self, IResourceContext *ctx);

//shouldn't really be derived from ARowBuilder - not a isA, but more efficient
class ECLRTL_API RtlDatasetBuilder : protected ARowBuilder, public RtlCInterface
{
public:
    RtlDatasetBuilder();
    ~RtlDatasetBuilder();
    RTLIMPLEMENT_IINTERFACE

    void getData(size32_t & len, void * & data);
    size32_t getSize();
    byte * queryData();
    void queryData(size32_t & len, void * & data);

    byte * createRow() { return rowBuilder().getSelf(); }
    void finalizeRow(size32_t rowSize) { totalSize += rowSize; self = NULL; }

    inline ARowBuilder & rowBuilder() { return *this; }

//IRowBuilder
    virtual byte * ensureCapacity(size32_t required, const char * fieldName);
    virtual void reportMissingRow() const;
    virtual byte * createSelf() = 0;

protected:
    void ensure(size32_t required);

protected:
    virtual void flushDataset();

protected:
    size32_t maxSize;
    size32_t totalSize;
    byte * buffer;
};


class ECLRTL_API RtlFixedDatasetBuilder : public RtlDatasetBuilder
{
public:
    RtlFixedDatasetBuilder(unsigned _recordSize, unsigned _initRows);

    byte * createSelf();
    virtual IEngineRowAllocator *queryAllocator() const { return NULL; }

protected:
    unsigned recordSize;
};

class ECLRTL_API RtlLimitedFixedDatasetBuilder : public RtlFixedDatasetBuilder
{
public: 
    RtlLimitedFixedDatasetBuilder(unsigned _recordSize, unsigned _maxRows, DefaultRowCreator _rowCreator, IResourceContext *_ctx);

    byte * createRow();

protected:
    virtual void flushDataset();

protected:
    unsigned maxRows;
    DefaultRowCreator rowCreator;
    IResourceContext *ctx;
};

class ECLRTL_API RtlVariableDatasetBuilder : public RtlDatasetBuilder
{
public:
    RtlVariableDatasetBuilder(IRecordSize & _recordSize);

    void deserializeRow(IOutputRowDeserializer & deserializer, IRowDeserializerSource & in);
    virtual byte * createSelf();
    virtual IEngineRowAllocator *queryAllocator() const { return NULL; }

protected:
    IRecordSize * recordSize;
    unsigned maxRowSize;
};

class ECLRTL_API RtlLimitedVariableDatasetBuilder : public RtlVariableDatasetBuilder
{
public:
    RtlLimitedVariableDatasetBuilder(IRecordSize & _recordSize, unsigned _maxRows, DefaultRowCreator _rowCreator, IResourceContext * _ctx);

    byte * createRow();

protected:
    virtual void flushDataset();

protected:
    unsigned numRows;
    unsigned maxRows;
    DefaultRowCreator rowCreator;
    IResourceContext *ctx;
};

//---------------------------------------------------------------------------

class ECLRTL_API rtlRowAttr
{
public:
    inline rtlRowAttr()                 { row=NULL; };
    inline ~rtlRowAttr()                { dispose(); }

    inline void clear()                 { dispose(); row = NULL; }
    inline byte * getClear()            { byte * ret = row; row = NULL; return ret; }
    inline byte * getbytes() const      { return row; }
    inline byte * link() const          { return (byte *)rtlLinkRow(row); }

    inline void set(byte * _row)        { rtlLinkRow(_row); dispose(); row = _row; }
    inline void setown(byte * _row)     { dispose(); row = _row; }
    inline void set(const void * _row)  { rtlLinkRow(_row); setown(_row); }
    inline void setown(const void * _row)   { dispose(); row = static_cast<byte *>(const_cast<void *>(_row)); } // ugly - need to clean up const tracking in code generator
    inline void set(const rtlRowAttr & other) { set(other.getbytes()); }

protected:
    inline void dispose()               { rtlReleaseRow(row); }

private:
    //Force errors....
    inline rtlRowAttr(const rtlRowAttr &) {}
    inline rtlRowAttr & operator = (const rtlRowAttr & other) { row = NULL; return *this; }

protected:
    byte * row;
};


class ECLRTL_API rtlRowsAttr
{
public:
    inline rtlRowsAttr()                { count = 0; rows=NULL; };
    inline ~rtlRowsAttr()               { dispose(); }

    inline void clear()                 { dispose(); rows = NULL; }

    inline byte * * * addrrows()        { clear(); return &rows; }

    inline byte * * queryrows() const   { return rows; }
    inline unsigned querycount() const  { return count; }

    byte * * linkrows() const;

    inline byte * * & refrows()         { clear(); return rows; }

    void set(size32_t _count, byte * * _rows);
    void setRow(IEngineRowAllocator * rowAllocator, const byte * row);
    void setown(size32_t _count, byte * * _rows);

protected:
    void dispose();

private:
    //Force errors....
    inline rtlRowsAttr(const rtlRowsAttr &) {}
    inline rtlRowsAttr & operator = (const rtlRowsAttr & other) { count = 0; rows = NULL; return *this; }

public:
    unsigned count;

protected:
    byte * * rows;
};

//---------------------------------------------------------------------------

void ECLRTL_API rtlReportFieldOverflow(unsigned size, unsigned max, const RtlFieldInfo * field);

class ECLRTL_API RtlRowBuilderBase : implements ARowBuilder, public RtlCInterface
{
public:
    RTLIMPLEMENT_IINTERFACE

    virtual void reportMissingRow() const;
};

class ECLRTL_API RtlDynamicRowBuilder : implements RtlRowBuilderBase
{
public:
    inline RtlDynamicRowBuilder(IEngineRowAllocator * _rowAllocator) : rowAllocator(_rowAllocator) 
    {
        if (rowAllocator)
            create();
        else
        {
            self = NULL;
            maxLength = 0;
        }
    }
    virtual IEngineRowAllocator *queryAllocator() const
    {
        return rowAllocator;
    }
    inline RtlDynamicRowBuilder(IEngineRowAllocator * _rowAllocator, bool createInitial) : rowAllocator(_rowAllocator) 
    {
        if (rowAllocator && createInitial)
            create();
        else
        {
            self = NULL;
            maxLength = 0;
        }
    }
    //This is here to allow group aggregates to perform resizing.
    inline RtlDynamicRowBuilder(IEngineRowAllocator * _rowAllocator, size32_t _maxLength, void * _self) : rowAllocator(_rowAllocator)
    { 
        self = static_cast<byte *>(_self); 
        maxLength = _maxLength;
    }
    inline ~RtlDynamicRowBuilder() { clear(); }

    virtual byte * ensureCapacity(size32_t required, const char * fieldName);

    inline RtlDynamicRowBuilder & ensureRow() { if (!self) create(); return *this; }
    inline bool exists() { return (self != NULL); }
    inline const void * finalizeRowClear(size32_t len) 
    { 
        const unsigned finalMaxLength = maxLength;
        maxLength = 0;
        return rowAllocator->finalizeRow(len, getUnfinalizedClear(), finalMaxLength);
    }
    inline size32_t getMaxLength() const { return maxLength; }
    inline void * getUnfinalizedClear() { void * ret = self; self = NULL; return ret; }

    inline void clear() { if (self) { rowAllocator->releaseRow(self); self = NULL; } }
    inline RtlDynamicRowBuilder & setAllocator(IEngineRowAllocator * _rowAllocator) { rowAllocator = _rowAllocator; return *this; }
    inline void setown(size32_t _maxLength, void * _self) { self = static_cast<byte *>(_self); maxLength = _maxLength; }

    void swapWith(RtlDynamicRowBuilder & other);

protected:
    inline byte * create() { self = static_cast<byte *>(rowAllocator->createRow(maxLength)); return self; }
    virtual byte * createSelf() { return create(); }

protected:
    IEngineRowAllocator * rowAllocator;     // does this need to be linked???
    size32_t maxLength;
};


class ECLRTL_API RtlStaticRowBuilder : extends RtlRowBuilderBase
{
public:
    inline RtlStaticRowBuilder(void * _self, size32_t _maxLength)
    { 
        self = static_cast<byte *>(_self); 
        maxLength = _maxLength;
    }

    virtual byte * ensureCapacity(size32_t required, const char * fieldName);
    virtual IEngineRowAllocator *queryAllocator() const
    {
        return NULL;
    }

    inline void clear() { self = NULL; maxLength = 0; }
    inline void set(size32_t _maxLength, void * _self) { self = static_cast<byte *>(_self); maxLength = _maxLength; }

protected:
    virtual byte * createSelf();

protected:
    size32_t maxLength;
};


class ECLRTL_API RtlNestedRowBuilder : extends RtlRowBuilderBase
{
public:
    inline RtlNestedRowBuilder(ARowBuilder & _container, size32_t _offset, size32_t _suffix)
        : container(_container), offset(_offset), suffix(_suffix)
    { 
        self = container.row()+offset;
    }

    virtual byte * ensureCapacity(size32_t required, const char * fieldName)
    {
        self = container.ensureCapacity(offset+required+suffix, fieldName) + offset;
        return self;
    }
    virtual IEngineRowAllocator *queryAllocator() const
    {
        return container.queryAllocator();
    }

protected:
    virtual byte * createSelf()
    {
        self = container.getSelf()+offset;
        return self;
    }

protected:
    ARowBuilder & container;
    size32_t offset;
    size32_t suffix;
};

extern ECLRTL_API const void * rtlCloneRow(IEngineRowAllocator * rowAllocator, size32_t len, const void * row);
extern ECLRTL_API void rtlCopyRowLinkChildren(void * self, size32_t len, const void * row, IOutputMetaData & meta);
extern ECLRTL_API void rtlLinkChildren(void * self, IOutputMetaData & meta);

//---------------------------------------------------------------------------

class ECLRTL_API CSimpleSourceRowPrefetcher : public ISourceRowPrefetcher, public RtlCInterface
{
public:
    CSimpleSourceRowPrefetcher(IOutputMetaData & _meta, ICodeContext * _ctx, unsigned _activityId)
    {
        deserializer.setown(_meta.querySerializedDiskMeta()->createDiskDeserializer(_ctx, _activityId));
        rowAllocator.setown(_ctx->getRowAllocator(&_meta, _activityId));
    }

    RTLIMPLEMENT_IINTERFACE

    virtual void readAhead(IRowDeserializerSource & in)
    {
        RtlDynamicRowBuilder rowBuilder(rowAllocator);
        size32_t len = deserializer->deserialize(rowBuilder, in);
        rtlReleaseRow(rowBuilder.finalizeRowClear(len));
    }

protected:
    Owned<IOutputRowDeserializer> deserializer;
    Owned<IEngineRowAllocator> rowAllocator;
};

//---------------------------------------------------------------------------

class ECLRTL_API RtlLinkedDatasetBuilder
{
public:
    RtlLinkedDatasetBuilder(IEngineRowAllocator * _rowAllocator, int _choosenLimit=-1);
    ~RtlLinkedDatasetBuilder();

    void append(const void * source);
    void appendRows(size32_t num, byte * * rows);
    inline void appendEOG() { appendOwn(NULL); }
    byte * createRow();
    void cloneRow(size32_t len, const void * ptr);
    void deserializeRow(IOutputRowDeserializer & deserializer, IRowDeserializerSource & in);
    void expand(size32_t required);
    void resize(size32_t required);
    void finalizeRows();
    void finalizeRow(size32_t len);
    void clear();

    inline RtlDynamicRowBuilder & rowBuilder() { return builder; }
    inline void ensure(size32_t required) { if (required > max) expand(required); }
    inline size32_t getcount() { finalizeRows(); return count; }
    byte * * linkrows();
    inline byte * * queryrows() { finalizeRows(); return rowset; }
    void appendOwn(const void * row);

    inline byte * row() const { return builder.row(); }

protected:
    IEngineRowAllocator * rowAllocator;
    RtlDynamicRowBuilder builder;
    byte * * rowset;
    size32_t count;
    size32_t max;
    size32_t choosenLimit;
};

class ECLRTL_API RtlLinkedDictionaryBuilder
{
public:
    RtlLinkedDictionaryBuilder(IEngineRowAllocator * _rowAllocator, IHThorHashLookupInfo *_hashInfo, unsigned _initialTableSize);
    RtlLinkedDictionaryBuilder(IEngineRowAllocator * _rowAllocator, IHThorHashLookupInfo *_hashInfo);
    ~RtlLinkedDictionaryBuilder();

    void append(const void * source);
    void appendOwn(const void * source);
    void appendRows(size32_t num, byte * * rows);

    inline size32_t getcount() { return tableSize; }
    inline byte * * linkrows() { return rtlLinkRowset(table); }

    byte * createRow()         { return builder.getSelf(); }
    void finalizeRow(size32_t len);
    inline RtlDynamicRowBuilder & rowBuilder() { return builder; }
    void cloneRow(size32_t len, const void * ptr);
    void deserializeRow(IOutputRowDeserializer & deserializer, IRowDeserializerSource & in);

    /*
        Not clear which if any of these we will want...
    void expand(size32_t required);
    void resize(size32_t required);
    void finalizeRows();

    inline void ensure(size32_t required) { if (required > max) expand(required); }
    */
    inline byte * * queryrows() { return table; }
    inline byte * row() const { return builder.row(); }

protected:
    void checkSpace();
    void init(IEngineRowAllocator * _rowAllocator, IHThorHashLookupInfo *_hashInfo, unsigned _initialTableSize);

protected:
    IEngineRowAllocator *rowAllocator;
    IHash *hash;
    ICompare *compare;
    RtlDynamicRowBuilder builder;
    byte * * table;
    size32_t usedCount;
    size32_t usedLimit;
    size32_t initialSize;
    size32_t tableSize;
};

extern ECLRTL_API unsigned __int64 rtlDictionaryCount(size32_t tableSize, byte **table);
extern ECLRTL_API bool rtlDictionaryExists(size32_t tableSize, byte **table);
extern ECLRTL_API byte *rtlDictionaryLookup(IHThorHashLookupInfo &hashInfo, size32_t tableSize, byte **table, const byte *source, byte *defaultRow);
extern ECLRTL_API byte *rtlDictionaryLookupString(size32_t tableSize, byte **table, size32_t len, const char *source, byte *defaultRow);
extern ECLRTL_API byte *rtlDictionaryLookupStringN(size32_t tableSize, byte **table, size32_t N, size32_t len, const char *source, byte *defaultRow);
extern ECLRTL_API byte *rtlDictionaryLookupSigned(size32_t tableSize, byte **table, __int64 source, byte *defaultRow);
extern ECLRTL_API byte *rtlDictionaryLookupUnsigned(size32_t tableSize, byte **table, __uint64 source, byte *defaultRow);
extern ECLRTL_API byte *rtlDictionaryLookupSignedN(size32_t tableSize, byte **table, size32_t size, __int64 source, byte *defaultRow);
extern ECLRTL_API byte *rtlDictionaryLookupUnsignedN(size32_t tableSize, byte **table, size32_t size, __uint64 source, byte *defaultRow);

extern ECLRTL_API bool rtlDictionaryLookupExists(IHThorHashLookupInfo &hashInfo, size32_t tableSize, byte **table, const byte *source);
extern ECLRTL_API bool rtlDictionaryLookupExistsString(size32_t tableSize, byte **table, size32_t len, const char *source);
extern ECLRTL_API bool rtlDictionaryLookupExistsStringN(size32_t tableSize, byte **table, size32_t N, size32_t len, const char *source);
extern ECLRTL_API bool rtlDictionaryLookupExistsSigned(size32_t tableSize, byte **table, __int64 source);
extern ECLRTL_API bool rtlDictionaryLookupExistsUnsigned(size32_t tableSize, byte **table, __uint64 source);
extern ECLRTL_API bool rtlDictionaryLookupExistsSignedN(size32_t tableSize, byte **table, size32_t size, __uint64 source);
extern ECLRTL_API bool rtlDictionaryLookupExistsUnsignedN(size32_t tableSize, byte **table, size32_t size, __uint64 source);

extern ECLRTL_API void appendRowsToRowset(size32_t & targetCount, byte * * & targetRowset, IEngineRowAllocator * rowAllocator, size32_t count, byte * * rows);


//Functions that are used to convert between the serialized and internal memory format
//functions containing child also serialize the length of the dataset, functions without it pass it independently

extern ECLRTL_API void rtlDeserializeChildRowset(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, IRowDeserializerSource & in);
extern ECLRTL_API void rtlDeserializeChildGroupRowset(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, IRowDeserializerSource & in);
extern ECLRTL_API void rtlSerializeChildRowset(IRowSerializerTarget & out, IOutputRowSerializer * serializer, size32_t count, byte * * rows);
extern ECLRTL_API void rtlSerializeChildGroupRowset(IRowSerializerTarget & out, IOutputRowSerializer * serializer, size32_t count, byte * * rows);

//Functions for converting between rowsets and complete datasets (where length is known)
extern ECLRTL_API void rtlDataset2RowsetX(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src, bool isGrouped);
extern ECLRTL_API void rtlRowset2DatasetX(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows, bool isGrouped);

extern ECLRTL_API void rtlDataset2RowsetX(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src);
extern ECLRTL_API void rtlGroupedDataset2RowsetX(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src);
extern ECLRTL_API void rtlRowset2DatasetX(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows);
extern ECLRTL_API void rtlGroupedRowset2DatasetX(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows);

extern ECLRTL_API size32_t rtlDeserializeToBuilder(ARowBuilder & builder, IOutputRowDeserializer * deserializer, const void * src);
extern ECLRTL_API size32_t rtlSerializeToBuilder(ARowBuilder & builder, IOutputRowSerializer * serializer, const void * src);

//Dictionary serialization/deserialization
extern ECLRTL_API void rtlDeserializeDictionary(size32_t & count, byte * * & rowset, IEngineRowAllocator * rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src);
extern ECLRTL_API void rtlDeserializeDictionaryFromDataset(size32_t & count, byte * * & rowset, IEngineRowAllocator * rowAllocator, IOutputRowDeserializer * deserializer, IHThorHashLookupInfo & hashInfo, size32_t lenSrc, const void * src);
extern ECLRTL_API void rtlSerializeDictionary(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows);
extern ECLRTL_API void rtlSerializeDictionaryToDataset(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows);

extern ECLRTL_API void rtlSerializeDictionary(IRowSerializerTarget & out, IOutputRowSerializer * serializer, size32_t count, byte * * rows);
extern ECLRTL_API void rtlSerializeDictionaryToDataset(IRowSerializerTarget & out, IOutputRowSerializer * serializer, size32_t count, byte * * rows);

//Dictionary serialization/deserialization to a stream - includes length prefix
extern ECLRTL_API void rtlDeserializeChildDictionary(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, IRowDeserializerSource & in);
extern ECLRTL_API void rtlDeserializeChildDictionaryFromDataset(size32_t & count, byte * * & rowset, IEngineRowAllocator * rowAllocator, IOutputRowDeserializer * deserializer, IHThorHashLookupInfo & hashInfo, IRowDeserializerSource & in);
extern ECLRTL_API void rtlSerializeChildDictionary(IRowSerializerTarget & out, IOutputRowSerializer * serializer, size32_t count, byte * * rows);
extern ECLRTL_API void rtlSerializeChildDictionaryToDataset(IRowSerializerTarget & out, IOutputRowSerializer * serializer, size32_t count, byte * * rows);

//---------------------------------------------------------------------------

class ECLRTL_API ARtlDatasetCursor
{
    //if we ever made the functions virtual, then they would go in here
};

class ECLRTL_API RtlDatasetCursor : public ARtlDatasetCursor
{
public:
    RtlDatasetCursor(size32_t _len, const void * _data);

    bool exists();
    const byte * first();
    const byte * get();
    void setDataset(size32_t len, const void * data);
    bool isValid();
    //const byte * next();

protected:
    const byte * buffer;
    const byte * end;
    const byte * cur;
};

class ECLRTL_API RtlFixedDatasetCursor : public RtlDatasetCursor
{
public:
    RtlFixedDatasetCursor(size32_t _len, const void * _data, unsigned _recordSize);
    RtlFixedDatasetCursor();

    void init(size32_t _len, const void * _data, unsigned _recordSize);

    size32_t count();
    size32_t getSize();
    const byte * next();
    const byte * select(unsigned idx);

protected:
    unsigned recordSize;
};

class ECLRTL_API RtlVariableDatasetCursor : public RtlDatasetCursor
{
public:
    RtlVariableDatasetCursor(size32_t _len, const void * _data, IRecordSize & _recordSize);
    RtlVariableDatasetCursor();

    void init(size32_t _len, const void * _data, IRecordSize & _recordSize);

    size32_t count();
    size32_t getSize();
    const byte * next();
    const byte * select(unsigned idx);

protected:
    IRecordSize * recordSize;
};


class ECLRTL_API RtlLinkedDatasetCursor : public ARtlDatasetCursor
{
public:
    RtlLinkedDatasetCursor(unsigned _numRows, byte * * _rows);
    RtlLinkedDatasetCursor();

    void setDataset(unsigned _numRows, byte * * _rows) { init(_numRows, _rows); }
    void init(unsigned _numRows, byte * * _rows);

    inline size32_t count() { return numRows; }
    inline bool exists() { return numRows != 0; }
//  size32_t getSize() // no sensible implementation
    const byte * next();
    const byte * select(unsigned idx);
    const byte * first();
    const byte * get();
    bool isValid();

protected:
    byte * * rows;
    unsigned numRows;
    unsigned cur;
};

//MORE: The inheritance for this class is wrong.  Both classes should derive from a common
//base with no parameterised constructor or init.  But that needs to wait until 5.0 so prevent breaking binary
//compatibility
class ECLRTL_API RtlSafeLinkedDatasetCursor : public RtlLinkedDatasetCursor
{
public:
    RtlSafeLinkedDatasetCursor(unsigned _numRows, byte * * _rows);
    RtlSafeLinkedDatasetCursor() {}
    ~RtlSafeLinkedDatasetCursor();

    void setDataset(unsigned _numRows, byte * * _rows) { init(_numRows, _rows); }
    void init(unsigned _numRows, byte * * _rows);

};

class ECLRTL_API RtlStreamedDatasetCursor : public ARtlDatasetCursor
{
public:
    RtlStreamedDatasetCursor(IRowStream * _stream);
    RtlStreamedDatasetCursor();

    void init(IRowStream * _stream);

    const byte * next();
    const byte * first();
    const byte * get() { return cur.getbytes(); }
    inline bool isValid() { return cur.getbytes() != NULL; }

protected:
    Owned<IRowStream> stream;
    rtlRowAttr cur;
};


//Some sample helper functions/classes:
interface ICompare;
extern bool ECLRTL_API rtlCheckInList(const void * lhs, IRtlDatasetCursor * cursor, ICompare * compare);

class ECLRTL_API RtlHashInlistChecker
{
public:
    RtlHashInlistChecker(IHash * hashLeft, IHash * hashList, IRtlDatasetCursor * cursor);

    bool inList(const void * lhs);
};



class ECLRTL_API RtlCompoundIterator
{
public:
    RtlCompoundIterator();
    ~RtlCompoundIterator();

    void init(unsigned numLevels);
    void addIter(unsigned idx, IRtlDatasetSimpleCursor * iter, byte * * cursor);
    bool first(unsigned level);
    bool next(unsigned level);

    inline bool first()             { ok = first(numLevels-1); return ok; }
    inline bool next()              { ok = next(numLevels-1); return ok; }
    inline bool isValid()           { return ok; }

protected:
    inline void setCursor(unsigned level, const void * value)
    {
        *cursors[level] = (byte *)value;
    }

protected:
    IRtlDatasetSimpleCursor * * iters;
    byte * * * cursors;
    unsigned numLevels;
    bool ok;
};


//Probably generate inline , rather than use this class, since very simple code...
class ECLRTL_API RtlSimpleIterator
{
public:
    void addIter(unsigned idx, IRtlDatasetSimpleCursor * iter, byte * * cursor);
    bool first();
    bool next();

    inline bool isValid()           { return (*cursor != NULL); }

protected:
    IRtlDatasetSimpleCursor * iter;
    byte * * cursor;
};

#endif
