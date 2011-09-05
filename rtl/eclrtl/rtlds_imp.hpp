/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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


//---------------------------------------------------------------------------

class ECLRTL_API CSimpleSourceRowPrefetcher : public ISourceRowPrefetcher, public RtlCInterface
{
public:
    CSimpleSourceRowPrefetcher(IOutputMetaData & _meta, ICodeContext * _ctx, unsigned _activityId)
    {
        deserializer.setown(_meta.querySerializedMeta()->createRowDeserializer(_ctx, _activityId));
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
    void cancelRow();
    void cloneRow(size32_t len, const void * ptr);
    void deserialize(IOutputRowDeserializer & deserializer, IRowDeserializerSource & in, bool isGrouped);
    void deserializeRow(IOutputRowDeserializer & deserializer, IRowDeserializerSource & in);
    void expand(size32_t required);
    void resize(size32_t required);
    void finalizeRows();
    void finalizeRow(size32_t len);

    inline RtlDynamicRowBuilder & rowBuilder() { return builder; }
    inline void ensure(size32_t required) { if (required > max) expand(required); }
    inline size32_t getcount() { finalizeRows(); return count; }
    byte * * linkrows();
    inline byte * * queryrows() { finalizeRows(); return rowset; }
    void appendOwn(const void * row);

    inline byte * row() const { return builder.row(); }

protected:
    void flush();

protected:
    IEngineRowAllocator * rowAllocator;
    RtlDynamicRowBuilder builder;
    byte * * rowset;
    size32_t count;
    size32_t max;
    size32_t choosenLimit;
};

extern ECLRTL_API void appendRowsToRowset(size32_t & targetCount, byte * * & targetRowset, IEngineRowAllocator * rowAllocator, size32_t count, byte * * rows);


extern ECLRTL_API void rtlDeserializeRowset(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, IRowDeserializerSource & in);
extern ECLRTL_API void rtlDeserializeRowset(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src);
extern ECLRTL_API void rtlSerializeRowset(IRowSerializerTarget & out, IOutputRowSerializer * serializer, size32_t count, byte * * rows);
extern ECLRTL_API void rtlSerializeRowset(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows);

extern ECLRTL_API void rtlDataset2RowsetX(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src, bool isGrouped);
extern ECLRTL_API void rtlRowset2DatasetX(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows, bool isGrouped);

extern ECLRTL_API void rtlDataset2RowsetX(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src);
extern ECLRTL_API void rtlGroupedDataset2RowsetX(size32_t & count, byte * * & rowset, IEngineRowAllocator * _rowAllocator, IOutputRowDeserializer * deserializer, size32_t lenSrc, const void * src);
extern ECLRTL_API void rtlRowset2DatasetX(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows);
extern ECLRTL_API void rtlGroupedRowset2DatasetX(unsigned & tlen, void * & tgt, IOutputRowSerializer * serializer, size32_t count, byte * * rows);

extern ECLRTL_API size32_t rtlDeserializeRow(size32_t lenOut, void * out, IOutputRowDeserializer * deserializer, const void * src);
extern ECLRTL_API size32_t rtlSerializeRow(size32_t lenOut, void * out, IOutputRowSerializer * serializer, const void * src);
extern ECLRTL_API size32_t rtlDeserializeToBuilder(ARowBuilder & builder, IOutputRowDeserializer * deserializer, const void * src);
extern ECLRTL_API size32_t rtlSerializeToBuilder(ARowBuilder & builder, IOutputRowSerializer * serializer, const void * src);

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
