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

#ifndef FVSOURCE_IPP
#define FVSOURCE_IPP

#include "fvdatasource.hpp"
#include "dllserver.hpp"
#include "hqlexpr.hpp"
#include "eclhelper.hpp"

//Following constants configure different sizes etc.

#define DISK_BLOCK_SIZE     8096            // Size of chunks read directly from file.
#define PAGED_WU_LIMIT      0x20000         // Page load work unit results >= this size.
#define WU_BLOCK_SIZE       0x4000          // Size of chunks read from Work unit
#define DISKREAD_PAGE_SIZE  200             // Number of rows to read in each chunk from file.


interface IRecordSizeEx : public IRecordSize
{
    IRecordSize::getRecordSize;
    virtual size32_t getRecordSize(unsigned maxLength, const void *rec) = 0;
};

class RecordSizeToEx : public CInterface, implements IRecordSizeEx
{
public:
    RecordSizeToEx(IRecordSize * _recordSize) : recordSize(_recordSize) {}
    IMPLEMENT_IINTERFACE

    virtual size32_t getRecordSize(const void *rec)
    {
        return recordSize->getRecordSize(rec);
    }
    virtual size32_t getRecordSize(unsigned maxLength, const void *rec)
    {
        return recordSize->getRecordSize(rec);
    }
    virtual size32_t getFixedSize() const
    {
        return recordSize->getFixedSize();
    }
private:
    Linked<IRecordSize> recordSize;
};
    

//NB: In the following the following convention is used:
// storedX - size/structure in WU/on disk
// returnedX - size/structure of the data actually sent to the program
// transformedX - size/structure of data after applying transformation.
// for workunit storedX == returnedX  for disk returnedX==transformedX

class DataSourceMetaData;
class DataSourceMetaItem : public CInterface
{
public:
    DataSourceMetaItem(unsigned _flags, const char * _name, const char * _xpath, ITypeInfo * _type);
    DataSourceMetaItem(unsigned flags, MemoryBuffer & in);
    virtual void serialize(MemoryBuffer & out) const;
    virtual DataSourceMetaData * queryChildMeta() { return NULL; }

public:
    StringAttr  name;
    StringAttr  xpath;
    OwnedITypeInfo type;
    byte           flags;
};

class DataSourceMetaData : public CInterface, implements IFvDataSourceMetaData, public IRecordSizeEx
{
    friend class DataSourceSetItem;
public:
    DataSourceMetaData(IHqlExpression * _record, byte _numFieldsToIgnore, bool _randomIsOk, bool _isGrouped, unsigned _keyedSize);
    DataSourceMetaData();           // for NULL implementation
    DataSourceMetaData(type_t type);
    DataSourceMetaData(MemoryBuffer & in);
    IMPLEMENT_IINTERFACE

    virtual unsigned numColumns() const;
    virtual ITypeInfo * queryType(unsigned column) const;
    virtual const char * queryName(unsigned column) const;
    virtual const char * queryXPath(unsigned column) const;
    virtual bool supportsRandomSeek() const;
    virtual void serialize(MemoryBuffer & out) const;
    virtual unsigned queryFieldFlags(unsigned column) const;
    virtual IFvDataSourceMetaData * queryChildMeta(unsigned column) const;
    virtual IFvDataSource * createChildDataSource(unsigned column, unsigned len, const void * data);
    virtual unsigned numKeyedColumns() const;

    void addFileposition();
    void addGrouping();
    void addVirtualField(const char * name, const char * xpath, ITypeInfo * type);

    void extractKeyedInfo(UnsignedArray & offsets, TypeInfoArray & types);
    unsigned fixedSize() { return storedFixedSize; }
    bool isFixedSize() { return isStoredFixedWidth; }
    bool isSingleSet() { return ((fields.ordinality() == 1) && (fields.item(0).type->getTypeCode() == type_set)); }
    inline unsigned getMaxRecordSize()                      { return maxRecordSize; }
    inline bool isKey()                                     { return keyedSize != 0; }

//IRecordSizeEx....
    virtual size32_t getRecordSize(const void *rec);
    virtual size32_t getFixedSize() const;
    virtual size32_t getRecordSize(unsigned maxLength, const void *rec)
    {
        return getRecordSize(rec);
    }

protected:
    void addSimpleField(const char * name, const char * xpath, ITypeInfo * type);
    void gatherFields(IHqlExpression * expr, bool isConditional);
    void gatherChildFields(IHqlExpression * expr, bool isConditional);
    void init();

protected:
    CIArrayOf<DataSourceMetaItem> fields;
    unsigned keyedSize;
    unsigned storedFixedSize;
    unsigned maxRecordSize;
    unsigned bitsRemaining;
    unsigned numVirtualFields;
    bool isStoredFixedWidth;
    bool randomIsOk;
    byte numFieldsToIgnore;
};


class DataSourceDatasetItem : public DataSourceMetaItem
{
public:
    DataSourceDatasetItem(const char * _name, const char * _xpath, IHqlExpression * expr);
    DataSourceDatasetItem(unsigned flags, MemoryBuffer & in);

    virtual DataSourceMetaData * queryChildMeta() { return &record; }
    virtual void serialize(MemoryBuffer & out) const;

protected:
    DataSourceMetaData record;
};

class DataSourceSetItem : public DataSourceMetaItem
{
public:
    DataSourceSetItem(const char * _name, const char * _xpath, ITypeInfo * _type);
    DataSourceSetItem(unsigned flags, MemoryBuffer & in);

    virtual DataSourceMetaData * queryChildMeta() { return &record; }
    virtual void serialize(MemoryBuffer & out) const;

protected:
    void createChild();

protected:
    DataSourceMetaData record;
};


//---------------------------------------------------------------------------

class RowBlock : public CInterface
{
public:
    RowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset);
    RowBlock(__int64 _start, __int64 _startOffset);

    virtual const void * fetchRow(__int64 offset, size32_t & len) = 0;
    virtual const void * getRow(__int64 search, size32_t & len, unsigned __int64 & rowOffset) = 0;
    __int64 getStartRow() const { return start; }
    __int64 getNextRow()  const { return start + numRows; }

    virtual void getNextStoredOffset(__int64 & row, offset_t & offset);

protected:
    MemoryBuffer buffer;
    __int64 start;
    __int64 startOffset;
    unsigned numRows;
};

class FixedRowBlock : public RowBlock
{
public:
    FixedRowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset, size32_t _fixedRecordSize);

    virtual const void * fetchRow(__int64 offset, size32_t & len);
    virtual const void * getRow(__int64 search, size32_t & len, unsigned __int64 & rowOffset);

protected:
    size32_t fixedRecordSize;
};

class VariableRowBlock : public RowBlock
{
public:
    VariableRowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset, IRecordSizeEx * recordSize, bool isLast);
    VariableRowBlock(MemoryBuffer & inBuffer, __int64 _start);  // used by remote 

    virtual const void * fetchRow(__int64 offset, size32_t & len);
    virtual const void * getRow(__int64 search, size32_t & len, unsigned __int64 & rowOffset);

protected:
    UnsignedArray rowIndex;
};

//---------------------------------------------------------------------------

class FilePosFixedRowBlock : public FixedRowBlock
{
public:
    FilePosFixedRowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset, size32_t _fixedRecordSize) : FixedRowBlock(_buffer, _start, _startOffset, _fixedRecordSize) {}

    virtual void getNextStoredOffset(__int64 & row, offset_t & offset);
};


class FilePosVariableRowBlock : public VariableRowBlock
{
public:
    FilePosVariableRowBlock(MemoryBuffer & _buffer, __int64 _start, __int64 _startOffset, IRecordSizeEx * recordSize, bool isLast) : VariableRowBlock(_buffer, _start, _startOffset, recordSize, isLast) {}

    virtual void getNextStoredOffset(__int64 & row, offset_t & offset);
};


//---------------------------------------------------------------------------

struct RowLocation
{
    RowLocation()           { matchRow = 0; matchLength = 0; bestRow = 0; bestOffset = 0; }

    const void *    matchRow;
    size32_t            matchLength;
    __int64         bestRow;
    offset_t        bestOffset;
};

class RowCache
{
enum { MaxBlocksCached = 20, MinBlocksCached = 10 };

public:
    void addRowsOwn(RowBlock * rows);
    bool getCacheRow(__int64 row, RowLocation & location);

protected:
    void makeRoom();
    unsigned getBestRow(__int64 row);
    unsigned getInsertPosition(__int64 row);

protected:
    CIArrayOf<RowBlock> allRows;
    Int64Array ages;
};



//---------------------------------------------------------------------------

class FVDataSource : public ADataSource
{
public:
    FVDataSource();
    ~FVDataSource();

    virtual IFvDataSourceMetaData * queryMetaData();

    virtual bool fetchRow(MemoryBuffer & out, __int64 offset);
    virtual bool fetchRawRow(MemoryBuffer & out, __int64 offset);
    virtual bool getRow(MemoryBuffer & out, __int64 row);
    virtual bool getRawRow(MemoryBuffer & out, __int64 row);

    virtual void onClose()  { openCount--; }
    virtual void onOpen()   { openCount++; }

protected:
    virtual bool fetchRowData(MemoryBuffer & out, __int64 offset) = 0;
    virtual bool getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset) = 0;

protected:
    void addFileposition();
    void copyRow(MemoryBuffer & out, const void * src, size32_t length);
    void loadDll(const char * wuid);
    bool setReturnedInfoFromResult();

protected:
    StringAttr wuid;
    Owned<IConstWUResult> wuResult;
    HqlExprAttr returnedRecord;
    Owned<DataSourceMetaData> returnedMeta;
    Owned<IRecordSizeEx> returnedRecordSize;
    Owned<DataSourceMetaData> transformedMeta;
    HqlExprAttr transformedRecord;
    Owned<ILoadedDllEntry> loadedDll;
    Array pluginDlls;
    rowTransformFunction transformer;
    unsigned extraFieldsSize;
    unsigned openCount;
    bool appendFileposition;
};

class PagedDataSource : public FVDataSource
{
public:
    PagedDataSource()   { totalRows = UNKNOWN_NUM_ROWS; }

    virtual __int64 numRows(bool force = false);
    virtual bool getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset);

protected:
    virtual bool loadBlock(__int64 startRow, offset_t startOffset) = 0;
    virtual void improveLocation(__int64 row, RowLocation & location);

protected:
    unsigned __int64 totalRows;
    RowCache cache;
};



class NullDataSource : public ADataSource
{
public:
    NullDataSource() {}
    NullDataSource(IHqlExpression * _record, bool _isGrouped, unsigned _keyedSize);

    virtual bool init() { return true; }
    virtual IFvDataSourceMetaData * queryMetaData()     { return &meta; }
    virtual __int64 numRows(bool force = false)         { return 0; }
    virtual bool fetchRow(MemoryBuffer & out, __int64 offset) { return false; }
    virtual bool fetchRawRow(MemoryBuffer & out, __int64 offset) { return false; }
    virtual bool getRow(MemoryBuffer & out, __int64 row){ return false; }
    virtual bool getRawRow(MemoryBuffer & out, __int64 row){ return false; }
    virtual bool isIndex() { return false; }
    virtual bool optimizeFilter(unsigned offset, unsigned len, const void * data) { return true; }      // empty anyway...
    virtual void onClose()  { }
    virtual void onOpen()   { }

protected:
    DataSourceMetaData meta;
};


class NestedDataSource : public FVDataSource
{
public:
    NestedDataSource(DataSourceMetaData & _meta, unsigned len, const void * data);

//interface IFvDataSource
    virtual bool fetchRowData(MemoryBuffer & out, __int64 offset)   { return false; }
    virtual bool getRowData(__int64 row, size32_t & length, const void * & data, unsigned __int64 & offset);
    virtual bool init();
    virtual bool isIndex() { return false; }
    virtual __int64 numRows(bool force = false);
    virtual bool optimizeFilter(unsigned offset, unsigned len, const void * data) { return false; }

protected:
    unsigned __int64 totalSize;
    Owned<RowBlock> rows;
};


class FailureDataSource : public NullDataSource
{
public:
    FailureDataSource(IHqlExpression * _record, IException * _error, bool _isGrouped, unsigned _keyedSize);

    virtual void onOpen()   { throw LINK(error); }

protected:
    Linked<IException> error;
};


#define FullStringMatch ((unsigned)-1)

extern IHqlExpression * parseQuery(const char * text);

#endif
