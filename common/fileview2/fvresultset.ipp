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

#ifndef FVRESULTSET_IPP
#define FVRESULTSET_IPP

#include "fileview.hpp"
#include "fvdatasource.hpp"
#include "fvtransform.ipp"

//---------------------------------------------------------------------------

//Internal interface used to help implement the related dataset browsing
interface IExtendedResultSetCursor : extends IResultSetCursor
{
public:
    virtual void noteRelatedFileChanged() = 0;
};


//---------------------------------------------------------------------------

struct MemoryAttrItem : public CInterface, public MemoryAttr
{
    MemoryAttrItem() : MemoryAttr() {}
    MemoryAttrItem(unsigned len, const void * ptr) : MemoryAttr(len, ptr) {}

};
typedef CIArrayOf<MemoryAttrItem> MemoryAttrArray;

class CStrColumnFilter : public CInterface
{
public:
    void addValue(unsigned len, const char * value)     { values.append(* new MemoryAttrItem(len, value)); }
    void clear()                            { values.kill(); }

public:
    MemoryAttrArray values;
};
typedef CIArrayOf<CStrColumnFilter> StrColumnFilterArray;

//---------------------------------------------------------------------------

class CResultSet;
class CResultSetCursor;
class ViewJoinColumn;

class CResultSetColumnInfo : public CInterface
{
public:
    ITypeInfo * type;
    Owned<IResultSetMetaData> childMeta;
    ViewFieldTransformerArray getTransforms;
    ViewFieldTransformerArray setTransforms;
    StringAttr naturalName;
    byte flag;
};

class CResultSetMetaData : public CInterface, implements IResultSetMetaData
{
    friend class CResultSet;
    friend class CResultSetCursor;
public:
    CResultSetMetaData(IFvDataSourceMetaData * _meta, bool _useXPath);
    CResultSetMetaData(const CResultSetMetaData & _meta);
    IMPLEMENT_IINTERFACE

    virtual IResultSetMetaData * getChildMeta(int column) const;
    virtual int getColumnCount() const;
    virtual DisplayType getColumnDisplayType(int column) const;
    virtual IStringVal & getColumnLabel(IStringVal & s, int column) const;
    virtual IStringVal & getColumnEclType(IStringVal & s, int column) const;
    virtual IStringVal & getColumnXmlType(IStringVal & s, int column) const;
    virtual bool isSigned(int column) const;
    virtual bool isEBCDIC(int column) const;
    virtual bool isBigEndian(int column) const;
    virtual unsigned getColumnRawType(int column) const;
    virtual unsigned getColumnRawSize(int column) const;
    virtual IStringVal & getXmlSchema(IStringVal & s, bool addHeader) const;
    virtual IStringVal & getXmlXPathSchema(IStringVal & str, bool addHeader) const;
    virtual unsigned getNumKeyedColumns() const;
    virtual bool hasSetTranslation(int column) const;
    virtual bool hasGetTranslation(int column) const;
    virtual IStringVal & getNaturalColumnLabel(IStringVal &s, int column) const;
    virtual bool isVirtual(int column) const;

    void calcFieldOffsets(const byte * data, unsigned * offsets) const;

    inline bool isFixedSize() const { return fixedSize; }
    inline char queryFlags(int column) const                    { return columns.item(column).flag; }
    inline const CResultSetColumnInfo & queryColumn(int column) const { return columns.item(column); }
    inline ITypeInfo * queryType(int column) const              { return columns.item(column).type; }
    inline const char * queryXPath(int column) const            { return meta->queryXPath(column); }

    void getXmlSchema(ISchemaBuilder & builder, bool useXPath) const;
    unsigned queryColumnIndex(unsigned firstField, const char * fieldName) const;

protected:
    IFvDataSourceMetaData * meta;
    CIArrayOf<CResultSetColumnInfo> columns;
    bool fixedSize;
    bool alwaysUseXPath;
};

//Extend the Result interface with the stateless functions required by the cursors
interface IExtendedNewResultSet : extends INewResultSet
{
    virtual IExtendedNewResultSet * cloneForFilter() = 0;
    virtual bool fetch(MemoryBuffer & out, __int64 fileoffset) = 0;
    virtual bool fetchRaw(MemoryBuffer & out, __int64 fileoffset) = 0;
    virtual bool getRow(MemoryBuffer & out, __int64 row) = 0;
    virtual bool getRawRow(MemoryBuffer & out, __int64 row) = 0;
    virtual void serialize(MemoryBuffer & out) = 0;
    virtual bool isMappedIndexField(unsigned columnIndex) = 0;
    virtual void onOpen() = 0;
    virtual void onClose() = 0;
    virtual IFvDataSource * queryDataSource() = 0;
};

class CResultSetBase : public CInterface, implements IExtendedNewResultSet
{
public:
    IMPLEMENT_IINTERFACE;

//interface IResultSet
    virtual IResultSetCursor * createCursor();
    virtual IResultSetCursor * createCursor(IDataVal & buffer);
    virtual IFilteredResultSet * createFiltered();
    virtual IResultSetCursor * createSortedCursor(unsigned column, bool descend);

protected:
    const CResultSetMetaData & getMeta() { return static_cast<const CResultSetMetaData &>(getMetaData()); }

    CResultSetCursor * doCreateCursor();
};


class CResultSet : public CResultSetBase
{
public:
    CResultSet(IFvDataSource * _dataSource, bool _useXPath);

//interface IResultSet
    virtual int findColumn(const char * columnName) const;
    virtual const IResultSetMetaData & getMetaData() const;
    virtual __int64 getNumRows() const;
    virtual bool supportsRandomSeek() const;

    virtual IExtendedNewResultSet * cloneForFilter();
    virtual bool fetch(MemoryBuffer & out, __int64 fileoffset);
    virtual bool fetchRaw(MemoryBuffer & out, __int64 fileoffset);
    virtual bool getRow(MemoryBuffer & out, __int64 row);
    virtual bool getRawRow(MemoryBuffer & out, __int64 row);
    virtual void serialize(MemoryBuffer & out);
    virtual bool isMappedIndexField(unsigned columnIndex);
    virtual void onOpen() { dataSource->onOpen(); }
    virtual void onClose() { dataSource->onClose(); }
    virtual IFvDataSource * queryDataSource() { return dataSource; }

    void setColumnMapping(IDistributedFile * df);

protected:
    void calcMappedFields();

protected:
    CResultSetMetaData  meta;
    Linked<IFvDataSource> dataSource;
    BoolArray mappedFields;
    CriticalSection cs;
};


class CColumnFilter : public CInterface
{
public:
    CColumnFilter(unsigned _whichColumn, ITypeInfo * _type, bool _isMappedIndexField)
        : whichColumn(_whichColumn)
    { type.set(_type); optimized = false; isMappedIndexField = _isMappedIndexField; subLen = 0; }

    void addValue(unsigned lenText, const char * value);

    void deserialize(MemoryBuffer & buffer);
    bool isValid(const byte * rowValue, const unsigned * offsets);
    bool optimizeFilter(IFvDataSource * dataSource);
    void serialize(MemoryBuffer & buffer);

protected:
    bool matches(const byte * rowValue, unsigned valueLen, const byte * value);

protected:
    MemoryAttrArray values;
    OwnedITypeInfo type;
    unsigned subLen;
    unsigned whichColumn;
    bool optimized;
    bool isMappedIndexField;
};
typedef CIArrayOf<CColumnFilter> ColumnFilterArray;


class CIndirectResultSet : public CResultSetBase
{
public:
    CIndirectResultSet(IExtendedNewResultSet * _parent) 
        : parent(_parent), meta(static_cast<const CResultSetMetaData &>(_parent->getMetaData()))
    {
    }

    virtual const IResultSetMetaData & getMetaData() const
    {
        return parent->getMetaData();
    }
    virtual __int64 getNumRows() const
    {
        return parent->getNumRows();
    }
    virtual bool supportsRandomSeek() const
    {
        return parent->supportsRandomSeek();
    }
    virtual IExtendedNewResultSet * cloneForFilter()
    {
        return NULL;
    }
    virtual bool fetch(MemoryBuffer & out, __int64 fileoffset)
    {
        return parent->fetch(out, fileoffset);
    }
    virtual bool fetchRaw(MemoryBuffer & out, __int64 fileoffset)
    {
        return parent->fetchRaw(out, fileoffset);
    }
    virtual bool getRow(MemoryBuffer & out, __int64 row)
    {
        return parent->getRow(out, row);
    }
    virtual bool getRawRow(MemoryBuffer & out, __int64 row)
    {
        return parent->getRawRow(out, row);
    }
    virtual void serialize(MemoryBuffer & out)
    {
        parent->serialize(out);
    }
    virtual bool isMappedIndexField(unsigned columnIndex)
    {
        return parent->isMappedIndexField(columnIndex);
    }
    virtual void onOpen()
    {
        parent->onOpen();
    }
    virtual void onClose()
    {
        parent->onClose();
    }
    virtual IFvDataSource * queryDataSource() 
    { 
        return parent->queryDataSource(); 
    }

protected:
    Linked<IExtendedNewResultSet> parent;
    const CResultSetMetaData & meta;
};


class CFilteredResultSet : public CIndirectResultSet
{
public:
    CFilteredResultSet(IExtendedNewResultSet * parent, ColumnFilterArray & _filters);
//  CResultSetFilteredCursor(const CResultSetMetaData & _meta, CResultSet * _resultSet, MemoryBuffer & buffer);
    ~CFilteredResultSet();

    virtual bool getRow(MemoryBuffer & out, __int64 row);
    virtual bool getRawRow(MemoryBuffer & out, __int64 row);
    virtual void serialize(MemoryBuffer & out);
    virtual __int64 getNumRows() const;

protected:
    void initExtra();
    bool rowMatchesFilter(const byte * row);
    __int64 translateRow(__int64 row);

protected:
    ColumnFilterArray filters;
    UInt64Array validPositions;
    unsigned * offsets;
    bool readAll;
};

class CFetchFilteredResultSet : public CIndirectResultSet
{
public:
    CFetchFilteredResultSet(IExtendedNewResultSet * _parent, const CStrColumnFilter & _filters);

    virtual IFilteredResultSet * createFiltered()           { UNIMPLEMENTED; }  // Would need to clone filters into new resultset
    virtual bool getRow(MemoryBuffer & out, __int64 row);
    virtual bool getRawRow(MemoryBuffer & out, __int64 row);
    virtual void serialize(MemoryBuffer & out);
    virtual __int64 getNumRows() const;

protected:
    UInt64Array validOffsets;
};

class CResultSetCursor : public CInterface, implements IExtendedResultSetCursor
{
public:
    CResultSetCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet);
    CResultSetCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet, MemoryBuffer & val);
    ~CResultSetCursor();
    IMPLEMENT_IINTERFACE;

//interface IResultSet, IResultSetCursor shared
    virtual bool absolute(__int64 row);
    virtual void afterLast();
    virtual void beforeFirst();
    virtual bool fetch(__int64 fileoffset);
    virtual bool first();
    virtual bool getBoolean(int columnIndex);
    virtual IDataVal & getBytes(IDataVal &d, int columnIndex);
    virtual IResultSetCursor * getChildren(int columnIndex) const;
    virtual xdouble getDouble(int columnIndex);
    virtual int getFetchSize() const;
    virtual bool getIsAll(int columnIndex) const;
    virtual __int64 getInt(int columnIndex);
    virtual IDataVal & getRaw(IDataVal &d, int columnIndex);
    virtual IDataVal & getRawRow(IDataVal &d);
    virtual int getType();
    virtual const IResultSetMetaData & getMetaData() const;
    virtual __int64 getNumRows() const;
    virtual IStringVal & getString(IStringVal & ret, int columnIndex);
    virtual bool isAfterLast() const;
    virtual bool isBeforeFirst() const;
    virtual bool isFirst() const;
    virtual bool isLast() const;
    virtual bool isNull(int columnIndex) const;
    virtual bool isValid() const;
    virtual bool last();
    virtual bool next();
    virtual bool previous();
    virtual INewResultSet * queryResultSet() { return resultSet; }
    virtual bool relative(__int64 rows);
    virtual void setFetchSize(int rows);
    virtual bool supportsRandomSeek() const;
    virtual void serialize(IDataVal & d);
    virtual IStringVal & getDisplayText(IStringVal &ret, int columnIndex);
    virtual IStringVal & getXml(IStringVal & ret, int columnIndex);
    virtual IStringVal & getXmlRow(IStringVal &ret);
    virtual IStringVal & getXmlItem(IStringVal & ret);

    virtual void beginWriteXmlRows(IXmlWriter & writer);
    virtual void writeXmlRow(IXmlWriter &writer);
    virtual void endWriteXmlRows(IXmlWriter & writer);
    virtual void writeXmlItem(IXmlWriter &writer);

//IExtendedResultSetCursor
    virtual void noteRelatedFileChanged() {}

protected:
    void calcFieldOffsets();
    void init(IExtendedNewResultSet * _resultSet);
    bool isMappedIndexField(unsigned columnIndex) { return resultSet->isMappedIndexField(columnIndex); }
    const byte * getColumn(unsigned idx) const      { return (const byte *)curRowData.toByteArray() + offsets[idx]; }
    void getXmlText(StringBuffer & out, int columnIndex, const char *tag=NULL);
    void getXmlAttrText(StringBuffer & out, int columnIndex, const char *tag=NULL);
    void writeXmlText(IXmlWriter &writer, int columnIndex, const char *tag=NULL);
    void writeXmlAttrText(IXmlWriter &writer, int columnIndex, const char *tag=NULL);

    virtual __int64 getCurRow() const;
    virtual __int64 translateRow(__int64 row) const;
    virtual void serializeType(MemoryBuffer & buffer);
    virtual void serialize(MemoryBuffer & buffer);

protected:
    const CResultSetMetaData & meta;
    Owned<IExtendedNewResultSet> resultSet;
    MemoryBuffer curRowData;
    __int64 curRow;
    unsigned * offsets;
};

//---------------------------------------------------------------------------

class IndirectResultSetCursor : public CInterface, implements IExtendedResultSetCursor
{
public:
    IMPLEMENT_IINTERFACE;

//interface IResultSet, IResultSetCursor shared
    virtual bool absolute(__int64 row);
    virtual void afterLast();
    virtual void beforeFirst();
    virtual bool fetch(__int64 fileoffset);
    virtual bool first();
    virtual bool getBoolean(int columnIndex);
    virtual IDataVal & getBytes(IDataVal &d, int columnIndex);
    virtual IResultSetCursor * getChildren(int columnIndex) const;
    virtual xdouble getDouble(int columnIndex);
    virtual int getFetchSize() const;
    virtual bool getIsAll(int columnIndex) const;
    virtual __int64 getInt(int columnIndex);
    virtual IDataVal & getRaw(IDataVal &d, int columnIndex);
    virtual IDataVal & getRawRow(IDataVal &d);
    virtual __int64 getNumRows() const;
    virtual IStringVal & getString(IStringVal & ret, int columnIndex);
    virtual bool isAfterLast() const;
    virtual bool isBeforeFirst() const;
    virtual bool isFirst() const;
    virtual bool isLast() const;
    virtual bool isNull(int columnIndex) const;
    virtual bool isValid() const;
    virtual bool last();
    virtual bool next();
    virtual bool previous();
    virtual INewResultSet * queryResultSet();
    virtual bool relative(__int64 rows);
    virtual void serialize(IDataVal & d);
    virtual IStringVal & getDisplayText(IStringVal &ret, int columnIndex);
    virtual IStringVal & getXml(IStringVal & ret, int columnIndex);
    virtual IStringVal & getXmlRow(IStringVal &ret);
    virtual IStringVal & getXmlItem(IStringVal &ret);
    virtual void beginWriteXmlRows(IXmlWriter & writer);
    virtual void writeXmlRow(IXmlWriter &writer);
    virtual void endWriteXmlRows(IXmlWriter & writer);
    virtual void writeXmlItem(IXmlWriter &writer);
    virtual void noteRelatedFileChanged();

protected:
    virtual IExtendedResultSetCursor * queryBase() = 0;
    virtual const IExtendedResultSetCursor * queryBase() const
    {
        return const_cast<IndirectResultSetCursor *>(this)->queryBase();
    }
};


class NotifyingResultSetCursor : public IndirectResultSetCursor
{
public:
    NotifyingResultSetCursor(IExtendedResultSetCursor * _cursor) : cursor(_cursor) {}

    inline void addDependent(IExtendedResultSetCursor & next) { dependents.append(OLINK(next)); }

    virtual bool absolute(__int64 row);
    virtual void afterLast();
    virtual void beforeFirst();
    virtual bool fetch(__int64 fileoffset);
    virtual bool first();
    virtual bool last();
    virtual bool next();
    virtual bool previous();
    virtual bool relative(__int64 rows);
    virtual void noteRelatedFileChanged();
    virtual IExtendedResultSetCursor * queryBase() { return cursor; }

protected:
    void notifyChanged();

protected:
    Linked<IExtendedResultSetCursor> cursor;
    IArrayOf<IExtendedResultSetCursor> dependents;
};

class DelayedFilteredResultSetCursor : public IndirectResultSetCursor
{
public:
    DelayedFilteredResultSetCursor(INewResultSet * _resultSet);

    virtual IExtendedResultSetCursor * queryBase();
    virtual void ensureFiltered();
    virtual void noteRelatedFileChanged();

    void clearFilters();
    inline void clearFilter(unsigned columnIndex)
    {
        clearCursor();
        filtered->clearFilter(columnIndex);
    }
    inline void addFilter(unsigned columnIndex, unsigned length, const char * utf8Value)
    {
        clearCursor();
        filtered->addFilter(columnIndex, length, utf8Value);
    }
    inline void addNaturalFilter(unsigned columnIndex, unsigned length, const char * utf8Value)
    {
        clearCursor();
        filtered->addNaturalFilter(columnIndex, length, utf8Value);
    }

protected:
    void clearCursor();

protected:
    Linked<IFilteredResultSet> filtered;
    Owned<INewResultSet> resultSet;
    Owned<IExtendedResultSetCursor> cursor;
};

//---------------------------------------------------------------------------

class CResultSetSortedCursor : public CResultSetCursor
{
public:
    CResultSetSortedCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet, unsigned _column, bool _desc);
    CResultSetSortedCursor(const CResultSetMetaData & _meta, IExtendedNewResultSet * _resultSet, unsigned _column, bool _desc, MemoryBuffer & buffer);
    ~CResultSetSortedCursor();

    virtual bool absolute(__int64 row);

protected:
    void buildIndex();
    virtual __int64 getCurRow() const;
    virtual __int64 translateRow(__int64 row) const;
    virtual void serializeType(MemoryBuffer & buffer);
    virtual void serialize(MemoryBuffer & buffer);

protected:
    unsigned column;
    bool desc;
    unsigned numEntries;
    unsigned * elements;
    __int64 lastRow;
};


class CFilteredResultSetBuilder : public CInterface, implements IFilteredResultSet
{
public:
    CFilteredResultSetBuilder(IExtendedNewResultSet * _resultSet);
    IMPLEMENT_IINTERFACE

    virtual INewResultSet * create();
    virtual void addFilter(unsigned columnIndex, const char * value);
    virtual void addFilter(unsigned columnIndex, unsigned len, const char * value);
    virtual void addNaturalFilter(unsigned columnIndex, unsigned len, const char * value);
    virtual void clearFilter(unsigned columnIndex);
    virtual void clearFilters();

protected:
    CIArrayOf<CStrColumnFilter> filters;
    Owned<IExtendedNewResultSet>    resultSet;
};


class ADataSource;
class CResultSetFactoryBase : public CInterface, implements IResultSetFactory
{
public:
    CResultSetFactoryBase(const char * _username, const char * _password);
    CResultSetFactoryBase(ISecManager &secmgr, ISecUser &secuser);
    IMPLEMENT_IINTERFACE

protected:
    StringAttr username;
    StringAttr password;
    Owned<ISecManager> secMgr;
    Owned<ISecUser> secUser;
};

class CResultSetFactory : public CResultSetFactoryBase
{
public:
    CResultSetFactory(const char * _username, const char * _password);
    CResultSetFactory(ISecManager &secmgr, ISecUser &secuser);

    virtual INewResultSet * createNewResultSet(IConstWUResult * wuResult, const char * wuid);
    virtual INewResultSet * createNewFileResultSet(const char * logicalFile, const char * cluster);
    virtual INewResultSet * createNewResultSet(const char * wuid, unsigned sequence, const char * name);
    virtual INewResultSet * createNewFileResultSet(const char * logicalFile);
    virtual IResultSetMetaData * createResultSetMeta(IConstWUResult * wuResult);
    virtual IResultSetMetaData * createResultSetMeta(const char * wuid, unsigned sequence, const char * name);

protected:
    CResultSet * createResultSet(IFvDataSource * ds, bool _useXPath);
    IDistributedFile * lookupLogicalName(const char * logicalName);
};


class CRemoteResultSetServer  : public CInterface
{
public:
    void start();

protected:
    CResultSetFactory factory;
};


class CRemoteResultSetFactory : public CResultSetFactoryBase
{
public:
    CRemoteResultSetFactory(const char * remoteServer, const char * _username, const char * _password);
    CRemoteResultSetFactory(const char * remoteServer, ISecManager &secmgr, ISecUser &secuser);
    IMPLEMENT_IINTERFACE

    virtual INewResultSet * createNewResultSet(IConstWUResult * wuResult, const char * wuid);
    virtual INewResultSet * createNewFileResultSet(const char * logicalFile, const char * cluster);
    virtual INewResultSet * createNewResultSet(const char * wuid, unsigned sequence, const char * name);
    virtual INewResultSet * createNewFileResultSet(const char * logicalFile);
    virtual IResultSetMetaData * createResultSetMeta(IConstWUResult * wuResult);
    virtual IResultSetMetaData * createResultSetMeta(const char * wuid, unsigned sequence, const char * name);

protected:
    SocketEndpoint serverEP;
};


#endif // _resultsetscm_SCM_INCL
//end
